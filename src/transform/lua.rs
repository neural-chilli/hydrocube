// src/transform/lua.rs
//
// Lua transform step — full implementation with batch contract.
// transform_batch(messages) receives the entire window's messages as one Lua
// array (one marshalling round-trip).  A per-message transform(msg) fallback
// exists for simpler scripts.

use mlua::{Lua, Value as LuaValue};
use serde_json::Value;
use std::fs;

use crate::error::{HcError, HcResult};

pub struct LuaTransform {
    lua: Lua,
    function_name: String,
}

impl LuaTransform {
    /// Load Lua source code inline (for testing).
    pub fn from_source(
        source: String,
        function_name: String,
        init_function: Option<String>,
    ) -> HcResult<Self> {
        let lua = Lua::new();
        lua.load(&source)
            .exec()
            .map_err(|e| HcError::Transform(format!("Lua load error: {e}")))?;

        if let Some(init_fn) = init_function {
            let func: mlua::Function = lua
                .globals()
                .get(init_fn.as_str())
                .map_err(|e| HcError::Transform(format!("Lua init function not found: {e}")))?;
            func.call::<()>(())
                .map_err(|e| HcError::Transform(format!("Lua init function error: {e}")))?;
        }

        Ok(Self { lua, function_name })
    }

    /// Load Lua source from a file.
    pub fn from_file(
        script_path: String,
        function_name: String,
        init_function: Option<String>,
    ) -> HcResult<Self> {
        let source = fs::read_to_string(&script_path).map_err(|e| {
            HcError::Transform(format!("failed to read Lua script {script_path}: {e}"))
        })?;
        Self::from_source(source, function_name, init_function)
    }

    /// Call transform_batch(messages) — one marshalling round-trip for the
    /// entire window.
    pub fn execute_batch(
        &self,
        input: Vec<Vec<Value>>,
        col_names: &[String],
    ) -> HcResult<Vec<Vec<Value>>> {
        if input.is_empty() {
            return Ok(vec![]);
        }

        let func: mlua::Function = self
            .lua
            .globals()
            .get(self.function_name.as_str())
            .map_err(|e| {
                HcError::Transform(format!(
                    "Lua function '{}' not found: {e}",
                    self.function_name
                ))
            })?;

        let lua_input = rows_to_lua_table(&self.lua, input, col_names)?;
        let result: LuaValue = func
            .call(lua_input)
            .map_err(|e| HcError::Transform(format!("Lua batch call error: {e}")))?;

        lua_result_to_rows(result)
    }

    /// Call transform(msg) once per message and accumulate results.
    pub fn execute_per_message(
        &self,
        input: Vec<Vec<Value>>,
        col_names: &[String],
    ) -> HcResult<Vec<Vec<Value>>> {
        if input.is_empty() {
            return Ok(vec![]);
        }

        let func: mlua::Function = self
            .lua
            .globals()
            .get(self.function_name.as_str())
            .map_err(|e| {
                HcError::Transform(format!(
                    "Lua function '{}' not found: {e}",
                    self.function_name
                ))
            })?;

        let mut output = Vec::new();
        for row in input {
            let lua_row = row_to_lua_table(&self.lua, row, col_names)?;
            let result: LuaValue = func
                .call(lua_row)
                .map_err(|e| HcError::Transform(format!("Lua per-message call error: {e}")))?;
            let mut rows = lua_result_to_rows(result)?;
            output.append(&mut rows);
        }

        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert serde_json::Value → mlua::Value.
fn json_to_lua(lua: &Lua, value: Value) -> HcResult<LuaValue> {
    let v = match value {
        Value::Null => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else {
                LuaValue::Number(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => LuaValue::String(
            lua.create_string(s.as_str())
                .map_err(|e| HcError::Transform(format!("Lua string create error: {e}")))?,
        ),
        // Arrays and objects are not needed for the current contract — treat as Nil.
        _ => LuaValue::Nil,
    };
    Ok(v)
}

/// Convert mlua::Value → serde_json::Value.
fn lua_to_json(value: LuaValue) -> Value {
    match value {
        LuaValue::Nil => Value::Null,
        LuaValue::Boolean(b) => Value::Bool(b),
        LuaValue::Integer(i) => Value::Number(i.into()),
        LuaValue::Number(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LuaValue::String(s) => {
            let text = s.to_str().map(|t| t.to_string()).unwrap_or_default();
            Value::String(text)
        }
        _ => Value::Null,
    }
}

/// Build a Lua table `{col_name = value, ...}` from a single row.
fn row_to_lua_table(lua: &Lua, row: Vec<Value>, col_names: &[String]) -> HcResult<mlua::Table> {
    let tbl = lua
        .create_table()
        .map_err(|e| HcError::Transform(format!("Lua table create error: {e}")))?;
    for (i, val) in row.into_iter().enumerate() {
        if let Some(name) = col_names.get(i) {
            let lua_val = json_to_lua(lua, val)?;
            tbl.set(name.as_str(), lua_val)
                .map_err(|e| HcError::Transform(format!("Lua table set error: {e}")))?;
        }
    }
    Ok(tbl)
}

/// Build a Lua array of row tables.
fn rows_to_lua_table(
    lua: &Lua,
    rows: Vec<Vec<Value>>,
    col_names: &[String],
) -> HcResult<mlua::Table> {
    let arr = lua
        .create_table()
        .map_err(|e| HcError::Transform(format!("Lua array create error: {e}")))?;
    for (i, row) in rows.into_iter().enumerate() {
        let row_tbl = row_to_lua_table(lua, row, col_names)?;
        arr.set(i + 1, row_tbl)
            .map_err(|e| HcError::Transform(format!("Lua array set error: {e}")))?;
    }
    Ok(arr)
}

/// Convert the Lua return value (array of tables) back to Vec<Vec<Value>>.
/// The VALUES are extracted in insertion order; keys are not used for ordering.
fn lua_result_to_rows(result: LuaValue) -> HcResult<Vec<Vec<Value>>> {
    let tbl = match result {
        LuaValue::Table(t) => t,
        LuaValue::Nil => return Ok(vec![]),
        other => {
            return Err(HcError::Transform(format!(
                "Lua transform must return a table, got: {:?}",
                other
            )))
        }
    };

    let mut rows = Vec::new();
    // Iterate as an array: integer keys 1, 2, ...
    for pair in tbl.sequence_values::<mlua::Value>() {
        let row_val =
            pair.map_err(|e| HcError::Transform(format!("Lua result iter error: {e}")))?;
        match row_val {
            LuaValue::Table(row_tbl) => {
                let mut row = Vec::new();
                // Collect all values from the row table (preserving insertion order via pairs).
                for pair in row_tbl.pairs::<LuaValue, LuaValue>() {
                    let (_k, v) =
                        pair.map_err(|e| HcError::Transform(format!("Lua row iter error: {e}")))?;
                    row.push(lua_to_json(v));
                }
                rows.push(row);
            }
            _ => {
                return Err(HcError::Transform(
                    "Lua transform: each element of result must be a table".into(),
                ))
            }
        }
    }

    Ok(rows)
}
