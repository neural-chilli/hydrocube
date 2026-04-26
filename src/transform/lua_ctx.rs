// src/transform/lua_ctx.rs
//
// Lua transform execution with identity cache context.
// Passes ctx.seen / ctx.mark_seen into the Lua environment.

use mlua::{Lua, Table, Value as LuaValue};
use serde_json::Value;

use crate::error::{HcError, HcResult};
use crate::identity::IdentityCache;

/// Run a Lua transform function with an identity cache context.
///
/// Returns:
/// - The transformed rows (column-aligned Vec<Vec<Value>>)
/// - A list of keys that `ctx.mark_seen` was called with
pub fn run_lua_with_identity_ctx(
    lua_code: &str,
    function_name: &str,
    rows: &[Vec<Value>],
    col_names: &[String],
    cache: &IdentityCache,
) -> HcResult<(Vec<Vec<Value>>, Vec<String>)> {
    let lua = Lua::new();

    // Load user code
    lua.load(lua_code)
        .exec()
        .map_err(|e| HcError::Transform(format!("Lua load: {e}")))?;

    // Build the batch table: array of {col_name = value, ...} tables
    let batch_table = lua.create_table()
        .map_err(|e| HcError::Transform(e.to_string()))?;

    for (i, row) in rows.iter().enumerate() {
        let row_table = lua.create_table()
            .map_err(|e| HcError::Transform(e.to_string()))?;
        for (j, col_name) in col_names.iter().enumerate() {
            let val = row.get(j).cloned().unwrap_or(Value::Null);
            let lua_val = json_to_lua(&lua, &val)?;
            row_table.set(col_name.as_str(), lua_val)
                .map_err(|e| HcError::Transform(e.to_string()))?;
        }
        batch_table.set(i + 1, row_table)
            .map_err(|e| HcError::Transform(e.to_string()))?;
    }

    // __mark_seen_out: collects keys marked during this call
    let mark_seen_out = lua.create_table()
        .map_err(|e| HcError::Transform(e.to_string()))?;
    lua.globals().set("__mark_seen_out", mark_seen_out)
        .map_err(|e| HcError::Transform(e.to_string()))?;

    // Build __seen_set as a Lua table snapshot: {key = true, ...}
    let seen_lua: Table = lua.create_table()
        .map_err(|e| HcError::Transform(e.to_string()))?;
    for key in cache.all_keys() {
        seen_lua.set(key.as_str(), true)
            .map_err(|e| HcError::Transform(e.to_string()))?;
    }
    lua.globals().set("__seen_set", seen_lua)
        .map_err(|e| HcError::Transform(e.to_string()))?;

    // Build ctx table with seen() and mark_seen() closures
    let ctx = lua.create_table()
        .map_err(|e| HcError::Transform(e.to_string()))?;

    // ctx.seen = function(key) return __seen_set[key] == true end
    let seen_fn = lua.create_function(|lua_ctx, key: String| {
        let seen_set: Table = lua_ctx.globals().get("__seen_set")?;
        let val: LuaValue = seen_set.get(key.as_str())?;
        Ok(matches!(val, LuaValue::Boolean(true)))
    }).map_err(|e| HcError::Transform(e.to_string()))?;
    ctx.set("seen", seen_fn)
        .map_err(|e| HcError::Transform(e.to_string()))?;

    // ctx.mark_seen = function(key) table.insert(__mark_seen_out, key) end
    let mark_fn = lua.create_function(|lua_ctx, key: String| {
        let out: Table = lua_ctx.globals().get("__mark_seen_out")?;
        let len = out.raw_len();
        out.set(len + 1, key)?;
        Ok(())
    }).map_err(|e| HcError::Transform(e.to_string()))?;
    ctx.set("mark_seen", mark_fn)
        .map_err(|e| HcError::Transform(e.to_string()))?;

    // Call the user function: transform(batch, ctx)
    let func: mlua::Function = lua.globals().get(function_name)
        .map_err(|e| HcError::Transform(format!("function '{}' not found: {e}", function_name)))?;

    let result: Table = func.call((batch_table, ctx))
        .map_err(|e| HcError::Transform(format!("Lua call: {e}")))?;

    // Collect output rows (declared columns only)
    let out_rows = lua_table_to_rows(&result, col_names)?;

    // Collect mark_seen calls
    let mark_out: Table = lua.globals().get("__mark_seen_out")
        .map_err(|e| HcError::Transform(e.to_string()))?;
    let marks: Vec<String> = mark_out.sequence_values::<String>()
        .filter_map(|r| r.ok())
        .collect();

    Ok((out_rows, marks))
}

fn json_to_lua(lua: &Lua, val: &Value) -> HcResult<LuaValue> {
    match val {
        Value::Null    => Ok(LuaValue::Nil),
        Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else {
                Ok(LuaValue::Number(n.as_f64().unwrap_or(0.0)))
            }
        }
        Value::String(s) => {
            Ok(LuaValue::String(lua.create_string(s.as_str())
                .map_err(|e| HcError::Transform(e.to_string()))?))
        }
        _ => Ok(LuaValue::Nil),
    }
}

fn lua_table_to_rows(table: &Table, col_names: &[String]) -> HcResult<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    let mut i = 1;
    loop {
        let val: LuaValue = table.get(i).map_err(|e| HcError::Transform(e.to_string()))?;
        match val {
            LuaValue::Nil => break,
            LuaValue::Table(row_tbl) => {
                // Collect declared columns only
                let mut row = Vec::new();
                for col_name in col_names {
                    let v: LuaValue = row_tbl.get(col_name.as_str())
                        .map_err(|e| HcError::Transform(e.to_string()))?;
                    row.push(lua_to_json(v));
                }
                rows.push(row);
            }
            _ => break,
        }
        i += 1;
    }
    Ok(rows)
}

fn lua_to_json(val: LuaValue) -> Value {
    match val {
        LuaValue::Nil         => Value::Null,
        LuaValue::Boolean(b)  => Value::Bool(b),
        LuaValue::Integer(i)  => Value::Number(i.into()),
        LuaValue::Number(f)   => {
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        LuaValue::String(s)   => Value::String(s.to_str().map(|t| t.to_owned()).unwrap_or_default()),
        _ => Value::Null,
    }
}
