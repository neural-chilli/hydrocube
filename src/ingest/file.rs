// src/ingest/file.rs
//
// File source loading: reads Parquet, CSV, JSON Lines directly into DuckDB
// using DuckDB's native file readers.

use mlua::{Lua, Value as LuaValue};

use crate::config::DataFormat;
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};

/// Load a file into a DuckDB table using the appropriate native reader.
///
/// - `table_name`  — name of the existing DuckDB table to INSERT into
/// - `path`        — absolute path to the file
/// - `format`      — parsing hint
///
/// Uses `INSERT INTO ... SELECT * FROM read_*(path)` so the file reader
/// handles type inference and column mapping automatically.
pub async fn load_file_into_table(
    db: &DbManager,
    table_name: &str,
    path: &str,
    format: DataFormat,
) -> HcResult<()> {
    let reader_expr = file_reader_expr(path, format);
    let sql = format!("INSERT INTO {} SELECT * FROM {}", table_name, reader_expr);
    db.execute(&sql, vec![]).await?;
    Ok(())
}

/// Build the DuckDB reader expression for a given path and format.
fn file_reader_expr(path: &str, format: DataFormat) -> String {
    match format {
        DataFormat::Parquet => format!("read_parquet('{path}')"),
        DataFormat::Csv => format!("read_csv_auto('{path}')"),
        DataFormat::Json => format!("read_json_auto('{path}')"),
        DataFormat::JsonLines => {
            // DuckDB reads JSON Lines via read_json_auto with format='newline_delimited'
            format!("read_json_auto('{path}', format='newline_delimited')")
        }
    }
}

/// Load multiple files (e.g., from a glob or path list) into a table.
pub async fn load_files_into_table(
    db: &DbManager,
    table_name: &str,
    paths: &[String],
    format: DataFormat,
) -> HcResult<()> {
    for path in paths {
        load_file_into_table(db, table_name, path, format.clone()).await?;
    }
    Ok(())
}

/// Run a Lua function to resolve file paths.
/// The function must return either a string or a table of strings.
pub fn resolve_paths_with_lua(lua_code: &str, function_name: &str) -> HcResult<Vec<String>> {
    let lua = Lua::new();
    lua.load(lua_code)
        .exec()
        .map_err(|e| HcError::Transform(format!("Lua load error: {e}")))?;

    let func: mlua::Function = lua.globals().get(function_name).map_err(|e| {
        HcError::Transform(format!("Lua function '{function_name}' not found: {e}"))
    })?;

    let result: LuaValue = func
        .call(())
        .map_err(|e| HcError::Transform(format!("Lua call error: {e}")))?;

    match result {
        LuaValue::String(s) => Ok(vec![s
            .to_str()
            .map_err(|e| HcError::Transform(e.to_string()))?
            .to_owned()]),
        LuaValue::Table(t) => {
            let mut paths = Vec::new();
            for pair in t.sequence_values::<String>() {
                paths.push(pair.map_err(|e| HcError::Transform(e.to_string()))?);
            }
            Ok(paths)
        }
        other => Err(HcError::Transform(format!(
            "Lua path resolver must return string or table, got: {:?}",
            other.type_name()
        ))),
    }
}
