// src/transform/lua.rs
//
// Lua transform step — stub implementation.
// Full Lua support will be added in Task 8.

use serde_json::Value;

use crate::config::ColumnDef;
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};

pub struct LuaTransform;

impl LuaTransform {
    pub fn new(_script: &str, _function: &str, _init: Option<&str>) -> HcResult<Self> {
        Err(HcError::Transform(
            "Lua transforms are not yet implemented".into(),
        ))
    }

    pub async fn execute(
        &self,
        _db: &DbManager,
        _columns: &[ColumnDef],
        _input: Vec<Vec<Value>>,
    ) -> HcResult<Vec<Vec<Value>>> {
        Err(HcError::Transform(
            "Lua transforms are not yet implemented".into(),
        ))
    }
}
