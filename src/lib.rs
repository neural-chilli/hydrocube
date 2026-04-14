// src/lib.rs
// Public library surface — used by integration tests.

pub mod aggregation;
pub mod cli;
pub mod compaction;
pub mod config;
pub mod db_manager;
pub mod delta;
pub mod error;
pub mod ingest;
pub mod persistence;
pub mod publish;
pub mod retention;
pub mod transform;
pub mod web;
