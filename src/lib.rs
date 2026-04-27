// src/lib.rs
// Public library surface — used by integration tests.

pub mod aggregation;
pub mod auth;
pub mod cli;
pub mod compaction;
pub mod config;
pub mod db_manager;
pub mod delta;
pub mod engine;
pub mod error;
pub mod hooks;
pub mod identity;
pub mod ingest;
pub mod peers;
pub mod persistence;
pub mod publish;
pub mod retention;
pub mod shutdown;
pub mod startup;
pub mod tables;
pub mod transform;
pub mod web;
