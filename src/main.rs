// src/main.rs
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

use clap::Parser;
use cli::Cli;
use error::exit_code;

fn main() {
    let cli = Cli::parse();

    // Load config
    let yaml = match std::fs::read_to_string(&cli.config) {
        Ok(y) => y,
        Err(e) => {
            eprintln!("ERROR: cannot read config file {:?}: {}", cli.config, e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    let config: config::CubeConfig = match serde_yaml::from_str(&yaml) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: invalid config: {}", e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    if let Err(e) = config.validate() {
        eprintln!("ERROR: {}", e);
        std::process::exit(exit_code::CONFIG_ERROR);
    }

    if cli.validate {
        println!("Config OK: cube '{}'", config.name);
        std::process::exit(exit_code::OK);
    }

    println!("HydroCube: cube '{}' loaded", config.name);
}
