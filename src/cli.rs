// src/cli.rs
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "hydrocube", about = "Real-time aggregation engine")]
pub struct Cli {
    #[arg(long, required = true)]
    pub config: PathBuf,

    #[arg(long)]
    pub validate: bool,

    #[arg(long)]
    pub rebuild: bool,

    #[arg(long)]
    pub snapshot: Option<PathBuf>,

    #[arg(long)]
    pub reset: bool,

    #[arg(long)]
    pub log_level: Option<String>,

    #[arg(long)]
    pub no_ui: bool,

    #[arg(long)]
    pub ui_port: Option<u16>,
}
