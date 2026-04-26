// src/ingest/mod.rs
pub mod file;
pub mod nats;
pub mod parser;

#[cfg(feature = "kafka")]
pub mod kafka;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::config::DataFormat;
use crate::error::HcResult;

/// A raw message from any ingest source.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub table: String,       // target table name
    pub bytes: Vec<u8>,      // raw wire bytes
    pub format: DataFormat,  // parsing hint
}

pub type IngestSender   = mpsc::Sender<RawMessage>;
pub type IngestReceiver = mpsc::Receiver<RawMessage>;

/// Ingest source abstraction. Implementors stream raw messages to the
/// provided channel and respect the shutdown signal.
#[async_trait::async_trait]
pub trait IngestSource: Send + Sync {
    /// Start consuming messages, forwarding `RawMessage` values to `tx`.
    ///
    /// Returns when the shutdown watch fires `true` or an unrecoverable error
    /// occurs.
    async fn run(&self, tx: IngestSender, shutdown: watch::Receiver<bool>)
        -> HcResult<()>;

    /// Commit offsets / acknowledge the current position.
    async fn commit(&self) -> HcResult<()>;
}
