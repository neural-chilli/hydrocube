// src/ingest/mod.rs
pub mod parser;

#[cfg(feature = "kafka")]
pub mod kafka;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::error::HcResult;

/// Ingest source abstraction. Implementors stream raw message bytes to the
/// provided channel and respect the shutdown signal.
#[async_trait::async_trait]
pub trait IngestSource: Send + Sync {
    /// Start consuming messages, forwarding raw bytes to `tx`.
    ///
    /// Returns when the shutdown watch fires `true` or an unrecoverable error
    /// occurs.
    async fn run(&self, tx: mpsc::Sender<Vec<u8>>, shutdown: watch::Receiver<bool>)
        -> HcResult<()>;

    /// Commit offsets / acknowledge the current position.
    async fn commit(&self) -> HcResult<()>;
}
