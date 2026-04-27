// src/ingest/mod.rs
pub mod avro;
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
    pub table: String,      // target table name
    pub bytes: Vec<u8>,     // raw wire bytes
    pub format: DataFormat, // parsing hint
}

pub type IngestSender = mpsc::Sender<RawMessage>;
pub type IngestReceiver = mpsc::Receiver<RawMessage>;

/// Ingest source abstraction. Implementors stream raw messages to the
/// provided channel and respect the shutdown signal.
#[async_trait::async_trait]
pub trait IngestSource: Send + Sync {
    /// Start consuming messages, forwarding `RawMessage` values to `tx`.
    ///
    /// Returns when the shutdown watch fires `true` or an unrecoverable error
    /// occurs.
    async fn run(&self, tx: IngestSender, shutdown: watch::Receiver<bool>) -> HcResult<()>;

    /// Commit offsets / acknowledge the current position.
    async fn commit(&self) -> HcResult<()>;
}

use std::collections::HashMap;

pub enum CompiledDecoder {
    Avro(avro::AvroDecoder),
}

pub type CompiledDecoderMap = HashMap<String, CompiledDecoder>;

pub fn build_decoder_map(
    config: &crate::config::CubeConfig,
) -> crate::error::HcResult<CompiledDecoderMap> {
    let mut map = CompiledDecoderMap::new();
    for src in &config.sources {
        if src.format == crate::config::DataFormat::Avro {
            let schema_json = src
                .avro_schema
                .as_deref()
                .expect("validated before this point");
            if let Some(table_cfg) = config.table(&src.table) {
                let decoder = avro::AvroDecoder::new(schema_json, &table_cfg.schema.columns)?;
                map.insert(src.table.clone(), CompiledDecoder::Avro(decoder));
            }
        }
    }
    Ok(map)
}
