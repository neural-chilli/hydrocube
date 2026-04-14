// src/publish.rs
//
// Arrow IPC serialization, DeltaEvent type, and optional NATS publisher.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;

// Use arrow re-exported from duckdb so the RecordBatch type is always the same
// instance as the one the DB manager produces — no cross-version type mismatch.
use crate::db_manager::arrow::ipc::writer::StreamWriter;
use crate::db_manager::arrow::record_batch::RecordBatch;

use crate::config::NatsPublishConfig;
use crate::error::{HcError, HcResult};

// ---------------------------------------------------------------------------
// Arrow IPC serialization
// ---------------------------------------------------------------------------

/// Serialize a `RecordBatch` to Arrow IPC stream bytes.
pub fn batch_to_arrow_ipc(batch: &RecordBatch) -> HcResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| HcError::Publish(format!("Arrow IPC writer error: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| HcError::Publish(format!("Arrow IPC write error: {e}")))?;
    writer
        .finish()
        .map_err(|e| HcError::Publish(format!("Arrow IPC finish error: {e}")))?;
    Ok(buf)
}

/// Serialize a `RecordBatch` to Arrow IPC bytes and base64-encode the result.
/// This is the payload format used for SSE `data:` lines.
pub fn batch_to_base64_arrow(batch: &RecordBatch) -> HcResult<String> {
    let ipc_bytes = batch_to_arrow_ipc(batch)?;
    Ok(B64.encode(&ipc_bytes))
}

// ---------------------------------------------------------------------------
// DeltaEvent
// ---------------------------------------------------------------------------

/// A broadcast-able event that carries a single window-delta as base64 Arrow IPC.
#[derive(Clone, Debug)]
pub struct DeltaEvent {
    /// Base64-encoded Arrow IPC payload (one or more record batches).
    pub base64_arrow: String,
    /// Number of rows in the delta batch.
    pub row_count: usize,
}

// ---------------------------------------------------------------------------
// Optional NATS publisher
// ---------------------------------------------------------------------------

/// Publishes delta Arrow IPC bytes to a NATS subject.
pub struct NatsPublisher {
    client: async_nats::Client,
    subject_prefix: String,
}

impl NatsPublisher {
    /// Connect to the NATS server described by `config`.
    ///
    /// Logs a warning when the server's max payload size is below 4 MiB, because
    /// large Arrow IPC frames may be rejected.
    pub async fn connect(config: &NatsPublishConfig) -> HcResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| HcError::Publish(format!("NATS connect error: {e}")))?;

        let server_info = client.server_info();
        let max_payload = server_info.max_payload;
        const FOUR_MIB: usize = 4 * 1024 * 1024;
        if max_payload < FOUR_MIB {
            tracing::warn!(
                target: "hydrocube::publish",
                max_payload_bytes = max_payload,
                "NATS server max_payload < 4 MiB; large Arrow IPC frames may be rejected"
            );
        }

        Ok(Self {
            client,
            subject_prefix: config.subject_prefix.clone(),
        })
    }

    /// Publish raw IPC bytes to `{subject_prefix}.all`.
    pub async fn publish(&self, ipc_bytes: &[u8]) -> HcResult<()> {
        let subject = format!("{}.all", self.subject_prefix);
        self.client
            .publish(subject, ipc_bytes.to_vec().into())
            .await
            .map_err(|e| HcError::Publish(format!("NATS publish error: {e}")))?;
        Ok(())
    }
}
