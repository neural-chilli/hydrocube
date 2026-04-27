// src/ingest/nats.rs
//
// NATS ingest source — Core (at-most-once) and JetStream (at-least-once) modes.

use futures::StreamExt;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{NatsMode, SourceConfig};
use crate::error::{HcError, HcResult};
use crate::ingest::{IngestSender, RawMessage};

pub struct NatsSource {
    config: SourceConfig,
}

impl NatsSource {
    pub fn new(config: SourceConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self, tx: IngestSender, shutdown: watch::Receiver<bool>) -> HcResult<()> {
        let url = self
            .config
            .url
            .as_deref()
            .ok_or_else(|| HcError::Config("NATS source requires 'url'".into()))?;
        let subject = self
            .config
            .subject
            .as_deref()
            .ok_or_else(|| HcError::Config("NATS source requires 'subject'".into()))?;

        let client = async_nats::connect(url)
            .await
            .map_err(|e| HcError::Ingest(format!("NATS connect error: {e}")))?;

        info!(target: "nats", "connected to {url}, subscribing to '{subject}'");

        match self.config.mode.as_ref().unwrap_or(&NatsMode::Core) {
            NatsMode::Core => self.run_core(client, subject, tx, shutdown).await,
            NatsMode::Jetstream => self.run_jetstream(client, tx, shutdown).await,
        }
    }

    async fn run_core(
        &self,
        client: async_nats::Client,
        subject: &str,
        tx: IngestSender,
        mut shutdown: watch::Receiver<bool>,
    ) -> HcResult<()> {
        let mut sub = client
            .subscribe(subject.to_owned())
            .await
            .map_err(|e| HcError::Ingest(format!("NATS subscribe error: {e}")))?;

        loop {
            tokio::select! {
                msg = sub.next() => {
                    match msg {
                        Some(m) => {
                            let raw = RawMessage {
                                table: self.config.table.clone(),
                                bytes: m.payload.to_vec(),
                                format: self.config.format.clone(),
                            };
                            if tx.send(raw).await.is_err() {
                                // Channel closed — engine shutting down
                                return Ok(());
                            }
                        }
                        None => break,
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(target: "nats", "NATS core source shutting down");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_jetstream(
        &self,
        client: async_nats::Client,
        tx: IngestSender,
        mut shutdown: watch::Receiver<bool>,
    ) -> HcResult<()> {
        let stream_name = self
            .config
            .stream
            .as_deref()
            .ok_or_else(|| HcError::Config("JetStream source requires 'stream'".into()))?;
        let consumer_name = self
            .config
            .consumer
            .as_deref()
            .ok_or_else(|| HcError::Config("JetStream source requires 'consumer'".into()))?;

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .get_stream(stream_name)
            .await
            .map_err(|e| HcError::Ingest(format!("JetStream get_stream error: {e}")))?;

        let consumer = stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(consumer_name)
            .await
            .map_err(|e| HcError::Ingest(format!("JetStream get_consumer error: {e}")))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| HcError::Ingest(format!("JetStream messages error: {e}")))?;

        loop {
            tokio::select! {
                msg = messages.next() => {
                    match msg {
                        Some(Ok(m)) => {
                            let raw = RawMessage {
                                table: self.config.table.clone(),
                                bytes: m.payload.to_vec(),
                                format: self.config.format.clone(),
                            };
                            if tx.send(raw).await.is_err() {
                                return Ok(());
                            }
                            // Acknowledge after successful forward
                            if let Err(e) = m.ack().await {
                                warn!(target: "nats", "JetStream ack failed: {e}");
                            }
                        }
                        Some(Err(e)) => {
                            error!(target: "nats", "JetStream message error: {e}");
                        }
                        None => break,
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(target: "nats", "NATS JetStream source shutting down");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
