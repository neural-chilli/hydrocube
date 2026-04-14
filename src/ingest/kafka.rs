// src/ingest/kafka.rs
#[cfg(feature = "kafka")]
use futures::StreamExt;
#[cfg(feature = "kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::consumer::{Consumer, StreamConsumer};
#[cfg(feature = "kafka")]
use rdkafka::Message;
#[cfg(feature = "kafka")]
use tokio::sync::mpsc;
#[cfg(feature = "kafka")]
use tokio::sync::watch;
#[cfg(feature = "kafka")]
use tracing::{debug, error, info, warn};

#[cfg(feature = "kafka")]
use crate::config::SourceConfig;
#[cfg(feature = "kafka")]
use crate::error::{HcError, HcResult};

#[cfg(feature = "kafka")]
pub struct KafkaSource {
    brokers: String,
    topic: String,
    group_id: String,
}

#[cfg(feature = "kafka")]
impl KafkaSource {
    /// Create a new KafkaSource, validating that required fields are present.
    pub fn new(config: &SourceConfig) -> HcResult<Self> {
        let brokers = config
            .brokers
            .as_ref()
            .ok_or_else(|| HcError::Ingest("kafka source requires 'brokers'".into()))?
            .join(",");

        let topic = config
            .topic
            .clone()
            .ok_or_else(|| HcError::Ingest("kafka source requires 'topic'".into()))?;

        let group_id = config
            .group_id
            .clone()
            .ok_or_else(|| HcError::Ingest("kafka source requires 'group_id'".into()))?;

        Ok(Self {
            brokers,
            topic,
            group_id,
        })
    }
}

#[cfg(feature = "kafka")]
#[async_trait::async_trait]
impl super::IngestSource for KafkaSource {
    async fn run(
        &self,
        tx: mpsc::Sender<Vec<u8>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> HcResult<()> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()
            .map_err(|e| HcError::Ingest(format!("failed to create Kafka consumer: {}", e)))?;

        consumer
            .subscribe(&[&self.topic])
            .map_err(|e| HcError::Ingest(format!("failed to subscribe to topic: {}", e)))?;

        info!(target: "hydrocube::ingest::kafka", topic = %self.topic, "Kafka consumer started");

        let mut stream = consumer.stream();

        loop {
            tokio::select! {
                // Shutdown signal
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(target: "hydrocube::ingest::kafka", "shutdown signal received, stopping consumer");
                        break;
                    }
                }
                // Next message from Kafka
                msg = stream.next() => {
                    match msg {
                        Some(Ok(m)) => {
                            if let Some(payload) = m.payload() {
                                debug!(
                                    target: "hydrocube::ingest::kafka",
                                    partition = m.partition(),
                                    offset = m.offset(),
                                    bytes = payload.len(),
                                    "received message"
                                );
                                if tx.send(payload.to_vec()).await.is_err() {
                                    warn!(target: "hydrocube::ingest::kafka", "ingest channel closed, stopping consumer");
                                    break;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(target: "hydrocube::ingest::kafka", error = %e, "Kafka consume error");
                        }
                        None => {
                            // Stream ended unexpectedly
                            warn!(target: "hydrocube::ingest::kafka", "Kafka stream ended unexpectedly");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn commit(&self) -> HcResult<()> {
        // TODO: implement manual offset commit when auto-commit is disabled
        Ok(())
    }
}
