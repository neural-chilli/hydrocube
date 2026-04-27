pub mod gossip;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PeerStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub name: String,
    pub url: String,
    pub description: String,
    pub status: PeerStatus,
}

#[derive(Clone)]
pub struct PeerRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

struct RegistryInner {
    own_info: PeerRecord,
    peers: HashMap<String, PeerEntry>,
}

struct PeerEntry {
    record: PeerRecord,
    consecutive_failures: u32,
}

impl PeerRegistry {
    pub fn new(own_info: PeerRecord) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                own_info,
                peers: HashMap::new(),
            })),
        }
    }

    pub fn own_info(&self) -> PeerRecord {
        self.inner.lock().unwrap().own_info.clone()
    }

    pub fn upsert(&self, record: PeerRecord) {
        let mut inner = self.inner.lock().unwrap();
        if record.url == inner.own_info.url {
            return;
        }
        inner.peers.insert(
            record.url.clone(),
            PeerEntry {
                record: PeerRecord {
                    status: PeerStatus::Online,
                    ..record
                },
                consecutive_failures: 0,
            },
        );
    }

    pub fn list(&self) -> Vec<PeerRecord> {
        self.inner
            .lock()
            .unwrap()
            .peers
            .values()
            .map(|e| e.record.clone())
            .collect()
    }

    pub fn record_failure(&self, url: &str, max_failures: u32) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.peers.get_mut(url) {
            entry.consecutive_failures += 1;
            if entry.consecutive_failures >= max_failures {
                entry.record.status = PeerStatus::Offline;
            }
        }
    }

    pub fn record_success(&self, url: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.peers.get_mut(url) {
            entry.consecutive_failures = 0;
            entry.record.status = PeerStatus::Online;
        }
    }

    pub fn peer_urls_except(&self, exclude_url: &str) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .peers
            .keys()
            .filter(|u| u.as_str() != exclude_url)
            .cloned()
            .collect()
    }
}
