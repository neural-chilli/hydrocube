use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::PeersConfig;
use crate::peers::{PeerRecord, PeerRegistry};

#[derive(Deserialize)]
struct GetPeersResponse {
    #[serde(rename = "self")]
    self_info: PeerRecord,
    peers: Vec<PeerRecord>,
}

#[derive(Serialize)]
struct RegisterRequest<'a> {
    name: &'a str,
    url: &'a str,
    description: &'a str,
    forwarded: bool,
}

/// Perform startup gossip bootstrap against all configured seeds.
/// Seeds that cannot be reached are logged as warnings; the cube starts
/// standalone and gains peers as seeds come back up during health checks.
pub async fn bootstrap(
    own_info: &PeerRecord,
    config: &PeersConfig,
    registry: &PeerRegistry,
    client: &Client,
) {
    for seed_url in &config.seeds {
        let base = seed_url.trim_end_matches('/');

        // Step 1: fetch known peers from seed.
        let peers_url = format!("{base}/api/peers");
        match client
            .get(&peers_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => match resp.json::<GetPeersResponse>().await {
                Ok(body) => {
                    registry.upsert(body.self_info);
                    for peer in body.peers {
                        if peer.url != own_info.url {
                            registry.upsert(peer);
                        }
                    }
                    info!(target: "hydrocube::peers", "Fetched peer list from seed {}", seed_url);
                }
                Err(e) => {
                    warn!(target: "hydrocube::peers", "Could not parse peers response from {}: {}", seed_url, e);
                }
            },
            Ok(resp) => {
                warn!(target: "hydrocube::peers", "Seed {} returned HTTP {} for GET /api/peers", seed_url, resp.status());
            }
            Err(e) => {
                warn!(target: "hydrocube::peers", "Could not reach seed {} for GET /api/peers: {}", seed_url, e);
            }
        }

        // Step 2: register ourselves with seed.
        let register_url = format!("{base}/api/peers/register");
        let body = RegisterRequest {
            name: &own_info.name,
            url: &own_info.url,
            description: &own_info.description,
            forwarded: false,
        };
        match client
            .post(&register_url)
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<Vec<PeerRecord>>().await {
                    Ok(peers) => {
                        for peer in peers {
                            if peer.url != own_info.url {
                                registry.upsert(peer);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(target: "hydrocube::peers", "Could not parse register response from {}: {}", seed_url, e);
                    }
                }
                info!(target: "hydrocube::peers", "Registered with seed {}", seed_url);
            }
            Ok(resp) => {
                warn!(target: "hydrocube::peers", "Seed {} returned HTTP {} for POST /api/peers/register", seed_url, resp.status());
            }
            Err(e) => {
                warn!(target: "hydrocube::peers", "Registration to {} failed: {}", seed_url, e);
            }
        }
    }

    info!(
        target: "hydrocube::peers",
        "Bootstrap complete — {} peer(s) known",
        registry.list().len()
    );
}

/// Background task: ping every known peer's /api/status on a fixed interval.
/// Runs indefinitely; cancel by dropping the task handle.
pub async fn run_health_checks(registry: PeerRegistry, config: PeersConfig, client: Client) {
    let interval = Duration::from_secs(config.health_check_interval_secs);
    let max_failures = config.health_check_failures_before_offline;

    loop {
        tokio::time::sleep(interval).await;

        for peer in registry.list() {
            let status_url = format!("{}/api/status", peer.url.trim_end_matches('/'));
            match client
                .get(&status_url)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    registry.record_success(&peer.url);
                    debug!(target: "hydrocube::peers", "Health check OK: {}", peer.url);
                }
                Ok(resp) => {
                    warn!(
                        target: "hydrocube::peers",
                        "Health check {} returned HTTP {}",
                        peer.url,
                        resp.status()
                    );
                    registry.record_failure(&peer.url, max_failures);
                }
                Err(e) => {
                    warn!(target: "hydrocube::peers", "Health check {} failed: {}", peer.url, e);
                    registry.record_failure(&peer.url, max_failures);
                }
            }
        }
    }
}
