// src/main.rs
//
// HydroCube entry point.

use clap::Parser;
use tokio::sync::broadcast;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

// Re-use the modules from the library crate.
use hydrocube::aggregation::window;
use hydrocube::cli::Cli;
use hydrocube::compaction::CompactionThread;
use hydrocube::config::CubeConfig;
use hydrocube::db_manager::DbManager;
use hydrocube::error::exit_code;
use hydrocube::hooks::cron::{spawn_housekeeping_cron_tasks, spawn_snapshot_cron_tasks};
use hydrocube::peers::gossip;
use hydrocube::peers::{PeerRecord, PeerRegistry, PeerStatus};
use hydrocube::persistence;
use hydrocube::publish::DeltaEvent;
use hydrocube::shutdown::shutdown_signal;
use hydrocube::startup::{run_reset_sequence, run_startup_sequence};
use hydrocube::web::server::start_server;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // -------------------------------------------------------------------------
    // 1. Parse CLI args
    // -------------------------------------------------------------------------
    let cli = Cli::parse();

    // -------------------------------------------------------------------------
    // 2. Load + validate config
    // -------------------------------------------------------------------------
    let yaml = match std::fs::read_to_string(&cli.config) {
        Ok(y) => y,
        Err(e) => {
            eprintln!("ERROR: cannot read config file {:?}: {}", cli.config, e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    let config: CubeConfig = match serde_yaml::from_str(&yaml) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: invalid config YAML: {}", e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    if let Err(e) = config.validate() {
        eprintln!("ERROR: {}", e);
        std::process::exit(exit_code::CONFIG_ERROR);
    }

    // -------------------------------------------------------------------------
    // 3. --validate: print OK and exit
    // -------------------------------------------------------------------------
    if cli.validate {
        println!("Config OK: cube '{}'", config.name);
        std::process::exit(exit_code::OK);
    }

    // -------------------------------------------------------------------------
    // 4. Set up tracing
    // -------------------------------------------------------------------------
    let log_level = cli
        .log_level
        .clone()
        .unwrap_or_else(|| config.log_level.clone());

    let filter = EnvFilter::try_new(&log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    fmt().with_env_filter(filter).with_target(true).init();

    // -------------------------------------------------------------------------
    // 5. Open DuckDB
    // -------------------------------------------------------------------------
    let db_path = &config.persistence.path;
    let db = if db_path == ":memory:" || !config.persistence.enabled {
        match DbManager::open_in_memory() {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to open in-memory DuckDB: {}", e);
                std::process::exit(exit_code::PERSISTENCE_FAILURE);
            }
        }
    } else {
        match DbManager::open(db_path) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to open DuckDB at {}: {}", db_path, e);
                std::process::exit(exit_code::PERSISTENCE_FAILURE);
            }
        }
    };

    // -------------------------------------------------------------------------
    // 6. Handle --reset
    // -------------------------------------------------------------------------
    if cli.reset {
        if let Err(e) = persistence::reset(&db, &config).await {
            error!("Reset failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
        if let Err(e) = run_reset_sequence(&db, &config).await {
            error!("Reset hook failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
        info!("Reset complete.");
        std::process::exit(exit_code::OK);
    }

    // -------------------------------------------------------------------------
    // 7. Handle --rebuild (rebuild, then continue running)
    // -------------------------------------------------------------------------
    if cli.rebuild {
        if let Err(e) = persistence::rebuild(&db, &config).await {
            error!("Rebuild failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
        info!("Rebuild complete. Continuing startup.");
    }

    // -------------------------------------------------------------------------
    // 8. Init persistence
    // -------------------------------------------------------------------------
    if let Err(e) = persistence::init(&db, &config).await {
        error!("Persistence init failed: {}", e);
        std::process::exit(exit_code::PERSISTENCE_FAILURE);
    }

    // Create all user-declared tables (e.g. "trades") from the config.
    // `persistence::init` only creates the engine-internal tables (slices,
    // consolidated, _cube_metadata); the named tables the engine actually
    // inserts into must be created separately.
    if let Err(e) = persistence::init_tables(&db, &config.tables).await {
        error!("Table init failed: {}", e);
        std::process::exit(exit_code::PERSISTENCE_FAILURE);
    }

    // -------------------------------------------------------------------------
    // 9. Verify config hash (unless --rebuild just reset it)
    // -------------------------------------------------------------------------
    if !cli.rebuild {
        if let Err(e) = persistence::verify_config_hash(&db, &config).await {
            error!("{}", e);
            std::process::exit(exit_code::CONFIG_HASH_MISMATCH);
        }
    }

    // -------------------------------------------------------------------------
    // 9b. Run startup hook + pre-populate identity cache
    // -------------------------------------------------------------------------
    let _identity_cache = match run_startup_sequence(&db, &config).await {
        Ok(c) => c,
        Err(e) => {
            error!("Startup sequence failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
    };

    // -------------------------------------------------------------------------
    // 10. Restore window state from metadata
    // -------------------------------------------------------------------------
    let last_window_id = match persistence::load_last_window_id(&db).await {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to load last window id: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
    };

    let compaction_cutoff = match persistence::load_compaction_cutoff(&db).await {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to load compaction cutoff: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
    };

    window::restore_state(last_window_id, compaction_cutoff);
    info!(
        "Window state restored: last_window_id={}, compaction_cutoff={}",
        last_window_id, compaction_cutoff
    );

    // -------------------------------------------------------------------------
    // 11. Set up shutdown signal
    // -------------------------------------------------------------------------
    let (_shutdown_tx, shutdown_rx) = shutdown_signal();

    // -------------------------------------------------------------------------
    // 12. Set up broadcast channel
    // -------------------------------------------------------------------------
    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<DeltaEvent>(1024);

    // -------------------------------------------------------------------------
    // Peer registry and gossip (optional — only when peers: config present)
    // -------------------------------------------------------------------------
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("failed to build reqwest HTTP client");

    let peer_registry: Option<Arc<PeerRegistry>> = if let Some(peers_cfg) = &config.peers {
        let own_info = PeerRecord {
            name: config.name.clone(),
            url: peers_cfg.url.clone(),
            description: peers_cfg.description.clone(),
            status: PeerStatus::Online,
        };
        let registry = Arc::new(PeerRegistry::new(own_info.clone()));

        // Gossip bootstrap — runs once at startup, fire-and-forget.
        {
            let bootstrap_registry = registry.clone();
            let bootstrap_cfg = peers_cfg.clone();
            let bootstrap_client = http_client.clone();
            tokio::spawn(async move {
                gossip::bootstrap(
                    &own_info,
                    &bootstrap_cfg,
                    &bootstrap_registry,
                    &bootstrap_client,
                )
                .await;
            });
        }

        // Health-check loop — runs until process exits.
        {
            let hc_registry = (*registry).clone();
            let hc_cfg = peers_cfg.clone();
            let hc_client = http_client.clone();
            tokio::spawn(async move {
                gossip::run_health_checks(hc_registry, hc_cfg, hc_client).await;
            });
        }

        info!(
            target: "hydrocube",
            "Peer discovery enabled — {} seed(s) configured",
            peers_cfg.seeds.len()
        );
        Some(registry)
    } else {
        info!(target: "hydrocube", "Peer discovery disabled (no peers: config section)");
        None
    };

    // -------------------------------------------------------------------------
    // 13. Start compaction thread
    // -------------------------------------------------------------------------
    let compaction = CompactionThread::new(db.clone(), config.clone());
    let compaction_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = compaction.run(compaction_shutdown).await {
            error!("Compaction thread error: {}", e);
        }
    });

    // -------------------------------------------------------------------------
    // 13b. Raw message channel for ingest → engine
    // -------------------------------------------------------------------------
    let (raw_tx, raw_rx) = tokio::sync::mpsc::channel::<hydrocube::ingest::RawMessage>(10_000);

    // Spawn one KafkaSource task per kafka-type source in the config.
    #[cfg(feature = "kafka")]
    {
        use hydrocube::config::SourceType;
        use hydrocube::ingest::kafka::KafkaSource;
        use hydrocube::ingest::IngestSource;

        for source_cfg in config
            .sources
            .iter()
            .filter(|s| s.source_type == SourceType::Kafka)
        {
            match KafkaSource::new(source_cfg) {
                Ok(source) => {
                    let tx = raw_tx.clone();
                    let shutdown = shutdown_rx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = source.run(tx, shutdown).await {
                            error!("Kafka source error: {}", e);
                        }
                    });
                    info!(
                        topic = source_cfg.topic.as_deref().unwrap_or("?"),
                        table = %source_cfg.table,
                        "Kafka source started"
                    );
                }
                Err(e) => {
                    error!("Failed to create Kafka source: {}", e);
                }
            }
        }
    }

    // Keep a clone for the HTTP ingest endpoint, then drop the original so the
    // channel closes when all sources (and the web server) are done.
    let web_ingest_tx = raw_tx.clone();
    drop(raw_tx);

    // Spawn hot path engine
    let engine_db = db.clone();
    let engine_config = config.clone();
    let engine_broadcast = broadcast_tx.clone();
    let engine_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = hydrocube::engine::run_hot_path(
            engine_db,
            engine_config,
            raw_rx,
            engine_broadcast,
            engine_shutdown,
        )
        .await
        {
            error!("Engine error: {}", e);
        }
    });

    // -------------------------------------------------------------------------
    // 13c. Spawn snapshot and housekeeping cron tasks
    // -------------------------------------------------------------------------
    let _snapshot_handles = spawn_snapshot_cron_tasks(&config, db.clone(), shutdown_rx.clone());
    let _hk_handles = spawn_housekeeping_cron_tasks(&config, db.clone(), shutdown_rx.clone());

    // -------------------------------------------------------------------------
    // 14. Start web server if UI enabled
    // -------------------------------------------------------------------------
    // In the new config, UI is always enabled unless --no-ui is passed.
    let ui_enabled = !cli.no_ui;

    if ui_enabled {
        let port = cli.ui_port.unwrap_or(8080);

        let db_web = db.clone();
        let config_web = config.clone();
        let broadcast_tx_web = broadcast_tx.clone();
        let peer_registry_web = peer_registry.clone();
        let http_client_web = http_client.clone();

        tokio::spawn(async move {
            if let Err(e) = start_server(
                db_web,
                config_web,
                broadcast_tx_web,
                port,
                Some(web_ingest_tx),
                peer_registry_web,
                http_client_web,
            )
            .await
            {
                error!("Web server error: {}", e);
            }
        });
    }

    // -------------------------------------------------------------------------
    // 15. Ready
    // -------------------------------------------------------------------------
    info!("HydroCube running. Press Ctrl+C to stop.");

    // -------------------------------------------------------------------------
    // 16. Wait for shutdown signal
    // -------------------------------------------------------------------------
    let mut shutdown_wait = shutdown_rx.clone();
    // Wait until the channel receives `true`.
    loop {
        if shutdown_wait.changed().await.is_err() {
            break;
        }
        if *shutdown_wait.borrow() {
            break;
        }
    }

    // -------------------------------------------------------------------------
    // 17. Save final window state
    // -------------------------------------------------------------------------
    use hydrocube::aggregation::window::WINDOW_ID;
    use std::sync::atomic::Ordering;

    let final_window_id = WINDOW_ID.load(Ordering::SeqCst);
    if let Err(e) = persistence::save_window_id(&db, final_window_id).await {
        error!("Failed to save final window id: {}", e);
    }

    let final_cutoff = window::compaction_cutoff();
    if let Err(e) = persistence::save_compaction_cutoff(&db, final_cutoff).await {
        error!("Failed to save final compaction cutoff: {}", e);
    }

    db.shutdown().await;

    // -------------------------------------------------------------------------
    // 18. Done
    // -------------------------------------------------------------------------
    info!("HydroCube stopped.");
}
