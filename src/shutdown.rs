// src/shutdown.rs
//
// Shutdown signal handling for HydroCube.
// Listens for SIGINT (Ctrl+C) and, on Unix, SIGTERM.
// Sends `true` to a tokio watch channel when a signal is received.

use tokio::sync::watch;
use tracing::info;

/// Spawn a background task that waits for SIGINT or SIGTERM and broadcasts
/// `true` on the returned watch channel.
///
/// Returns `(sender, receiver)`.  Pass the `receiver` to any task that needs
/// to honour graceful shutdown.
pub fn shutdown_signal() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    let (tx, rx) = watch::channel(false);

    let tx_clone = tx.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        info!(target: "hydrocube", "shutdown signal received");
        let _ = tx_clone.send(true);
    });

    (tx, rx)
}

/// Platform-specific signal waiting.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");

        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        // On non-Unix platforms (Windows), only SIGINT (Ctrl+C) is available.
        tokio::signal::ctrl_c().await.expect("Ctrl+C handler");
    }
}
