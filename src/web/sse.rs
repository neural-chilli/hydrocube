// src/web/sse.rs
//
// SSE stream wrapper that fans out DeltaEvents to browser clients.
// Uses BroadcastStream from tokio-stream to avoid the busy-loop that
// results from calling wake_by_ref() on TryRecvError::Empty.

use std::convert::Infallible;

use axum::response::sse::{Event, Sse};
use futures::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::publish::DeltaEvent;

/// Build an SSE stream from a broadcast receiver.
///
/// Lagged messages are silently skipped with a warning; a closed channel
/// terminates the stream.
pub fn sse_stream(
    rx: broadcast::Receiver<DeltaEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => Some(Ok(Event::default().event("delta").data(event.base64_arrow))),
        Err(_) => {
            tracing::warn!(target: "sse", "SSE client lagged, skipping events");
            None
        }
    })
}

/// axum handler: subscribe a new client to the broadcast channel and return
/// an SSE response.
pub async fn sse_handler(
    axum::extract::State(broadcast_tx): axum::extract::State<broadcast::Sender<DeltaEvent>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = broadcast_tx.subscribe();
    Sse::new(sse_stream(rx))
}
