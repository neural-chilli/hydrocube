// src/web/sse.rs
//
// SSE stream wrapper that fans out DeltaEvents to browser clients.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::extract::State;
use axum::response::sse::{Event, Sse};
use futures::Stream;
use tokio::sync::broadcast;
use tracing::warn;

use crate::publish::DeltaEvent;

// ---------------------------------------------------------------------------
// SseStream — wraps a broadcast receiver
// ---------------------------------------------------------------------------

/// An SSE stream backed by a `tokio::broadcast::Receiver<DeltaEvent>`.
///
/// Yields one `Event` per `DeltaEvent`, with event-type `"delta"` and the
/// base64-encoded Arrow IPC payload as the data field.
pub struct SseStream {
    rx: broadcast::Receiver<DeltaEvent>,
}

impl SseStream {
    pub fn new(rx: broadcast::Receiver<DeltaEvent>) -> Self {
        Self { rx }
    }
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.rx.try_recv() {
                Ok(event) => {
                    let sse_event = Event::default().event("delta").data(event.base64_arrow);
                    return Poll::Ready(Some(Ok(sse_event)));
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    // No messages ready — register the waker so we are polled again
                    // when the next message arrives.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    warn!(
                        target: "hydrocube::web::sse",
                        skipped = n,
                        "SSE client lagged; skipped messages"
                    );
                    // Continue the loop to consume the next available message.
                    continue;
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    // Channel closed — signal end of stream.
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// axum handler
// ---------------------------------------------------------------------------

/// axum handler: subscribe a new client to the broadcast channel and return
/// an SSE response.
pub async fn sse_handler(
    State(broadcast_tx): State<broadcast::Sender<DeltaEvent>>,
) -> Sse<SseStream> {
    let rx = broadcast_tx.subscribe();
    Sse::new(SseStream::new(rx))
}
