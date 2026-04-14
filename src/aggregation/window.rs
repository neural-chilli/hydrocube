// src/aggregation/window.rs
//
// Atomic window-state shared across tasks.
//
// WINDOW_ID   — monotonically increasing counter; each flush increments it.
// COMPACTION_CUTOFF — slices at or below this ID have been compacted and can
//                     be excluded from hot-path slice aggregation.

use std::sync::atomic::{AtomicU64, Ordering};

/// The ID that will be assigned to the *next* window after the current one.
/// Initialised to 0; the first real window gets ID 1.
pub static WINDOW_ID: AtomicU64 = AtomicU64::new(0);

/// The highest window ID whose slices have been fully compacted.
/// Slice aggregation queries filter `_window_id > COMPACTION_CUTOFF`.
pub static COMPACTION_CUTOFF: AtomicU64 = AtomicU64::new(0);

/// Atomically claim the next window ID. Returns the newly assigned ID.
pub fn next_window_id() -> u64 {
    WINDOW_ID.fetch_add(1, Ordering::SeqCst) + 1
}

/// Read the current compaction cutoff (relaxed — approximate reads are fine
/// for query planning; exact consistency is not required here).
pub fn compaction_cutoff() -> u64 {
    COMPACTION_CUTOFF.load(Ordering::Acquire)
}

/// Advance the compaction cutoff. Called by the compaction task after it
/// has durably written compacted rows to DuckDB.
pub fn set_compaction_cutoff(cutoff: u64) {
    COMPACTION_CUTOFF.store(cutoff, Ordering::Release);
}

/// Restore window state from durable storage on startup.
///
/// `last_window_id` — the highest window ID persisted before shutdown.
/// `cutoff`         — the compaction cutoff persisted before shutdown.
pub fn restore_state(last_window_id: u64, cutoff: u64) {
    WINDOW_ID.store(last_window_id, Ordering::SeqCst);
    COMPACTION_CUTOFF.store(cutoff, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_window_id_increments() {
        // Reset to a known baseline to avoid test-ordering issues.
        WINDOW_ID.store(100, Ordering::SeqCst);
        assert_eq!(next_window_id(), 101);
        assert_eq!(next_window_id(), 102);
    }

    #[test]
    fn set_and_get_compaction_cutoff() {
        set_compaction_cutoff(42);
        assert_eq!(compaction_cutoff(), 42);
    }

    #[test]
    fn restore_state_sets_both() {
        restore_state(500, 490);
        assert_eq!(WINDOW_ID.load(Ordering::SeqCst), 500);
        assert_eq!(compaction_cutoff(), 490);
    }
}
