use hydrocube::hooks::cron::{next_fire_after, parse_schedule};
use chrono::{TimeZone, Timelike, Utc};

#[test]
fn test_parse_five_field_schedule() {
    // Standard 5-field cron: "0 2 * * *" (2am daily)
    let sched = parse_schedule("0 2 * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 4, 26, 1, 59, 0).unwrap();
    let next = sched.after(&now).next().unwrap();
    assert_eq!(next.hour(), 2);
    assert_eq!(next.minute(), 0);
}

#[test]
fn test_parse_six_field_schedule() {
    // 6-field cron with seconds: "0 0 2 * * *" (2am daily)
    let sched = parse_schedule("0 0 2 * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 4, 26, 1, 59, 0).unwrap();
    let next = sched.after(&now).next().unwrap();
    assert_eq!(next.hour(), 2);
}

#[test]
fn test_next_fire_after_returns_future_time() {
    let sched = parse_schedule("0 * * * *").unwrap(); // every hour
    let now = Utc::now();
    let next = next_fire_after(&sched, &now);
    assert!(next.is_some());
    assert!(next.unwrap() > now);
}

#[test]
fn test_invalid_schedule_returns_error() {
    let result = parse_schedule("not a cron expression");
    assert!(result.is_err());
}

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::sync::watch;
use hydrocube::hooks::cron::spawn_cron_task;

#[tokio::test]
async fn test_cron_task_fires_callback() {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = counter.clone();

    // Fire every second: "* * * * * *" (every second, 6-field)
    let schedule = parse_schedule("* * * * * *").unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handle = spawn_cron_task(schedule, move |_fired_at| {
        let c = counter_clone.clone();
        async move {
            c.fetch_add(1, Ordering::SeqCst);
        }
    }, shutdown_rx);

    // Wait 2.5 seconds — should have fired at least 2 times
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let _ = shutdown_tx.send(true);
    let _ = handle.await;

    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 2, "expected at least 2 fires in 2.5s, got {count}");
}
