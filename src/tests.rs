use super::*;
use crate::test_support::{OtherKey, OtherRecord, Record, RecordKey};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
fn local(options: CacheOptions) -> CacheCoherence {
    let c = CacheCoherence::builder(Arc::new(DatabaseConnection::Disconnected))
        .register::<Record>()
        .register::<OtherRecord>()
        .options(options)
        .build();
    c.connection_changed(true);
    c.heartbeat_received();
    c
}
fn change(key: RecordKey) -> KeyChange {
    let (index, key) = key.parts();
    KeyChange {
        table: TableIdentity::of::<Record>().key(),
        index: Some(index.into()),
        key,
        origin: Some(uuid::Uuid::new_v4().to_string()),
        emitted_at_ms: diagnostics::unix_millis(),
    }
}
#[tokio::test]
async fn targeted_invalidation_leaves_other_keys_indexes_and_entities_warm() {
    let c = local(Default::default());
    let scope = c.entity::<Record>();
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    for id in [a, b] {
        scope
            .get_or_load(RecordKey::Id(id), || async { Ok(1u32) })
            .await
            .unwrap();
    }
    scope
        .get_or_load(RecordKey::Name("alias".into()), || async { Ok(2u32) })
        .await
        .unwrap();
    c.entity::<OtherRecord>()
        .get_or_load(OtherKey::Id(a), || async { Ok(3u32) })
        .await
        .unwrap();
    c.apply_changes(&[change(RecordKey::Id(a))], true);
    assert_eq!(
        scope
            .get_or_load(RecordKey::Id(a), || async { Ok(9u32) })
            .await
            .unwrap(),
        9
    );
    assert_eq!(
        scope
            .get_or_load::<u32, _, _>(RecordKey::Id(b), || async {
                panic!("other user invalidated")
            })
            .await
            .unwrap(),
        1u32
    );
    assert_eq!(
        scope
            .get_or_load::<u32, _, _>(RecordKey::Name("alias".into()), || async {
                panic!("undeclared alias invalidated")
            })
            .await
            .unwrap(),
        2u32
    );
    assert_eq!(
        c.entity::<OtherRecord>()
            .get_or_load::<u32, _, _>(OtherKey::Id(a), || async {
                panic!("other entity invalidated")
            })
            .await
            .unwrap(),
        3u32
    );
}
#[tokio::test]
async fn concurrent_misses_coalesce_and_errors_do_not_poison_the_key() {
    let c = local(Default::default());
    let s = c.entity::<Record>();
    let id = uuid::Uuid::new_v4();
    let loads = AtomicUsize::new(0);
    let results = futures::future::join_all((0..24).map(|_| {
        s.get_or_load(RecordKey::Id(id), || async {
            loads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err::<u32, _>(anyhow!("temporary failure"))
        })
    }))
    .await;
    assert!(results.iter().all(Result::is_err));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    let results = futures::future::join_all((0..24).map(|_| {
        s.get_or_load(RecordKey::Id(id), || async {
            loads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(7u32)
        })
    }))
    .await;
    assert!(results.into_iter().all(|r| r.unwrap() == 7));
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn racing_fill_cannot_publish_or_return_stale_value() {
    let c = local(Default::default());
    let id = uuid::Uuid::new_v4();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let reader = {
        let (c, started, release) = (c.clone(), started.clone(), release.clone());
        tokio::spawn(async move {
            let calls = AtomicUsize::new(0);
            c.entity::<Record>()
                .get_or_load(RecordKey::Id(id), || async {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        started.notify_one();
                        release.notified().await;
                        Ok(1u32)
                    } else {
                        Ok(2u32)
                    }
                })
                .await
                .unwrap()
        })
    };
    started.notified().await;
    c.apply_changes(&[change(RecordKey::Id(id))], true);
    release.notify_one();
    assert_eq!(reader.await.unwrap(), 2);
    assert_eq!(
        c.entity::<Record>()
            .get_or_load::<u32, _, _>(RecordKey::Id(id), || async {
                panic!("fresh value missing")
            })
            .await
            .unwrap(),
        2u32
    );
}
#[tokio::test]
async fn own_echo_does_not_invalidate_a_post_commit_fill() {
    let c = local(Default::default());
    let id = uuid::Uuid::new_v4();
    let mut event = change(RecordKey::Id(id));
    event.origin = Some(c.0.instance_id.to_string());
    c.apply_changes(&[event.clone()], true);
    c.entity::<Record>()
        .get_or_load(RecordKey::Id(id), || async { Ok(2u32) })
        .await
        .unwrap();
    c.apply_changes(&[event], false);
    assert_eq!(
        c.entity::<Record>()
            .get_or_load::<u32, _, _>(RecordKey::Id(id), || async {
                panic!("echo invalidated fresh value")
            })
            .await
            .unwrap(),
        2u32
    );
    assert_eq!(
        c.diagnostics()
            .await
            .scopes
            .iter()
            .map(|s| s.remote_writes)
            .sum::<u64>(),
        0
    );
}
#[tokio::test]
async fn negative_entries_expire_on_absolute_ttl_and_default_is_an_hour() {
    assert_eq!(CacheOptions::default().ttl, Duration::from_secs(3600));
    let c = local(CacheOptions {
        ttl: Duration::from_millis(200),
        ..Default::default()
    });
    let s = c.entity::<Record>();
    let id = uuid::Uuid::new_v4();
    s.get_or_load(RecordKey::Id(id), || async { Ok(None::<u32>) })
        .await
        .unwrap();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            s.get_or_load::<Option<u32>, _, _>(RecordKey::Id(id), || async {
                panic!("negative cache missed")
            })
            .await
            .unwrap(),
            None::<u32>
        );
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        s.get_or_load(RecordKey::Id(id), || async { Ok(Some(3u32)) })
            .await
            .unwrap(),
        Some(3)
    );
}
#[tokio::test]
async fn expired_health_lease_and_disconnect_bypass_and_reconnect_clears() {
    let c = local(Default::default());
    let s = c.entity::<Record>();
    let id = uuid::Uuid::new_v4();
    s.get_or_load(RecordKey::Id(id), || async { Ok(1u32) })
        .await
        .unwrap();
    c.0.state.lock().unwrap().healthy_at = Some(Instant::now() - HEALTH_LEASE);
    assert!(!c.diagnostics().await.reads_enabled);
    assert_eq!(
        s.get_or_load(RecordKey::Id(id), || async { Ok(2u32) })
            .await
            .unwrap(),
        2
    );
    c.connection_changed(false);
    assert_eq!(
        s.get_or_load(RecordKey::Id(id), || async { Ok(3u32) })
            .await
            .unwrap(),
        3
    );
    c.connection_changed(true); // subscription alone must not enable reads
    assert!(!c.diagnostics().await.reads_enabled);
    c.heartbeat_received();
    assert_eq!(
        s.get_or_load(RecordKey::Id(id), || async { Ok(4u32) })
            .await
            .unwrap(),
        4
    );
}
#[tokio::test]
async fn cancelling_loader_releases_fill_permit_and_waiters() {
    let c = local(Default::default());
    let id = uuid::Uuid::new_v4();
    let started = Arc::new(Notify::new());
    let reader = {
        let (c, started) = (c.clone(), started.clone());
        tokio::spawn(async move {
            c.entity::<Record>()
                .get_or_load(RecordKey::Id(id), || async {
                    started.notify_one();
                    std::future::pending::<()>().await;
                    Ok(1u32)
                })
                .await
        })
    };
    started.notified().await;
    reader.abort();
    let _ = reader.await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            c.entity::<Record>()
                .get_or_load(RecordKey::Id(id), || async { Ok(2u32) })
        )
        .await
        .unwrap()
        .unwrap(),
        2
    );
    assert_eq!(c.0.loads.available_permits(), 4);
}
#[tokio::test]
async fn shared_capacity_registry_and_fill_concurrency_are_bounded() {
    let c = local(CacheOptions {
        max_entries: 5,
        ..Default::default()
    });
    let s = c.entity::<Record>();
    for _ in 0..600 {
        s.get_or_load(RecordKey::Id(uuid::Uuid::new_v4()), || async { Ok(1u32) })
            .await
            .unwrap();
    }
    c.0.data.run_pending_tasks().await;
    assert!(c.0.data.entry_count() <= 5);
    // Sweep weak bookkeeping without retaining historical notification keys indefinitely.
    prune_key_slots(&mut c.0.state.lock().unwrap(), &c.0.data);
    assert!(c.0.state.lock().unwrap().keys.len() <= 5);
    c.connection_changed(false);
    let active = AtomicUsize::new(0);
    let max = AtomicUsize::new(0);
    futures::future::join_all((0..20).map(|_| {
        s.get_or_load(RecordKey::Id(uuid::Uuid::new_v4()), || async {
            let n = active.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(n, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(3)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(1u32)
        })
    }))
    .await
    .into_iter()
    .for_each(|r| assert_eq!(r.unwrap(), 1));
    assert_eq!(max.load(Ordering::SeqCst), 4);
}
#[tokio::test]
async fn continuous_invalidation_bounds_retries_and_types_are_checked() {
    let c = local(Default::default());
    let s = c.entity::<Record>();
    let id = uuid::Uuid::new_v4();
    let calls = AtomicUsize::new(0);
    assert!(s
        .get_or_load(RecordKey::Id(id), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            c.apply_changes(&[change(RecordKey::Id(id))], true);
            Ok(1u32)
        })
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), MAX_LOAD_ATTEMPTS);
    assert!(s
        .get_or_load(RecordKey::Id(id), || async { Ok("wrong result type") })
        .await
        .is_err());
}
#[tokio::test]
async fn diagnostics_are_passive_bounded_and_do_not_expose_keys_values_or_errors() {
    let c = local(Default::default());
    let s = c.entity::<Record>();
    s.get_or_load(RecordKey::Name("private key".into()), || async {
        Ok("private value")
    })
    .await
    .unwrap();
    for _ in 0..150 {
        c.apply_changes(&[change(RecordKey::Name("private key".into()))], true);
    }
    let before = c.0.state.lock().unwrap().healthy_at;
    let d = c.diagnostics().await;
    assert_eq!(d.recent_events.len(), 100);
    assert_eq!(d.recent_events[0].id, 150);
    assert!(!format!("{d:?}").contains("private"));
    assert_eq!(d.health_checks, 1);
    assert_eq!(c.0.state.lock().unwrap().healthy_at, before);
    assert_eq!(d.indexes.len(), 4);
    assert_eq!(d.shared_capacity, 10_000);
}
#[test]
fn duplicate_registration_and_unregistered_entities() {
    let c = CacheCoherence::builder(Arc::new(DatabaseConnection::Disconnected))
        .register::<Record>()
        .register::<Record>()
        .build();
    assert_eq!(c.0.state.lock().unwrap().scopes.len(), 1);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.entity::<OtherRecord>()))
            .is_err()
    );
    c.entity::<Record>();
}

#[tokio::test]
async fn remote_timings_exclude_unknown_origin_and_negative_clock_differences() {
    let c = local(Default::default());
    let id = uuid::Uuid::new_v4();
    let mut event = change(RecordKey::Id(id));
    event.emitted_at_ms = diagnostics::unix_millis().saturating_sub(100);
    c.apply_changes(&[event.clone()], false);
    event.emitted_at_ms = u64::MAX;
    c.apply_changes(&[event.clone()], false);
    event.origin = None;
    c.apply_changes(&[event], false);
    let d = c.diagnostics().await;
    let scope = d
        .scopes
        .iter()
        .find(|s| s.table == TableIdentity::of::<Record>().key())
        .unwrap();
    assert_eq!(scope.remote_writes, 2);
    assert_eq!(scope.remote_delay_samples, 1);
    assert!(scope.remote_delay_total_ms >= 100);
    assert_eq!(scope.rejected_delay_samples, 1);
    assert_eq!(scope.unattributed_writes, 1);
}
