use super::*;
use crate::test_support::{OtherRecord, Record, RecordKey};
use sea_orm::Database;
use sea_orm_migration::SchemaManager;

async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition did not converge");
}
struct Node {
    cache: CacheCoherence,
    stop: tokio::sync::watch::Sender<bool>,
    listener: tokio::task::JoinHandle<()>,
}
impl Node {
    async fn new(db: Arc<DatabaseConnection>) -> Self {
        let cache = CacheCoherence::builder(db)
            .register::<Record>()
            .register::<OtherRecord>()
            .build();
        cache.validate_schema().await.unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let c = cache.clone();
        let listener = tokio::spawn(async move { c.run_listener(rx).await });
        wait(|| cache.0.state.lock().unwrap().reads_enabled()).await;
        Self {
            cache,
            stop,
            listener,
        }
    }
    async fn close(self) {
        self.stop.send(true).unwrap();
        self.listener.await.unwrap();
    }
}
async fn insert(db: &impl ConnectionTrait, id: uuid::Uuid, group: uuid::Uuid, name: &str) {
    db.execute(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO cache_test_records (id,workspace_id,name) VALUES ($1,$2,$3)",
        [id.into(), group.into(), name.into()],
    ))
    .await
    .unwrap();
}
async fn rename(db: &impl ConnectionTrait, id: uuid::Uuid, name: &str) {
    db.execute(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE cache_test_records SET name=$2 WHERE id=$1",
        [id.into(), name.into()],
    ))
    .await
    .unwrap();
}
async fn name(db: &DatabaseConnection, id: uuid::Uuid) -> anyhow::Result<Option<String>> {
    db.query_one(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT name FROM cache_test_records WHERE id=$1",
        [id.into()],
    ))
    .await?
    .map(|r| r.try_get("", "name").map_err(Into::into))
    .transpose()
}
async fn by_name(db: &DatabaseConnection, value: &str) -> anyhow::Result<Option<uuid::Uuid>> {
    db.query_one(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT id FROM cache_test_records WHERE name=$1",
        [value.into()],
    ))
    .await?
    .map(|r| r.try_get("", "id").map_err(Into::into))
    .transpose()
}
async fn members(db: &DatabaseConnection, group: uuid::Uuid) -> anyhow::Result<Vec<String>> {
    db.query_all(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT name FROM cache_test_records WHERE workspace_id=$1 ORDER BY name",
        [group.into()],
    ))
    .await?
    .into_iter()
    .map(|r| r.try_get("", "name").map_err(Into::into))
    .collect()
}
async fn reads(cache: &CacheCoherence, db: &DatabaseConnection, id: uuid::Uuid) -> Option<String> {
    cache
        .entity::<Record>()
        .get_or_load(RecordKey::Id(id), || name(db, id))
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn targeted_aliases_negative_entries_and_workspace_collections_on_two_nodes() {
    let db = test_support::database().await;
    let a = Node::new(db.clone()).await;
    let b = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    let untouched = uuid::Uuid::new_v4();
    let group = uuid::Uuid::new_v4();
    let other_group = uuid::Uuid::new_v4();
    let old = format!("old-{id}");
    let new = format!("new-{id}");
    insert(db.as_ref(), untouched, other_group, "untouched").await;
    // Cache both an absent ID and an empty collection: insertion must invalidate both.
    for c in [&a.cache, &b.cache] {
        assert!(reads(c, &db, id).await.is_none());
        reads(c, &db, untouched).await;
        assert!(c
            .entity::<Record>()
            .get_or_load(RecordKey::Workspace(group), || members(&db, group))
            .await
            .unwrap()
            .is_empty());
        c.entity::<Record>()
            .get_or_load(RecordKey::Workspace(other_group), || {
                members(&db, other_group)
            })
            .await
            .unwrap();
        assert!(c
            .entity::<Record>()
            .get_or_load(RecordKey::Name(new.clone()), || by_name(&db, &new))
            .await
            .unwrap()
            .is_none());
    }
    let write = a.cache.entity::<Record>().begin_write().await.unwrap();
    insert(write.connection(), id, group, &old).await;
    assert!(reads(&b.cache, &db, id).await.is_none());
    write.commit().await.unwrap();
    assert_eq!(
        reads(&a.cache, &db, id).await.as_deref(),
        Some(old.as_str())
    );
    wait(|| {
        b.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()].notifications
            >= 3
    })
    .await;
    assert_eq!(
        reads(&b.cache, &db, id).await.as_deref(),
        Some(old.as_str())
    );
    for c in [&a.cache, &b.cache] {
        assert_eq!(
            c.entity::<Record>()
                .get_or_load(RecordKey::Name(old.clone()), || by_name(&db, &old))
                .await
                .unwrap(),
            Some(id)
        );
        assert_eq!(
            c.entity::<Record>()
                .get_or_load(RecordKey::Workspace(group), || members(&db, group))
                .await
                .unwrap(),
            vec![old.clone()]
        );
    }
    let before =
        b.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()].notifications;
    let write = a.cache.entity::<Record>().begin_write().await.unwrap();
    rename(write.connection(), id, &new).await;
    write.commit().await.unwrap();
    // Immediate local read and eventual remote invalidation; the writer's own echo must not clear it again.
    assert_eq!(
        reads(&a.cache, &db, id).await.as_deref(),
        Some(new.as_str())
    );
    wait(|| {
        b.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()].notifications
            >= before + 4
    })
    .await;
    for c in [&a.cache, &b.cache] {
        assert_eq!(reads(c, &db, id).await.as_deref(), Some(new.as_str()));
        assert_eq!(
            c.entity::<Record>()
                .get_or_load(RecordKey::Name(old.clone()), || by_name(&db, &old))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            c.entity::<Record>()
                .get_or_load(RecordKey::Name(new.clone()), || by_name(&db, &new))
                .await
                .unwrap(),
            Some(id)
        );
        assert_eq!(
            c.entity::<Record>()
                .get_or_load(RecordKey::Workspace(group), || members(&db, group))
                .await
                .unwrap(),
            vec![new.clone()]
        );
        // Other user and workspace entries MUST remain hits, not just return equivalent data.
        assert_eq!(
            c.entity::<Record>()
                .get_or_load::<Option<String>, _, _>(RecordKey::Id(untouched), || async {
                    panic!("unrelated user evicted")
                })
                .await
                .unwrap()
                .as_deref(),
            Some("untouched")
        );
        c.entity::<Record>()
            .get_or_load::<Vec<String>, _, _>(RecordKey::Workspace(other_group), || async {
                panic!("unrelated workspace evicted")
            })
            .await
            .unwrap();
    }
    // Delete invalidates the former aliases and collection through the same trigger.
    let write = a.cache.entity::<Record>().begin_write().await.unwrap();
    write
        .connection()
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM cache_test_records WHERE id=$1",
            [id.into()],
        ))
        .await
        .unwrap();
    write.commit().await.unwrap();
    assert!(reads(&a.cache, &db, id).await.is_none());
    let d = b.cache.diagnostics().await;
    assert!(d.scopes.iter().any(|s| s.remote_delay_samples > 0));
    a.close().await;
    b.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn rollback_savepoints_noop_updates_and_pooled_origin_cleanup() {
    let db = test_support::database().await;
    let reader = Node::new(db.clone()).await;
    let mut options =
        sea_orm::ConnectOptions::new(std::env::var("CACHE_TEST_DATABASE_URL").unwrap());
    options.max_connections(1).min_connections(1);
    let writer_db = Arc::new(Database::connect(options).await.unwrap());
    let writer = CacheCoherence::builder(writer_db.clone())
        .register::<Record>()
        .build();
    writer.connection_changed(true);
    writer.heartbeat_received(); // deliberately no listener: proves local commit path
    let id = uuid::Uuid::new_v4();
    let group = uuid::Uuid::new_v4();
    let w = writer.entity::<Record>().begin_write().await.unwrap();
    insert(w.connection(), id, group, "before").await;
    w.commit().await.unwrap();
    wait(|| {
        reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications
            >= 3
    })
    .await;
    let before = reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
        .notifications;
    assert_eq!(
        reads(&writer, &writer_db, id).await.as_deref(),
        Some("before")
    );
    let rollback = writer.entity::<Record>().begin_write().await.unwrap();
    rename(rollback.connection(), id, "rolled back").await;
    drop(rollback);
    let w = writer.entity::<Record>().begin_write().await.unwrap();
    w.connection()
        .execute_unprepared("SAVEPOINT cache_test")
        .await
        .unwrap();
    rename(w.connection(), id, "savepoint rollback").await;
    w.connection()
        .execute_unprepared("ROLLBACK TO SAVEPOINT cache_test")
        .await
        .unwrap();
    rename(w.connection(), id, "before").await; // unchanged row, no key invalidation
    w.commit().await.unwrap();
    writer
        .entity::<Record>()
        .get_or_load::<Option<String>, _, _>(RecordKey::Id(id), || async {
            panic!("rollback/noop invalidated")
        })
        .await
        .unwrap();
    // Raw SQL uses the same physical writer connection. The previous origin/GUC cannot leak.
    rename(writer_db.as_ref(), id, "external").await;
    wait(|| {
        reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications
            >= before + 4
    })
    .await;
    let s = reader.cache.diagnostics().await;
    assert_eq!(
        s.scopes
            .iter()
            .find(|s| s.table == TableIdentity::of::<Record>().key())
            .unwrap()
            .unattributed_writes,
        4
    );
    reader.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn reconnect_clears_missed_keys_and_real_backend_termination_recovers() {
    let db = test_support::database().await;
    let node = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    insert(db.as_ref(), id, uuid::Uuid::new_v4(), "old").await;
    // Drain notifications before warming so a pending insert cannot explain later invalidation.
    wait(|| {
        node.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications
            >= 3
    })
    .await;
    assert_eq!(reads(&node.cache, &db, id).await.as_deref(), Some("old"));
    node.listener.abort();
    let _ = node.listener.await;
    assert!(!node.cache.diagnostics().await.reads_enabled);
    rename(db.as_ref(), id, "missed").await;
    let (stop, rx) = tokio::sync::watch::channel(false);
    let c = node.cache.clone();
    let listener = tokio::spawn(async move { c.run_listener(rx).await });
    wait(|| node.cache.0.state.lock().unwrap().reads_enabled()).await;
    assert_eq!(reads(&node.cache, &db, id).await.as_deref(), Some("missed"));
    let resets = node.cache.diagnostics().await.cache_resets;
    db.execute_unprepared("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='coherent-cache-keys-v2' AND pid<>pg_backend_pid()").await.unwrap();
    wait(|| {
        let s = node.cache.0.state.lock().unwrap();
        s.cache_resets > resets && s.reads_enabled()
    })
    .await;
    assert_eq!(reads(&node.cache, &db, id).await.as_deref(), Some("missed"));
    stop.send(true).unwrap();
    listener.await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn cancelled_commit_still_invalidates_and_multi_entity_truncate_works() {
    let db = test_support::database().await;
    let node = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    let group = uuid::Uuid::new_v4();
    assert!(reads(&node.cache, &db, id).await.is_none());
    let w = node
        .cache
        .begin_write::<(Record, OtherRecord)>()
        .await
        .unwrap();
    insert(w.connection(), id, group, "cancelled caller").await;
    w.connection()
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO cache_test_other_records (id,name) VALUES ($1,'other')",
            [id.into()],
        ))
        .await
        .unwrap();
    w.commit().await.unwrap();
    // Block COMMIT in a deferred trigger so cancellation is deterministic.
    db.execute_unprepared("CREATE OR REPLACE FUNCTION cache_test_slow_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.2); RETURN NULL; END $$;
        CREATE CONSTRAINT TRIGGER cache_test_delay AFTER INSERT ON cache_test_records DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cache_test_slow_commit();").await.unwrap();
    let cancelled_id = uuid::Uuid::new_v4();
    assert!(reads(&node.cache, &db, cancelled_id).await.is_none());
    let w = node.cache.entity::<Record>().begin_write().await.unwrap();
    insert(w.connection(), cancelled_id, group, "detached").await;
    let commit = tokio::spawn(async move { w.commit().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    commit.abort();
    let _ = commit.await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        reads(&node.cache, &db, cancelled_id).await.as_deref(),
        Some("detached")
    );
    db.execute_unprepared("DROP TRIGGER cache_test_delay ON cache_test_records; DROP FUNCTION cache_test_slow_commit()").await.unwrap();
    let w = node.cache.entity::<Record>().begin_write().await.unwrap();
    w.connection()
        .execute_unprepared("TRUNCATE cache_test_records")
        .await
        .unwrap();
    w.commit().await.unwrap();
    assert!(reads(&node.cache, &db, id).await.is_none());
    assert!(reads(&node.cache, &db, cancelled_id).await.is_none());
    node.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn schema_validation_and_keyed_migration_rollback_preserve_legacy_protocol() {
    let db = test_support::database().await;
    let c = CacheCoherence::builder(db.clone())
        .register::<Record>()
        .build();
    let manager = SchemaManager::new(db.as_ref());
    c.validate_schema().await.unwrap();
    db.execute_unprepared("ALTER TABLE cache_test_records DISABLE TRIGGER cache_keys_v2_row")
        .await
        .unwrap();
    assert!(c.validate_schema().await.is_err());
    db.execute_unprepared("ALTER TABLE cache_test_records ENABLE TRIGGER cache_keys_v2_row")
        .await
        .unwrap();
    schema::untrack_keyed_table(&manager, &TableIdentity::of::<Record>())
        .await
        .unwrap();
    assert!(c.validate_schema().await.is_err());
    let id = uuid::Uuid::new_v4();
    insert(db.as_ref(), id, uuid::Uuid::new_v4(), "legacy survives").await;
    let generation = db
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT generation FROM cache_generations WHERE scope=$1",
            [TableIdentity::of::<Record>().key().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "generation")
        .unwrap();
    assert!(generation > 0);
    schema::track_keyed::<Record>(&manager).await.unwrap();
    c.validate_schema().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn deferred_commit_failure_invalidates_locally_without_publishing_a_change() {
    let db = test_support::database().await;
    let writer = CacheCoherence::builder(db.clone())
        .register::<Record>()
        .build();
    writer.connection_changed(true);
    writer.heartbeat_received();
    let reader = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    insert(
        db.as_ref(),
        id,
        uuid::Uuid::new_v4(),
        "before failed commit",
    )
    .await;
    wait(|| {
        reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications
            >= 3
    })
    .await;
    let before = reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
        .notifications;
    reads(&writer, &db, id).await;
    db.execute_unprepared("CREATE OR REPLACE FUNCTION cache_test_reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rejected commit'; END $$;
        CREATE CONSTRAINT TRIGGER cache_test_reject AFTER UPDATE ON cache_test_records DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cache_test_reject_commit();").await.unwrap();
    let w = writer.entity::<Record>().begin_write().await.unwrap();
    rename(w.connection(), id, "rejected").await;
    assert!(w.commit().await.is_err());
    db.execute_unprepared("DROP TRIGGER cache_test_reject ON cache_test_records; DROP FUNCTION cache_test_reject_commit()").await.unwrap();
    assert_eq!(
        name(&db, id).await.unwrap().as_deref(),
        Some("before failed commit")
    );
    let calls = std::sync::atomic::AtomicUsize::new(0);
    writer
        .entity::<Record>()
        .get_or_load(RecordKey::Id(id), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            name(&db, id).await
        })
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1); // guard invalidated despite commit failing
    assert_eq!(
        reader.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications,
        before
    );
    reader.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn many_rows_in_one_workspace_deduplicate_collection_invalidation_and_malformed_notify_recovers(
) {
    let db = test_support::database().await;
    let node = Node::new(db.clone()).await;
    let group = uuid::Uuid::new_v4();
    node.cache
        .entity::<Record>()
        .get_or_load(RecordKey::Workspace(group), || members(&db, group))
        .await
        .unwrap();
    let w = node.cache.entity::<Record>().begin_write().await.unwrap();
    for _ in 0..40 {
        let id = uuid::Uuid::new_v4();
        insert(w.connection(), id, group, &id.to_string()).await;
    }
    let payload = w
        .connection()
        .query_one(Statement::from_string(
            DbBackend::Postgres,
            "SELECT current_setting('coherent_cache.keys_v2') AS changes",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "changes")
        .unwrap();
    let changes: std::collections::HashMap<String, KeyChange> =
        serde_json::from_str(&payload).unwrap();
    assert_eq!(
        changes
            .values()
            .filter(|v| v.index.as_deref() == Some("workspace_id"))
            .count(),
        1
    );
    assert_eq!(changes.len(), 81); // 40 IDs, 40 aliases, one collection
    w.commit().await.unwrap();
    assert_eq!(
        node.cache
            .entity::<Record>()
            .get_or_load(RecordKey::Workspace(group), || members(&db, group))
            .await
            .unwrap()
            .len(),
        40
    );
    let resets = node.cache.0.state.lock().unwrap().cache_resets;
    db.execute_unprepared("SELECT pg_notify('cache_keys_v2','malformed')")
        .await
        .unwrap();
    wait(|| {
        let s = node.cache.0.state.lock().unwrap();
        s.cache_resets > resets && s.reads_enabled()
    })
    .await;
    assert!(node.cache.diagnostics().await.health_check_failures > 0);
    node.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn heartbeat_renews_expired_lease_without_flushing_a_healthy_idle_cache() {
    let db = test_support::database().await;
    let node = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    insert(db.as_ref(), id, uuid::Uuid::new_v4(), "warm through idle").await;
    wait(|| {
        node.cache.0.state.lock().unwrap().scopes[&TableIdentity::of::<Record>().key()]
            .notifications
            >= 3
    })
    .await;
    reads(&node.cache, &db, id).await;
    let (checks, resets) = {
        let mut s = node.cache.0.state.lock().unwrap();
        s.healthy_at = Some(Instant::now() - HEALTH_LEASE);
        (s.health_checks, s.cache_resets)
    };
    assert!(!node.cache.diagnostics().await.reads_enabled);
    wait(|| node.cache.0.state.lock().unwrap().health_checks > checks).await;
    assert_eq!(node.cache.diagnostics().await.cache_resets, resets);
    node.cache
        .entity::<Record>()
        .get_or_load::<Option<String>, _, _>(RecordKey::Id(id), || async {
            panic!("idle heartbeat flushed unrelated data")
        })
        .await
        .unwrap();
    node.close().await;
}

#[tokio::test]
#[ignore = "requires disposable CACHE_TEST_DATABASE_URL; run serially"]
async fn keyed_protocol_works_without_any_generation_or_log_table() {
    let db = test_support::database().await;
    let manager = SchemaManager::new(db.as_ref());
    schema::untrack::<Record>(&manager).await.unwrap();
    schema::untrack::<OtherRecord>(&manager).await.unwrap();
    schema::teardown(&manager).await.unwrap();
    assert!(!manager.has_table("cache_generations").await.unwrap());
    let node = Node::new(db.clone()).await;
    let id = uuid::Uuid::new_v4();
    assert!(reads(&node.cache, &db, id).await.is_none());
    let w = node.cache.entity::<Record>().begin_write().await.unwrap();
    insert(w.connection(), id, uuid::Uuid::new_v4(), "no tables needed").await;
    w.commit().await.unwrap();
    assert_eq!(
        reads(&node.cache, &db, id).await.as_deref(),
        Some("no tables needed")
    );
    node.close().await;
    schema::bootstrap(&manager).await.unwrap();
    schema::track::<Record>(&manager).await.unwrap();
    schema::track::<OtherRecord>(&manager).await.unwrap();
}
