use super::{
    CacheCoherence, Cacheable, EntityScope, EntitySet, KeyChange, HEALTH_INTERVAL, HEALTH_TIMEOUT,
    KEY_NOTIFY_CHANNEL,
};
use sea_orm::{ConnectionTrait, DatabaseTransaction, DbBackend, Statement, TransactionTrait};
use sqlx::{
    postgres::{PgListener, PgPoolOptions},
    Executor,
};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::sync::watch;

impl CacheCoherence {
    /// One private, single-connection pool: listener session settings never leak into application
    /// transactions. Heartbeats exercise the notification receive path, not a different DB socket.
    pub async fn run_listener(&self, mut shutdown: watch::Receiver<bool>) {
        if self.0.listener_running.swap(true, Ordering::SeqCst) {
            tracing::error!("cache listener already running");
            return;
        }
        let _guard = ListenerGuard(self.clone());
        let options = self
            .0
            .db
            .get_postgres_connection_pool()
            .connect_options()
            .as_ref()
            .clone()
            .application_name("coherent-cache-keys-v2")
            .options([
                ("idle_in_transaction_session_timeout", "15000"),
                ("statement_timeout", "5000"),
                ("tcp_keepalives_idle", "15"),
                ("tcp_keepalives_interval", "5"),
                ("tcp_keepalives_count", "3"),
            ]);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(HEALTH_TIMEOUT)
            .connect_lazy_with(options);
        let mut backoff = Duration::from_millis(250);
        loop {
            if *shutdown.borrow() {
                break;
            }
            let result = tokio::select! {
                _ = shutdown.changed() => break,
                result = self.listen_until_disconnect(&pool) => result,
            };
            self.connection_changed(false);
            if let Err(error) = result {
                self.0.state.lock().unwrap().health_check_failures += 1;
                tracing::warn!(%error, "cache listener failed; bypassing cached reads until resubscribed");
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
        self.connection_changed(false);
        // Pool close must not indefinitely delay application shutdown on a blackholed socket.
        let _ = tokio::time::timeout(HEALTH_TIMEOUT, pool.close()).await;
    }

    async fn listen_until_disconnect(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let mut listener =
            tokio::time::timeout(HEALTH_TIMEOUT, PgListener::connect_with(pool)).await??;
        listener.eager_reconnect(false);
        let heartbeat_channel = format!("cache_health_{}", self.0.instance_id.simple());
        tokio::time::timeout(
            HEALTH_TIMEOUT,
            listener.listen_all([KEY_NOTIFY_CHANNEL, heartbeat_channel.as_str()]),
        )
        .await??;
        tokio::time::timeout(HEALTH_TIMEOUT, self.validate_schema()).await??;
        // Subscription is committed first; reset every entry before trusting the new connection.
        self.connection_changed(true);
        let mut next_health = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(next_health) => {
                    let nonce = uuid::Uuid::new_v4().to_string();
                    let payload = serde_json::json!({"heartbeat": nonce}).to_string();
                    // A timed end-to-end probe also drains older buffered notifications before
                    // renewing the health lease. Busy traffic cannot starve this deadline.
                    tokio::time::timeout(HEALTH_TIMEOUT, async {
                        listener.execute(sqlx::query("SELECT pg_notify($1,$2)").bind(&heartbeat_channel).bind(payload)).await?;
                        loop {
                            let notification = listener.try_recv().await?.ok_or_else(|| anyhow::anyhow!("listener disconnected"))?;
                            let value: serde_json::Value = serde_json::from_str(notification.payload())?;
                            if value.get("heartbeat").and_then(|v| v.as_str()) == Some(&nonce) { break; }
                            self.receive_notification(value)?;
                        }
                        Ok::<_, anyhow::Error>(())
                    }).await??;
                    // A healthy existing socket can retain unaffected entries after idle periods:
                    // the probe has drained earlier invalidations. Only reconnection clears all.
                    self.heartbeat_received();
                    next_health = tokio::time::Instant::now() + HEALTH_INTERVAL;
                }
                notification = listener.try_recv() => {
                    let notification = notification?.ok_or_else(|| anyhow::anyhow!("listener disconnected"))?;
                    self.receive_notification(serde_json::from_str(notification.payload())?)?;
                }
            }
        }
    }
    fn receive_notification(&self, value: serde_json::Value) -> anyhow::Result<()> {
        if value.get("heartbeat").is_some() {
            return Ok(());
        }
        let change: KeyChange = serde_json::from_value(value)?;
        self.apply_changes(&[change], false);
        Ok(())
    }

    pub async fn begin_write<T: EntitySet>(&self) -> anyhow::Result<CacheWrite> {
        self.begin_tables(T::tables().into_iter().map(|t| t.key()).collect())
            .await
    }
    async fn begin_tables(&self, mut tables: Vec<String>) -> anyhow::Result<CacheWrite> {
        tables.sort();
        tables.dedup();
        anyhow::ensure!(!tables.is_empty(), "cached write needs an entity");
        {
            let state = self.0.state.lock().unwrap();
            for table in &tables {
                anyhow::ensure!(
                    state.scopes.contains_key(table),
                    "unregistered cached write"
                );
            }
        }
        let transaction = self.0.db.begin().await?;
        // The generated initializer also preserves legacy lock ordering while old revisions
        // coexist. Once legacy infrastructure is removed it needs no generation table.
        transaction
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT public.begin_cache_write_v2($1::jsonb,$2)",
                [
                    serde_json::to_string(&tables)?.into(),
                    self.0.instance_id.to_string().into(),
                ],
            ))
            .await?;
        Ok(CacheWrite {
            transaction,
            coherence: self.clone(),
        })
    }
}
impl<E: Cacheable> EntityScope<E> {
    pub async fn begin_write(&self) -> anyhow::Result<CacheWrite> {
        self.coherence.begin_tables(vec![self.table.clone()]).await
    }
}
#[must_use = "commit the scoped transaction to persist the write"]
pub struct CacheWrite {
    transaction: DatabaseTransaction,
    coherence: CacheCoherence,
}
impl CacheWrite {
    /// Do not issue raw COMMIT/ROLLBACK or change the library's transaction-local settings.
    pub fn connection(&self) -> &DatabaseTransaction {
        &self.transaction
    }
    pub async fn commit(self) -> anyhow::Result<()> {
        let Self {
            transaction,
            coherence,
        } = self;
        // Capture and commit run in a detached task. Cancelling an HTTP request cannot leave a
        // committed mutation without synchronous local invalidation. Rollback discards the GUC.
        tokio::spawn(async move {
            let row = transaction
                .query_one(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT current_setting('coherent_cache.keys_v2',true) AS changes".to_owned(),
                ))
                .await?
                .unwrap();
            let json: String = row.try_get("", "changes")?;
            let changes: std::collections::HashMap<String, KeyChange> =
                serde_json::from_str(&json)?;
            let _invalidate = InvalidateOnDrop {
                coherence,
                changes: changes.into_values().collect(),
            };
            transaction.commit().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
struct InvalidateOnDrop {
    coherence: CacheCoherence,
    changes: Vec<KeyChange>,
}
impl Drop for InvalidateOnDrop {
    fn drop(&mut self) {
        self.coherence.apply_changes(&self.changes, true);
    }
}
struct ListenerGuard(CacheCoherence);
impl Drop for ListenerGuard {
    fn drop(&mut self) {
        self.0.connection_changed(false);
        self.0 .0.listener_running.store(false, Ordering::SeqCst);
    }
}
