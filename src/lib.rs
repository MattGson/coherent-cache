#![doc = include_str!("../README.md")]

pub mod diagnostics;
pub mod identity;
mod keys;
mod postgres;
pub mod schema;
pub use identity::{TableIdentity, NOTIFY_CHANNEL};
pub use keys::{CacheKey, Cacheable, KeyIndex};
pub use postgres::CacheWrite;
#[doc(hidden)]
pub use serde_json as __serde_json;

use anyhow::{anyhow, Context};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    future::Future,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const MAX_LOAD_ATTEMPTS: usize = 3;
const HEALTH_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_LEASE: Duration = Duration::from_secs(15);
pub const KEY_NOTIFY_CHANNEL: &str = "cache_keys_v2";

#[derive(Clone, Copy, Debug)]
pub struct CacheOptions {
    pub ttl: Duration,
    pub max_entries: u64,
}
impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(3600),
            max_entries: 10_000,
        }
    }
}

#[derive(Debug, Default)]
struct ScopeState {
    indexes: Vec<KeyIndex>,
    epoch: u64,
    local_invalidations: u64,
    notifications: u64,
    remote_writes: u64,
    unattributed_writes: u64,
    remote_delay_samples: u64,
    remote_delay_total_ms: u64,
    remote_delay_max_ms: u64,
    rejected_delay_samples: u64,
}
#[derive(Debug, Default)]
struct State {
    scopes: HashMap<String, ScopeState>,
    connected: bool,
    healthy_at: Option<Instant>,
    reset: u64,
    health_checks: u64,
    health_check_failures: u64,
    cache_resets: u64,
    events: std::collections::VecDeque<diagnostics::InvalidationEvent>,
    next_event_id: u64,
    keys: HashMap<WireKey, Weak<KeyState>>,
    next_key_id: u64,
    lookups: u64,
    types: HashMap<(String, String), TypeId>,
}
impl State {
    fn reads_enabled(&self) -> bool {
        self.connected
            && self
                .healthy_at
                .is_some_and(|at| at.elapsed() < HEALTH_LEASE)
    }
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WireKey {
    table: String,
    index: String,
    value: String,
}
#[derive(Debug)]
struct KeyState {
    id: u64,
    version: AtomicU64,
    readers: AtomicU64,
}
struct KeyUse(Arc<KeyState>);
impl Drop for KeyUse {
    fn drop(&mut self) {
        self.0.readers.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct StorageKey {
    wire: WireKey,
    id: u64,
    version: u64,
    reset: u64,
}
struct CachedValue {
    value: Arc<dyn Any + Send + Sync>,
    _key: Arc<KeyState>,
}
struct Inner {
    db: Arc<DatabaseConnection>,
    state: Mutex<State>,
    loads: Semaphore,
    data: moka::future::Cache<StorageKey, Arc<CachedValue>>,
    indexes: HashMap<(String, String), Arc<diagnostics::IndexMetrics>>,
    options: CacheOptions,
    instance_id: uuid::Uuid,
    started_at: Instant,
    listener_running: std::sync::atomic::AtomicBool,
}
#[derive(Clone)]
pub struct CacheCoherence(Arc<Inner>);
impl fmt::Debug for CacheCoherence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheCoherence")
            .field("instance", &self.0.instance_id)
            .finish_non_exhaustive()
    }
}
pub struct CacheCoherenceBuilder {
    db: Arc<DatabaseConnection>,
    scopes: HashMap<String, ScopeState>,
    max_concurrent_loads: usize,
    options: CacheOptions,
}
impl CacheCoherenceBuilder {
    pub fn register<E: Cacheable>(mut self) -> Self {
        let indexes = E::cache_indexes();
        assert!(!indexes.is_empty(), "cacheable entity needs keys");
        let unique: std::collections::HashSet<_> =
            indexes.iter().map(|i| i.column.as_str()).collect();
        assert_eq!(unique.len(), indexes.len(), "duplicate cache index");
        self.scopes
            .entry(TableIdentity::of::<E>().key())
            .or_insert_with(|| ScopeState {
                indexes,
                ..Default::default()
            });
        self
    }
    pub fn options(mut self, options: CacheOptions) -> Self {
        assert!(options.max_entries > 0 && !options.ttl.is_zero());
        self.options = options;
        self
    }
    pub fn max_concurrent_loads(mut self, limit: usize) -> Self {
        assert!(limit > 0);
        self.max_concurrent_loads = limit;
        self
    }
    pub fn build(self) -> CacheCoherence {
        assert!(!self.scopes.is_empty(), "register cached entities");
        let indexes = self
            .scopes
            .iter()
            .flat_map(|(table, scope)| {
                scope.indexes.iter().map(|index| {
                    (
                        (table.clone(), index.column.clone()),
                        Arc::new(diagnostics::IndexMetrics::new(table, &index.label)),
                    )
                })
            })
            .collect();
        CacheCoherence(Arc::new(Inner {
            db: self.db,
            state: Mutex::new(State {
                scopes: self.scopes,
                ..Default::default()
            }),
            data: moka::future::Cache::builder()
                .time_to_live(self.options.ttl)
                .max_capacity(self.options.max_entries)
                .build(),
            loads: Semaphore::new(self.max_concurrent_loads),
            indexes,
            options: self.options,
            instance_id: uuid::Uuid::new_v4(),
            started_at: Instant::now(),
            listener_running: false.into(),
        }))
    }
}
impl CacheCoherence {
    pub fn builder(db: Arc<DatabaseConnection>) -> CacheCoherenceBuilder {
        CacheCoherenceBuilder {
            db,
            scopes: HashMap::new(),
            max_concurrent_loads: 4,
            options: Default::default(),
        }
    }
    pub fn entity<E: Cacheable>(&self) -> EntityScope<E> {
        let table = TableIdentity::of::<E>().key();
        let registered = self.0.state.lock().unwrap().scopes.contains_key(&table);
        assert!(registered, "unregistered cached entity: {table}");
        EntityScope {
            coherence: self.clone(),
            table,
            entity: PhantomData,
        }
    }
    /// Startup/reconnect-only verification. No reads query generations or perform reconciliation.
    pub async fn validate_schema(&self) -> anyhow::Result<()> {
        let registrations: Vec<_> = self
            .0
            .state
            .lock()
            .unwrap()
            .scopes
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    s.indexes
                        .iter()
                        .map(|i| i.column.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        for (table, columns) in registrations {
            let definition = serde_json::to_string(&columns)?;
            let row = self.0.db.query_one(Statement::from_sql_and_values(DbBackend::Postgres,
                "SELECT count(*) AS n FROM pg_trigger t WHERE to_regprocedure('public.begin_cache_write_v2(jsonb,text)') IS NOT NULL AND t.tgrelid=to_regclass($1) AND NOT t.tgisinternal
                 AND t.tgenabled IN ('O','A') AND t.tgfoid=to_regprocedure('public.notify_cache_keys_v2()')
                 AND ((t.tgname='cache_keys_v2_row' AND t.tgtype=29) OR (t.tgname='cache_keys_v2_truncate' AND t.tgtype=32))
                 AND t.tgnargs=2 AND t.tgargs=convert_to($1,'UTF8') || decode('00','hex') || convert_to($2,'UTF8') || decode('00','hex')",
                 [table.clone().into(), definition.into()])).await?.context("cache schema check")?;
            anyhow::ensure!(
                row.try_get::<i64>("", "n")? == 2,
                "missing/disabled/mismatched keyed cache triggers: {table}"
            );
        }
        Ok(())
    }
    fn connection_changed(&self, connected: bool) {
        let mut state = self.0.state.lock().unwrap();
        state.connected = connected;
        state.healthy_at = None;
        state.reset += 1;
        if connected {
            state.cache_resets += 1;
            for scope in state.scopes.values_mut() {
                scope.epoch += 1;
            }
        }
        // Moka invalidation is synchronous; in-flight loads also check reset before returning.
        self.0.data.invalidate_all();
    }
    fn heartbeat_received(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.healthy_at = Some(Instant::now());
        state.health_checks += 1;
    }
    fn apply_changes(&self, changes: &[KeyChange], local: bool) {
        let mut state = self.0.state.lock().unwrap();
        for change in changes {
            // Our commit wrapper handles these even if a listener echo wins the race.
            if !local && change.origin.as_deref() == Some(&self.0.instance_id.to_string()) {
                continue;
            }
            let Some(scope) = state.scopes.get_mut(&change.table) else {
                continue;
            };
            if let Some(index) = &change.index {
                if !scope.indexes.iter().any(|i| &i.column == index) {
                    continue;
                }
            }
            if local {
                scope.local_invalidations += 1;
            } else {
                scope.notifications += 1;
            }
            if change.index.is_none() {
                scope.epoch += 1;
            }
            let origin = change
                .origin
                .as_ref()
                .filter(|id| uuid::Uuid::parse_str(id).is_ok())
                .cloned();
            let delay = if !local && origin.is_some() {
                scope.remote_writes += 1;
                let delay = diagnostics::unix_millis().checked_sub(change.emitted_at_ms);
                if let Some(ms) = delay {
                    scope.remote_delay_samples += 1;
                    scope.remote_delay_total_ms = scope.remote_delay_total_ms.saturating_add(ms);
                    scope.remote_delay_max_ms = scope.remote_delay_max_ms.max(ms);
                } else {
                    scope.rejected_delay_samples += 1;
                }
                delay
            } else {
                if !local {
                    scope.unattributed_writes += 1;
                }
                None
            };
            if let Some(index) = &change.index {
                let wire = WireKey {
                    table: change.table.clone(),
                    index: index.clone(),
                    value: change.key.to_string(),
                };
                if let Some(slot) = state.keys.get(&wire).and_then(Weak::upgrade) {
                    slot.version.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                // TRUNCATE is the sole ordinary entity-wide invalidation operation.
                for (key, slot) in &state.keys {
                    if key.table == change.table {
                        if let Some(slot) = slot.upgrade() {
                            slot.version.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
            diagnostics::push_event(
                &mut state,
                &change.table,
                if local {
                    diagnostics::InvalidationCause::LocalWrite
                } else if origin.is_some() {
                    diagnostics::InvalidationCause::RemoteWrite
                } else {
                    diagnostics::InvalidationCause::UnattributedWrite
                },
                origin,
                None,
                delay,
            );
        }
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct KeyChange {
    table: String,
    index: Option<String>,
    key: serde_json::Value,
    origin: Option<String>,
    #[serde(rename = "emittedAtMs")]
    emitted_at_ms: u64,
}
#[derive(Clone, Debug)]
pub struct EntityScope<E: Cacheable> {
    coherence: CacheCoherence,
    table: String,
    entity: PhantomData<E>,
}
impl<E: Cacheable> EntityScope<E> {
    /// Each declared index has one result type/loader contract. Empty/absent results are cached.
    pub async fn get_or_load<V, F, Fut>(&self, key: E::Key, loader: F) -> anyhow::Result<V>
    where
        V: Clone + Send + Sync + 'static,
        F: Fn() -> Fut,
        Fut: Future<Output = anyhow::Result<V>>,
    {
        let (index, value) = key.parts();
        let wire = WireKey {
            table: self.table.clone(),
            index: index.into(),
            value: value.to_string(),
        };
        let metrics = self
            .coherence
            .0
            .indexes
            .get(&(self.table.clone(), index.into()))
            .context("undeclared cache key")?;
        for _ in 0..MAX_LOAD_ATTEMPTS {
            let snapshot = {
                let mut state = self.coherence.0.state.lock().unwrap();
                let ty = state
                    .types
                    .entry((self.table.clone(), index.into()))
                    .or_insert(TypeId::of::<V>());
                anyhow::ensure!(
                    *ty == TypeId::of::<V>(),
                    "cache index used with inconsistent result types"
                );
                if !state.reads_enabled() {
                    None
                } else {
                    state.lookups += 1;
                    if state.lookups.is_multiple_of(256) {
                        prune_key_slots(&mut state, &self.coherence.0.data);
                    }
                    let slot = state
                        .keys
                        .get(&wire)
                        .and_then(Weak::upgrade)
                        .unwrap_or_else(|| {
                            state.next_key_id += 1;
                            let slot = Arc::new(KeyState {
                                id: state.next_key_id,
                                version: 0.into(),
                                readers: 0.into(),
                            });
                            state.keys.insert(wire.clone(), Arc::downgrade(&slot));
                            slot
                        });
                    let storage = StorageKey {
                        wire: wire.clone(),
                        id: slot.id,
                        version: slot.version.load(Ordering::SeqCst),
                        reset: state.reset,
                    };
                    slot.readers.fetch_add(1, Ordering::SeqCst);
                    Some((KeyUse(slot), storage))
                }
            };
            let Some((key_use, storage)) = snapshot else {
                metrics.bypasses.fetch_add(1, Ordering::Relaxed);
                return self.load(&loader, metrics).await;
            };
            let slot = &key_use.0;
            let result = if let Some(value) = self.coherence.0.data.get(&storage).await {
                metrics.hits.fetch_add(1, Ordering::Relaxed);
                Ok(value)
            } else {
                metrics.misses.fetch_add(1, Ordering::Relaxed);
                self.coherence
                    .0
                    .data
                    .try_get_with(storage.clone(), async {
                        let value = self.load(&loader, metrics).await?;
                        Ok::<_, anyhow::Error>(Arc::new(CachedValue {
                            value: Arc::new(value),
                            _key: slot.clone(),
                        }))
                    })
                    .await
            };
            let state = self.coherence.0.state.lock().unwrap();
            if !state.reads_enabled()
                || state.reset != storage.reset
                || slot.version.load(Ordering::SeqCst) != storage.version
            {
                metrics.retries.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            return result
                .map_err(|error| anyhow!("{error}"))
                .and_then(|value| {
                    value
                        .value
                        .downcast_ref::<V>()
                        .cloned()
                        .context("cache type mismatch")
                });
        }
        Err(anyhow!(
            "cache key changed repeatedly during load; retry request"
        ))
    }
    async fn load<V, F, Fut>(
        &self,
        loader: &F,
        metrics: &diagnostics::IndexMetrics,
    ) -> anyhow::Result<V>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = anyhow::Result<V>>,
    {
        let _permit = self.coherence.0.loads.acquire().await?;
        metrics.loads_started.fetch_add(1, Ordering::Relaxed);
        let result = loader().await;
        if result.is_err() {
            metrics.load_errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
// Moka may retain evicted values temporarily for epoch reclamation. Weak strong-count alone
// therefore isn't a reliable residency test. Keep precisely resident/in-flight slot identities.
fn prune_key_slots(state: &mut State, data: &moka::future::Cache<StorageKey, Arc<CachedValue>>) {
    let resident: std::collections::HashSet<_> = data.iter().map(|(key, _)| key.id).collect();
    state.keys.retain(|_, weak| {
        weak.upgrade().is_some_and(|slot| {
            slot.readers.load(Ordering::SeqCst) > 0 || resident.contains(&slot.id)
        })
    });
}

pub trait EntitySet {
    fn tables() -> Vec<TableIdentity>;
}
macro_rules! entity_set {
    ($($entity:ident),+) => { impl<$($entity: Cacheable),+> EntitySet for ($($entity,)+) {
        fn tables() -> Vec<TableIdentity> { vec![$(TableIdentity::of::<$entity>()),+] }
    }};
}
entity_set!(A);
entity_set!(A, B);
entity_set!(A, B, C);
entity_set!(A, B, C, D);
#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
