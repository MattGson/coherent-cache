//! Passive metrics for the shared keyed cache; no keys, row data, or database queries.
use crate::CacheCoherence;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostics {
    pub protocol: &'static str,
    pub instance_id: String,
    pub uptime_seconds: u64,
    pub listener_connected: bool,
    pub reads_enabled: bool,
    pub last_health_check_seconds_ago: Option<u64>,
    pub health_checks: u64,
    pub health_check_failures: u64,
    pub cache_resets: u64,
    pub shared_capacity: u64,
    // Kept during rolling frontend/backend upgrades. V2 never reconciles generations.
    pub last_reconciled_seconds_ago: Option<u64>,
    pub reconciliations: u64,
    pub reconciliation_failures: u64,
    pub scopes: Vec<ScopeDiagnostics>,
    pub indexes: Vec<IndexDiagnostics>,
    pub recent_events: Vec<InvalidationEvent>,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeDiagnostics {
    pub table: String,
    pub epoch: u64,
    pub last_verified_generation: Option<i64>,
    pub local_invalidations: u64,
    pub notifications: u64,
    pub reconciliation_invalidations: u64,
    pub remote_writes: u64,
    pub unattributed_writes: u64,
    pub remote_delay_samples: u64,
    pub remote_delay_total_ms: u64,
    pub remote_delay_max_ms: u64,
    pub rejected_delay_samples: u64,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexDiagnostics {
    pub name: String,
    pub table: String,
    pub resident_entries: u64,
    pub max_entries: u64,
    pub ttl_seconds: u64,
    pub hits: u64,
    pub misses: u64,
    pub bypasses: u64,
    pub loads_started: u64,
    pub load_errors: u64,
    pub retries: u64,
}
#[derive(Default)]
pub(crate) struct IndexMetrics {
    name: String,
    table: String,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub bypasses: AtomicU64,
    pub loads_started: AtomicU64,
    pub load_errors: AtomicU64,
    pub retries: AtomicU64,
}
impl IndexMetrics {
    pub fn new(table: &str, name: &str) -> Self {
        Self {
            table: table.into(),
            name: name.into(),
            ..Default::default()
        }
    }
}
impl CacheCoherence {
    pub async fn diagnostics(&self) -> Diagnostics {
        self.0.data.run_pending_tasks().await;
        let mut counts = std::collections::HashMap::new();
        for (key, _) in self.0.data.iter() {
            *counts
                .entry((key.wire.table.clone(), key.wire.index.clone()))
                .or_insert(0) += 1;
        }
        let mut indexes: Vec<_> = self
            .0
            .indexes
            .iter()
            .map(|(id, m)| IndexDiagnostics {
                name: m.name.clone(),
                table: m.table.clone(),
                resident_entries: counts.get(id).copied().unwrap_or(0),
                max_entries: self.0.options.max_entries,
                ttl_seconds: self.0.options.ttl.as_secs(),
                hits: m.hits.load(Ordering::Relaxed),
                misses: m.misses.load(Ordering::Relaxed),
                bypasses: m.bypasses.load(Ordering::Relaxed),
                loads_started: m.loads_started.load(Ordering::Relaxed),
                load_errors: m.load_errors.load(Ordering::Relaxed),
                retries: m.retries.load(Ordering::Relaxed),
            })
            .collect();
        indexes.sort_by(|a, b| (&a.table, &a.name).cmp(&(&b.table, &b.name)));
        let state = self.0.state.lock().unwrap();
        let mut scopes: Vec<_> = state
            .scopes
            .iter()
            .map(|(table, s)| ScopeDiagnostics {
                table: table.clone(),
                epoch: s.epoch,
                last_verified_generation: None,
                local_invalidations: s.local_invalidations,
                notifications: s.notifications,
                reconciliation_invalidations: 0,
                remote_writes: s.remote_writes,
                unattributed_writes: s.unattributed_writes,
                remote_delay_samples: s.remote_delay_samples,
                remote_delay_total_ms: s.remote_delay_total_ms,
                remote_delay_max_ms: s.remote_delay_max_ms,
                rejected_delay_samples: s.rejected_delay_samples,
            })
            .collect();
        scopes.sort_by(|a, b| a.table.cmp(&b.table));
        Diagnostics {
            protocol: "keys-v2",
            instance_id: self.0.instance_id.to_string(),
            uptime_seconds: self.0.started_at.elapsed().as_secs(),
            listener_connected: state.connected,
            reads_enabled: state.reads_enabled(),
            last_health_check_seconds_ago: state.healthy_at.map(|at| at.elapsed().as_secs()),
            health_checks: state.health_checks,
            health_check_failures: state.health_check_failures,
            cache_resets: state.cache_resets,
            shared_capacity: self.0.options.max_entries,
            last_reconciled_seconds_ago: None,
            reconciliations: 0,
            reconciliation_failures: 0,
            scopes,
            indexes,
            recent_events: state.events.iter().rev().cloned().collect(),
        }
    }
}
/// Bounded activity history, with no cache keys or row data. Sequence IDs are process-local.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvalidationEvent {
    pub id: u64,
    pub observed_at_ms: u64,
    pub table: String,
    pub cause: InvalidationCause,
    pub origin_instance_id: Option<String>,
    pub generation: Option<i64>,
    pub notification_delay_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InvalidationCause {
    LocalWrite,
    RemoteWrite,
    UnattributedWrite,
    Reconciliation,
}

const MAX_EVENTS: usize = 100;

pub(crate) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn push_event(
    state: &mut crate::State,
    table: &str,
    cause: InvalidationCause,
    origin_instance_id: Option<String>,
    generation: Option<i64>,
    notification_delay_ms: Option<u64>,
) {
    state.next_event_id += 1;
    if state.events.len() == MAX_EVENTS {
        state.events.pop_front();
    }
    state.events.push_back(InvalidationEvent {
        id: state.next_event_id,
        observed_at_ms: unix_millis(),
        table: table.to_owned(),
        cause,
        origin_instance_id,
        generation,
        notification_delay_ms,
    });
}
