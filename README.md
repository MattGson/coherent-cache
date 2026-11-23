# coherent-cache

Typed, process-local read-through caching for SeaORM applications, with targeted
invalidation coordinated through PostgreSQL transactions and `LISTEN`/`NOTIFY`.

Each process owns a bounded in-memory store. PostgreSQL triggers notify other
processes when the row-column keys they depend on change. Loaders keep database
queries, filtering, and domain conversion in your application.

## Status and installation

Version `0.1.0` is prepared for its first publication; it is not yet on crates.io.
Until publication, use a local path to this repository:

```toml
[dependencies]
coherent-cache = { path = "../coherent-cache" }
```

After publication, use `coherent-cache = "0.1.0"`. Requires Rust 1.88 or later,
Tokio, SeaORM 1.1, and PostgreSQL. The library uses native TLS; Linux builds need
OpenSSL development headers and `pkg-config`.

## Quick start

The complete example below defines a SeaORM entity, installs the keyed triggers,
starts the listener, reads through the cache, and commits a write. It can be run
against a disposable database:

```sh
DATABASE_URL=postgres://localhost/cache_example cargo run --locked --example basic
```

For a consuming application, also add `anyhow = "1"`,
`tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }`,
`sea-orm = { version = "1.1", default-features = false, features = ["macros", "runtime-tokio-native-tls", "sqlx-postgres"] }`,
and `sea-orm-migration = { version = "1.1", default-features = false, features = ["runtime-tokio-native-tls", "sqlx-postgres"] }`.

```rust,no_run
use coherent_cache::{schema, CacheCoherence};
use sea_orm::{entity::prelude::*, ActiveValue::Set, Database};
use sea_orm_migration::SchemaManager;
use std::sync::Arc;
use tokio::sync::watch;

mod users {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "cache_example_users")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: i32,
        pub name: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
    impl ActiveModelBehavior for ActiveModel {}
}

coherent_cache::cacheable!(users::Entity, pub enum UserKey {
    Id(i32) => ("id", "By ID"),
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Database::connect(std::env::var("DATABASE_URL")?).await?);
    // Demo setup only. In production, install frozen definitions in a migration.
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS cache_example_users (id integer PRIMARY KEY, name text NOT NULL)",
    )
    .await?;
    schema::track_keyed::<users::Entity>(&SchemaManager::new(db.as_ref())).await?;

    let cache = CacheCoherence::builder(db.clone())
        .register::<users::Entity>()
        .build();
    cache.validate_schema().await?;
    let (stop, shutdown) = watch::channel(false);
    let listener_cache = cache.clone();
    let listener = tokio::spawn(async move { listener_cache.run_listener(shutdown).await });

    // Reads safely use the loader until the listener has established its health lease.
    let users = cache.entity::<users::Entity>();
    let before: Option<users::Model> = users
        .get_or_load(UserKey::Id(1), || async {
            Ok(users::Entity::find_by_id(1).one(db.as_ref()).await?)
        })
        .await?;
    println!("Before: {before:?}");

    let write = users.begin_write().await?;
    users::Entity::insert(users::ActiveModel {
        id: Set(1),
        name: Set("Ada".to_owned()),
    })
    .on_conflict(
        sea_orm::sea_query::OnConflict::column(users::Column::Id)
            .update_column(users::Column::Name)
            .to_owned(),
    )
    .exec(write.connection())
    .await?;
    write.commit().await?;

    let after: Option<users::Model> = users
        .get_or_load(UserKey::Id(1), || async {
            Ok(users::Entity::find_by_id(1).one(db.as_ref()).await?)
        })
        .await?;
    assert_eq!(after.as_ref().map(|user| user.name.as_str()), Some("Ada"));
    println!("After commit: {after:?}");

    stop.send(true)?;
    listener.await?;
    Ok(())
}
```

Keep the shutdown sender alive for the lifetime of the listener. Without a running,
healthy listener, reads use the database loader and do not populate or serve the
cache. `validate_schema()` checks trigger installation; it does not start the listener.
Use the diagnostics snapshot to observe listener health.

## Cache configuration and keys

One `CacheCoherence` instance owns the shared store. Defaults are 10,000 entries,
a one-hour absolute TTL, and at most four concurrent loaders. Configure these with
`CacheCoherenceBuilder::options(CacheOptions { ttl, max_entries })` and
`max_concurrent_loads(...)`. Clone the cache handle to share it within a process.

Each lookup index must use one consistent result type and loader contract. A key must fully identify
its result: include the workspace key for workspace collections. Keep authorization and filtering
in the repository SQL. Single-column UUID/string/integer keys must serialize exactly as PostgreSQL's
JSON representation of that column; arbitrary expressions/composite keys are not supported yet.

## Writes and recovery

Generated row triggers derive affected keys from both row images, covering insert, update, delete,
negative results, aliases, and workspace collections. Unchanged rows produce no keyed notification.
`TRUNCATE` invalidates the entire entity. Changes are deduplicated in a transaction-local setting,
not a durable table; rollback/savepoints discard their changes. The commit wrapper reads that setting
before commit and invalidates locally before returning, even if the caller cancels or commit fails
ambiguously. Do not commit manually or overwrite the library's settings through `connection()`.

A dedicated listener consumes only the keyed protocol. It ignores its own process's notifications,
which the commit wrapper handles. A private notification heartbeat runs every five seconds with a
five-second deadline. Cached reads require a health lease younger than 15 seconds. Failures bypass
caching; resubscription clears all entries before enabling hits. Idle lease renewal on the same
healthy connection preserves unaffected entries. In-flight fills cannot return stale pre-invalidation
data. Missed invalidations that escape detection are bounded by TTL; this is eventual consistency,
not a substitute for authoritative database reads in cross-node concurrency decisions.

The listener uses a private one-connection pool and session-local statement, idle-in-transaction and
TCP keepalive timeouts. These settings cannot leak into normal application transactions.

## Migration and rollout

Install `schema::track_keyed::<Entity>` in a migration, or use `track_keyed_table` with frozen table
and column definitions so historical migrations do not follow future entity changes. Install the
migration before deploying the new app. Startup/reconnect validation rejects missing, disabled or
mismatched triggers. Ordinary reads never poll generations.

New installations only need the keyed helpers; `bootstrap`, `track`, and write-observation
helpers are legacy compatibility APIs. Existing generation/notification infrastructure
can remain for older revisions during rollout. V2 does
not consume those notifications or reconcile its counters. It adds no tables, log retention or
pruning. Remove legacy infrastructure only in a separate migration after all older revisions retire;
rolling back the keyed migration preserves legacy behavior but requires rolling back the app too.

The transaction-local key set grows with distinct keys touched by a transaction. Keep bulk writes
bounded; it is not a general-purpose CDC stream. Notification payloads contain key identifiers, not
cached values. Diagnostics expose no keys or row data, and report key invalidations rather than
counts of business writes. Remote timing includes remaining transaction time and assumes aligned clocks.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for CI-equivalent checks and disposable
PostgreSQL setup, and [RELEASING.md](RELEASING.md) for publishing instructions.


`cargo test -p coherent-cache` runs unit tests. PostgreSQL tests need a disposable database:

```sh
CACHE_TEST_DATABASE_URL=postgres://... cargo test -p coherent-cache --lib -- --ignored --test-threads=1
```

## Limitations

- This is eventual consistency. Use authoritative database reads for decisions
  requiring cross-process serialization or transaction isolation.
- Cached results must depend only on the declared table and column key. Changes to
  joined tables or time-dependent values do not automatically invalidate an entry.
- Each index has one shared result type and loader contract. Do not mix permission-
  or tenant-dependent results under the same key. Composite keys are not supported.
- Key identifiers travel in PostgreSQL notifications. Do not use secret values as keys.
- PostgreSQL limits notification payload size; long keys or large transactions can
  exceed database limits. Keep keys short and bulk writes bounded.
- The listener needs a persistent PostgreSQL session that supports `LISTEN`/`NOTIFY`.
  Use a direct connection or session pooling, not transaction pooling.

## License

Licensed under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
