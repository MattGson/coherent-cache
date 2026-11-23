# coherent-cache

Typed, process-local read-through caching for SeaORM applications, with targeted
invalidation coordinated through PostgreSQL transactions and `LISTEN`/`NOTIFY`.

Each process owns a bounded in-memory store. PostgreSQL triggers notify other
processes when the row-column keys they depend on change. Loaders keep database
queries, filtering, and domain conversion in your application.

## Status and installation

Version `0.1.0` is prepared for its first publication; it is not yet on crates.io.
Until publication, use the public Git repository:

```toml
[dependencies]
coherent-cache = { git = "https://github.com/MattGson/coherent-cache", version = "0.1.0" }
```

Commit your application lockfile to pin the resolved Git revision. You can also
add a `rev` field to select an exact commit. After publication, use
`coherent-cache = "0.1.0"`. Requires Rust 1.88 or later,
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
    // Demo setup only. In production, create the table and install triggers in a migration.
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

    // Reads use the database until the listener is ready.
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

PostgreSQL tracks changes made by your application or external SQL. Committed
changes invalidate the affected cached results, including cached "not found"
results and collections. Rolled-back writes do not affect other processes' caches.

Wrap application writes in `begin_write()` and finish with `CacheWrite::commit()`.
Affected local entries are invalidated before commit returns. If the caller is
cancelled while commit is in progress, invalidation still completes.

The cache continuously checks that notifications are working. When it detects a
connection problem, reads fall back to the database. After reconnecting, it clears
old entries before serving cached results again. If a result is invalidated while
loading, the read retries instead of returning it.

Updates reach other processes asynchronously. Every cached value also expires
after its configured TTL, limiting staleness if a notification is missed.

## Database setup

Install triggers for each cached table in a database migration using
`schema::track_keyed::<Entity>`. To keep migrations independent of future entity
changes, use `schema::track_keyed_table` with explicit table and column definitions.

Apply the migration before starting the application. `validate_schema()` checks
that the required triggers are installed, enabled, and match your cache keys.

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
