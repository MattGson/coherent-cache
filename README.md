# coherent-cache

A low-latency, eventually consistent in-memory cache for Rust applications using
SeaORM and PostgreSQL. Reduce database load and serve repeated reads directly from
application memory, with automatic invalidation across application instances.

No additional services to deploy. `coherent-cache` uses your existing PostgreSQL
database to coordinate changes, while each application instance keeps its own
local cache.

## Features

- **Fast local reads.** Cache hits return from memory without a database query or
  network round trip. On a miss, the cache runs your async loader and stores the
  result.

- **Automatic, targeted invalidation.** Committed database changes invalidate
  affected keys across application instances, preserving unrelated cached results.
  Database triggers track writes from your application, background jobs, and
  external SQL.

- **Request coalescing.** Concurrent misses for the same key within an instance
  share a single in-flight load while caching is enabled. This avoids duplicate
  queries when a popular entry expires or is invalidated.

- **Controlled database load.** A configurable concurrency limit bounds how many
  loaders can run at once, including when reads fall back to the database.

- **Automatic failure recovery.** The cache monitors its notification connection.
  When it detects a failure, reads fall back to the database. It reconnects
  automatically and clears old entries before resuming caching.

- **Reliable writes.** The transaction wrapper invalidates affected local entries
  before commit returns, even if the caller is cancelled during commit. Reads
  invalidated while loading retry rather than returning the invalidated result.

- **Typed keys and flexible results.** Cache individual records, collections, and
  "not found" results. You define the keys and loaders, keeping queries, filtering,
  and domain conversion in your application.

- **Configurable capacity and expiration.** Set an entry limit and TTL to control
  cache growth and how long values can remain cached.

- **Built-in diagnostics.** Inspect hits, misses, loader errors, invalidations,
  and listener health to understand cache behavior in production.

## Consistency

Each application instance caches independently. PostgreSQL notifications propagate
committed changes asynchronously, so another instance may briefly serve an older
value before receiving the invalidation. TTL provides an additional expiration
bound if a notification is missed.

Use direct database reads where stronger consistency is required. Automatic
invalidation covers the declared table and key dependencies; changes to joined
tables or time-dependent results need separate handling.

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

This example separates the entity, database migration, application startup, and
repository functions. The snippets below correspond to the files in the
[runnable example](examples/basic.rs).

For a consuming application, add these dependencies alongside `coherent-cache`:

```toml
anyhow = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
sea-orm = { version = "1.1", default-features = false, features = ["macros", "runtime-tokio-native-tls", "sqlx-postgres"] }
sea-orm-migration = { version = "1.1", default-features = false, features = ["runtime-tokio-native-tls", "sqlx-postgres"] }
```

### 1. Define the entity and cache keys

In [users.rs](examples/basic/users.rs), define a normal SeaORM entity and declare
which column identifies a cached lookup. Here, each user is cached by ID.

```rust,no_run
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "cache_example_users")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}

coherent_cache::cacheable!(Entity, pub enum UserKey {
    Id(i32) => ("id", "By ID"),
});
```

### 2. Create the table and install invalidation triggers

In [migration.rs](examples/basic/migration.rs), use a SeaORM migration to create
the table and register its cache keys. The table and column names are defined in
the migration so later entity changes do not alter its behavior.

If your application already has a migrator, add this migration to its existing
list instead of defining a second `Migrator`.

```rust,no_run
use coherent_cache::{schema, KeyIndex, TableIdentity};
use sea_orm_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(Migration)]
    }
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Users::Table)
                    .col(
                        ColumnDef::new(Users::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Users::Name).string().not_null())
                    .to_owned(),
            )
            .await?;

        schema::track_keyed_table(
            manager,
            &TableIdentity::new("public", "cache_example_users"),
            &[KeyIndex::new("id", "By ID")],
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        schema::untrack_keyed_table(
            manager,
            &TableIdentity::new("public", "cache_example_users"),
        )
        .await?;
        manager
            .drop_table(Table::drop().table(Users::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Users {
    #[sea_orm(iden = "cache_example_users")]
    Table,
    Id,
    Name,
}
```

### 3. Set up the application

Create one cache at startup, register the entity, and start its notification
listener. Share the database and cache handles with your application services.
This [main function](examples/basic.rs) calls the read and write functions defined
below, then shuts the listener down.

```rust,ignore
mod migration;
mod repository;
mod users;

use coherent_cache::CacheCoherence;
use repository::{create_user, get_user};
use sea_orm::Database;
use sea_orm_migration::MigratorTrait;
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Database::connect(std::env::var("DATABASE_URL")?).await?);
    migration::Migrator::up(db.as_ref(), None).await?;

    let cache = CacheCoherence::builder(db.clone())
        .register::<users::Entity>()
        .build();
    cache.validate_schema().await?;

    let (stop, shutdown) = watch::channel(false);
    let listener_cache = cache.clone();
    let listener = tokio::spawn(async move { listener_cache.run_listener(shutdown).await });

    let id = create_user(&cache, "Ada").await?;
    let user = get_user(db.as_ref(), &cache, id).await?;
    assert_eq!(user.as_ref().map(|user| user.name.as_str()), Some("Ada"));
    println!("Created user: {user:?}");

    stop.send(true)?;
    listener.await?;
    Ok(())
}
```

The example runs migrations at startup; an application can instead run them as a
deployment step. Reads use the database until the listener is ready. Keep the
shutdown sender alive while the application is running, and signal it when the
application shuts down.

### 4. Read through the cache

In [repository.rs](examples/basic/repository.rs), wrap the database query with
`get_or_load`. The loader runs on a cache miss or while caching is unavailable.
Both a found user and a missing result (`None`) can be cached.

```rust,ignore
use crate::users::{self, UserKey};
use coherent_cache::CacheCoherence;
use sea_orm::{ActiveValue::Set, DatabaseConnection, EntityTrait};

pub async fn get_user(
    db: &DatabaseConnection,
    cache: &CacheCoherence,
    id: i32,
) -> anyhow::Result<Option<users::Model>> {
    cache
        .entity::<users::Entity>()
        .get_or_load(UserKey::Id(id), || async {
            Ok(users::Entity::find_by_id(id).one(db).await?)
        })
        .await
}
```

### 5. Write in a cache-aware transaction

Add this function to the same repository module. Run the insert using the write
transaction's connection, then commit through the wrapper. It returns the new
user's database-generated ID after affected local entries have been invalidated.

```rust,ignore
pub async fn create_user(cache: &CacheCoherence, name: &str) -> anyhow::Result<i32> {
    let write = cache.entity::<users::Entity>().begin_write().await?;
    let inserted = users::Entity::insert(users::ActiveModel {
        name: Set(name.to_owned()),
        ..Default::default()
    })
    .exec(write.connection())
    .await?;

    write.commit().await?;
    Ok(inserted.last_insert_id)
}
```

To run the complete example from this repository against a disposable PostgreSQL
database:

```sh
DATABASE_URL=postgres://localhost/cache_example cargo run --locked --example basic
```

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
