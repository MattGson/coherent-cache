//! Run against a disposable PostgreSQL database with DATABASE_URL set.
#[path = "basic/migration.rs"]
mod migration;
#[path = "basic/repository.rs"]
mod repository;
#[path = "basic/users.rs"]
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
