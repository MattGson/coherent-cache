//! Run against a disposable PostgreSQL database with DATABASE_URL set.
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
