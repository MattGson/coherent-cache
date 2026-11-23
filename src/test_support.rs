//! Package-owned fixtures: no dependency on the application's entities or migrations.
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};
use sea_orm_migration::SchemaManager;
use std::sync::Arc;

pub mod records {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "cache_test_records")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        pub name: String,
        pub workspace_id: Uuid,
    }
    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
    impl ActiveModelBehavior for ActiveModel {}
}

pub mod other_records {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "cache_test_other_records")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        pub name: String,
        pub workspace_id: Uuid,
    }
    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
    impl ActiveModelBehavior for ActiveModel {}
}

pub use other_records::Entity as OtherRecord;
pub use records::Entity as Record;

/// Only the explicit disposable test URL is used. Database tests run serially because they
/// exercise disabling/removing triggers and rolling the shared cache infrastructure back.
pub async fn database() -> Arc<DatabaseConnection> {
    let url = std::env::var("CACHE_TEST_DATABASE_URL").expect("disposable test database required");
    let db = Arc::new(Database::connect(url).await.unwrap());
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS cache_test_records (id uuid PRIMARY KEY, name text NOT NULL, workspace_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001');
         CREATE TABLE IF NOT EXISTS cache_test_other_records (id uuid PRIMARY KEY, name text NOT NULL, workspace_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001');",
    ).await.unwrap();
    let manager = SchemaManager::new(db.as_ref());
    if !manager.has_table("cache_generations").await.unwrap() {
        crate::schema::bootstrap(&manager).await.unwrap();
    }
    crate::schema::enable_write_observations(&manager)
        .await
        .unwrap();
    crate::schema::track::<Record>(&manager).await.unwrap();
    crate::schema::track::<OtherRecord>(&manager).await.unwrap();
    crate::schema::track_keyed::<Record>(&manager)
        .await
        .unwrap();
    crate::schema::track_keyed::<OtherRecord>(&manager)
        .await
        .unwrap();
    db
}

crate::cacheable!(Record, pub enum RecordKey {
    Id(uuid::Uuid) => ("id", "By ID"),
    Name(String) => ("name", "By name"),
    Workspace(uuid::Uuid) => ("workspace_id", "By workspace"),
});
crate::cacheable!(OtherRecord, pub enum OtherKey {
    Id(uuid::Uuid) => ("id", "By ID"),
});
