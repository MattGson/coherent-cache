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
