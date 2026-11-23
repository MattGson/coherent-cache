//! Reusable Postgres cache migration helpers. Call inside a normal SeaORM migration.
//!
//! `track::<Entity>` needs no trigger SQL or scope strings. If a table is renamed later,
//! preserve its historical identity using `track_table` / `untrack_table` in old migrations
//! (or a migration-local EntityName implementation). Never let a historical migration silently
//! follow a changed current entity mapping. Register after creating the source table; untrack
//! before dropping it. Keep registrations while older deployed revisions still depend on them.
use crate::identity::{quote_identifier, TableIdentity, NOTIFY_CHANNEL, TRIGGER_NAME};
use sea_orm_migration::{
    prelude::*,
    sea_orm::{DbBackend, EntityName, Statement},
};

pub async fn bootstrap(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(&format!(
            r#"
        CREATE TABLE public.cache_generations (
            scope text PRIMARY KEY,
            generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0)
        );
        CREATE FUNCTION public.notify_cache_invalidation_v1() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE public.cache_generations SET generation = generation + 1
                WHERE scope = TG_ARGV[0];
            IF NOT FOUND THEN
                RAISE EXCEPTION 'Missing cache generation for %', TG_ARGV[0];
            END IF;
            PERFORM pg_notify({}, TG_ARGV[0]);
            RETURN NULL;
        END;
        $$;
    "#,
            sql_literal(NOTIFY_CHANNEL)
        ))
        .await?;
    Ok(())
}

/// Add optional write telemetry without changing the legacy invalidation payload/channel.
/// Install in a new migration; old and new application revisions can overlap safely.
/// One extra notification per statement, delivered only on commit. No row data is included.
pub async fn enable_write_observations(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    replace_notification_function(manager, true).await
}

pub async fn disable_write_observations(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    replace_notification_function(manager, false).await
}

async fn replace_notification_function(
    manager: &SchemaManager<'_>,
    enabled: bool,
) -> Result<(), DbErr> {
    let observation = if enabled {
        format!(
            r#"
            PERFORM pg_notify({}, json_build_object(
                'table', TG_ARGV[0],
                'origin', nullif(current_setting('coherent_cache.instance_id', true), ''),
                'generation', current_generation,
                'emittedAtMs', floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
            )::text);"#,
            sql_literal(crate::identity::WRITE_OBSERVATIONS_CHANNEL)
        )
    } else {
        String::new()
    };
    manager
        .get_connection()
        .execute_unprepared(&format!(
            r#"
        CREATE OR REPLACE FUNCTION public.notify_cache_invalidation_v1() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE current_generation bigint;
        BEGIN
            UPDATE public.cache_generations SET generation = generation + 1
                WHERE scope = TG_ARGV[0] RETURNING generation INTO current_generation;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'Missing cache generation for %', TG_ARGV[0];
            END IF;
            PERFORM pg_notify({}, TG_ARGV[0]);
            {observation}
            RETURN NULL;
        END;
        $$;
    "#,
            sql_literal(NOTIFY_CHANNEL)
        ))
        .await?;
    Ok(())
}

pub async fn teardown(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    // No CASCADE: forgotten registrations should fail rollback instead of silently losing triggers.
    manager.get_connection().execute_unprepared(
        "DROP FUNCTION public.notify_cache_invalidation_v1(); DROP TABLE public.cache_generations;"
    ).await?;
    Ok(())
}

pub async fn track<E: EntityName>(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    track_table(manager, &TableIdentity::of::<E>()).await
}

pub async fn untrack<E: EntityName>(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    untrack_table(manager, &TableIdentity::of::<E>()).await
}

pub async fn track_table(manager: &SchemaManager<'_>, table: &TableIdentity) -> Result<(), DbErr> {
    let key = table.key();
    manager.get_connection().execute(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO public.cache_generations (scope) VALUES ($1) ON CONFLICT (scope) DO NOTHING",
        [key.clone().into()],
    )).await?;
    // Trigger names are per table. A constant avoids Postgres's identifier truncation limit,
    // including when the source schema/table names themselves are near that limit.
    let trigger = quote_identifier(TRIGGER_NAME);
    manager
        .get_connection()
        .execute_unprepared(&format!(
            "DROP TRIGGER IF EXISTS {trigger} ON {key}; \
         CREATE TRIGGER {trigger} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {key} \
         FOR EACH STATEMENT EXECUTE FUNCTION public.notify_cache_invalidation_v1({});",
            sql_literal(&key),
        ))
        .await?;
    Ok(())
}

pub async fn untrack_table(
    manager: &SchemaManager<'_>,
    table: &TableIdentity,
) -> Result<(), DbErr> {
    let key = table.key();
    manager
        .get_connection()
        .execute_unprepared(&format!(
            "DROP TRIGGER IF EXISTS {} ON {key}",
            quote_identifier(TRIGGER_NAME)
        ))
        .await?;
    manager
        .get_connection()
        .execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM public.cache_generations WHERE scope = $1",
            [key.into()],
        ))
        .await?;
    Ok(())
}

fn sql_literal(value: &str) -> String {
    // DDL trigger arguments cannot use bind parameters. Explicit E strings escape backslashes
    // and quotes independently of standard_conforming_strings or unusual identifier contents.
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// Install targeted triggers alongside legacy statement triggers for a rolling upgrade.
/// No new tables: local invalidations are accumulated in a transaction-local PostgreSQL setting.
pub async fn track_keyed<E: crate::Cacheable>(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    track_keyed_table(manager, &TableIdentity::of::<E>(), &E::cache_indexes()).await
}

/// Use frozen table/column identities in historical migrations. Labels are application-only.
pub async fn track_keyed_table(
    manager: &SchemaManager<'_>,
    table: &TableIdentity,
    indexes: &[crate::KeyIndex],
) -> Result<(), DbErr> {
    let columns: Vec<_> = indexes.iter().map(|i| i.column.as_str()).collect();
    if columns.is_empty() {
        return Err(DbErr::Custom("cache keys cannot be empty".into()));
    }
    let definition = serde_json::to_string(&columns).map_err(|e| DbErr::Custom(e.to_string()))?;
    // Resolve every declared column now, rather than producing a silently ineffective trigger.
    for column in &columns {
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "SELECT {} FROM {} LIMIT 0",
                quote_identifier(column),
                table.key()
            ))
            .await?;
    }
    manager.get_connection().execute_unprepared(r#"
        CREATE OR REPLACE FUNCTION public.begin_cache_write_v2(scopes jsonb, writer_instance text)
        RETURNS void LANGUAGE plpgsql AS $$
        DECLARE scope_name text;
        BEGIN
            -- Compatibility only: old revisions lock these rows before writing. The keyed
            -- protocol also works in a fresh database with no generation table at all.
            IF to_regclass('public.cache_generations') IS NOT NULL THEN
                FOR scope_name IN SELECT jsonb_array_elements_text(scopes) ORDER BY 1 LOOP
                    EXECUTE 'SELECT scope FROM public.cache_generations WHERE scope=$1 FOR UPDATE' USING scope_name;
                END LOOP;
            END IF;
            PERFORM set_config('coherent_cache.instance_id',writer_instance,true);
            PERFORM set_config('coherent_cache.keys_v2','{}',true);
        END;
        $$;
        CREATE OR REPLACE FUNCTION public.notify_cache_keys_v2() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            old_row jsonb; new_row jsonb; item jsonb; col text; key_value jsonb;
            pending jsonb; identity text; payload jsonb; origin text; timestamp_ms bigint;
        BEGIN
            -- Composite equality fails for tables containing json (which has no = operator).
            IF TG_OP = 'UPDATE' AND to_jsonb(OLD) IS NOT DISTINCT FROM to_jsonb(NEW) THEN RETURN NULL; END IF;
            pending := coalesce(nullif(current_setting('coherent_cache.keys_v2',true),''),'{}')::jsonb;
            origin := nullif(current_setting('coherent_cache.instance_id',true),'');
            timestamp_ms := floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint;
            IF TG_OP = 'TRUNCATE' THEN
                identity := jsonb_build_array(TG_ARGV[0], null, null)::text;
                payload := jsonb_build_object('table',TG_ARGV[0],'index',null,'key',null,'origin',origin,'emittedAtMs',timestamp_ms);
                IF NOT pending ? identity THEN
                    PERFORM pg_notify('cache_keys_v2',payload::text);
                    pending := pending || jsonb_build_object(identity,payload);
                END IF;
            ELSE
                IF TG_OP <> 'INSERT' THEN old_row := to_jsonb(OLD); END IF;
                IF TG_OP <> 'DELETE' THEN new_row := to_jsonb(NEW); END IF;
                FOR item IN SELECT value FROM jsonb_array_elements(jsonb_build_array(old_row,new_row)) LOOP
                    IF item = 'null'::jsonb THEN CONTINUE; END IF;
                    FOR col IN SELECT jsonb_array_elements_text(TG_ARGV[1]::jsonb) LOOP
                        key_value := item -> col;
                        identity := jsonb_build_array(TG_ARGV[0],col,key_value)::text;
                        IF pending ? identity THEN CONTINUE; END IF;
                        payload := jsonb_build_object('table',TG_ARGV[0],'index',col,'key',key_value,'origin',origin,'emittedAtMs',timestamp_ms);
                        PERFORM pg_notify('cache_keys_v2',payload::text);
                        pending := pending || jsonb_build_object(identity,payload);
                    END LOOP;
                END LOOP;
            END IF;
            PERFORM set_config('coherent_cache.keys_v2',pending::text,true);
            RETURN NULL;
        END;
        $$;
    "#).await?;
    manager
        .get_connection()
        .execute_unprepared(&format!(
            r#"
        DROP TRIGGER IF EXISTS cache_keys_v2_row ON {table};
        DROP TRIGGER IF EXISTS cache_keys_v2_truncate ON {table};
        CREATE TRIGGER cache_keys_v2_row AFTER INSERT OR UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION public.notify_cache_keys_v2({scope},{definition});
        CREATE TRIGGER cache_keys_v2_truncate AFTER TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION public.notify_cache_keys_v2({scope},{definition});
    "#,
            table = table.key(),
            scope = sql_literal(&table.key()),
            definition = sql_literal(&definition)
        ))
        .await?;
    Ok(())
}

pub async fn untrack_keyed_table(
    manager: &SchemaManager<'_>,
    table: &TableIdentity,
) -> Result<(), DbErr> {
    manager.get_connection().execute_unprepared(&format!(
        "DROP TRIGGER IF EXISTS cache_keys_v2_row ON {0}; DROP TRIGGER IF EXISTS cache_keys_v2_truncate ON {0};", table.key())).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm_migration::sea_orm::{DatabaseConnection, TransactionTrait};

    mod unusual {
        use sea_orm_migration::sea_orm;
        use sea_orm_migration::sea_orm::entity::prelude::*;
        #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
        #[sea_orm(
            table_name = "a 'quoted\\ table\"",
            schema_name = "cache test'\\\"schema"
        )]
        pub struct Model {
            #[sea_orm(primary_key)]
            pub id: i32,
            pub name: String,
        }
        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}
        impl ActiveModelBehavior for ActiveModel {}
    }

    async fn generation(db: &DatabaseConnection, table: &TableIdentity) -> i64 {
        db.query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT generation FROM public.cache_generations WHERE scope = $1",
            [table.key().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "generation")
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires a disposable CACHE_TEST_DATABASE_URL database; run database tests serially"]
    async fn cache_schema_postgres_helpers_and_migration_roundtrip() {
        let db = crate::test_support::database().await;
        let manager = SchemaManager::new(db.as_ref());
        let table = TableIdentity::of::<unusual::Entity>();
        let other = TableIdentity::new(table.schema(), "other table");
        let schema = quote_identifier(table.schema());
        db.execute_unprepared(&format!("CREATE SCHEMA {schema}; CREATE TABLE {} (id int PRIMARY KEY, name text NOT NULL); CREATE TABLE {} (id int);", table.key(), other.key())).await.unwrap();
        track::<unusual::Entity>(&manager).await.unwrap();
        track_table(&manager, &other).await.unwrap();
        assert_eq!(generation(db.as_ref(), &table).await, 0);
        let mut expected = 0;
        for mutation in [
            format!("INSERT INTO {} VALUES (1,'one'), (2,'two')", table.key()),
            format!("UPDATE {} SET name = 'updated'", table.key()),
            format!("DELETE FROM {} WHERE id = 1", table.key()),
            format!("TRUNCATE {}", table.key()),
        ] {
            db.execute_unprepared(&mutation).await.unwrap();
            expected += 1;
            assert_eq!(generation(db.as_ref(), &table).await, expected);
            assert_eq!(generation(db.as_ref(), &other).await, 0);
        }
        // Reattaching a trigger must never reset the durable version.
        track::<unusual::Entity>(&manager).await.unwrap();
        assert_eq!(generation(db.as_ref(), &table).await, expected);
        let rollback = db.begin().await.unwrap();
        rollback
            .execute_unprepared(&format!(
                "INSERT INTO {} VALUES (3,'rollback')",
                table.key()
            ))
            .await
            .unwrap();
        rollback.rollback().await.unwrap();
        assert_eq!(generation(db.as_ref(), &table).await, expected);
        untrack::<unusual::Entity>(&manager).await.unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO {} VALUES (4,'untracked')",
            table.key()
        ))
        .await
        .unwrap();
        let count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT count(*) AS n FROM public.cache_generations WHERE scope = $1",
                [table.key().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap();
        assert_eq!(count, 0);
        untrack_table(&manager, &other).await.unwrap();
        db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
        // Package-owned source tables survive tearing down and reinstalling the cache schema.
        untrack::<crate::test_support::Record>(&manager)
            .await
            .unwrap();
        untrack::<crate::test_support::OtherRecord>(&manager)
            .await
            .unwrap();
        teardown(&manager).await.unwrap();
        assert!(!manager.has_table("cache_generations").await.unwrap());
        assert!(manager.has_table("cache_test_records").await.unwrap());
        bootstrap(&manager).await.unwrap();
        track::<crate::test_support::Record>(&manager)
            .await
            .unwrap();
        track::<crate::test_support::OtherRecord>(&manager)
            .await
            .unwrap();
        assert_eq!(
            generation(
                db.as_ref(),
                &TableIdentity::of::<crate::test_support::Record>()
            )
            .await,
            0
        );
    }
}
