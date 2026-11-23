//! Shared identity for cache registrations, migration triggers, and notification routing.
//! Explicitly defaults to `public`; it does not depend on a connection's mutable search_path.
use sea_orm::EntityName;

pub const NOTIFY_CHANNEL: &str = "cache_invalidations";
pub const WRITE_OBSERVATIONS_CHANNEL: &str = "cache_write_observations_v1";
pub const TRIGGER_NAME: &str = "cache_invalidate_v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableIdentity {
    schema: String,
    table: String,
}

impl TableIdentity {
    pub fn of<E: EntityName>() -> Self {
        let entity = E::default();
        Self::new(
            entity.schema_name().unwrap_or("public"),
            entity.table_name(),
        )
    }

    /// Also supports migration-local frozen identities when an entity is renamed later.
    /// Reject identifiers Postgres would truncate (63 UTF-8 bytes), avoiding identity collisions.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        let identity = Self {
            schema: schema.into(),
            table: table.into(),
        };
        for name in [&identity.schema, &identity.table] {
            assert!(
                !name.is_empty() && !name.contains('\0') && name.len() <= 63,
                "invalid cache table identifier"
            );
        }
        identity
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Quoted qualified SQL is also an unambiguous, stable wire key. Unlike Rust type names,
    /// it survives module refactors; unlike table OIDs, it survives restoring a database.
    pub fn key(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table)
        )
    }
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_identity_from_existing_entity() {
        assert_eq!(
            TableIdentity::of::<crate::test_support::Record>().key(),
            "\"public\".\"cache_test_records\""
        );
    }

    #[test]
    fn identity_quotes_names_and_separates_schemas() {
        assert_eq!(
            TableIdentity::new("odd\"schema", "user's table").key(),
            "\"odd\"\"schema\".\"user's table\""
        );
        assert_ne!(
            TableIdentity::new("a.b", "c").key(),
            TableIdentity::new("a", "b.c").key()
        );
        assert_ne!(
            TableIdentity::new("a", "users").key(),
            TableIdentity::new("b", "users").key()
        );
    }
}
