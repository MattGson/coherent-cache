//! Declarative key dependencies. Loaders (including filtering and authorization) remain in the app.
use sea_orm::EntityTrait;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyIndex {
    pub column: String,
    pub label: String,
}
impl KeyIndex {
    pub fn new(column: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            label: label.into(),
        }
    }
}

pub trait CacheKey: Clone + Send + Sync + 'static {
    fn parts(&self) -> (&'static str, Value);
}
pub trait Cacheable: EntityTrait {
    type Key: CacheKey;
    fn cache_indexes() -> Vec<KeyIndex>;
}

/// Define stable database column dependencies and typed keys in one place. Column names are wire
/// identities, not caller-provided strings. Keep a frozen copy in migrations when changing them.
#[macro_export]
macro_rules! cacheable {
    ($entity:ty, $vis:vis enum $key:ident { $($variant:ident($ty:ty) => ($column:literal, $label:literal)),+ $(,)? }) => {
        // UUID/String keys differ by a few bytes; boxing would add an allocation per lookup.
        #[allow(clippy::large_enum_variant)]
        #[derive(Clone, Debug)]
        $vis enum $key { $($variant($ty)),+ }
        impl $crate::CacheKey for $key {
            fn parts(&self) -> (&'static str, $crate::__serde_json::Value) {
                match self { $(Self::$variant(value) => ($column, $crate::__serde_json::to_value(value).expect("serializable cache key"))),+ }
            }
        }
        impl $crate::Cacheable for $entity {
            type Key = $key;
            fn cache_indexes() -> Vec<$crate::KeyIndex> {
                vec![$($crate::KeyIndex::new($column, $label)),+]
            }
        }
    };
}
