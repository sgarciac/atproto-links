use crate::db_types::{DbKeyWithPrefix, DbStaticStr, StaticStr};
use crate::{Cursor, Nsid};

#[derive(Debug, PartialEq)]
pub struct _ByCollectionStaticStr {}
impl StaticStr for _ByCollectionStaticStr {
    fn static_str() -> &'static str {
        "by_collection"
    }
}
type ByCollectionPrefix = DbStaticStr<_ByCollectionStaticStr>;
pub type ByCollectionKey = DbKeyWithPrefix<ByCollectionPrefix, DbKeyWithPrefix<Nsid, Cursor>>;
impl ByCollectionKey {
    pub fn new(prefix: Nsid, suffix: Cursor) -> Self {
        Self {
            prefix: Default::default(),
            suffix: DbKeyWithPrefix { prefix, suffix },
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ByCollectionKey, Cursor, Nsid};
    use crate::db_types::DbBytes;

    #[test]
    fn test_by_collection_key() -> Result<(), Box<dyn std::error::Error>> {
        let original = ByCollectionKey::new(
            Nsid::new("ab.cd.efg".to_string()).unwrap(),
            Cursor::from_raw_u64(456),
        );
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ByCollectionKey::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());
        Ok(())
    }
}
