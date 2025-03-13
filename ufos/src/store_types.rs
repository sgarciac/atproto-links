use crate::db_types::{DbBytes, DbConcat, DbStaticStr, EncodingError, StaticStr};
use crate::{Cursor, Nsid};

#[derive(Debug, PartialEq)]
pub struct _ByCollectionStaticStr {}
impl StaticStr for _ByCollectionStaticStr {
    fn static_str() -> &'static str {
        "by_collection"
    }
}
type ByCollectionPrefix = DbStaticStr<_ByCollectionStaticStr>;
pub type ByCollectionKey = DbConcat<DbConcat<ByCollectionPrefix, Nsid>, Cursor>;
impl ByCollectionKey {
    pub fn new(nsid: Nsid, cursor: Cursor) -> Self {
        Self {
            prefix: DbConcat::from_pair(Default::default(), nsid),
            suffix: cursor,
        }
    }
    pub fn prefix_from_nsid(nsid: Nsid) -> Result<Vec<u8>, EncodingError> {
        DbConcat::from_pair(ByCollectionPrefix::default(), nsid).to_db_bytes()
    }
    pub fn nsid(&self) -> Nsid {
        self.prefix.suffix.clone()
    }
    pub fn cursor(&self) -> Cursor {
        self.suffix.clone()
    }
}

#[cfg(test)]
mod test {
    use super::{ByCollectionKey, Cursor, Nsid};
    use crate::db_types::DbBytes;

    #[test]
    fn test_by_collection_key() -> Result<(), Box<dyn std::error::Error>> {
        let nsid = Nsid::new("ab.cd.efg".to_string()).unwrap();
        let original = ByCollectionKey::new(nsid.clone(), Cursor::from_raw_u64(456));
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ByCollectionKey::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        let serialized_prefix = original.to_prefix_db_bytes()?;
        assert!(serialized.starts_with(&serialized_prefix));
        let just_prefix = ByCollectionKey::prefix_from_nsid(nsid)?;
        assert_eq!(just_prefix, serialized_prefix);
        assert!(just_prefix.starts_with("by_collection".as_bytes()));

        Ok(())
    }
}
