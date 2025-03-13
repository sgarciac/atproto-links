use crate::db_types::{
    DbBytes, DbConcat, DbEmpty, DbStaticStr, EncodingError, StaticStr, UseBincodePlz,
};
use crate::{Cursor, Did, Nsid, RecordKey};
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct SeenCounter(pub u64);
impl SeenCounter {
    pub fn new(n: u64) -> Self {
        Self(n)
    }
}
impl UseBincodePlz for SeenCounter {}

#[derive(Debug, PartialEq)]
pub struct _ByCollectionStaticStr {}
impl StaticStr for _ByCollectionStaticStr {
    fn static_str() -> &'static str {
        "by_collection"
    }
}
type ByCollectionPrefix = DbStaticStr<_ByCollectionStaticStr>;
/// key format: ["by_collection"|collection|js_cursor]
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
}
impl From<ByCollectionKey> for (Nsid, Cursor) {
    fn from(k: ByCollectionKey) -> Self {
        (k.prefix.suffix, k.suffix)
    }
}

#[derive(Debug, PartialEq, Encode, Decode)]
pub struct ByCollectionValueInfo {
    #[bincode(with_serde)]
    pub did: Did,
    #[bincode(with_serde)]
    pub rkey: RecordKey,
}
impl UseBincodePlz for ByCollectionValueInfo {}
/// value format: contains did, rkey, record
pub type ByCollectionValue = DbConcat<ByCollectionValueInfo, serde_json::Value>;
impl ByCollectionValue {
    pub fn new(did: Did, rkey: RecordKey, record: serde_json::Value) -> Self {
        Self {
            prefix: ByCollectionValueInfo { did, rkey },
            suffix: record,
        }
    }
}
impl From<ByCollectionValue> for (Did, RecordKey, serde_json::Value) {
    fn from(v: ByCollectionValue) -> Self {
        (v.prefix.did, v.prefix.rkey, v.suffix)
    }
}

#[derive(Debug, PartialEq)]
pub struct _ByIdStaticStr {}
impl StaticStr for _ByIdStaticStr {
    fn static_str() -> &'static str {
        "by_id"
    }
}
type ByIdStaticPrefix = DbStaticStr<_ByIdStaticStr>;
pub type ByIdDidPrefix = DbConcat<ByIdStaticPrefix, Did>;
pub type ByIdCollectionPrefix = DbConcat<ByIdDidPrefix, Nsid>;
pub type ByIdRecordPrefix = DbConcat<ByIdCollectionPrefix, RecordKey>;
/// look up records by user or directly, instead of by collections
///
/// required to support deletes; did first prefix for account deletes.
/// key format: ["by_id"|did|collection|rkey|js_cursor]
pub type ByIdKey = DbConcat<ByIdRecordPrefix, Cursor>;
impl ByIdKey {
    pub fn new(did: Did, collection: Nsid, rkey: RecordKey, cursor: Cursor) -> Self {
        Self {
            prefix: ByIdRecordPrefix {
                prefix: ByIdCollectionPrefix {
                    prefix: ByIdDidPrefix::from_pair(Default::default(), did),
                    suffix: collection,
                },
                suffix: rkey,
            },
            suffix: cursor,
        }
    }
}
impl From<ByIdKey> for (Did, Nsid, RecordKey, Cursor) {
    fn from(k: ByIdKey) -> Self {
        (
            k.prefix.prefix.prefix.suffix,
            k.prefix.prefix.suffix,
            k.prefix.suffix,
            k.suffix,
        )
    }
}

pub type ByIdValue = DbEmpty;

#[derive(Debug, PartialEq)]
pub struct _ByCursorSeenStaticStr {}
impl StaticStr for _ByCursorSeenStaticStr {
    fn static_str() -> &'static str {
        "seen_by_js_cursor"
    }
}
type ByCursorSeenPrefix = DbStaticStr<_ByCursorSeenStaticStr>;
/// key format: ["seen_by_js_cursor"|js_cursor|collection]
pub type ByCursorSeenKey = DbConcat<DbConcat<ByCursorSeenPrefix, Cursor>, Nsid>;
impl ByCursorSeenKey {
    pub fn new(cursor: Cursor, nsid: Nsid) -> Self {
        Self {
            prefix: DbConcat::from_pair(Default::default(), cursor),
            suffix: nsid,
        }
    }
    pub fn prefix_from_cursor(cursor: Cursor) -> Result<Vec<u8>, EncodingError> {
        DbConcat::from_pair(ByCursorSeenPrefix::default(), cursor).to_db_bytes()
    }
}
impl From<ByCursorSeenKey> for (Cursor, Nsid) {
    fn from(k: ByCursorSeenKey) -> Self {
        (k.prefix.suffix, k.suffix)
    }
}

pub type ByCursorSeenValue = SeenCounter;

#[cfg(test)]
mod test {
    use super::{ByCollectionKey, ByCollectionValue, Cursor, Did, EncodingError, Nsid, RecordKey};
    use crate::db_types::DbBytes;

    #[test]
    fn test_by_collection_key() -> Result<(), EncodingError> {
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

    #[test]
    fn test_by_collection_value() -> Result<(), EncodingError> {
        let did = Did::new("did:plc:inze6wrmsm7pjl7yta3oig77".to_string()).unwrap();
        let rkey = RecordKey::new("asdfasdf".to_string()).unwrap();
        let record = serde_json::Value::String("hellooooo".into());

        let original = ByCollectionValue::new(did, rkey, record);
        let serialized = original.to_db_bytes()?;
        let (restored, bytes_consumed) = ByCollectionValue::from_db_bytes(&serialized)?;
        assert_eq!(restored, original);
        assert_eq!(bytes_consumed, serialized.len());

        Ok(())
    }
}
