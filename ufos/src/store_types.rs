use cardinality_estimator::CardinalityEstimator;
use crate::db_types::{
    DbBytes, DbConcat, DbEmpty, DbStaticStr, EncodingError, StaticStr, UseBincodePlz, SerdeBytes,
};
use crate::{Cursor, Did, Nsid, RecordKey, UFOsCommit, PutAction};
use bincode::{Decode, Encode};
use std::ops::Range;

/// key format: ["js_cursor"]
#[derive(Debug, PartialEq)]
pub struct JetstreamCursorKey {}
impl StaticStr for JetstreamCursorKey {
    fn static_str() -> &'static str {
        "js_cursor"
    }
}
pub type JetstreamCursorValue = Cursor;

/// key format: ["mod_cursor"]
#[derive(Debug, PartialEq)]
pub struct ModCursorKey {}
impl StaticStr for ModCursorKey {
    fn static_str() -> &'static str {
        "mod_cursor"
    }
}
pub type ModCursorValue = Cursor;

/// key format: ["rollup_cursor"]
#[derive(Debug, PartialEq)]
pub struct RollupCursorKey {}
impl StaticStr for RollupCursorKey {
    fn static_str() -> &'static str {
        "rollup_cursor"
    }
}
/// value format: [rollup_cursor(Cursor)|collection(Nsid)]
pub type RollupCursorValue = DbConcat<Cursor, Nsid>;

/// key format: ["js_endpoint"]
#[derive(Debug, PartialEq)]
pub struct JetstreamEndpointKey {}
impl StaticStr for JetstreamEndpointKey {
    fn static_str() -> &'static str {
        "js_endpoint"
    }
}
#[derive(Debug, PartialEq)]
pub struct JetstreamEndpointValue(pub String);
/// String wrapper for jetstream endpoint value
///
/// Warning: this is a non-terminating byte representation of a string: it cannot be used in prefix position of DbConcat
impl DbBytes for JetstreamEndpointValue {
    // TODO: maybe make a helper type in db_types
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.0.as_bytes().to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let s = std::str::from_utf8(bytes)?.to_string();
        Ok((Self(s), bytes.len()))
    }
}

pub type NsidRecordFeedKey = DbConcat<Nsid, Cursor>;
pub type NsidRecordFeedVal = DbConcat<Did, DbConcat<RecordKey, String>>;
impl From<(&Did, &RecordKey, &str)> for NsidRecordFeedVal {
    fn from((did, rkey, rev): (&Did, &RecordKey, &str)) -> Self {
        Self::from_pair(
            did.clone(),
            DbConcat::from_pair(rkey.clone(), rev.to_string()))
    }
}

pub type RecordLocationKey = DbConcat<Did, DbConcat<Nsid, RecordKey>>;
impl From<(&UFOsCommit, &Nsid)> for RecordLocationKey {
    fn from((commit, collection): (&UFOsCommit, &Nsid)) -> Self {
        Self::from_pair(commit.did.clone(), DbConcat::from_pair(collection.clone(), commit.rkey.clone()))
    }
}
#[derive(Debug, PartialEq, Encode, Decode)]
pub struct RecordLocationMeta {
    pub cursor: u64, // ugh no bincode impl
    pub is_update: bool,
    pub rev: String,
}
impl UseBincodePlz for RecordLocationMeta {}

impl DbBytes for Vec<u8> {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Ok(self.to_vec())
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        Ok((bytes.to_owned(), bytes.len()))
    }
}

pub type RecordLocationVal = DbConcat<RecordLocationMeta, Vec<u8>>;
impl From<(Cursor, &str, PutAction)> for RecordLocationVal {
    fn from((cursor, rev, put): (Cursor, &str, PutAction)) -> Self {
        let meta = RecordLocationMeta {
            cursor: cursor.to_raw_u64(),
            is_update: put.is_update,
            rev: rev.to_string(),
        };
        Self::from_pair(meta, put.record.get().into())
    }
}

#[derive(Debug, PartialEq)]
pub struct _LiveRecordsStaticStr {}
impl StaticStr for _LiveRecordsStaticStr {
    fn static_str() -> &'static str {
        "live_records"
    }
}
type LiveRecordsStaticPrefix = DbStaticStr<_LiveRecordsStaticStr>;
pub type LiveRecordsKey = DbConcat<LiveRecordsStaticPrefix, DbConcat<Cursor, Nsid>>;
impl From<(Cursor, &Nsid)> for LiveRecordsKey {
    fn from((cursor, collection): (Cursor, &Nsid)) -> Self {
        Self::from_pair(
            Default::default(),
            DbConcat::from_pair(cursor, collection.clone()),
        )
    }
}
#[derive(Debug, PartialEq, Decode, Encode)]
pub struct LiveRecordsValue(pub u64);
impl UseBincodePlz for LiveRecordsValue {}

#[derive(Debug, PartialEq)]
pub struct _LiveDidsStaticStr {}
impl StaticStr for _LiveDidsStaticStr {
    fn static_str() -> &'static str {
        "live_dids"
    }
}
pub type LiveDidsStaticPrefix = DbStaticStr<_LiveDidsStaticStr>;
pub type LiveDidsKey = DbConcat<LiveDidsStaticPrefix, DbConcat<Cursor, Nsid>>;
impl From<(Cursor, &Nsid)> for LiveDidsKey {
    fn from((cursor, collection): (Cursor, &Nsid)) -> Self {
        Self::from_pair(
            Default::default(),
            DbConcat::from_pair(cursor, collection.clone()),
        )
    }
}
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveDidsValue(pub CardinalityEstimator::<Did>);
impl SerdeBytes for LiveDidsValue {}
impl DbBytes for LiveDidsValue {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        SerdeBytes::to_bytes(self)
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        SerdeBytes::from_bytes(bytes)
    }
}

#[derive(Debug, PartialEq)]
pub struct _DeleteAccountStaticStr {}
impl StaticStr for _DeleteAccountStaticStr {
    fn static_str() -> &'static str {
        "delete_acount"
    }
}
pub type DeleteAccountStaticPrefix = DbStaticStr<_DeleteAccountStaticStr>;
pub type DeleteAccountQueueKey = DbConcat<DeleteAccountStaticPrefix, Cursor>;
impl DeleteAccountQueueKey {
    pub fn new(cursor: Cursor) -> Self {
        Self::from_pair(Default::default(), cursor)
    }
}
pub type DeleteAccountQueueVal = Did;


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
    pub fn new(collection: Nsid, cursor: Cursor) -> Self {
        Self {
            prefix: DbConcat::from_pair(Default::default(), collection),
            suffix: cursor,
        }
    }
    pub fn prefix_from_collection(collection: Nsid) -> Result<Vec<u8>, EncodingError> {
        DbConcat::from_pair(ByCollectionPrefix::default(), collection).to_db_bytes()
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
        Self::from_pair(Self::record_prefix(did, collection, rkey), cursor)
    }
    pub fn record_prefix(did: Did, collection: Nsid, rkey: RecordKey) -> ByIdRecordPrefix {
        ByIdRecordPrefix {
            prefix: ByIdCollectionPrefix {
                prefix: Self::did_prefix(did),
                suffix: collection,
            },
            suffix: rkey,
        }
    }
    pub fn did_prefix(did: Did) -> ByIdDidPrefix {
        ByIdDidPrefix::from_pair(Default::default(), did)
    }
    pub fn cursor(&self) -> Cursor {
        self.suffix.clone()
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
type ByCursorSeenCursorPrefix = DbConcat<ByCursorSeenPrefix, Cursor>;
/// key format: ["seen_by_js_cursor"|js_cursor|collection]
pub type ByCursorSeenKey = DbConcat<ByCursorSeenCursorPrefix, Nsid>;
impl ByCursorSeenKey {
    pub fn new(cursor: Cursor, nsid: Nsid) -> Self {
        Self {
            prefix: DbConcat::from_pair(Default::default(), cursor),
            suffix: nsid,
        }
    }
    pub fn full_range() -> Result<Range<Vec<u8>>, EncodingError> {
        let prefix = ByCursorSeenCursorPrefix::from_pair(Default::default(), Cursor::from_start());
        prefix.range()
    }
    pub fn range_from(&self) -> Result<Range<Vec<u8>>, EncodingError> {
        let start = self.to_db_bytes()?;
        let end = self.prefix.range_end()?;
        Ok(start..end)
    }
    pub fn collection(&self) -> &Nsid {
        &self.suffix
    }
}
impl From<RollupCursorValue> for ByCursorSeenKey {
    fn from(v: RollupCursorValue) -> Self {
        Self::new(v.prefix, v.suffix)
    }
}
impl From<ByCursorSeenKey> for (Cursor, Nsid) {
    fn from(k: ByCursorSeenKey) -> Self {
        (k.prefix.suffix, k.suffix)
    }
}

pub type ByCursorSeenValue = SeenCounter;

#[derive(Debug, PartialEq)]
pub struct _ModQueueItemStaticStr {}
impl StaticStr for _ModQueueItemStaticStr {
    fn static_str() -> &'static str {
        "mod_queue"
    }
}
pub type ModQueueItemPrefix = DbStaticStr<_ModQueueItemStaticStr>;
/// key format: ["mod_queue"|js_cursor]
pub type ModQueueItemKey = DbConcat<ModQueueItemPrefix, Cursor>;
impl ModQueueItemKey {
    pub fn new(cursor: Cursor) -> Self {
        Self::from_pair(Default::default(), cursor)
    }
}
// todo: remove this? all we need is the ModCursorValue version?
impl From<ModQueueItemKey> for Cursor {
    fn from(k: ModQueueItemKey) -> Self {
        k.suffix
    }
}
impl From<&ModQueueItemKey> for ModCursorValue {
    fn from(k: &ModQueueItemKey) -> Self {
        k.suffix.clone()
    }
}

#[derive(Debug, Encode, Decode)]
pub enum ModQueueItemStringValue {
    DeleteAccount(String),                        // did
    DeleteRecord(String, String, String),         // did, collection, rkey
    UpdateRecord(String, String, String, String), // did, collection, rkey, json record
}
impl UseBincodePlz for ModQueueItemStringValue {}
#[derive(Debug, Clone, PartialEq)]
pub enum ModQueueItemValue {
    DeleteAccount(Did),
    DeleteRecord(Did, Nsid, RecordKey),
    UpdateRecord(Did, Nsid, RecordKey, serde_json::Value),
}
impl From<ModQueueItemValue> for ModQueueItemStringValue {
    fn from(v: ModQueueItemValue) -> Self {
        match v {
            ModQueueItemValue::DeleteAccount(did) => {
                ModQueueItemStringValue::DeleteAccount(did.to_string())
            }
            ModQueueItemValue::DeleteRecord(did, collection, rkey) => {
                ModQueueItemStringValue::DeleteRecord(
                    did.to_string(),
                    collection.to_string(),
                    rkey.to_string(),
                )
            }
            ModQueueItemValue::UpdateRecord(did, collection, rkey, record) => {
                ModQueueItemStringValue::UpdateRecord(
                    did.to_string(),
                    collection.to_string(),
                    rkey.to_string(),
                    record.to_string(),
                )
            }
        }
    }
}
impl TryFrom<ModQueueItemStringValue> for ModQueueItemValue {
    type Error = EncodingError;
    fn try_from(v: ModQueueItemStringValue) -> Result<Self, Self::Error> {
        match v {
            ModQueueItemStringValue::DeleteAccount(did) => Ok(ModQueueItemValue::DeleteAccount(
                Did::new(did).map_err(EncodingError::BadAtriumStringType)?,
            )),
            ModQueueItemStringValue::DeleteRecord(did, collection, rkey) => {
                Ok(ModQueueItemValue::DeleteRecord(
                    Did::new(did).map_err(EncodingError::BadAtriumStringType)?,
                    Nsid::new(collection).map_err(EncodingError::BadAtriumStringType)?,
                    RecordKey::new(rkey).map_err(EncodingError::BadAtriumStringType)?,
                ))
            }
            ModQueueItemStringValue::UpdateRecord(did, collection, rkey, record) => {
                Ok(ModQueueItemValue::UpdateRecord(
                    Did::new(did).map_err(EncodingError::BadAtriumStringType)?,
                    Nsid::new(collection).map_err(EncodingError::BadAtriumStringType)?,
                    RecordKey::new(rkey).map_err(EncodingError::BadAtriumStringType)?,
                    record.parse()?,
                ))
            }
        }
    }
}
impl DbBytes for ModQueueItemValue {
    fn to_db_bytes(&self) -> Result<Vec<u8>, EncodingError> {
        Into::<ModQueueItemStringValue>::into(self.clone()).to_db_bytes()
    }
    fn from_db_bytes(bytes: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (stringy, n) = ModQueueItemStringValue::from_db_bytes(bytes)?;
        let me = TryInto::<ModQueueItemValue>::try_into(stringy)?;
        Ok((me, n))
    }
}

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
        let just_prefix = ByCollectionKey::prefix_from_collection(nsid)?;
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
