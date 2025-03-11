pub mod consumer;
pub mod store;

use jetstream::events::Cursor;
use jetstream::exports::{Did, Nsid, RecordKey};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct SetRecord {
    pub new: bool,
    pub did: Did,
    pub rkey: RecordKey,
    pub record: serde_json::Value,
    pub cursor: Cursor,
}

#[derive(Debug, Default)]
pub struct CollectionSamples {
    pub total_seen: usize,
    pub samples: VecDeque<SetRecord>,
}

#[derive(Debug)]
pub struct DeleteRecord {
    pub did: Did,
    pub collection: Nsid,
    pub rkey: RecordKey,
    pub cursor: Cursor,
}

#[derive(Debug)]
pub struct DeleteAccount {
    pub did: Did,
    pub cursor: Cursor,
}

#[derive(Debug, Default)]
pub struct EventBatch {
    pub records: HashMap<Nsid, CollectionSamples>,
    pub record_deletes: Vec<DeleteRecord>,
    pub account_removes: Vec<DeleteAccount>,
    pub first_jetstream_cursor: Option<Cursor>,
    pub last_jetstream_cursor: Option<Cursor>,
}
