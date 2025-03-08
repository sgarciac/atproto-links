pub mod consumer;
pub mod store;

use jetstream::exports::{Did, Nsid, RecordKey};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct SetRecord {
    pub new: bool,
    pub did: Did,
    pub rkey: RecordKey,
    pub record: serde_json::Value,
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
}

#[derive(Debug, Default)]
pub struct EventBatch {
    pub records: HashMap<Nsid, CollectionSamples>,
    pub record_deletes: Vec<DeleteRecord>,
    pub account_removes: Vec<Did>,
}
