pub mod consumer;
pub mod db_types;
pub mod server;
pub mod store;
pub mod store_types;

use jetstream::events::Cursor;
use jetstream::exports::{Did, Nsid, RecordKey};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone)]
pub struct CreateRecord {
    pub did: Did,
    // collection omitted because the batch keys off it
    pub rkey: RecordKey,
    pub record: serde_json::Value,
    pub cursor: Cursor,
}

#[derive(Debug, Default, Clone)]
pub struct CollectionSamples {
    pub total_seen: usize,
    pub samples: VecDeque<CreateRecord>,
}

#[derive(Debug)]
pub struct UpdateRecord {
    pub did: Did,
    pub collection: Nsid,
    pub rkey: RecordKey,
    pub record: serde_json::Value,
    pub cursor: Cursor,
}

#[derive(Debug)]
pub struct DeleteRecord {
    pub did: Did,
    pub collection: Nsid,
    pub rkey: RecordKey,
    pub cursor: Cursor,
}

#[derive(Debug)]
pub enum ModifyRecord {
    Update(UpdateRecord),
    Delete(DeleteRecord),
}

#[derive(Debug)]
pub struct DeleteAccount {
    pub did: Did,
    pub cursor: Cursor,
}

#[derive(Debug, Default)]
pub struct EventBatch {
    pub record_creates: HashMap<Nsid, CollectionSamples>,
    pub record_modifies: Vec<ModifyRecord>,
    pub account_removes: Vec<DeleteAccount>,
    pub first_jetstream_cursor: Option<Cursor>,
    pub last_jetstream_cursor: Option<Cursor>,
}
