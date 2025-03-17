use serde::Deserialize;

use crate::{
    events::EventInfo,
    exports,
};

/// An event representing a repo commit, which can be a `create`, `update`, or `delete` operation.
#[derive(Deserialize, Debug)]
#[serde(untagged, rename_all = "snake_case")]
pub enum CommitEvent<R> {
    CreateOrUpdate {
        #[serde(flatten)]
        info: EventInfo,
        commit: CommitData<R>,
    },
    Delete {
        #[serde(flatten)]
        info: EventInfo,
        commit: CommitInfo,
    },
}

/// The type of commit operation that was performed.
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CommitType {
    Create,
    Update,
    Delete,
}

/// Basic commit specific info bundled with every event, also the only data included with a `delete`
/// operation.
#[derive(Deserialize, Debug)]
pub struct CommitInfo {
    /// The type of commit operation that was performed.
    pub operation: CommitType,
    pub rev: String,
    pub rkey: exports::RecordKey,
    /// The NSID of the record type that this commit is associated with.
    pub collection: exports::Nsid,
}

/// Detailed data bundled with a commit event. This data is only included when the event is
/// `create` or `update`.
#[derive(Deserialize, Debug)]
pub struct CommitData<R> {
    #[serde(flatten)]
    pub info: CommitInfo,
    /// The CID of the record that was operated on.
    pub cid: exports::Cid,
    /// The record that was operated on.
    pub record: R,
}
