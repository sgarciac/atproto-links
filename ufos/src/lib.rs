pub mod consumer;
pub mod db_types;
pub mod error;
pub mod server;
pub mod storage;
pub mod storage_fjall;
pub mod store_types;

use cardinality_estimator::CardinalityEstimator;
use error::FirehoseEventError;
use jetstream::events::{CommitEvent, CommitOp, Cursor};
use jetstream::exports::{Did, Nsid, RecordKey};
use serde_json::value::RawValue;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Default, Clone)]
pub struct CollectionCommits {
    pub total_seen: usize,
    pub dids_estimate: CardinalityEstimator<Did>,
    pub commits: VecDeque<UFOsCommit>,
}

impl CollectionCommits {
    pub fn truncating_insert(&mut self, commit: UFOsCommit, limit: usize) {
        self.total_seen += 1;
        self.dids_estimate.insert(&commit.did);
        self.commits.truncate(limit - 1);
        self.commits.push_front(commit);
    }
}

#[derive(Debug, Clone)]
pub struct DeleteAccount {
    pub did: Did,
    pub cursor: Cursor,
}

#[derive(Debug, Clone)]
pub enum CommitAction {
    Put(PutAction),
    Cut,
}

#[derive(Debug, Clone)]
pub struct PutAction {
    record: Box<RawValue>,
    is_update: bool,
}

#[derive(Debug, Clone)]
pub struct UFOsCommit {
    cursor: Cursor,
    did: Did,
    rkey: RecordKey,
    rev: String,
    action: CommitAction,
}

impl UFOsCommit {
    pub fn from_commit_info(
        commit: CommitEvent,
        did: Did,
        cursor: Cursor,
    ) -> Result<(Self, Nsid), FirehoseEventError> {
        let action = match commit.operation {
            CommitOp::Delete => CommitAction::Cut,
            cru @ _ => CommitAction::Put(PutAction {
                record: commit.record.ok_or(FirehoseEventError::CruMissingRecord)?,
                is_update: cru == CommitOp::Update,
            }),
        };
        let batched = Self {
            cursor,
            did,
            rkey: commit.rkey,
            rev: commit.rev,
            action,
        };
        Ok((batched, commit.collection))
    }
}

#[derive(Debug, Default, Clone)]
pub struct EventBatch {
    pub commits_by_nsid: HashMap<Nsid, CollectionCommits>,
    pub account_removes: Vec<DeleteAccount>,
}

impl EventBatch {
    pub fn total_records(&self) -> usize {
        self.commits_by_nsid.values().map(|v| v.commits.len()).sum()
    }
    pub fn total_seen(&self) -> usize {
        self.commits_by_nsid.values().map(|v| v.total_seen).sum()
    }
    pub fn total_collections(&self) -> usize {
        self.commits_by_nsid.len()
    }
    pub fn account_removes(&self) -> usize {
        self.account_removes.len()
    }
    pub fn estimate_dids(&self) -> usize {
        let mut estimator = CardinalityEstimator::<Did>::new();
        for commits in self.commits_by_nsid.values() {
            estimator.merge(&commits.dids_estimate);
        }
        estimator.estimate()
    }
    pub fn latest_cursor(&self) -> Option<Cursor> {
        let mut oldest = Cursor::from_start();
        for commits in self.commits_by_nsid.values() {
            if let Some(commit) = commits.commits.front() {
                if commit.cursor > oldest {
                    oldest = commit.cursor;
                }
            }
        }
        if let Some(del) = self.account_removes.last() {
            if del.cursor > oldest {
                oldest = del.cursor;
            }
        }
        if oldest > Cursor::from_start() {
            Some(oldest)
        } else {
            None
        }
    }
    pub fn is_empty(&self) -> bool {
        self.commits_by_nsid.is_empty() && self.account_removes.is_empty()
    }
}
