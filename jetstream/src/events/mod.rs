pub mod account;
pub mod commit;
pub mod identity;

use std::time::{
    Duration,
    SystemTime,
    UNIX_EPOCH,
};

use serde::Deserialize;

use crate::exports;

/// Opaque wrapper for the time_us cursor used by jetstream
///
/// Generally, you should use a cursor
#[derive(Deserialize, Debug, Clone)]
pub struct Cursor(u64);

/// Basic data that is included with every event.
#[derive(Deserialize, Debug)]
pub struct EventInfo {
    pub did: exports::Did,
    pub time_us: Cursor,
    pub kind: EventKind,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum JetstreamEvent<R> {
    Commit(commit::CommitEvent<R>),
    Identity(identity::IdentityEvent),
    Account(account::AccountEvent),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Commit,
    Identity,
    Account,
}

impl<R> JetstreamEvent<R> {
    pub fn cursor(&self) -> Cursor {
        match self {
            JetstreamEvent::Commit(commit::CommitEvent::Create { info, .. }) => {
                info.time_us.clone()
            }
            JetstreamEvent::Commit(commit::CommitEvent::Update { info, .. }) => {
                info.time_us.clone()
            }
            JetstreamEvent::Commit(commit::CommitEvent::Delete { info, .. }) => {
                info.time_us.clone()
            }
            JetstreamEvent::Identity(e) => e.info.time_us.clone(),
            JetstreamEvent::Account(e) => e.info.time_us.clone(),
        }
    }
}

impl Cursor {
    /// Get a cursor that will consume all available jetstream replay
    ///
    /// This sets the cursor to zero.
    ///
    /// Jetstream instances typically only have a few days of replay.
    pub fn from_start() -> Self {
        Self(0)
    }
    /// Get a cursor for a specific time
    ///
    /// Panics: if t is older than the unix epoch: Jan 1, 1970.
    ///
    /// If you want to receive all available jetstream replay (typically a few days), use
    /// .from_start()
    pub fn at(t: SystemTime) -> Self {
        let unix_dt = t
            .duration_since(UNIX_EPOCH)
            .expect("cannot set jetstream cursor earlier than unix epoch");
        Self(unix_dt.as_micros() as u64)
    }
    /// Get a cursor rewound from now by this amount
    ///
    /// Panics: if d is greater than the time since the unix epoch: Jan 1, 1970.
    ///
    /// Jetstream instances typically only have a few days of replay.
    pub fn back_by(d: Duration) -> Self {
        Self::at(SystemTime::now() - d)
    }
    /// Get a Cursor from a raw u64
    ///
    /// For example, from a jetstream event's `time_us` field.
    pub fn from_raw_u64(time_us: u64) -> Self {
        Self(time_us)
    }
    /// Get the raw u64 value from this cursor.
    pub fn to_raw_u64(&self) -> u64 {
        self.0
    }
    /// Format the cursor value for use in a jetstream connection url querystring
    pub fn to_jetstream(&self) -> String {
        self.0.to_string()
    }
}
