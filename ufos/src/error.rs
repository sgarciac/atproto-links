use crate::db_types::EncodingError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FirehoseEventError {
    #[error("Create/Update commit operation missing record data")]
    CruMissingRecord,
    #[error("Account event missing account info")]
    AccountEventMissingAccount,
    #[error("Commit event missing commit info")]
    CommitEventMissingCommit,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Failed to initialize: {0}")]
    InitError(String),
    #[error("DB seems to be in a bad state: {0}")]
    BadStateError(String),
    #[error("Fjall error")]
    FjallError(#[from] fjall::Error),
    #[error("Bytes encoding error")]
    EncodingError(#[from] EncodingError),
}
