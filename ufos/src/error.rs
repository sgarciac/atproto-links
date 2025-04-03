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
