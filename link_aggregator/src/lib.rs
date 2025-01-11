use links::CollectedLink;
use std::convert::From;
use std::ops::Deref;

#[derive(Debug, PartialEq)]
pub enum ActionableEvent {
    CreateLinks {
        record_id: RecordId,
        links: Vec<CollectedLink>,
    },
    UpdateLinks {
        record_id: RecordId,
        new_links: Vec<CollectedLink>,
    },
    DeleteRecord(RecordId),
    ActivateAccount(Did),
    DeactivateAccount(Did),
    DeleteAccount(Did),
}

#[derive(Debug, PartialEq)]
pub struct Did(String);

impl AsRef<str> for Did {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for Did {
    fn from(s: T) -> Self {
        Self(s.into())
    }
}

impl Deref for Did {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct RecordId {
    pub did: Did,
    pub collection: String,
    pub rkey: String,
}
