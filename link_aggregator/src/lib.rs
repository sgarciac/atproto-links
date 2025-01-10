use links::CollectedLink;

#[derive(Debug, PartialEq)]
pub enum ActionableEvent {
    CreateLinks {
        did: String,
        collection: String,
        rkey: String,
        links: Vec<CollectedLink>,
    },
    UpdateLinks {
        did: String,
        collection: String,
        rkey: String,
        new_links: Vec<CollectedLink>,
    },
    DeleteRecord {
        did: String,
        collection: String,
        rkey: String,
    },
    ActivateAccount {
        did: String,
    },
    DeactivateAccount {
        did: String,
    },
    DeleteAccount {
        did: String,
    },
}
