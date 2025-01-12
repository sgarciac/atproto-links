use anyhow::Result;
use link_aggregator::{ActionableEvent, Did, RecordId};
use links::CollectedLink;

pub mod mem_store;
pub use mem_store::MemStorage;

pub mod rocks_store;
pub use rocks_store::RocksStorage;

/// consumer-side storage api, independent of actual storage backend
pub trait LinkStorage: StorageBackend {
    fn push(&self, event: &ActionableEvent) -> Result<()> {
        match event {
            ActionableEvent::CreateLinks { record_id, links } => self.add_links(record_id, links),
            ActionableEvent::UpdateLinks {
                record_id,
                new_links,
            } => self.update_links(record_id, new_links),
            ActionableEvent::DeleteRecord(record_id) => self.remove_links(record_id),
            ActionableEvent::ActivateAccount(did) => self.set_account(did, true),
            ActionableEvent::DeactivateAccount(did) => self.set_account(did, false),
            ActionableEvent::DeleteAccount(did) => self.delete_account(did),
        }
        Ok(())
    }
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        self.count(target, collection, path)
    }
}

/// persistent data stores
pub trait StorageBackend {
    fn add_links(&self, record_id: &RecordId, links: &[CollectedLink]);
    fn remove_links(&self, record_id: &RecordId);
    fn update_links(&self, record_id: &RecordId, new_links: &[CollectedLink]) {
        self.remove_links(record_id);
        self.add_links(record_id, new_links);
    }
    fn set_account(&self, did: &Did, active: bool);
    fn delete_account(&self, did: &Did);

    fn count(&self, target: &str, collection: &str, path: &str) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_each_storage {
        ($test_name:ident, |$storage_label:ident| $test_code:block) => {
            #[test]
            fn $test_name() -> Result<()> {
                let __stores: Vec<(&str, Box<dyn LinkStorage>)> = vec![
                    ("memstorage", Box::new(MemStorage::new())),
                    ("rocksdb", Box::new(RocksStorage::new())),
                ];
                for (__backend_name, __storage) in __stores {
                    println!("=> testing with {__backend_name} backend");
                    let $storage_label = __storage;
                    $test_code
                }
                Ok(())
            }
        };
    }

    test_each_storage!(mem_empty, |storage| {
        assert_eq!(storage.get_count("", "", "")?, 0);
        assert_eq!(storage.get_count("a", "b", "c")?, 0);
        assert_eq!(
            storage.get_count(
                "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f",
                "app.t.c",
                ".reply.parent.uri"
            )?,
            0
        );
    });

    test_each_storage!(mem_links, |storage| {
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("bad.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("e.com", "app.t.c", ".def.uri")?, 0);

        // delete under the wrong collection
        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.test.wrongcollection".into(),
            rkey: "fdsa".into(),
        }))?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // delete under the wrong rkey
        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.t.c".into(),
            rkey: "wrongkey".into(),
        }))?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // finally actually delete it
        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.t.c".into(),
            rkey: "fdsa".into(),
        }))?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);

        // put it back
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // add another link from this user
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa2".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);

        // add a link from someone else
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdfasdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 3);

        // aaaand delete the first one again
        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.t.c".into(),
            rkey: "fdsa".into(),
        }))?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);
    });

    test_each_storage!(mem_two_user_links_delete_one, |storage| {
        // create the first link
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "A".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // create the second link (same user, different rkey)
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "B".into(),
            },
            links: vec![CollectedLink {
                target: "e.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);

        // aaaand delete the first link
        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.t.c".into(),
            rkey: "A".into(),
        }))?;

        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
    });

    test_each_storage!(mem_accounts, |storage| {
        // create two links
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "A".into(),
            },
            links: vec![CollectedLink {
                target: "a.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "B".into(),
            },
            links: vec![CollectedLink {
                target: "b.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 1);

        // and a third from a different account
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:fdsa".into(),
                collection: "app.t.c".into(),
                rkey: "A".into(),
            },
            links: vec![CollectedLink {
                target: "a.com".into(),
                path: ".abc.uri".into(),
            }],
        })?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 2);

        // delete the first account
        storage.push(&ActionableEvent::DeleteAccount("did:plc:asdf".into()))?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 0);
    });

    test_each_storage!(multi_link, |storage| {
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            links: vec![
                CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                },
                CollectedLink {
                    target: "f.com".into(),
                    path: ".xyz[].uri".into(),
                },
                CollectedLink {
                    target: "g.com".into(),
                    path: ".xyz[].uri".into(),
                },
            ],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);

        storage.push(&ActionableEvent::DeleteRecord(RecordId {
            did: "did:plc:asdf".into(),
            collection: "app.t.c".into(),
            rkey: "fdsa".into(),
        }))?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
    });

    test_each_storage!(update_link, |storage| {
        // create the links
        storage.push(&ActionableEvent::CreateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            links: vec![
                CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                },
                CollectedLink {
                    target: "f.com".into(),
                    path: ".xyz[].uri".into(),
                },
                CollectedLink {
                    target: "g.com".into(),
                    path: ".xyz[].uri".into(),
                },
            ],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);

        // update them
        storage.push(&ActionableEvent::UpdateLinks {
            record_id: RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            },
            new_links: vec![
                CollectedLink {
                    target: "h.com".into(),
                    path: ".abc.uri".into(),
                },
                CollectedLink {
                    target: "f.com".into(),
                    path: ".xyz[].uri".into(),
                },
                CollectedLink {
                    target: "i.com".into(),
                    path: ".xyz[].uri".into(),
                },
            ],
        })?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("h.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("i.com", "app.t.c", ".xyz[].uri")?, 1);
    });
}
