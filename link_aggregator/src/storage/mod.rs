use anyhow::Result;
use link_aggregator::{ActionableEvent, Did, RecordId};
use links::CollectedLink;
use std::collections::HashMap;

pub mod mem_store;
pub use mem_store::MemStorage;

#[cfg(feature = "rocks")]
pub mod rocks_store;
#[cfg(feature = "rocks")]
pub use rocks_store::RocksStorage;

#[derive(Debug, PartialEq)]
pub struct PagedAppendingCollection<T> {
    version: (u64, u64), // (collection length, deleted item count)
    items: Vec<T>,
}

pub trait LinkStorage: Send + Sync {
    fn get_cursor(&mut self) -> Result<Option<u64>> {
        Ok(None)
    }
    fn push(&mut self, event: &ActionableEvent, _cursor: u64) -> Result<()> {
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

    fn add_links(&mut self, record_id: &RecordId, links: &[CollectedLink]);
    fn remove_links(&mut self, record_id: &RecordId);
    fn update_links(&mut self, record_id: &RecordId, new_links: &[CollectedLink]) {
        self.remove_links(record_id);
        self.add_links(record_id, new_links);
    }
    fn set_account(&mut self, did: &Did, active: bool);
    fn delete_account(&mut self, did: &Did);

    // readers are  off from the writer instance
    fn to_readable(&mut self) -> impl LinkReader;
}

pub trait LinkReader: Clone + Send + Sync + 'static {
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64>;
    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> Result<PagedAppendingCollection<RecordId>>;
    fn get_all_counts(&self, _target: &str) -> Result<HashMap<String, HashMap<String, u64>>>;

    // todo: remove it
    fn summarize(&self, qsize: u32) {
        println!("queue: {qsize}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use links::Link;

    macro_rules! test_each_storage {
        ($test_name:ident, |$storage_label:ident| $test_code:block) => {
            #[test]
            fn $test_name() -> Result<()> {
                {
                    println!("=> testing with memstorage backend");
                    #[allow(unused_mut)]
                    let mut $storage_label = MemStorage::new();
                    $test_code
                }

                #[cfg(feature = "rocks")]
                {
                    println!("=> testing with rocksdb backend");
                    let rocks_db_path = tempfile::tempdir()?;
                    #[allow(unused_mut)]
                    let mut $storage_label = RocksStorage::new(rocks_db_path.path())?;
                    $test_code
                }

                Ok(())
            }
        };
    }

    test_each_storage!(test_empty, |storage| {
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

    test_each_storage!(test_add_link, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("bad.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("e.com", "app.t.c", ".bad.uri")?, 0);
    });

    test_each_storage!(test_links, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;

        // delete under the wrong collection
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.test.wrongcollection".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // delete under the wrong rkey
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "wrongkey".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // finally actually delete it
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);

        // put it back
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // add another link from this user
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa2".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);

        // add a link from someone else
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdfasdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 3);

        // aaaand delete the first one again
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);
    });

    test_each_storage!(test_two_user_links_delete_one, |storage| {
        // create the first link
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // create the second link (same user, different rkey)
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "B".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);

        // aaaand delete the first link
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "A".into(),
            }),
            0,
        )?;

        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
    });

    test_each_storage!(test_accounts, |storage| {
        // create two links
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "B".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("b.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 1);

        // and a third from a different account
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:fdsa".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 2);

        // delete the first account
        storage.push(&ActionableEvent::DeleteAccount("did:plc:asdf".into()), 0)?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 0);
    });

    test_each_storage!(multi_link, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("e.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("g.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);

        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
    });

    test_each_storage!(update_link, |storage| {
        // create the links
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("e.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("g.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);

        // update them
        storage.push(
            &ActionableEvent::UpdateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                new_links: vec![
                    CollectedLink {
                        target: Link::Uri("h.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("i.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("h.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("i.com", "app.t.c", ".xyz[].uri")?, 1);
    });

    test_each_storage!(update_no_links_to_links, |storage| {
        // update without prior create (consumer would have filtered out the original)
        storage.push(
            &ActionableEvent::UpdateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                new_links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
    });

    test_each_storage!(delete_multi_link_same_target, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".def.uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("a.com", "app.t.c", ".def.uri")?, 1);

        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "asdf".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("a.com", "app.t.c", ".def.uri")?, 0);
    });

    test_each_storage!(get_links_basic, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(
            storage.get_links("a.com", "app.t.c", ".abc.uri", 100, None)?,
            PagedAppendingCollection {
                version: (1, 0),
                items: vec![RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                }],
            }
        );
    });

    // todo: create get_links_paged test

    test_each_storage!(get_all_counts, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".def.uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_all_counts("a.com")?, {
            let mut counts = HashMap::new();
            let mut t_c_counts = HashMap::new();
            t_c_counts.insert(".abc.uri".into(), 1);
            t_c_counts.insert(".def.uri".into(), 1);
            counts.insert("app.t.c".into(), t_c_counts);
            counts
        });
    });

    test_each_storage!(get_all_counts_no_links, |storage| {
        assert_eq!(storage.get_all_counts("bad-example.com")?, HashMap::new());
    });
}
