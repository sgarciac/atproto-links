use anyhow::Result;
use link_aggregator::ActionableEvent;
use links::CollectedLink;
use std::collections::HashMap;

pub trait LinkStorage {
    fn push(&mut self, event: &ActionableEvent) -> Result<()>;
    fn get_count(&self, target: &str, path: &str) -> Result<u64>;
}

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug)]
struct MemStorage {
    dids: HashMap<String, bool>, // bool: active or nah
    targets: HashMap<String, HashMap<String, Vec<String>>>, // target -> path -> did[]
    links: HashMap<String, HashMap<String, Vec<(String, String)>>>, // did -> collection:rkey -> (target, path)[]
}

impl MemStorage {
    pub fn new() -> Self {
        Self {
            dids: HashMap::new(),
            targets: HashMap::new(),
            links: HashMap::new(),
        }
    }

    fn _col_rkey(collection: &str, rkey: &str) -> String {
        [collection, rkey].join(":")
    }
}

impl LinkStorage for MemStorage {
    fn push(&mut self, event: &ActionableEvent) -> Result<()> {
        match event {
            ActionableEvent::CreateLinks {
                did,
                collection,
                rkey,
                links,
            } => {
                for link in links {
                    self.dids
                        .entry(did.clone())
                        .or_insert(true); // if they are inserting a link, presumably they are active
                    self.targets
                        .entry(link.target.clone())
                        .or_default()
                        .entry(link.path.clone())
                        .or_default()
                        .push(did.clone());
                    self.links
                        .entry(did.clone())
                        .or_default()
                        .entry(Self::_col_rkey(collection, rkey))
                        .or_insert(Vec::with_capacity(1))
                        .push((link.target.clone(), link.path.clone()))
                }
                Ok(())
            },
            ActionableEvent::UpdateLinks {
                ..
                // did,
                // collection,
                // rkey,
                // new_links,
            } => unimplemented!(), // for mem version probably delete old then add new?
            ActionableEvent::DeleteRecord {
                did,
                collection,
                rkey,
            } => {
                let col_rkey = Self::_col_rkey(collection, rkey);
                if let Some(Some(targets)) = self.links.get(did).map(|cr| cr.get(&col_rkey)) {
                    for (target, path) in targets {
                        let dids = self.targets
                            .get_mut(target)
                            .expect("must have the target if we have a link saved")
                            .get_mut(path)
                            .expect("must have the target at this path if we have a link to it saved");
                        // search from the end: more likely to be visible and deletes are usually soon after creates
                        // only delete one instance: a user can create multiple links to something, we're only deleting one
                        // (we don't know which one in the list we should be deleting, and it hopefully mostly doesn't matter)
                        let pos = dids.iter().rposition(|d| d == did).expect("must be in dids list if we have a link to it");
                        dids.remove(pos);
                    }
                }
                self.links.get_mut(did).map(|cr| cr.remove(&col_rkey));
                Ok(())
            },
            ActionableEvent::ActivateAccount { did } => {
                if let Some(account) = self.dids.get_mut(did) { // only act if we have any records by this account
                    *account = true;
                }
                Ok(())
            },
            ActionableEvent::DeactivateAccount { did } => {
                if let Some(account) = self.dids.get_mut(did) { // ignore deactivating unknown accounts should be ok
                    *account = false;
                }
                Ok(())
            },
            ActionableEvent::DeleteAccount { did } => {
                if let Some(links) = self.links.get(did) {
                    for targets in links.values() {
                        for (target, path) in targets {
                            self.targets.get_mut(target)
                                .expect("must have the target if we have a link saved")
                                .get_mut(path)
                                .expect("must have the target at this path if we have a link to it saved")
                                .retain(|d| d != did);
                        }
                    }
                }
                self.links.remove(did);
                self.dids.remove(did);
                Ok(())
            },
        }
    }
    fn get_count(&self, target: &str, path: &str) -> Result<u64> {
        let Some(paths) = self.targets.get(target) else {
            return Ok(0);
        };
        let Some(dids) = paths.get(path) else {
            return Ok(0);
        };
        let count = dids.len().try_into()?;
        Ok(count)
    }
}

// pub struct RocksStorage {}

// impl RocksStorage {
//     pub fn new() -> Self {
//         Self {}
//     }
// }

// impl LinkStorage for RocksStorage {
//     fn push(&mut self, event: &ActionableEvent) -> Result<()> {
//         match event {
//             ActionableEvent::CreateLinks {
//                 did,
//                 collection,
//                 rkey,
//                 links,
//             } => {}
//             ActionableEvent::UpdateLinks {
//                 did,
//                 collection,
//                 rkey,
//                 new_links,
//             } => {}
//             ActionableEvent::DeleteRecord {
//                 did,
//                 collection,
//                 rkey,
//             } => {}
//             ActionableEvent::ActivateAccount { did } => {}
//             ActionableEvent::DeactivateAccount { did } => {}
//             ActionableEvent::DeleteAccount { did } => {}
//         }
//         Ok(())
//     }
//     fn get_count(target: &str, path: &str) -> Result<u64> {
//         Ok(0)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_empty() {
        let storage = MemStorage::new();
        assert_eq!(storage.get_count("", "").unwrap(), 0);
        assert_eq!(storage.get_count("a", "b").unwrap(), 0);
        assert_eq!(
            storage
                .get_count(
                    "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f",
                    ".reply.parent.uri"
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_mem_links() {
        let mut storage = MemStorage::new();
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);
        assert_eq!(storage.get_count("bad.com", ".abc.uri").unwrap(), 0);
        assert_eq!(storage.get_count("e.com", ".def.uri").unwrap(), 0);

        // delete under the wrong collection
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.wrongcollection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);

        // delete under the wrong rkey
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "wrongkey".into(),
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);

        // finally actually delete it
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 0);

        // put it back
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);

        // add another link from this user
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa2".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 2);

        // add a link from someone else
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdfasdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 3);

        // aaaand delete the first one again
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 2);
    }

    #[test]
    fn test_mem_two_user_links_delete_one() {
        let mut storage = MemStorage::new();

        // create the first link
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "A".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);

        // create the second link (same user, different rkey)
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "B".into(),
                links: vec![CollectedLink {
                    target: "e.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 2);

        // aaaand delete the first link
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "A".into(),
            })
            .unwrap();
        // println!("{:?}", storage);
        assert_eq!(storage.get_count("e.com", ".abc.uri").unwrap(), 1);
    }

    #[test]
    fn test_mem_accounts() {
        let mut storage = MemStorage::new();

        // create two links
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "A".into(),
                links: vec![CollectedLink {
                    target: "a.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "B".into(),
                links: vec![CollectedLink {
                    target: "b.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        assert_eq!(storage.get_count("a.com", ".abc.uri").unwrap(), 1);
        assert_eq!(storage.get_count("b.com", ".abc.uri").unwrap(), 1);

        // and a third from a different account
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:fdsa".into(),
                collection: "app.test.collection".into(),
                rkey: "A".into(),
                links: vec![CollectedLink {
                    target: "a.com".into(),
                    path: ".abc.uri".into(),
                }],
            })
            .unwrap();
        let n = storage.get_count("a.com", ".abc.uri").unwrap();
        assert_eq!(n, 2);

        // delete the first account
        storage
            .push(&ActionableEvent::DeleteAccount {
                did: "did:plc:asdf".into(),
            })
            .unwrap();
        assert_eq!(storage.get_count("a.com", ".abc.uri").unwrap(), 1);
        assert_eq!(storage.get_count("b.com", ".abc.uri").unwrap(), 0);
    }
}
