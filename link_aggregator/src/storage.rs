use anyhow::Result;
use link_aggregator::ActionableEvent;
use std::collections::HashMap;
use std::sync::Mutex;

pub trait LinkStorage {
    fn push(&self, event: &ActionableEvent) -> Result<()>;
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64>;
}

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug)]
pub struct MemStorage(Mutex<MemStorageData>);

#[derive(Debug, Default)]
struct MemStorageData {
    dids: HashMap<String, bool>, // bool: active or nah
    targets: HashMap<String, HashMap<(String, String), Vec<String>>>, // target -> (collection, path) -> did[]
    #[allow(clippy::type_complexity)]
    links: HashMap<String, HashMap<String, Vec<(String, String, String)>>>, // did -> collection:rkey -> (target, collection, path)[]
}

impl MemStorage {
    pub fn new() -> Self {
        Self(Mutex::new(MemStorageData::default()))
    }

    pub fn summarize(&self, qsize: u32) {
        let data = self.0.lock().unwrap();
        let dids = data.dids.len();
        let targets = data.targets.len();
        let target_paths: usize = data.targets.values().map(|paths| paths.len()).sum();
        let links = data.links.len();

        let sample_target = data.targets.keys().nth(data.targets.len() / 2);
        let sample_path = sample_target.and_then(|t| data.targets.get(t).unwrap().keys().next());
        println!("queue: {qsize}. {dids} dids, {targets} targets from {target_paths} paths, {links} links. sample: {sample_target:?} {sample_path:?}");
    }

    fn _col_rkey(collection: &str, rkey: &str) -> String {
        [collection, rkey].join(":")
    }
}

impl LinkStorage for MemStorage {
    fn push(&self, event: &ActionableEvent) -> Result<()> {
        let mut data = self.0.lock().unwrap();
        match event {
            ActionableEvent::CreateLinks {
                did,
                collection,
                rkey,
                links,
            } => {
                for link in links {
                    data.dids
                        .entry(did.clone())
                        .or_insert(true); // if they are inserting a link, presumably they are active
                    data.targets
                        .entry(link.target.clone())
                        .or_default()
                        .entry((collection.clone(), link.path.clone()))
                        .or_default()
                        .push(did.clone());
                    data.links
                        .entry(did.clone())
                        .or_default()
                        .entry(Self::_col_rkey(collection, rkey))
                        .or_insert(Vec::with_capacity(1))
                        .push((link.target.clone(), collection.clone(), link.path.clone()))
                }
                Ok(())
            },
            ActionableEvent::UpdateLinks {
                ..
                // did,
                // collection,
                // rkey,
                // new_links,
            } => {
                // eprintln!("storage: ignoring update event for now...");
                Ok(())
            } //unimplemented!(), // for mem version probably delete old then add new?
            ActionableEvent::DeleteRecord {
                did,
                collection,
                rkey,
            } => {
                let col_rkey = Self::_col_rkey(collection, rkey);
                if let Some(Some(link_targets)) = data.links.get(did).map(|cr| cr.get(&col_rkey)) {
                    let link_targets = link_targets.clone(); // satisfy borrowck
                    for (target, collection, path) in link_targets {
                        let dids = data.targets
                            .get_mut(&target)
                            .expect("must have the target if we have a link saved")
                            .get_mut(&(collection.clone(), path.clone()))
                            .expect("must have the target at this path if we have a link to it saved");
                        // search from the end: more likely to be visible and deletes are usually soon after creates
                        // only delete one instance: a user can create multiple links to something, we're only deleting one
                        // (we don't know which one in the list we should be deleting, and it hopefully mostly doesn't matter)
                        let pos = dids.iter().rposition(|d| d == did).expect("must be in dids list if we have a link to it");
                        dids.remove(pos);
                    }
                }
                data.links.get_mut(did).map(|cr| cr.remove(&col_rkey));
                Ok(())
            },
            ActionableEvent::ActivateAccount { did } => {
                if let Some(account) = data.dids.get_mut(did) { // only act if we have any records by this account
                    *account = true;
                }
                Ok(())
            },
            ActionableEvent::DeactivateAccount { did } => {
                if let Some(account) = data.dids.get_mut(did) { // ignore deactivating unknown accounts should be ok
                    *account = false;
                }
                Ok(())
            },
            ActionableEvent::DeleteAccount { did } => {
                if let Some(links) = data.links.get(did) {
                    let links = links.clone();
                    for targets in links.values() {
                        for (target, collection, path) in targets {
                            data.targets.get_mut(target)
                                .expect("must have the target if we have a link saved")
                                .get_mut(&(collection.clone(), path.clone()))
                                .expect("must have the target at this path if we have a link to it saved")
                                .retain(|d| d != did);
                        }
                    }
                }
                data.links.remove(did);
                data.dids.remove(did);
                Ok(())
            },
        }
    }
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(target) else {
            return Ok(0);
        };
        let Some(dids) = paths.get(&(collection.to_string(), path.to_string())) else {
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
    use links::CollectedLink;

    #[test]
    fn test_mem_empty() {
        let storage = MemStorage::new();
        assert_eq!(storage.get_count("", "", "").unwrap(), 0);
        assert_eq!(storage.get_count("a", "b", "c").unwrap(), 0);
        assert_eq!(
            storage
                .get_count(
                    "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f",
                    "app.test.collection",
                    ".reply.parent.uri"
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_mem_links() {
        let storage = MemStorage::new();
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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .get_count("bad.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".def.uri")
                .unwrap(),
            0
        );

        // delete under the wrong collection
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.wrongcollection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );

        // delete under the wrong rkey
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "wrongkey".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );

        // finally actually delete it
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            0
        );

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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );

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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            2
        );

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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            3
        );

        // aaaand delete the first one again
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            2
        );
    }

    #[test]
    fn test_mem_two_user_links_delete_one() {
        let storage = MemStorage::new();

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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );

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
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            2
        );

        // aaaand delete the first link
        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "A".into(),
            })
            .unwrap();

        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_mem_accounts() {
        let storage = MemStorage::new();

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
        assert_eq!(
            storage
                .get_count("a.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .get_count("b.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );

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
        assert_eq!(
            storage
                .get_count("a.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            2
        );

        // delete the first account
        storage
            .push(&ActionableEvent::DeleteAccount {
                did: "did:plc:asdf".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("a.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .get_count("b.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_multi_link() {
        let storage = MemStorage::new();
        storage
            .push(&ActionableEvent::CreateLinks {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
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
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .get_count("f.com", "app.test.collection", ".xyz[].uri")
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .get_count("g.com", "app.test.collection", ".xyz[].uri")
                .unwrap(),
            1
        );

        storage
            .push(&ActionableEvent::DeleteRecord {
                did: "did:plc:asdf".into(),
                collection: "app.test.collection".into(),
                rkey: "fdsa".into(),
            })
            .unwrap();
        assert_eq!(
            storage
                .get_count("e.com", "app.test.collection", ".abc.uri")
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .get_count("f.com", "app.test.collection", ".xyz[].uri")
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .get_count("g.com", "app.test.collection", ".xyz[].uri")
                .unwrap(),
            0
        );
    }
}
