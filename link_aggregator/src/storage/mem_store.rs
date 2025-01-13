use super::{LinkStorage, StorageBackend};
use anyhow::Result;
use link_aggregator::{Did, RecordId};
use links::CollectedLink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct MemStorage(Arc<Mutex<MemStorageData>>);

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Target(String);

impl Target {
    fn new(t: &str) -> Self {
        Self(t.into())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Source {
    collection: String,
    path: String,
}

impl Source {
    fn new(collection: &str, path: &str) -> Self {
        Self {
            collection: collection.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RepoId {
    collection: String,
    rkey: String,
}

impl RepoId {
    fn from_record_id(record_id: &RecordId) -> Self {
        Self {
            collection: record_id.collection.clone(),
            rkey: record_id.rkey.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RecordPath(String);

impl RecordPath {
    fn new(rp: &str) -> Self {
        Self(rp.into())
    }
}

#[derive(Debug, Default)]
struct MemStorageData {
    dids: HashMap<Did, bool>,                            // bool: active or nah
    targets: HashMap<Target, HashMap<Source, Vec<Did>>>, // target -> (collection, path) -> did[]
    links: HashMap<Did, HashMap<RepoId, Vec<(RecordPath, Target)>>>, // did -> collection:rkey -> (path, target)[]
}

impl MemStorage {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MemStorageData::default())))
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
}

impl LinkStorage for MemStorage {} // defaults are fine

impl StorageBackend for MemStorage {
    fn add_links(&self, record_id: &RecordId, links: &[CollectedLink]) {
        let mut data = self.0.lock().unwrap();
        for link in links {
            data.dids.entry(record_id.did()).or_insert(true); // if they are inserting a link, presumably they are active
            data.targets
                .entry(Target::new(&link.target))
                .or_default()
                .entry(Source::new(&record_id.collection, &link.path))
                .or_default()
                .push(record_id.did());
            data.links
                .entry(record_id.did())
                .or_default()
                .entry(RepoId::from_record_id(record_id))
                .or_insert(Vec::with_capacity(1))
                .push((RecordPath::new(&link.path), Target::new(&link.target)))
        }
    }

    fn remove_links(&self, record_id: &RecordId) {
        let mut data = self.0.lock().unwrap();
        let repo_id = RepoId::from_record_id(record_id);
        if let Some(Some(link_targets)) = data.links.get(&record_id.did).map(|cr| cr.get(&repo_id))
        {
            let link_targets = link_targets.clone(); // satisfy borrowck
            for (record_path, target) in link_targets {
                let dids = data
                    .targets
                    .get_mut(&target)
                    .expect("must have the target if we have a link saved")
                    .get_mut(&Source::new(&record_id.collection, &record_path.0))
                    .expect("must have the target at this path if we have a link to it saved");
                // search from the end: more likely to be visible and deletes are usually soon after creates
                // only delete one instance: a user can create multiple links to something, we're only deleting one
                // (we don't know which one in the list we should be deleting, and it hopefully mostly doesn't matter)
                let pos = dids
                    .iter()
                    .rposition(|d| *d == record_id.did)
                    .expect("must be in dids list if we have a link to it");
                dids.remove(pos);
            }
        }
        data.links
            .get_mut(&record_id.did)
            .map(|cr| cr.remove(&repo_id));
    }

    fn set_account(&self, did: &Did, active: bool) {
        let mut data = self.0.lock().unwrap();
        if let Some(account) = data.dids.get_mut(did) {
            *account = active;
        }
    }

    fn delete_account(&self, did: &Did) {
        let mut data = self.0.lock().unwrap();
        if let Some(links) = data.links.get(did) {
            let links = links.clone();
            for (repo_id, targets) in links {
                let targets = targets.clone();
                for (record_path, target) in targets {
                    data.targets
                        .get_mut(&target)
                        .expect("must have the target if we have a link saved")
                        .get_mut(&Source::new(&repo_id.collection, &record_path.0))
                        .expect("must have the target at this path if we have a link to it saved")
                        .retain(|d| d != did);
                }
            }
        }
        data.links.remove(did);
        data.dids.remove(did);
    }

    fn count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(0);
        };
        let Some(dids) = paths.get(&Source::new(collection, path)) else {
            return Ok(0);
        };
        let count = dids.len().try_into()?;
        Ok(count)
    }
}
