use super::{LinkReader, LinkStorage, PagedAppendingCollection, StorageStats};
use crate::{ActionableEvent, CountsByCount, Did, RecordId};
use anyhow::Result;
use duckdb::{params, Connection};
use links::CollectedLink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct DuckStorage {
    pub connection: Arc<Mutex<Connection>>,
}

impl DuckStorage {
    pub fn new(connection: Connection) -> Self {
        let create_table_sql = "
            create table IF NOT EXISTS account_state_changes
            (
                ts TIMESTAMP NOT NULL,
                did TEXT NOT NULL,
                active BOOLEAN NOT NULL,
            );
            
            create table IF NOT EXISTS link_creations
            (
                ts TIMESTAMP NOT NULL,
                from_record_did TEXT not null,
                from_record_collection TEXT not null,
                from_record_rkey TEXT not null,
                from_record_field_path TEXT not null,
                to_uri TEXT not null,
            );

            create table IF NOT EXISTS record_links_resets
            (
                ts TIMESTAMP NOT NULL,
                did TEXT NOT NULL,
                collection TEXT NOT NULL,
                rkey TEXT NOT NULL,
            );

            create table IF NOT EXISTS record_deletes
            (
                ts TIMESTAMP NOT NULL,
                did TEXT NOT NULL,
                collection TEXT NOT NULL,
                rkey TEXT NOT NULL,
            );

            create table IF NOT EXISTS account_deletes
            (
                ts TIMESTAMP NOT NULL,
                did TEXT NOT NULL,
            );        

            create view if not exists active_links as select * from link_creations links where 
                not exists (select 1 from record_links_resets resets where resets.ts > links.ts and resets.did = links.from_record_did and resets.collection = links.from_record_collection and resets.rkey = links.from_record_rkey) 
                and not exists (select 1 from record_deletes deletes where deletes.ts > links.ts and deletes.did = links.from_record_did and deletes.collection = links.from_record_collection and deletes.rkey = links.from_record_rkey) 
                and not exists (select 1 from account_deletes deletes where deletes.ts > links.ts and deletes.did = links.from_record_did);

        ";
        connection.execute_batch(create_table_sql).unwrap();
        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }

    fn add_link_creations(&mut self, record_id: &RecordId, links: &[CollectedLink]) {
        if links.is_empty() {
            return;
        }
        let connection = self.connection.lock().unwrap();
        let mut activations_appender = connection.appender("account_state_changes").unwrap();
        let mut links_appender = connection.appender("link_creations").unwrap();
        let now = now_duration();

        activations_appender
            .append_row(params![now, record_id.did.0.clone(), true])
            .unwrap();
        for link in links {
            links_appender
                .append_row(params![
                    now,
                    record_id.did.0,
                    record_id.collection,
                    record_id.rkey,
                    link.path,
                    link.target.clone().into_string(),
                ])
                .unwrap();
        }
    }

    fn update_links(&mut self, record_id: &RecordId, new_links: &[CollectedLink]) {
        let connection = self.connection.lock().unwrap();
        let mut links_appender = connection.appender("link_creations").unwrap();
        let mut links_resets_appender = connection.appender("record_links_resets").unwrap();

        let now = now_duration();
        // ugly:
        let next_now = now + std::time::Duration::from_secs(1);

        links_resets_appender
            .append_row(params![
                next_now,
                record_id.did.0,
                record_id.collection,
                record_id.rkey
            ])
            .unwrap();

        for link in new_links {
            links_appender
                .append_row(params![
                    next_now,
                    record_id.did.0,
                    record_id.collection,
                    record_id.rkey,
                    link.path,
                    link.target.clone().into_string(),
                ])
                .unwrap();
        }
    }

    fn delete_record(&mut self, record_id: &RecordId) {
        let connection = self.connection.lock().unwrap();
        let mut links_appender = connection.appender("record_deletes").unwrap();
        links_appender
            .append_row(params![
                now_duration(),
                record_id.did.0,
                record_id.collection,
                record_id.rkey
            ])
            .unwrap();
    }

    fn delete_account(&mut self, did: &Did) {
        let connection = self.connection.lock().unwrap();
        let mut activations_appender = connection.appender("account_deletes").unwrap();
        activations_appender
            .append_row(params![now_duration(), did.0])
            .unwrap();
    }

    fn set_account(&mut self, did: &Did, active: bool) {
        let connection = self.connection.lock().unwrap();
        let mut activations_appender = connection.appender("account_state_changes").unwrap();
        activations_appender
            .append_row(params![now_duration(), did.0, active])
            .unwrap();
    }
}

impl LinkStorage for DuckStorage {
    fn push(&mut self, event: &ActionableEvent, _cursor: u64) -> Result<()> {
        match event {
            ActionableEvent::CreateLinks { record_id, links } => {
                self.add_link_creations(record_id, links)
            }
            ActionableEvent::UpdateLinks {
                record_id,
                new_links,
            } => self.update_links(record_id, new_links),
            ActionableEvent::DeleteRecord(record_id) => self.delete_record(record_id),
            ActionableEvent::ActivateAccount(did) => self.set_account(did, true),
            ActionableEvent::DeactivateAccount(did) => self.set_account(did, false),
            ActionableEvent::DeleteAccount(did) => self.delete_account(did),
        }
        Ok(())
    }

    fn to_readable(&mut self) -> impl LinkReader {
        self.clone()
    }
}

impl LinkReader for DuckStorage {
    // all links or only active ones?
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let connection = self.connection.lock().unwrap();
        let links_count : duckdb::Result<u64> = connection.query_row("
            select count(*) from active_links where from_record_did = ? and from_record_collection = ? and from_record_field_path = ?",
            params![target, collection, path], |row| row.get(0));
        Ok(links_count.unwrap_or(0))
    }

    fn get_distinct_did_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let connection = self.connection.lock().unwrap();
        let did_count: u64 = connection.query_row(
            "select count(distinct(from_record_did)) from active_links where to_uri = ?  and from_record_collection = ? and from_record_field_path = ?", 
                params![target, collection, path], |row| row.get(0)).unwrap_or(0);
        Ok(did_count)
    }

    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,         // page size
        until: Option<u64>, // a cursor?
    ) -> anyhow::Result<super::PagedAppendingCollection<crate::RecordId>> {
        let connection = self.connection.lock().unwrap();
        // *trying* to replicate the mem_store semantics, however, we store updates as a delete/create which makes it not always possible.
        let total_count: u64 = connection.query_row(
            "select count(*) from link_creations where to_uri = ?  and from_record_collection = ? and from_record_field_path = ?", 
                params![target, collection, path], |row| row.get(0)).unwrap_or(0);
        let alive_count: u64 = connection.query_row(
                    "select count(*) from active_links where to_uri = ?  and from_record_collection = ? and from_record_field_path = ?", 
                        params![target, collection, path], |row| row.get(0)).unwrap_or(0);

        let cursor = until.unwrap_or(0);
        let mut stmt = connection.prepare("select from_record_did, from_record_collection, from_record_rkey from active_links where to_uri = ?  and from_record_collection = ? and from_record_field_path = ? order by ts desc limit ? offset ?")?;

        let items = stmt
            .query_map(params![target, collection, path, limit, cursor], |row| {
                let did: String = row.get(0)?;
                let collection: String = row.get(1)?;
                let rkey: String = row.get(2)?;
                Ok(RecordId {
                    did: Did(did),
                    collection,
                    rkey,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let next = if items.len() == limit as usize {
            Some(cursor + limit)
        } else {
            None
        };
        Ok(PagedAppendingCollection {
            version: (alive_count, total_count - alive_count),
            items,
            next,
        })
    }

    fn get_distinct_dids(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> anyhow::Result<super::PagedAppendingCollection<crate::Did>> {
        let connection = self.connection.lock().unwrap();
        let total_count: u64 = connection.query_row(
            "select count(distinct(from_record_did)) from link_creations where to_uri = ?  and from_record_collection = ? and from_record_field_path = ?", 
                params![target, collection, path], |row| row.get(0)).unwrap_or(0);

        let alive_count: u64 = connection.query_row(
            "select count(distinct(from_record_did)) from active_links where to_uri = ?  and from_record_collection = ? and from_record_field_path = ?", 
                params![target, collection, path], |row| row.get(0)).unwrap_or(0);

        let cursor = until.unwrap_or(0);
        // ordering by did (?)
        let mut stmt = connection.prepare("select from_record_did, max(ts) as max_ts from active_links where to_uri = ?  and from_record_collection = ? and from_record_field_path = ? group by from_record_did order by max_ts desc limit ? offset ?")?;
        let items = stmt
            .query_map(params![target, collection, path, limit, cursor], |row| {
                let did: String = row.get(0)?;
                Ok(Did(did))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let next = if items.len() == limit as usize {
            Some(cursor + limit)
        } else {
            None
        };

        Ok(PagedAppendingCollection {
            version: (alive_count, total_count - alive_count),
            items,
            next,
        })
    }

    fn get_all_record_counts(
        &self,
        target: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, std::collections::HashMap<String, u64>>>
    {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare("select from_record_collection, from_record_field_path, count(*) from active_links where to_uri = ? group by from_record_collection, from_record_field_path")?;
        let rows = stmt.query_map(params![target], |row| {
            let collection: String = row.get(0)?;
            let path: String = row.get(1)?;
            let count: u64 = row.get(2)?;
            Ok((collection, path, count))
        })?;
        let mut out: HashMap<String, HashMap<String, u64>> = HashMap::new();
        for row in rows {
            let (collection, path, count) = row?;
            let entry = out.entry(collection).or_default();
            entry.insert(path, count);
        }
        Ok(out)
    }

    fn get_all_counts(
        &self,
        target: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<String, std::collections::HashMap<String, crate::CountsByCount>>,
    > {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare("select count(distinct(from_record_did)), from_record_collection, from_record_field_path, count(*) from active_links where to_uri = ? group by from_record_collection, from_record_field_path")?;
        let rows = stmt.query_map(params![target], |row| {
            let unique_dids_count = row.get(0)?;
            let collection: String = row.get(1)?;
            let path: String = row.get(2)?;
            let count: u64 = row.get(3)?;
            Ok((unique_dids_count, collection, path, count))
        })?;
        let mut out: HashMap<String, HashMap<String, CountsByCount>> = HashMap::new();
        for row in rows {
            let (unique_dids_count, collection, path, count) = row?;
            let entry = out.entry(collection).or_default();
            entry.insert(
                path,
                CountsByCount {
                    records: count,
                    distinct_dids: unique_dids_count,
                },
            );
        }
        Ok(out)
    }

    fn get_stats(&self) -> anyhow::Result<super::StorageStats> {
        let connection = self.connection.lock().unwrap();
        let dids: u64 = connection.query_row(
            "select count(distinct(from_record_did)) from link_creations",
            [],
            |row| row.get(0),
        )?;
        let targetables: u64 = connection.query_row(
            "select count(distinct(to_uri)) from link_creations",
            [],
            |row| row.get(0),
        )?;

        let linking_records: u64 = connection.query_row(
            "select count(distinct(from_record_did, from_record_rkey)) from link_creations",
            [],
            |row| row.get(0),
        )?;

        Ok(StorageStats {
            dids,
            targetables,
            linking_records,
        })
    }
}

fn now_duration() -> std::time::Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}
