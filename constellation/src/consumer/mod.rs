mod jetstream;

use std::collections::HashSet;

use crate::storage::DbStorage;
use crate::{ActionableEvent, RecordId};
use anyhow::Result;
use diesel_async::RunQueryDsl;
use jetstream::consume_jetstream;
use links::collect_links;
use tinyjson::JsonValue;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub async fn consume(
    mut store: DbStorage,
    stream: String,
    staying_alive: CancellationToken,
) -> Result<()> {
    let (mut receiver, consumer_handle) = {
        let (sender, receiver) = mpsc::channel(100_000_000);
        let cursor = store.get_cursor().await.unwrap();
        (
            receiver,
            tokio::spawn(async move {
                consume_jetstream(sender, cursor, stream, staying_alive)
                    .await
                    .unwrap();
                info!("consumer is done");
            }),
        )
    };

    info!("consumer started");
    let mut collections: HashSet<String> = HashSet::new();

    while let Some(update) = receiver.recv().await {
        info!("queue size: {}", receiver.len());
        if let Some((action, ts)) = get_actionable(&update) {
            if let ActionableEvent::CreateLinks {
                record_id: record_id,
                links: _,
            } = &action
            {
                collections.insert(record_id.collection.clone());
                info!("collection: {}", record_id.collection);
            }
            store.push(&action, ts).await?
        } else {
            // info!("non actionable {:?}", update);
        }
    }

    for collection in collections {
        info!("processing collection: {}", collection);
    }
    consumer_handle.await?;
    info!("consumer is finally done");
    Ok(())
}

pub fn get_actionable(event: &JsonValue) -> Option<(ActionableEvent, u64)> {
    let JsonValue::Object(root) = event else {
        return None;
    };
    let JsonValue::Number(time_us) = root.get("time_us")? else {
        return None;
    };
    let cursor = *time_us as u64;
    // todo: clean up
    match event {
        JsonValue::Object(root)
            if root.get("kind") == Some(&JsonValue::String("commit".to_string())) =>
        {
            let JsonValue::String(did) = root.get("did")? else {
                return None;
            };
            let JsonValue::Object(commit) = root.get("commit")? else {
                return None;
            };
            let JsonValue::String(collection) = commit.get("collection")? else {
                return None;
            };
            let JsonValue::String(rkey) = commit.get("rkey")? else {
                return None;
            };
            match commit.get("operation")? {
                JsonValue::String(op) if op == "create" => {
                    let links = collect_links(commit.get("record")?);
                    if links.is_empty() {
                        None
                    } else {
                        Some((
                            ActionableEvent::CreateLinks {
                                record_id: RecordId {
                                    did: did.into(),
                                    collection: collection.clone(),
                                    rkey: rkey.clone(),
                                },
                                links,
                            },
                            cursor,
                        ))
                    }
                }
                JsonValue::String(op) if op == "update" => {
                    let links = collect_links(commit.get("record")?);
                    Some((
                        ActionableEvent::UpdateLinks {
                            record_id: RecordId {
                                did: did.into(),
                                collection: collection.clone(),
                                rkey: rkey.clone(),
                            },
                            new_links: links,
                        },
                        cursor,
                    ))
                }
                JsonValue::String(op) if op == "delete" => Some((
                    ActionableEvent::DeleteRecord(RecordId {
                        did: did.into(),
                        collection: collection.clone(),
                        rkey: rkey.clone(),
                    }),
                    cursor,
                )),
                _ => None,
            }
        }
        JsonValue::Object(root)
            if root.get("kind") == Some(&JsonValue::String("account".to_string())) =>
        {
            let JsonValue::Object(account) = root.get("account")? else {
                return None;
            };
            let did = account.get("did")?.get::<String>()?.clone();
            match (account.get("active")?.get::<bool>()?, account.get("status")) {
                (true, None) => Some((ActionableEvent::ActivateAccount(did.into()), cursor)),
                (false, Some(JsonValue::String(status))) => match status.as_ref() {
                    "deactivated" => Some((ActionableEvent::DeactivateAccount(did.into()), cursor)),
                    "deleted" => Some((ActionableEvent::DeleteAccount(did.into()), cursor)),
                    // TODO: are we missing handling for suspended and deactivated accounts?
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}
