mod jetstream;
mod jsonl_file;

use crate::storage::AtprotoProcessor;
use crate::{ActionableEvent, RecordId};
use anyhow::Result;
use jetstream::consume_jetstream;
use jsonl_file::consume_jsonl_file;
use links::collect_links;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use tinyjson::JsonValue;
use tokio_util::sync::CancellationToken;

pub fn consume(
    mut store: impl AtprotoProcessor,
    qsize: Arc<AtomicU32>,
    fixture: Option<PathBuf>,
    stream: String,
    staying_alive: CancellationToken,
) -> Result<()> {
    let (receiver, consumer_handle) = if let Some(f) = fixture {
        let (sender, receiver) = flume::bounded(21);
        (
            receiver,
            thread::spawn(move || consume_jsonl_file(f, sender)),
        )
    } else {
        let (sender, receiver) = flume::bounded(32_768); // eek
        let cursor = store.get_cursor().unwrap();
        (
            receiver,
            thread::spawn(move || consume_jetstream(sender, cursor, stream, staying_alive)),
        )
    };

    for update in receiver.iter() {
        if let Some((action, ts)) = get_actionable(&update) {
            {
                store.push(&action, ts).unwrap();
                qsize.store(receiver.len().try_into().unwrap(), Ordering::Relaxed);
            }
        }
    }

    consumer_handle.join().unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;
    use links::{CollectedLink, Link};

    #[test]
    fn test_create_like() {
        let rec = r#"{
            "did":"did:plc:icprmty6ticzracr5urz4uum",
            "time_us":1736448492661668,
            "kind":"commit",
            "commit":{"rev":"3lfddpt5qa62c","operation":"create","collection":"app.bsky.feed.like","rkey":"3lfddpt5djw2c","record":{
                "$type":"app.bsky.feed.like",
                "createdAt":"2025-01-09T18:48:10.412Z",
                "subject":{"cid":"bafyreihazf62qvmusup55ojhkzwbmzee6rxtsug3e6eg33mnjrgthxvozu","uri":"at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23"}
            },
            "cid":"bafyreidgcs2id7nsbp6co42ind2wcig3riwcvypwan6xdywyfqklovhdjq"}
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: "did:plc:icprmty6ticzracr5urz4uum".into(),
                        collection: "app.bsky.feed.like".into(),
                        rkey: "3lfddpt5djw2c".into(),
                    },
                    links: vec![CollectedLink {
                    path: ".subject.uri".into(),
                    target: Link::AtUri(
                        "at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23"
                            .into()
                    )
                },],
                },
                1736448492661668
            ))
        )
    }

    #[test]
    fn test_update_profile() {
        let rec = r#"{
            "did":"did:plc:tcmiubbjtkwhmnwmrvr2eqnx",
            "time_us":1736453696817289,"kind":"commit",
            "commit":{
                "rev":"3lfdikw7q772c",
                "operation":"update",
                "collection":"app.bsky.actor.profile",
                "rkey":"self",
                "record":{
                    "$type":"app.bsky.actor.profile",
                    "avatar":{"$type":"blob","ref":{"$link":"bafkreidcg5jzz3hpdtlc7um7w5masiugdqicc5fltuajqped7fx66hje54"},"mimeType":"image/jpeg","size":295764},
                    "banner":{"$type":"blob","ref":{"$link":"bafkreiahaswf2yex2zfn3ynpekhw6mfj7254ra7ly27zjk73czghnz2wni"},"mimeType":"image/jpeg","size":856461},
                    "createdAt":"2024-08-30T21:33:06.945Z",
                    "description":"Professor, QUB | Belfast via Derry \\n\\nViews personal | Reposts are not an endorsement\\n\\nhttps://go.qub.ac.uk/charvey",
                    "displayName":"Colin Harvey",
                    "pinnedPost":{"cid":"bafyreifyrepqer22xsqqnqulpcxzpu7wcgeuzk6p5c23zxzctaiwmlro7y","uri":"at://did:plc:tcmiubbjtkwhmnwmrvr2eqnx/app.bsky.feed.post/3lf66ri63u22t"}
                },
                "cid":"bafyreiem4j5p7duz67negvqarq3s5h7o45fvytevhrzkkn2p6eqdkcf74m"
            }
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::UpdateLinks {
                    record_id: RecordId {
                        did: "did:plc:tcmiubbjtkwhmnwmrvr2eqnx".into(),
                        collection: "app.bsky.actor.profile".into(),
                        rkey: "self".into(),
                    },
                    new_links: vec![CollectedLink {
                    path: ".pinnedPost.uri".into(),
                    target: Link::AtUri(
                        "at://did:plc:tcmiubbjtkwhmnwmrvr2eqnx/app.bsky.feed.post/3lf66ri63u22t"
                            .into()
                    ),
                },],
                },
                1736453696817289
            ))
        )
    }

    #[test]
    fn test_delete_like() {
        let rec = r#"{
            "did":"did:plc:3pa2ss4l2sqzhy6wud4btqsj",
            "time_us":1736448492690783,
            "kind":"commit",
            "commit":{"rev":"3lfddpt7vnx24","operation":"delete","collection":"app.bsky.feed.like","rkey":"3lbiu72lczk2w"}
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::DeleteRecord(RecordId {
                    did: "did:plc:3pa2ss4l2sqzhy6wud4btqsj".into(),
                    collection: "app.bsky.feed.like".into(),
                    rkey: "3lbiu72lczk2w".into(),
                }),
                1736448492690783
            ))
        )
    }

    #[test]
    fn test_delete_account() {
        let rec = r#"{
            "did":"did:plc:zsgqovouzm2gyksjkqrdodsw",
            "time_us":1736451739215876,
            "kind":"account",
            "account":{"active":false,"did":"did:plc:zsgqovouzm2gyksjkqrdodsw","seq":3040934738,"status":"deleted","time":"2025-01-09T19:42:18.972Z"}
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::DeleteAccount("did:plc:zsgqovouzm2gyksjkqrdodsw".into()),
                1736451739215876
            ))
        )
    }

    #[test]
    fn test_deactivate_account() {
        let rec = r#"{
            "did":"did:plc:l4jb3hkq7lrblferbywxkiol","time_us":1736451745611273,"kind":"account","account":{"active":false,"did":"did:plc:l4jb3hkq7lrblferbywxkiol","seq":3040939563,"status":"deactivated","time":"2025-01-09T19:42:22.035Z"}
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::DeactivateAccount("did:plc:l4jb3hkq7lrblferbywxkiol".into()),
                1736451745611273
            ))
        )
    }

    #[test]
    fn test_activate_account() {
        let rec = r#"{
            "did":"did:plc:nct6zfb2j4emoj4yjomxwml2","time_us":1736451747292706,"kind":"account","account":{"active":true,"did":"did:plc:nct6zfb2j4emoj4yjomxwml2","seq":3040940775,"time":"2025-01-09T19:42:26.924Z"}
        }"#.parse().unwrap();
        let action = get_actionable(&rec);
        assert_eq!(
            action,
            Some((
                ActionableEvent::ActivateAccount("did:plc:nct6zfb2j4emoj4yjomxwml2".into()),
                1736451747292706
            ))
        )
    }
}
