use links::{collect_links, CollectedLink};
use tinyjson::JsonValue;

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

pub fn get_actionable(event: &JsonValue) -> Option<ActionableEvent> {
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
                        Some(ActionableEvent::CreateLinks {
                            did: did.clone(),
                            collection: collection.clone(),
                            rkey: rkey.clone(),
                            links,
                        })
                    }
                }
                JsonValue::String(op) if op == "update" => Some(ActionableEvent::UpdateLinks {
                    did: did.clone(),
                    collection: collection.clone(),
                    rkey: rkey.clone(),
                    new_links: collect_links(commit.get("record")?),
                }),
                JsonValue::String(op) if op == "delete" => Some(ActionableEvent::DeleteRecord {
                    did: did.clone(),
                    collection: collection.clone(),
                    rkey: rkey.clone(),
                }),
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
                (true, None) => Some(ActionableEvent::ActivateAccount { did }),
                (false, Some(JsonValue::String(status))) => match status.as_ref() {
                    "deactivated" => Some(ActionableEvent::DeactivateAccount { did }),
                    "deleted" => Some(ActionableEvent::DeleteAccount { did }),
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
            Some(ActionableEvent::CreateLinks {
                did: "did:plc:icprmty6ticzracr5urz4uum".to_string(),
                collection: "app.bsky.feed.like".to_string(),
                rkey: "3lfddpt5djw2c".to_string(),
                links: vec![CollectedLink {
                    path: ".subject.uri".into(),
                    target:
                        "at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23"
                            .into()
                },],
            })
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
            Some(ActionableEvent::UpdateLinks {
                did: "did:plc:tcmiubbjtkwhmnwmrvr2eqnx".to_string(),
                collection: "app.bsky.actor.profile".to_string(),
                rkey: "self".to_string(),
                new_links: vec![CollectedLink {
                    path: ".pinnedPost.uri".into(),
                    target:
                        "at://did:plc:tcmiubbjtkwhmnwmrvr2eqnx/app.bsky.feed.post/3lf66ri63u22t"
                            .into()
                },],
            })
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
            Some(ActionableEvent::DeleteRecord {
                did: "did:plc:3pa2ss4l2sqzhy6wud4btqsj".to_string(),
                collection: "app.bsky.feed.like".to_string(),
                rkey: "3lbiu72lczk2w".to_string(),
            })
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
            Some(ActionableEvent::DeleteAccount {
                did: "did:plc:zsgqovouzm2gyksjkqrdodsw".to_string(),
            })
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
            Some(ActionableEvent::DeactivateAccount {
                did: "did:plc:l4jb3hkq7lrblferbywxkiol".to_string(),
            })
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
            Some(ActionableEvent::ActivateAccount {
                did: "did:plc:nct6zfb2j4emoj4yjomxwml2".to_string(),
            })
        )
    }
}
