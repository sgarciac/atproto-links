use tinyjson::JsonValue;

use crate::parse_any_link;

pub fn walk_record(path: &str, v: &JsonValue, found: &mut Vec<(String, String)>) {
    match v {
        JsonValue::Object(o) => {
            for (key, child) in o {
                walk_record(&format!("{path}.{key}"), child, found)
            }
        }
        JsonValue::Array(a) => {
            let p = format!("{path}[]");
            for child in a {
                walk_record(&p, child, found)
            }
        }
        JsonValue::String(s) => {
            if let Some(link) = parse_any_link(s) {
                found.push((path.to_string(), link.into_string()));
            }
        }
        _ => {}
    }
}

pub fn collect_links(v: JsonValue) -> Vec<(String, String)> {
    let mut found = vec![];
    walk_record("", &v, &mut found);
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_links() {
        let rec = r#"{"a": "https://example.com", "b": "not a link"}"#;
        let json = collect_links(rec.parse().unwrap());
        assert_eq!(json, vec![(".a".into(), "https://example.com".into())]);
    }

    #[test]
    fn test_bsky_feed_post_record_reply() {
        let rec = r#"{
            "$type": "app.bsky.feed.post",
            "createdAt": "2025-01-08T20:52:43.041Z",
            "langs": [
                "en"
            ],
            "reply": {
                "parent": {
                    "cid": "bafyreifk3bwnmulk37ezrarg4ouheqnhgucypynftqafl4limssogvzk6i",
                    "uri": "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f"
                },
                "root": {
                    "cid": "bafyreifk3bwnmulk37ezrarg4ouheqnhgucypynftqafl4limssogvzk6i",
                    "uri": "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f"
                }
            },
            "text": "Yup!"
        }"#;
        let mut json = collect_links(rec.parse().unwrap());
        json.sort();
        assert_eq!(
            json,
            vec![
                (
                    ".reply.parent.uri".into(),
                    "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f".into()
                ),
                (
                    ".reply.root.uri".into(),
                    "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f".into()
                ),
            ]
        )
    }

    #[test]
    fn test_bsky_feed_post_record_embed() {
        let rec = r#"{
            "$type": "app.bsky.feed.post",
            "createdAt": "2025-01-08T20:52:39.539Z",
            "embed": {
                "$type": "app.bsky.embed.external",
                "external": {
                    "description": "YouTube video by More Perfect Union",
                    "thumb": {
                        "$type": "blob",
                        "ref": {
                            "$link": "bafkreifxuvkbqksq5usi4cryex37o4absjexuouvgenlb62ojsx443b2tm"
                        },
                        "mimeType": "image/jpeg",
                        "size": 477460
                    },
                    "title": "Corporations & Wealthy Elites Are Coopting Our Government. Who Can Stop Them?",
                    "uri": "https://youtu.be/oKXm4szEP1Q?si=_0n_uPu4qNKokMnq"
                }
            },
            "facets": [
                {
                    "features": [
                        {
                            "$type": "app.bsky.richtext.facet#link",
                            "uri": "https://youtu.be/oKXm4szEP1Q?si=_0n_uPu4qNKokMnq"
                        }
                    ],
                    "index": {
                        "byteEnd": 24,
                        "byteStart": 0
                    }
                }
            ],
            "langs": [
                "en"
            ],
            "text": "youtu.be/oKXm4szEP1Q?..."
        }"#;
        let mut json = collect_links(rec.parse().unwrap());
        json.sort();
        assert_eq!(
            json,
            vec![
                (
                    ".embed.external.uri".into(),
                    "https://youtu.be/oKXm4szEP1Q?si=_0n_uPu4qNKokMnq".into()
                ),
                (
                    ".facets[].features[].uri".into(),
                    "https://youtu.be/oKXm4szEP1Q?si=_0n_uPu4qNKokMnq".into()
                ),
            ]
        )
    }
}
