mod consumer;
mod storage;

// use storage::{LinkStorage, RocksStorage};

fn main() {
    // let _s = RocksStorage::new();
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use crate::consumer::get_actionable;
    use crate::storage::{LinkStorage, MemStorage};

    #[test]
    fn test_create_like_integrated() {
        let mut storage = MemStorage::new();

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
        let action = get_actionable(&rec).unwrap();
        storage.push(&action).unwrap();
        assert_eq!(
            storage
                .get_count(
                    "at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23",
                    ".subject.uri"
                )
                .unwrap(),
            1
        );
    }
}
