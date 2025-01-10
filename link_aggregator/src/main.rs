mod consumer;
mod jetstream;
mod server;
mod storage;

use anyhow::Result;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use tokio::runtime;

use consumer::consume;
use server::serve;
use storage::MemStorage;

fn main() -> Result<()> {
    println!("starting...");

    let storage = Arc::new(Mutex::new(MemStorage::new()));

    let qsize = Arc::new(AtomicU32::new(0));

    thread::spawn({
        let storage = storage.clone();
        let qsize = qsize.clone();
        move || consume(storage, qsize)
    });

    thread::spawn({
        let storage = storage.clone();
        move || loop {
            {
                storage
                    .lock()
                    .unwrap()
                    .summarize(qsize.load(Ordering::Relaxed));
            }
            thread::sleep(time::Duration::from_secs(3));
        }
    });

    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()?
        .block_on(async { serve(storage, "127.0.0.1:6789").await })?;

    unreachable!();
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
