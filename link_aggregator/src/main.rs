mod consumer;
mod server;
mod storage;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;
use tokio::runtime;
use tokio::sync::oneshot;

use consumer::consume;
use server::serve;
#[cfg(feature = "rocks")]
use storage::RocksStorage;
use storage::{LinkReader, LinkStorage, MemStorage};

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // TODO: make this part of rocks' own sub-config?
    /// Where to store data on disk, for backends that use disk storage
    #[arg(short, long)]
    data: Option<PathBuf>,
    /// Storage backend to use
    #[arg(short, long)]
    #[clap(value_enum, default_value_t = StorageBackend::Memory)]
    backend: StorageBackend,
    /// Saved jsonl from jetstream to use instead of a live subscription
    #[arg(short, long)]
    fixture: Option<PathBuf>,
}

#[derive(Debug, Clone, ValueEnum)]
enum StorageBackend {
    Memory,
    #[cfg(feature = "rocks")]
    Rocks,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("starting with storage backend: {:?}...", args.backend);

    let fixture = args.fixture;
    if let Some(ref p) = fixture {
        println!("using fixture at {p:?}...");
    }

    match args.backend {
        StorageBackend::Memory => run(MemStorage::new(), fixture, None),
        #[cfg(feature = "rocks")]
        StorageBackend::Rocks => {
            let storage_dir = args.data.clone().unwrap_or("rocks.test".into());
            println!("starting rocksdb...");
            let rocks = RocksStorage::new(storage_dir)?;
            println!("rocks ready.");
            run(rocks, fixture, args.data)
        }
    }
}

fn run(
    mut storage: impl LinkStorage,
    fixture: Option<PathBuf>,
    data_dir: Option<PathBuf>,
) -> Result<()> {
    let qsize = Arc::new(AtomicU32::new(0));

    thread::scope(|s| {
        let readable = storage.to_readable();

        let consumer = s.spawn({
            let qsize = qsize.clone();
            move || consume(storage, qsize, fixture)
        });

        let (stop_server, shutdown) = oneshot::channel::<()>();
        s.spawn({
            let readable = readable.clone();
            || {
                runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(2)
                    .enable_all()
                    .build()?
                    .block_on(async {
                        install_metrics_server()?;
                        serve(readable, "0.0.0.0:6789", shutdown).await
                    })
            }
        });

        s.spawn(move || {
            let process_collector = metrics_process::Collector::default();
            process_collector.describe();
            metrics::describe_gauge!(
                "storage_available",
                metrics::Unit::Bytes,
                "available to be allocated"
            );
            metrics::describe_gauge!(
                "storage_free",
                metrics::Unit::Bytes,
                "unused bytes in filesystem"
            );
            if let Some(ref p) = data_dir {
                if let Err(e) = fs4::available_space(p) {
                    eprintln!("fs4 failed to get available space. may not be supported here? space metrics may be absent. e: {e:?}");
                } else {
                    println!("disk space monitoring should work, watching at {p:?}");
                }
            }
            while !consumer.is_finished() {
                readable.summarize(qsize.load(Ordering::Relaxed));
                process_collector.collect();
                if let Some(ref p) = data_dir {
                    if let Ok(avail) = fs4::available_space(p) {
                        metrics::gauge!("storage.available").set(avail as f64);
                    }
                    if let Ok(free) = fs4::free_space(p) {
                        metrics::gauge!("storage.free").set(free as f64);
                    }
                }
                thread::sleep(time::Duration::from_secs(3));
            }
            let _ = stop_server.send(());
        });
    });
    println!("byeeee");

    Ok(())
}

fn install_metrics_server() -> Result<()> {
    println!("installing metrics server...");
    let host = [0, 0, 0, 0];
    let port = 8765;
    PrometheusBuilder::new()
        .set_enable_unit_suffix(true)
        .with_http_listener((host, port))
        .install()?;
    println!(
        "metrics server installed! listening on http://{}.{}.{}.{}:{port}",
        host[0], host[1], host[2], host[3]
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::consumer::get_actionable;
    use crate::storage::{LinkReader, LinkStorage, MemStorage};

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
        let (action, ts) = get_actionable(&rec).unwrap();
        storage.push(&action, ts).unwrap();
        assert_eq!(
            storage
                .get_count(
                    "at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23",
                    "app.bsky.feed.like",
                    ".subject.uri"
                )
                .unwrap(),
            1
        );
    }
}
