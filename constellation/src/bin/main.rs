use anyhow::Result;
use clap::{Parser, ValueEnum};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU32, Arc};
use std::thread;
use std::time;
use tokio::runtime;
use tokio_util::sync::CancellationToken;

use constellation::consumer::consume;
use constellation::server::serve;
#[cfg(feature = "rocks")]
use constellation::storage::RocksStorage;
use constellation::storage::{LinkReader, LinkStorage, MemStorage, StorageStats};

const MONITOR_INTERVAL: time::Duration = time::Duration::from_secs(15);

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(short, long)]
    jetstream: String,
    // TODO: make this part of rocks' own sub-config?
    /// Where to store data on disk, for backends that use disk storage
    #[arg(short, long)]
    data: Option<PathBuf>,
    /// Storage backend to use
    #[arg(short, long)]
    #[clap(value_enum, default_value_t = StorageBackend::Memory)]
    backend: StorageBackend,
    /// Initiate a database backup into this dir, if supported by the storage
    #[arg(long)]
    backup: Option<PathBuf>,
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

fn jetstream_url(provided: &str) -> String {
    match provided {
        "us-east-1" => "wss://jetstream1.us-east.bsky.network/subscribe".into(),
        "us-east-2" => "wss://jetstream2.us-east.bsky.network/subscribe".into(),
        "us-west-1" => "wss://jetstream1.us-west.bsky.network/subscribe".into(),
        "us-west-2" => "wss://jetstream2.us-west.bsky.network/subscribe".into(),
        custom => custom.into(),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("starting with storage backend: {:?}...", args.backend);

    let fixture = args.fixture;
    if let Some(ref p) = fixture {
        println!("using fixture at {p:?}...");
    }

    let stream = jetstream_url(&args.jetstream);
    println!("using jetstream server {stream:?}...",);

    match args.backend {
        StorageBackend::Memory => run(MemStorage::new(), fixture, None, stream),
        #[cfg(feature = "rocks")]
        StorageBackend::Rocks => {
            let storage_dir = args.data.clone().unwrap_or("rocks.test".into());
            println!("starting rocksdb...");
            let rocks = RocksStorage::new(storage_dir)?;
            if let Some(backup_dir) = args.backup {
                rocks.start_backup(backup_dir)?;
            }
            println!("rocks ready.");
            run(rocks, fixture, args.data, stream)
        }
    }
}

fn run(
    mut storage: impl LinkStorage,
    fixture: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    stream: String,
) -> Result<()> {
    let stay_alive = CancellationToken::new();

    ctrlc::set_handler({
        let mut desperation: u8 = 0;
        let stay_alive = stay_alive.clone();
        move || {
            match desperation {
                0 => {
                    println!("ok, shutting down...");
                    stay_alive.cancel();
                }
                1.. => panic!("fine, panicking!"),
            }
            desperation += 1;
        }
    })?;

    let qsize = Arc::new(AtomicU32::new(0));

    thread::scope(|s| {
        let readable = storage.to_readable();

        s.spawn({
            let qsize = qsize.clone();
            let stay_alive = stay_alive.clone();
            let staying_alive = stay_alive.clone();
            move || {
                if let Err(e) = consume(storage, qsize, fixture, stream, staying_alive) {
                    eprintln!("jetstream finished with error: {e}");
                }
                stay_alive.drop_guard();
            }
        });

        s.spawn({
            let readable = readable.clone();
            let stay_alive = stay_alive.clone();
            let staying_alive = stay_alive.clone();
            || {
                runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(2)
                    .enable_all()
                    .build()
                    .expect("axum startup")
                    .block_on(async {
                        install_metrics_server()?;
                        serve(readable, "0.0.0.0:6789", staying_alive).await
                    })
                    .unwrap();
                stay_alive.drop_guard();
            }
        });

        s.spawn(move || { // monitor thread
            let stay_alive = stay_alive.clone();
            let check_alive = stay_alive.clone();

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

            'monitor: loop {
                match readable.get_stats() {
                    Ok(StorageStats { dids, targetables, linking_records }) => {
                        metrics::gauge!("storage.stats.dids").set(dids as f64);
                        metrics::gauge!("storage.stats.targetables").set(targetables as f64);
                        metrics::gauge!("storage.stats.linking_records").set(linking_records as f64);
                    }
                    Err(e) => eprintln!("failed to get stats: {e:?}"),
                }

                process_collector.collect();
                if let Some(ref p) = data_dir {
                    if let Ok(avail) = fs4::available_space(p) {
                        metrics::gauge!("storage.available").set(avail as f64);
                    }
                    if let Ok(free) = fs4::free_space(p) {
                        metrics::gauge!("storage.free").set(free as f64);
                    }
                }
                let wait = time::Instant::now();
                while wait.elapsed() < MONITOR_INTERVAL {
                    thread::sleep(time::Duration::from_millis(100));
                    if check_alive.is_cancelled() {
                        break 'monitor
                    }
                }
            }
            stay_alive.drop_guard();
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
        .set_quantiles(&[0.5, 0.9, 0.99, 1.0])?
        .set_bucket_duration(time::Duration::from_secs(30))?
        .set_bucket_count(NonZero::new(10).unwrap()) // count * duration = 5 mins. stuff doesn't happen that fast here.
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
    use constellation::consumer::get_actionable;
    use constellation::storage::{LinkReader, LinkStorage, MemStorage};

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
