use anyhow::Result;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use std::sync::{atomic::AtomicU32, Arc};
use std::thread;
use tokio_util::sync::CancellationToken;

use constellation::consumer::consume;
use constellation::storage::{AtprotoProcessor, MemStorage};

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
    /// Start a background task to take backups every N hours
    #[arg(long)]
    backup_interval: Option<u64>,
    /// Keep at most this many backups purging oldest first, requires --backup-interval
    #[arg(long)]
    max_old_backups: Option<usize>,
    /// Saved jsonl from jetstream to use instead of a live subscription
    #[arg(short, long)]
    fixture: Option<PathBuf>,
}

#[derive(Debug, Clone, ValueEnum)]
enum StorageBackend {
    Memory,
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

    let stay_alive = CancellationToken::new();

    match args.backend {
        StorageBackend::Memory => run(MemStorage::new(), fixture, None, stream, stay_alive),
    }
}

fn run(
    storage: impl AtprotoProcessor,
    fixture: Option<PathBuf>,
    _data_dir: Option<PathBuf>,
    stream: String,
    stay_alive: CancellationToken,
) -> Result<()> {
    ctrlc::set_handler({
        let mut desperation: u8 = 0;
        let stay_alive = stay_alive.clone();
        move || match desperation {
            0 => {
                println!("ok, shutting down...");
                stay_alive.cancel();
                desperation += 1;
            }
            1.. => panic!("fine, panicking!"),
        }
    })?;

    let qsize = Arc::new(AtomicU32::new(0));

    thread::scope(|s| {
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
    });

    println!("byeeee");

    Ok(())
}
