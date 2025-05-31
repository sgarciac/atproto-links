use anyhow::Result;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use std::thread;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::fmt;

use constellation::consumer::consume;
use constellation::storage::{AtprotoProcessor, DbStorage, MemStorage};

const REQUIRED_ENV_VARS: [&str; 1] = ["DATABASE_URL"];

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(short, long)]
    jetstream: String,

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
    Database,
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

#[dotenvy::load]
fn main() -> Result<()> {
    // Fail early if we can't get the required environment variables.
    REQUIRED_ENV_VARS.iter().for_each(|var| {
        if std::env::var(var).is_err() {
            panic!("Environment variable {var} must be set");
        }
    });

    // install global collector configured based on RUST_LOG env var.
    fmt::init();

    let args = Args::parse();

    info!("starting with storage backend: {:?}...", args.backend);

    let fixture = args.fixture;
    if let Some(ref p) = fixture {
        info!("using fixture at {p:?}...");
    }

    let stream = jetstream_url(&args.jetstream);
    info!("using jetstream server {stream:?}...",);

    let stay_alive = CancellationToken::new();

    match args.backend {
        StorageBackend::Memory => run(MemStorage::new(), fixture, None, stream, stay_alive),
        StorageBackend::Database => run(DbStorage::default(), fixture, None, stream, stay_alive),
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
                info!("ok, shutting down...");
                stay_alive.cancel();
                desperation += 1;
            }
            1.. => panic!("fine, panicking!"),
        }
    })?;

    thread::scope(|s| {
        s.spawn({
            let stay_alive = stay_alive.clone();
            let staying_alive = stay_alive.clone();
            move || {
                if let Err(e) = consume(storage, fixture, stream, staying_alive) {
                    error!("jetstream finished with error: {e}");
                }
                stay_alive.drop_guard();
            }
        });
    });

    info!("adios");

    Ok(())
}
