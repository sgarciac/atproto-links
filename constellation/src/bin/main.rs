use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::fmt;

use constellation::consumer::consume;
use constellation::storage::Controller;

const REQUIRED_ENV_VARS: [&str; 1] = ["DATABASE_URL"];

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(short, long)]
    jetstream: String,

    /// default is false
    #[arg(short, long, default_value_t = false)]
    debug: bool,
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
#[tokio::main]
async fn main() -> Result<()> {
    REQUIRED_ENV_VARS.iter().for_each(|var| {
        if std::env::var(var).is_err() {
            panic!("Environment variable {var} must be set");
        }
    });

    let args = Args::parse();

    let debug = args.debug;

    if debug {
        info!("debug mode enabled");
        console_subscriber::init();
    } else {
        info!("debug mode disabled");
        fmt::init();
    }

    let stream = jetstream_url(&args.jetstream);
    info!("using jetstream server {stream:?}...",);

    let stay_alive = CancellationToken::new();
    run(
        Controller::new(&std::env::var("DATABASE_URL").unwrap()).await,
        None,
        stream,
        stay_alive,
    )
    .await
}

async fn run(
    storage: Controller,
    _data_dir: Option<PathBuf>,
    stream: String,
    stay_alive: CancellationToken,
) -> Result<()> {
    ctrlc::set_handler({
        let mut desperation: u8 = 0;
        let stay_alive = stay_alive.clone();
        move || match desperation {
            0 => {
                info!("Ctl-C'ed. Trying to close everything.");
                stay_alive.cancel();
                desperation += 1;
            }
            1.. => panic!("fine, panicking!"),
        }
    })?;

    let stay_alive = stay_alive.clone();
    if let Err(e) = consume(storage, stream, stay_alive).await {
        error!("jetstream finished with error: {e}");
    }
    //    stay_alive.drop_guard();

    info!("adios");

    Ok(())
}
