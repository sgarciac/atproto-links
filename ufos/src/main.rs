use clap::Parser;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jetstream::{
    events::{commit::CommitEvent, JetstreamEvent::Commit},
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
};

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// Location to store persist data to disk
    #[arg(long)]
    data: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config: JetstreamConfig<serde_json::Value> = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::endpoint_or_shortcut(&args.jetstream),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };

    let jetstream: JetstreamConnector<serde_json::Value> = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    println!("Jetstream ready");

    let print_throttle = Duration::from_millis(400);
    let mut last = Instant::now();
    while let Ok(event) = receiver.recv_async().await {
        if let Commit(CommitEvent::Create { commit, .. }) = event {
            let now = Instant::now();
            let since = now - last;
            if since >= print_throttle {
                let overshoot = since - print_throttle; // adjust to keep the rate on average
                last = now - overshoot;
                println!(
                    "{}: {}",
                    &*commit.info.collection,
                    serde_json::to_string(&commit.record)?
                );
            }
        }
    }

    Ok(())
}
