use clap::Parser;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::select;
use tokio_util::sync::CancellationToken;

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

    let stay_alive = CancellationToken::new();

    ctrlc::set_handler({
        let mut desperation: u8 = 0;
        let stay_alive = stay_alive.clone();
        move || match desperation {
            0 => {
                println!("ok, signalling shutdown...");
                stay_alive.cancel();
                desperation += 1;
            }
            1.. => panic!("fine, panicking!"),
        }
    })?;

    let jetstream: JetstreamConnector<serde_json::Value> = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    println!("Jetstream ready");

    let print_throttle = Duration::from_millis(400);
    let mut last = Instant::now();
    loop {
        select! {
            _ = stay_alive.cancelled() => {
                eprintln!("byeeee");
                break
            }
            ev = receiver.recv_async() => {
                match ev {
                    Ok(event) => {
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
                    },
                    Err(e) => {
                        eprintln!("jetstream event error: {e:?}");
                        break
                    }
                }
            }
        }
    }

    Ok(())
}
