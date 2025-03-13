use clap::Parser;
use std::path::PathBuf;
use ufos::{consumer, server, store};

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
    env_logger::init();
    let args = Args::parse();
    let (storage, cursor) = store::Storage::open(&args.data, &args.jetstream)?;

    println!("starting consumer with cursor: {cursor:?}");
    let batches = consumer::consume(&args.jetstream, cursor).await?;

    println!("starting server with storage...");
    let serving = server::serve(storage.clone());

    tokio::select! {
        v = serving => eprintln!("serving ended: {v:?}"),
        v = storage.receive(batches) => eprintln!("storage consumer ended: {v:?}"),
    };

    println!("bye!");

    Ok(())
}
