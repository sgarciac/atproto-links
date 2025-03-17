use clap::Parser;
use std::path::PathBuf;
use ufos::{consumer, server, store};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// allow changing jetstream endpoints
    #[arg(long, action)]
    jetstream_force: bool,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
    /// Location to store persist data to disk
    #[arg(long)]
    data: PathBuf,
}

// #[tokio::main]
#[tokio::main(flavor = "current_thread")] // TODO: move this to config via args
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let (storage, cursor) =
        store::Storage::open(args.data, &args.jetstream, args.jetstream_force).await?;

    println!(
        "starting consumer with cursor: {cursor:?} from {:?} ago",
        cursor.clone().map(|c| c.elapsed())
    );
    let batches = consumer::consume(&args.jetstream, cursor, args.jetstream_no_zstd).await?;

    println!("starting server with storage...");
    let serving = server::serve(storage.clone());

    tokio::select! {
        v = serving => eprintln!("serving ended: {v:?}"),
        v = storage.receive(batches) => eprintln!("storage consumer ended: {v:?}"),
        v = storage.rw_loop() => eprintln!("storage rw-loop ended: {v:?}"),
    };

    println!("bye!");

    Ok(())
}
