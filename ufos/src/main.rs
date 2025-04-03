use clap::Parser;
use std::path::PathBuf;
use ufos::{consumer, server, storage_fjall};

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
    /// DEBUG: don't start the jetstream consumer or its write loop
    #[arg(long, action)]
    pause_writer: bool,
    /// DEBUG: force the rw loop to fall behind  by pausing it
    #[arg(long, action)]
    pause_rw: bool,
}

// #[tokio::main(flavor = "current_thread")] // TODO: move this to config via args
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let (storage, cursor) =
        storage_fjall::Storage::open(args.data, &args.jetstream, args.jetstream_force).await?;

    println!("starting server with storage...");
    let serving = server::serve(storage.clone());

    let t1 = tokio::task::spawn(async {
        let r = serving.await;
        log::warn!("serving ended with: {r:?}");
    });

    let t2: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::task::spawn({
        let _storage = storage.clone();
        async move {
            if !args.pause_writer {
                println!(
                    "starting consumer with cursor: {cursor:?} from {:?} ago",
                    cursor.clone().map(|c| c.elapsed())
                );
                let batches =
                    consumer::consume(&args.jetstream, cursor, args.jetstream_no_zstd).await?;
                let r = storage.receive(batches).await;
                log::warn!("storage.receive ended with: {r:?}");
            } else {
                log::info!("not starting jetstream or the write loop.");
            }
            Ok(())
        }
    });

    // let t3 = tokio::task::spawn(async move {
    //     if !args.pause_rw {
    //         let r = storage.rw_loop().await;
    //         log::warn!("storage.rw_loop ended with: {r:?}");
    //     } else {
    //         log::info!("not starting rw loop.");
    //     }
    // });

    // tokio::select! {
    //     // v = serving => eprintln!("serving ended: {v:?}"),
    //     v = storage.receive(batches) => eprintln!("storage consumer ended: {v:?}"),
    //     v = storage.rw_loop() => eprintln!("storage rw-loop ended: {v:?}"),
    // };

    log::trace!("tasks running. waiting.");
    t1.await?;
    log::trace!("serve task ended.");
    t2.await??;
    log::trace!("storage receive task ended.");
    // t3.await?;
    // log::trace!("storage rw task ended.");

    println!("bye!");

    Ok(())
}
