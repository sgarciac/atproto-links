use clap::Parser;
use jetstream::events::Cursor;
use std::path::PathBuf;
use ufos::consumer;
use ufos::file_consumer;
use ufos::server;
use ufos::storage::{StorageWhatever, StoreBackground, StoreReader, StoreWriter};
use ufos::storage_fjall::FjallStorage;
use ufos::storage_mem::MemStorage;

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
    /// todo: restore this
    #[arg(long, action)]
    pause_writer: bool,
    /// DEBUG: force the rw loop to fall behind  by pausing it
    /// todo: restore this
    #[arg(long, action)]
    pause_rw: bool,
    /// DEBUG: use an in-memory store instead of fjall
    #[arg(long, action)]
    in_mem: bool,
    /// DEBUG: interpret jetstream as a file fixture
    #[arg(long, action)]
    jetstream_fixture: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let jetstream = args.jetstream.clone();
    if args.in_mem {
        let (read_store, write_store, cursor) = MemStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(
            args.jetstream,
            args.jetstream_fixture,
            args.pause_writer,
            read_store,
            write_store,
            cursor,
        )
        .await?;
    } else {
        let (read_store, write_store, cursor) = FjallStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(
            args.jetstream,
            args.jetstream_fixture,
            args.pause_writer,
            read_store,
            write_store,
            cursor,
        )
        .await?;
    }

    Ok(())
}

async fn go<B: StoreBackground>(
    jetstream: String,
    jetstream_fixture: bool,
    pause_writer: bool,
    read_store: impl StoreReader + 'static,
    mut write_store: impl StoreWriter<B> + 'static,
    cursor: Option<Cursor>,
) -> anyhow::Result<()> {
    println!("starting server with storage...");
    let serving = server::serve(read_store);

    if pause_writer {
        log::info!("not starting jetstream or the write loop.");
        serving.await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(());
    }

    let batches = if jetstream_fixture {
        log::info!("starting with jestream file fixture: {jetstream:?}");
        file_consumer::consume(jetstream.into()).await?
    } else {
        log::info!(
            "starting consumer with cursor: {cursor:?} from {:?} ago",
            cursor.map(|c| c.elapsed())
        );
        consumer::consume(&jetstream, cursor, false).await?
    };

    let rolling = write_store.background_tasks()?.run();
    let storing = write_store.receive_batches(batches);

    // let storing = tokio::task::spawn_blocking(move || {
    //     while let Some(event_batch) = batches.blocking_recv() {
    //         write_store.insert_batch(event_batch)?;
    //         write_store
    //             .step_rollup()
    //             .inspect_err(|e| log::error!("rollup error: {e:?}"))?;
    //     }
    //     Ok::<(), StorageError>(())
    // });

    tokio::select! {
        z = serving => log::warn!("serve task ended: {z:?}"),
        z = rolling => log::warn!("rollup task ended: {z:?}"),
        z = storing => log::warn!("storage task ended: {z:?}"),
    };

    println!("bye!");

    Ok(())
}
