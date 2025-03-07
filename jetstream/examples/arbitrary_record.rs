//! An example of how to listen for create/delete events on a specific DID and potentialy unknown NSID

use atrium_api::types::string;
use clap::Parser;
use jetstream::{
    events::{commit::CommitEvent, JetstreamEvent::Commit},
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The DIDs to listen for events on, if not provided we will listen for all DIDs.
    #[arg(short, long)]
    did: Option<Vec<string::Did>>,
    /// The NSID for the collection to listen for (e.g. `blue.flashes.feed.post`).
    #[arg(short, long)]
    nsid: string::Nsid,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dids = args.did.unwrap_or_default();
    let config: JetstreamConfig<serde_json::Value> = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![args.nsid.clone()],
        wanted_dids: dids.clone(),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };

    let jetstream: JetstreamConnector<serde_json::Value> = JetstreamConnector::new(config)?;
    let receiver = jetstream.connect().await?;

    println!(
        "Listening for '{}' events on DIDs: {:?}",
        args.nsid.to_string(),
        dids,
    );

    while let Ok(event) = receiver.recv_async().await {
        if let Commit(CommitEvent::Create { commit, .. }) = event {
            println!("got record {:?}", commit.record);
        }
    }

    Ok(())
}
