//! A very basic example of how to listen for create/delete events on a specific DID and NSID.

use atrium_api::{
    record::KnownRecord::AppBskyFeedPost,
    types::string,
};
use clap::Parser;
use jetstream::{
    events::{
        commit::CommitEvent,
        JetstreamEvent::Commit,
    },
    DefaultJetstreamEndpoints,
    JetstreamCompression,
    JetstreamConfig,
    JetstreamConnector,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The DIDs to listen for events on, if not provided we will listen for all DIDs.
    #[arg(short, long)]
    did: Option<Vec<string::Did>>,
    /// The NSID for the collection to listen for (e.g. `app.bsky.feed.post`).
    #[arg(short, long)]
    nsid: string::Nsid,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dids = args.did.unwrap_or_default();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![args.nsid.clone()],
        wanted_dids: dids.clone(),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };

    let jetstream = JetstreamConnector::new(config)?;
    let mut receiver = jetstream.connect().await?;

    println!(
        "Listening for '{}' events on DIDs: {:?}",
        args.nsid.as_str(),
        dids
    );

    while let Some(event) = receiver.recv().await {
        if let Commit(commit) = event {
            match commit {
                CommitEvent::Create { info: _, commit } => {
                    if let AppBskyFeedPost(record) = commit.record {
                        println!(
                            "New post created! ({})\n\n'{}'",
                            commit.info.rkey.as_str(),
                            record.text
                        );
                    }
                }
                CommitEvent::Delete { info: _, commit } => {
                    println!("A post has been deleted. ({})", commit.rkey.as_str());
                }
                _ => {}
            }
        }
    }

    Ok(())
}
