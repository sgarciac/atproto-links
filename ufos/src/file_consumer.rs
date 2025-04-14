use crate::consumer::{Batcher, LimitedBatch, BATCH_QUEUE_SIZE};
use anyhow::Result;
use jetstream::{error::JetstreamEventError, events::JetstreamEvent};
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    sync::mpsc::{channel, Receiver, Sender},
};

async fn read_jsonl(f: File, sender: Sender<JetstreamEvent>) -> Result<()> {
    let mut lines = BufReader::new(f).lines();
    while let Some(line) = lines.next_line().await? {
        let event: JetstreamEvent =
            serde_json::from_str(&line).map_err(JetstreamEventError::ReceivedMalformedJSON)?;
        if sender.send(event).await.is_err() {
            log::warn!("All receivers for the jsonl fixture have been dropped, bye.");
            return Err(JetstreamEventError::ReceiverClosedError.into());
        }
    }
    Ok(())
}

pub async fn consume(p: PathBuf) -> Result<Receiver<LimitedBatch>> {
    let f = File::open(p).await?;
    let (jsonl_sender, jsonl_receiver) = channel::<JetstreamEvent>(16);
    let (batch_sender, batch_reciever) = channel::<LimitedBatch>(BATCH_QUEUE_SIZE);
    let mut batcher = Batcher::new(jsonl_receiver, batch_sender);
    tokio::task::spawn(async move { read_jsonl(f, jsonl_sender).await });
    tokio::task::spawn(async move { batcher.run().await });
    Ok(batch_reciever)
}
