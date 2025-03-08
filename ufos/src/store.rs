use crate::EventBatch;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;

pub async fn receive(mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
    loop {
        eprintln!("receive loop, sleeping:");
        sleep(Duration::from_secs_f64(0.5)).await;
        eprintln!("slept.");
        if let Some(batch) = receiver.recv().await {
            eprintln!("got batch");
            summarize(batch)
        } else {
            anyhow::bail!("receive channel closed")
        }
    }
}

fn summarize(batch: EventBatch) {
    let EventBatch {
        records,
        record_deletes,
        account_removes,
    } = batch;
    println!(
        "got batch with {} collections, {} record deletes, {} account removes",
        records.len(),
        record_deletes.len(),
        account_removes.len()
    );
}
