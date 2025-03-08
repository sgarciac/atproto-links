use crate::EventBatch;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;

pub async fn receive(mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
    loop {
        sleep(Duration::from_secs_f64(0.5)).await;
        if let Some(batch) = receiver.recv().await {
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
    let total_records: usize = records.values().map(|v| v.len()).sum();
    println!(
        "got batch with {total_records} records in {} collections, {} record deletes, {} account removes",
        records.len(),
        record_deletes.len(),
        account_removes.len()
    );
}
