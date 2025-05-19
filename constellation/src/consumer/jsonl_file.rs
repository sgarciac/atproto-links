use anyhow::{bail, Result};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use tinyjson::JsonValue;
use tracing::error;

pub fn consume_jsonl_file(f: PathBuf, sender: flume::Sender<JsonValue>) -> Result<()> {
    let file = File::open(f)?;
    for line in io::BufReader::new(file).lines().map_while(Result::ok) {
        if let Err(flume::SendError(_rejected)) = sender.send(line.parse()?) {
            if sender.is_disconnected() {
                bail!("fixture: send channel disconnected -- nothing to do, bye.");
            }
            error!("fixture: failed to send on channel, dropping update! (FIXME / HANDLEME)");
        }
    }
    println!("reached end of jsonl file");
    Ok(())
}
