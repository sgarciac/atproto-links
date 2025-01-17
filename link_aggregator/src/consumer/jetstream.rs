use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use std::io::{Cursor, Read};
use std::thread;
use std::time;
use tinyjson::JsonValue;
use tungstenite::{Error as TError, Message};
use zstd::dict::DecoderDictionary;

const JETSTREAM_ZSTD_DICTIONARY: &[u8] = include_bytes!("../../zstd/dictionary");

const WS_URLS: [&str; 4] = [
    "wss://jetstream2.us-east.bsky.network/subscribe?compress=true", //&cursor=0",
    "wss://jetstream1.us-east.bsky.network/subscribe?compress=true", //&cursor=0",
    "wss://jetstream1.us-west.bsky.network/subscribe?compress=true", //&cursor=0",
    "wss://jetstream2.us-west.bsky.network/subscribe?compress=true", //&cursor=0",
];

pub fn consume_jetstream(sender: flume::Sender<JsonValue>) -> anyhow::Result<()> {
    describe_counter!(
        "jetstream.connnect",
        Unit::Count,
        "attempts to connect to a jetstream server"
    );
    describe_counter!(
        "jetstream.read",
        Unit::Count,
        "attempts to read an event from jetstream"
    );
    describe_counter!(
        "jetstream.read.fail",
        Unit::Count,
        "failures to read events from jetstream"
    );
    describe_counter!(
        "jetstream.read.bytes",
        Unit::Bytes,
        "total received message bytes from jetstream"
    );
    describe_counter!(
        "jetstream.read.bytes.decompressed",
        Unit::Bytes,
        "total decompressed message bytes from jetstream"
    );
    describe_histogram!(
        "jetstream.read.bytes.decompressed",
        Unit::Bytes,
        "decompressed size of jetstream messages"
    );
    describe_counter!(
        "jetstream.events",
        Unit::Count,
        "valid json messages received"
    );
    describe_gauge!(
        "jetstream.events.queued",
        Unit::Count,
        "event messages waiting in queue"
    );

    let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);
    let mut connect_retries = 0;
    'outer: loop {
        let stream = WS_URLS[connect_retries % WS_URLS.len()];
        counter!("jetstream.connect", "url" => stream, "is_retry" => (connect_retries > 0).to_string()).increment(1);
        println!("jetstream connecting, attempt #{connect_retries}: {stream}...");
        let mut socket = match tungstenite::connect(stream) {
            Ok((socket, _)) => {
                println!("jetstream connected.");
                connect_retries = 0;
                socket
            }
            Err(e) => {
                connect_retries += 1;
                if connect_retries >= 7 {
                    break;
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                eprintln!("jetstream failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue;
            }
        };

        loop {
            if !socket.can_read() {
                eprintln!("jetstream: socket says we cannot read -- flushing then breaking out.");
                if let Err(e) = socket.flush() {
                    eprintln!("error while flushing socket: {e:?}");
                }
                break;
            }

            counter!("jetstream.read").increment(1);
            let b = match socket.read() {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Text(_)) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "received text")
                        .increment(1);
                    eprintln!("jetstream: unexpected text message, should be binary for compressed (ignoring)");
                    continue;
                }
                Ok(Message::Close(f)) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "server closed")
                        .increment(1);
                    println!("jetstream: closing the connection: {f:?}");
                    continue;
                }
                Ok(m) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "unexpected message", "message" => format!("{m:?}")).increment(1);
                    eprintln!("jetstream: unexpected from read (ignoring): {m:?}");
                    continue;
                }
                Err(TError::ConnectionClosed) => {
                    // clean exit
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "clean close")
                        .increment(1);
                    println!("jetstream closed the websocket cleanly.");
                    break;
                }
                Err(TError::AlreadyClosed) => {
                    // programming error
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "already closed")
                        .increment(1);
                    eprintln!(
                        "jetstream: got AlreadyClosed trying to .read() websocket. probably a bug."
                    );
                    break;
                }
                Err(TError::Capacity(e)) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "capacity error")
                        .increment(1);
                    eprintln!("jetstream: capacity error (ignoring): {e:?}");
                    continue;
                }
                Err(TError::Utf8) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "utf8 error")
                        .increment(1);
                    eprintln!("jetstream: utf8 error (ignoring)");
                    continue;
                }
                Err(e) => {
                    eprintln!("jetstream: could not read message from socket. closing: {e:?}");
                    match socket.close(None) {
                        Err(TError::ConnectionClosed) => {
                            counter!("jetstream.read.fail", "url" => stream, "reason" => "clean close").increment(1);
                            println!("jetstream closed the websocket cleanly.");
                            break;
                        }
                        r => eprintln!("jetstream: close result after error: {r:?}"),
                    }
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "read error")
                        .increment(1);
                    // if we didn't immediately get ConnectionClosed, we should keep polling read
                    // until we get it.
                    continue;
                }
            };

            counter!("jetstream.read.bytes", "url" => stream).increment(b.len() as u64);
            let mut cursor = Cursor::new(b);
            let mut decoder = match zstd::stream::Decoder::with_prepared_dictionary(
                &mut cursor,
                &dict,
            ) {
                Ok(d) => d,
                Err(e) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "zstd decompress")
                        .increment(1);
                    eprintln!("jetstream: failed to decompress zstd message: {e:?}");
                    continue;
                }
            };

            let mut s = String::new();
            match decoder.read_to_string(&mut s) {
                Ok(n) => {
                    counter!("jetstream.read.bytes.decompressed", "url" => stream)
                        .increment(n as u64);
                    histogram!("jetstream.read.bytes.decompressed", "url" => stream)
                        .record(n as f64);
                }
                Err(e) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "zstd string decode").increment(1);
                    eprintln!("jetstream: failed to decode zstd: {e:?}");
                    continue;
                }
            }

            let v = match s.parse() {
                Ok(v) => v,
                Err(e) => {
                    counter!("jetstream.read.fail", "url" => stream, "reason" => "json parse")
                        .increment(1);
                    eprintln!("jetstream: failed to parse message as json: {e:?}");
                    continue;
                }
            };

            if let Err(flume::SendError(_rejected)) = sender.send(v) {
                counter!("jetstream.events", "url" => stream).increment(1);
                gauge!("jetstream.events.queued", "url" => stream).set(sender.len() as f64);
                if sender.is_disconnected() {
                    eprintln!("jetstream: send channel disconnected -- nothing to do, bye.");
                    break 'outer;
                }
                eprintln!(
                    "jetstream: failed to send on channel, dropping update! (FIXME / HANDLEME)"
                );
            }
        }
    }
    Err(anyhow::anyhow!("broke out of jetstream loop"))
}
