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
    let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);
    let mut connect_retries = 0;
    'outer: loop {
        let stream = WS_URLS[connect_retries % WS_URLS.len()];
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
            let b = match socket.read() {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Text(_)) => {
                    eprintln!("jetstream: unexpected text message, should be binary for compressed (ignoring)");
                    continue;
                }
                Ok(Message::Close(f)) => {
                    println!("jetstream: closing the connection: {f:?}");
                    continue;
                }
                Ok(m) => {
                    eprintln!("jetstream: unexpected from read (ignoring): {m:?}");
                    continue;
                }
                Err(TError::ConnectionClosed) => {
                    // clean exit
                    println!("jetstream closed the websocket cleanly.");
                    break;
                }
                Err(TError::AlreadyClosed) => {
                    // programming error
                    eprintln!(
                        "jetstream: got AlreadyClosed trying to .read() websocket. probably a bug."
                    );
                    break;
                }
                Err(TError::Capacity(e)) => {
                    eprintln!("jetstream: capacity error (ignoring): {e:?}");
                    continue;
                }
                Err(TError::Utf8) => {
                    eprintln!("jetstream: utf8 error (ignoring)");
                    continue;
                }
                Err(e) => {
                    eprintln!("jetstream: could not read message from socket. closing: {e:?}");
                    match socket.close(None) {
                        Err(TError::ConnectionClosed) => {
                            println!("jetstream closed the websocket cleanly.");
                            break;
                        }
                        r => eprintln!("jetstream: close result after error: {r:?}"),
                    }
                    // if we didn't immediately get ConnectionClosed, we should keep polling read
                    // until we get it.
                    continue;
                }
            };
            let mut cursor = Cursor::new(b);
            let mut decoder =
                match zstd::stream::Decoder::with_prepared_dictionary(&mut cursor, &dict) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("jetstream: failed to decompress zstd message: {e:?}");
                        continue;
                    }
                };
            let mut s = String::new();
            if let Err(e) = decoder.read_to_string(&mut s) {
                eprintln!("jetstream: failed to decode zstd: {e:?}");
                continue;
            };

            let v = match s.parse() {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("jetstream: failed to parse message as json: {e:?}");
                    continue;
                }
            };

            if let Err(flume::SendError(_rejected)) = sender.send(v) {
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
