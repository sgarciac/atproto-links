use anyhow::{bail, Result};
use std::io::{Cursor, ErrorKind, Read};
use std::net::ToSocketAddrs;
use std::thread;
use std::time;
use tinyjson::JsonValue;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::{debug, error};
use tungstenite::{client::IntoClientRequest, Error as TError, Message};
use zstd::dict::DecoderDictionary;

const JETSTREAM_ZSTD_DICTIONARY: &[u8] = include_bytes!("../../zstd/dictionary");

pub fn consume_jetstream(
    sender: flume::Sender<JsonValue>,
    cursor: Option<u64>,
    stream: String,
    staying_alive: CancellationToken,
) -> Result<()> {
    let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);
    let mut connect_retries = 0;
    let mut latest_cursor = cursor;
    'outer: loop {
        let stream_url = format!(
            "{stream}?compress=true{}",
            latest_cursor
                .map(|c| {
                    info!("starting with cursor from {:?} ago...", ts_age(c));
                    format!("&cursor={c}")
                })
                .unwrap_or("".into())
        );
        let mut req = (&stream_url).into_client_request()?;
        let ua = format!(
            "microcosm/constellation/sgarciac v{}",
            env!("CARGO_PKG_VERSION")
        );
        req.headers_mut().insert("user-agent", ua.parse()?);

        let host = req.uri().host().expect("jetstream request uri has a host");
        let port = req.uri().port().map(|p| p.as_u16()).unwrap_or(443);
        let dest = format!("{host}:{port}");
        let addr = match dest.to_socket_addrs().map(|mut d| d.next()) {
            Ok(Some(a)) => a,
            Ok(None) => {
                error!(
                    "jetstream: could not resolve an address for {dest:?}. retrying after a bit?"
                );
                thread::sleep(time::Duration::from_secs(15));
                continue;
            }
            Err(e) => {
                error!("jetstream failed to resolve address {dest:?}: {e:?} waiting and then retrying...");
                thread::sleep(time::Duration::from_secs(3));
                continue;
            }
        };
        let tcp_stream = match std::net::TcpStream::connect_timeout(
            &addr,
            time::Duration::from_secs(8),
        ) {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "jetstream failed to make tcp connection: {e:?}. (todo: clean up retry logic)"
                );
                connect_retries += 1;
                if connect_retries >= 7 {
                    error!("jetstream: no more connect retries, breaking out.");
                    break;
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                error!("jetstream tcp failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue;
            }
        };
        tcp_stream.set_read_timeout(Some(time::Duration::from_secs(4)))?;
        tcp_stream.set_write_timeout(Some(time::Duration::from_secs(4)))?;

        info!("jetstream connecting, attempt #{connect_retries}, {stream_url:?} with user-agent: {ua:?}");
        let mut socket = match tungstenite::client_tls(req, tcp_stream) {
            Ok((socket, _)) => {
                info!("jetstream connected.");
                // connect_retries = 0; // only reset once we have received a message vvv
                socket
            }
            Err(e) => {
                connect_retries += 1;
                if connect_retries >= 7 {
                    error!("jetstream: no more connect retries, breaking out.");
                    break;
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                error!("jetstream failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue;
            }
        };

        loop {
            debug!("jetstream: read");
            if !socket.can_read() {
                error!("jetstream: socket says we cannot read -- flushing then breaking out.");
                if let Err(e) = socket.flush() {
                    error!("error while flushing socket: {e:?}");
                }
                break;
            }

            if staying_alive.is_cancelled() {
                error!("jetstream: cancelling");
                // TODO: cleanly close the connection?
                break 'outer;
            }

            let b = match socket.read() {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Text(_)) => {
                    error!("jetstream: unexpected text message, should be binary for compressed (ignoring)");
                    continue;
                }
                Ok(Message::Close(_)) => {
                    continue;
                }
                Ok(Message::Ping(bytes)) => {
                    let _ = socket.send(Message::Pong(bytes));
                    continue;
                }
                Ok(m) => {
                    error!("jetstream: unexpected from read (ignoring): {m:?}");
                    continue;
                }
                Err(TError::ConnectionClosed) => {
                    // clean exit
                    info!("jetstream closed the websocket cleanly.");
                    break;
                }
                Err(TError::AlreadyClosed) => {
                    error!(
                        "jetstream: got AlreadyClosed trying to .read() websocket. probably a bug."
                    );
                    break;
                }
                Err(TError::Capacity(e)) => {
                    error!("jetstream: capacity error (ignoring): {e:?}");
                    continue;
                }
                Err(TError::Utf8) => {
                    error!("jetstream: utf8 error (ignoring)");
                    continue;
                }
                Err(e) => {
                    error!("jetstream: could not read message from socket. closing: {e:?}");
                    if let TError::Io(io_err) = e {
                        if matches!(io_err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                            info!("jetstream socket timed out. bailing to reconnect -- should we be trying to close first?");
                            break;
                        }
                    }
                    match socket.close(None) {
                        Err(TError::ConnectionClosed) => {
                            info!("jetstream closed the websocket cleanly.");
                            break;
                        }
                        r => error!("jetstream: close result after error: {r:?}"),
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
                        error!("jetstream: failed to decompress zstd message: {e:?}");
                        continue;
                    }
                };

            let mut s = String::new();
            match decoder.read_to_string(&mut s) {
                Ok(_) => {}
                Err(e) => {
                    error!("jetstream: failed to decode zstd: {e:?}");
                    continue;
                }
            }

            let v = match s.parse() {
                Ok(v) => v,
                Err(e) => {
                    error!("jetstream: failed to parse message as json: {e:?}");
                    continue;
                }
            };

            // bit of a hack to have this here for now...
            let ts = match get_event_time(&v) {
                Some(ts) => ts,
                None => {
                    error!("jetstream: encountered an event without a timestamp: ignoring it.");
                    continue;
                }
            };

            if let Err(flume::SendError(_rejected)) = sender.send(v) {
                if sender.is_disconnected() {
                    error!("jetstream: send channel disconnected -- nothing to do, bye.");
                    bail!("jetstream: send channel disconnected");
                }
                error!("jetstream: failed to send on channel, dropping update! (FIXME / HANDLEME)");
            }

            // only actually update our cursor after we've managed to queue the event
            latest_cursor = Some(ts);

            // great news if we got this far -- might be safe to assume the connection is up.
            connect_retries = 0;
        }
    }
    Ok(())
}

fn get_event_time(v: &JsonValue) -> Option<u64> {
    if let JsonValue::Object(root) = v {
        if let JsonValue::Number(time_us) = root.get("time_us")? {
            return Some(*time_us as u64);
        };
    };
    None
}

fn ts_age(ts: u64) -> time::Duration {
    (time::UNIX_EPOCH + time::Duration::from_micros(ts))
        .elapsed()
        .unwrap_or(time::Duration::from_secs(0)) // saturate zero if ts > our system time
}
