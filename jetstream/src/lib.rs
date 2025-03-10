pub mod error;
pub mod events;
pub mod exports;

use std::{
    io::{
        Cursor as IoCursor,
        Read,
    },
    marker::PhantomData,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use atrium_api::record::KnownRecord;
use futures_util::{
    stream::StreamExt,
    SinkExt,
};
use serde::de::DeserializeOwned;
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{
            channel,
            Receiver,
            Sender,
        },
        Mutex,
    },
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::{
            ClientRequestBuilder,
            IntoClientRequest,
        },
        handshake::client::Request,
        Message,
    },
    MaybeTlsStream,
    WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use zstd::dict::DecoderDictionary;

use crate::{
    error::{
        ConfigValidationError,
        ConnectionError,
        JetstreamEventError,
    },
    events::{
        Cursor,
        JetstreamEvent,
    },
};

/// The Jetstream endpoints officially provided by Bluesky themselves.
///
/// There are no guarantees that these endpoints will always be available, but you are free
/// to run your own Jetstream instance in any case.
pub enum DefaultJetstreamEndpoints {
    /// `jetstream1.us-east.bsky.network`
    USEastOne,
    /// `jetstream2.us-east.bsky.network`
    USEastTwo,
    /// `jetstream1.us-west.bsky.network`
    USWestOne,
    /// `jetstream2.us-west.bsky.network`
    USWestTwo,
}

impl DefaultJetstreamEndpoints {
    /// Helper to reference official jetstream instances by shortcut
    ///
    /// This function will pass through a jetstream endpoint URL unless it matches a shortcut,
    /// in which case it will be rewritten to the corresponding bluesky-operated jetstream endpoint
    /// URL.
    ///
    /// The shortcuts available are
    ///   - 'us-east-1'
    ///   - 'us-east-2'
    ///   - 'us-west-1'
    ///   - 'us-west-2'
    pub fn endpoint_or_shortcut(s: &str) -> String {
        match s {
            "us-east-1" => DefaultJetstreamEndpoints::USEastOne.into(),
            "us-east-2" => DefaultJetstreamEndpoints::USEastTwo.into(),
            "us-west-1" => DefaultJetstreamEndpoints::USWestOne.into(),
            "us-west-2" => DefaultJetstreamEndpoints::USWestTwo.into(),
            custom => custom.into(),
        }
    }
}

impl From<DefaultJetstreamEndpoints> for String {
    fn from(endpoint: DefaultJetstreamEndpoints) -> Self {
        match endpoint {
            DefaultJetstreamEndpoints::USEastOne => {
                "wss://jetstream1.us-east.bsky.network/subscribe".to_owned()
            }
            DefaultJetstreamEndpoints::USEastTwo => {
                "wss://jetstream2.us-east.bsky.network/subscribe".to_owned()
            }
            DefaultJetstreamEndpoints::USWestOne => {
                "wss://jetstream1.us-west.bsky.network/subscribe".to_owned()
            }
            DefaultJetstreamEndpoints::USWestTwo => {
                "wss://jetstream2.us-west.bsky.network/subscribe".to_owned()
            }
        }
    }
}

/// The maximum number of wanted collections that can be requested on a single Jetstream connection.
const MAX_WANTED_COLLECTIONS: usize = 100;
/// The maximum number of wanted DIDs that can be requested on a single Jetstream connection.
const MAX_WANTED_DIDS: usize = 10_000;

/// The custom `zstd` dictionary used for decoding compressed Jetstream messages.
///
/// Sourced from the [official Bluesky Jetstream repo.](https://github.com/bluesky-social/jetstream/tree/main/pkg/models)
const JETSTREAM_ZSTD_DICTIONARY: &[u8] = include_bytes!("../zstd/dictionary");

/// A receiver channel for consuming Jetstream events.
pub type JetstreamReceiver<R> = Receiver<JetstreamEvent<R>>;

/// An internal sender channel for sending Jetstream events to [JetstreamReceiver]'s.
type JetstreamSender<R> = Sender<JetstreamEvent<R>>;

/// A wrapper connector type for working with a WebSocket connection to a Jetstream instance to
/// receive and consume events. See [JetstreamConnector::connect] for more info.
pub struct JetstreamConnector<R: DeserializeOwned> {
    /// The configuration for the Jetstream connection.
    config: JetstreamConfig<R>,
}

pub enum JetstreamCompression {
    /// No compression, just raw plaintext JSON.
    None,
    /// Use the `zstd` compression algorithm, which can result in a ~56% smaller messages on
    /// average. See [here](https://github.com/bluesky-social/jetstream?tab=readme-ov-file#compression) for more info.
    Zstd,
}

impl From<JetstreamCompression> for bool {
    fn from(compression: JetstreamCompression) -> Self {
        match compression {
            JetstreamCompression::None => false,
            JetstreamCompression::Zstd => true,
        }
    }
}

pub struct JetstreamConfig<R: DeserializeOwned = KnownRecord> {
    /// A Jetstream endpoint to connect to with a WebSocket Scheme i.e.
    /// `wss://jetstream1.us-east.bsky.network/subscribe`.
    pub endpoint: String,
    /// A list of collection [NSIDs](https://atproto.com/specs/nsid) to filter events for.
    ///
    /// An empty list will receive events for *all* collections.
    ///
    /// Regardless of desired collections, all subscribers receive
    /// [AccountEvent](events::account::AccountEvent) and
    /// [IdentityEvent](events::identity::Identity) events.
    pub wanted_collections: Vec<exports::Nsid>,
    /// A list of repo [DIDs](https://atproto.com/specs/did) to filter events for.
    ///
    /// An empty list will receive events for *all* repos, which is a lot of events!
    pub wanted_dids: Vec<exports::Did>,
    /// The compression algorithm to request and use for the WebSocket connection (if any).
    pub compression: JetstreamCompression,
    /// User agent string to include with the jetstream connection request
    pub user_agent: Option<String>,
    /// Do not append jetstream client info to user agent string
    pub omit_user_agent_jetstream_info: bool,
    /// Enable automatic cursor for auto-reconnect
    ///
    /// By default, reconnects will never set a cursor for the connection, so a small number of
    /// events will always be dropped.
    ///
    /// If you want gapless playback across reconnects, set this to `true`. If you always want
    /// the latest available events and can tolerate missing some: `false`.
    pub replay_on_reconnect: bool,
    /// Maximum size of send channel for jetstream events.
    ///
    /// If your consuming task can't keep up with every new jetstream event in real-time,
    /// you might get disconnected from the server as a "slow consumer". Increasing channel_size
    /// can help prevent that if your consumer sometimes pauses, at a cost of higher memory
    /// usage while events are buffered.
    pub channel_size: usize,
    /// Marker for record deserializable type.
    ///
    /// See examples/arbitrary_record.rs for an example using serde_json::Value
    ///
    /// You can omit this if you construct `JetstreamConfig { a: b, ..Default::default() }.
    /// If you have to specify it, use `std::marker::PhantomData` with no type parameters.
    pub record_type: PhantomData<R>,
}

impl<R: DeserializeOwned> Default for JetstreamConfig<R> {
    fn default() -> Self {
        JetstreamConfig {
            endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
            wanted_collections: Vec::new(),
            wanted_dids: Vec::new(),
            compression: JetstreamCompression::None,
            user_agent: None,
            omit_user_agent_jetstream_info: false,
            replay_on_reconnect: false,
            channel_size: 4096, // a few seconds of firehose buffer
            record_type: PhantomData,
        }
    }
}

impl<R: DeserializeOwned> JetstreamConfig<R> {
    /// Constructs a new endpoint URL with the given [JetstreamConfig] applied.
    pub fn get_request_builder(
        &self,
    ) -> Result<impl Fn(Option<Cursor>) -> Result<Request, ConnectionError>, ConnectionError> {
        let _: Url = self.endpoint.parse()?; // fail early if the endpoint is invalid

        let did_search_query = self
            .wanted_dids
            .iter()
            .map(|s| ("wantedDids", s.to_string()));

        let collection_search_query = self
            .wanted_collections
            .iter()
            .map(|s| ("wantedCollections", s.to_string()));

        let compression = (
            "compress",
            match self.compression {
                JetstreamCompression::None => "false".to_owned(),
                JetstreamCompression::Zstd => "true".to_owned(),
            },
        );

        let base_params = did_search_query
            .chain(collection_search_query)
            .chain(std::iter::once(compression))
            .collect::<Vec<(&'static str, String)>>();

        let ua_info: Option<String> = if self.omit_user_agent_jetstream_info {
            None
        } else {
            Some(format!(
                "v{} via jetstream-oxide (microcosm/links fork)",
                env!("CARGO_PKG_VERSION")
            ))
        };
        let maybe_ua = match (&self.user_agent, ua_info) {
            (Some(ua), Some(info)) => Some(format!("{ua} {info}")),
            (Some(ua), None) => Some(ua.clone()),
            (None, Some(info)) => Some(info.clone()),
            (None, None) => None,
        };

        let endpoint = self.endpoint.clone();
        Ok(move |maybe_cursor: Option<Cursor>| {
            let mut params = base_params.clone();
            if let Some(ref cursor) = maybe_cursor {
                params.push(("cursor", cursor.to_jetstream()));
            }
            let url = Url::parse_with_params(&endpoint, params)?;

            let mut req = ClientRequestBuilder::new(url.as_str().parse()?);
            if let Some(ua) = &maybe_ua {
                req = req.with_header("user-agent", ua)
            };
            Ok(req.into_client_request()?)
        })
    }

    /// Validates the configuration to make sure it is within the limits of the Jetstream API.
    ///
    /// # Constants
    /// The following constants are used to validate the configuration and should only be changed
    /// if the Jetstream API has itself changed.
    /// - [MAX_WANTED_COLLECTIONS]
    /// - [MAX_WANTED_DIDS]
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        let collections = self.wanted_collections.len();
        let dids = self.wanted_dids.len();

        if collections > MAX_WANTED_COLLECTIONS {
            return Err(ConfigValidationError::TooManyWantedCollections(collections));
        }

        if dids > MAX_WANTED_DIDS {
            return Err(ConfigValidationError::TooManyDids(dids));
        }

        Ok(())
    }
}

impl<R: DeserializeOwned + Send + 'static> JetstreamConnector<R> {
    /// Create a Jetstream connector with a valid [JetstreamConfig].
    ///
    /// After creation, you can call [connect] to connect to the provided Jetstream instance.
    pub fn new(config: JetstreamConfig<R>) -> Result<Self, ConfigValidationError> {
        // We validate the configuration here so any issues are caught early.
        config.validate()?;
        Ok(JetstreamConnector { config })
    }

    /// Connects to a Jetstream instance as defined in the [JetstreamConfig].
    ///
    /// A [JetstreamReceiver] is returned which can be used to respond to events. When all instances
    /// of this receiver are dropped, the connection and task are automatically closed.
    pub async fn connect(&self) -> Result<JetstreamReceiver<R>, ConnectionError> {
        self.connect_cursor(None).await
    }

    /// Connects to a Jetstream instance as defined in the [JetstreamConfig] with playback from a
    /// cursor
    ///
    /// A cursor from the future will result in live-tail operation.
    ///
    /// The cursor is only used for first successfull connection -- on auto-reconnect it will
    /// live-tail by default. Set `replay_on_reconnect: true` in the config if you need to
    /// receive every event, which will keep track of the last-seen cursor and reconnect from
    /// there.
    pub async fn connect_cursor(
        &self,
        cursor: Option<Cursor>,
    ) -> Result<JetstreamReceiver<R>, ConnectionError> {
        // We validate the config again for good measure. Probably not necessary but it can't hurt.
        self.config
            .validate()
            .map_err(ConnectionError::InvalidConfig)?;

        let (send_channel, receive_channel) = channel(self.config.channel_size);
        let replay_on_reconnect = self.config.replay_on_reconnect;
        let build_request = self.config.get_request_builder()?;

        tokio::task::spawn(async move {
            // TODO: maybe return the task handle so we can surface any errors
            let max_retries = 30;
            let base_delay_ms = 1_000; // 1 second
            let max_delay_ms = 30_000; // 30 seconds
            let success_threshold_s = 15; // 15 seconds, retry count is reset if we were connected at least this long

            let mut retry_attempt = 0;
            let mut connect_cursor = cursor;
            loop {
                let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);

                let req = match build_request(connect_cursor.clone()) {
                    Ok(req) => req,
                    Err(e) => {
                        log::error!("Could not build jetstream websocket request: {e:?}");
                        break; // this is always fatal? no retry.
                    }
                };

                let mut last_cursor = connect_cursor.clone();
                retry_attempt += 1;
                if let Ok((ws_stream, _)) = connect_async(req).await {
                    let t_connected = Instant::now();
                    if let Err(e) =
                        websocket_task(dict, ws_stream, send_channel.clone(), &mut last_cursor)
                            .await
                    {
                        log::error!("Jetstream closed after encountering error: {e:?}");
                    } else {
                        log::error!("Jetstream connection closed cleanly");
                    }
                    if t_connected.elapsed() > Duration::from_secs(success_threshold_s) {
                        retry_attempt = 0;
                    }
                }

                if retry_attempt >= max_retries {
                    log::error!("hit max retries, bye");
                    break;
                }

                connect_cursor = if replay_on_reconnect {
                    last_cursor
                } else {
                    None
                };

                if retry_attempt > 0 {
                    // Exponential backoff
                    let delay_ms = base_delay_ms * (2_u64.pow(retry_attempt));
                    log::error!("Connection failed, retrying in {delay_ms}ms...");
                    tokio::time::sleep(Duration::from_millis(delay_ms.min(max_delay_ms))).await;
                    log::info!("Attempting to reconnect...");
                }
            }
            log::error!("Connection retries exhausted. Jetstream is disconnected.");
        });

        Ok(receive_channel)
    }
}

/// The main task that handles the WebSocket connection and sends [JetstreamEvent]'s to any
/// receivers that are listening for them.
async fn websocket_task<R: DeserializeOwned>(
    dictionary: DecoderDictionary<'_>,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    send_channel: JetstreamSender<R>,
    last_cursor: &mut Option<Cursor>,
) -> Result<(), JetstreamEventError> {
    // TODO: Use the write half to allow the user to change configuration settings on the fly.
    let (socket_write, mut socket_read) = ws.split();
    let shared_socket_write = Arc::new(Mutex::new(socket_write));

    let ping_cancellation_token = CancellationToken::new();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
    let ping_cancelled = ping_cancellation_token.clone();
    let ping_shared_socket_write = shared_socket_write.clone();
    tokio::spawn(async move {
        loop {
            ping_interval.tick().await;
            let false = ping_cancelled.is_cancelled() else {
                break;
            };
            if let Err(error) = ping_shared_socket_write
                .lock()
                .await
                .send(Message::Ping("ping".as_bytes().to_vec()))
                .await
            {
                log::error!("Ping failed: {error}");
                break;
            }
        }
        eprintln!("oh this is bad news.");
    });

    let mut closing_connection = false;
    loop {
        match socket_read.next().await {
            Some(Ok(message)) => {
                match message {
                    Message::Text(json) => {
                        let event: JetstreamEvent<R> = serde_json::from_str(&json)
                            .map_err(JetstreamEventError::ReceivedMalformedJSON)?;
                        let event_cursor = event.cursor();

                        if send_channel.send(event).await.is_err() {
                            // We can assume that all receivers have been dropped, so we can close
                            // the connection and exit the task.
                            log::info!(
                                "All receivers for the Jetstream connection have been dropped, closing connection."
                            );
                            closing_connection = true;
                        } else if let Some(v) = last_cursor.as_mut() {
                            *v = event_cursor;
                        }
                    }
                    Message::Binary(zstd_json) => {
                        let mut cursor = IoCursor::new(zstd_json);
                        let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(
                            &mut cursor,
                            &dictionary,
                        )
                        .map_err(JetstreamEventError::CompressionDictionaryError)?;

                        let mut json = String::new();
                        decoder
                            .read_to_string(&mut json)
                            .map_err(JetstreamEventError::CompressionDecoderError)?;

                        let event: JetstreamEvent<R> = serde_json::from_str(&json)
                            .map_err(JetstreamEventError::ReceivedMalformedJSON)?;
                        let event_cursor = event.cursor();

                        if send_channel.send(event).await.is_err() {
                            // We can assume that all receivers have been dropped, so we can close
                            // the connection and exit the task.
                            log::info!(
                                "All receivers for the Jetstream connection have been dropped, closing connection..."
                            );
                            closing_connection = true;
                        } else if let Some(v) = last_cursor.as_mut() {
                            *v = event_cursor;
                        }
                    }
                    Message::Ping(vec) => {
                        log::trace!("Ping recieved, responding");
                        shared_socket_write
                            .lock()
                            .await
                            .send(Message::Pong(vec))
                            .await
                            .map_err(JetstreamEventError::PingPongError)?;
                    }
                    Message::Close(close_frame) => {
                        if let Some(close_frame) = close_frame {
                            let reason = close_frame.reason;
                            let code = close_frame.code;
                            log::trace!("Connection closed. Reason: {reason}, Code: {code}");
                        }
                    }
                    Message::Pong(pong) => {
                        let pong_payload =
                            String::from_utf8(pong).unwrap_or("Invalid payload".to_string());
                        log::trace!("Pong recieved. Payload: {pong_payload}");
                    }
                    Message::Frame(_) => (),
                }
            }
            Some(Err(error)) => {
                log::error!("Web socket error: {error}");
                ping_cancellation_token.cancel();
                closing_connection = true;
            }
            None => {
                log::error!("No web socket result");
                ping_cancellation_token.cancel();
                closing_connection = true;
            }
        }
        if closing_connection {
            _ = shared_socket_write.lock().await.close().await;
            return Ok(());
        }
    }
}
