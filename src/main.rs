#[macro_use]
extern crate log;
extern crate config;

use influxdb::{Client, Query, Timestamp};
use simplelog::{Config, LevelFilter, TermLogError, TermLogger, TerminalMode};
use snafu::{ResultExt, Snafu};
use tokio::stream::StreamExt as _;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Initializing TermLogger: {}", source))]
    InitTermLogger { source: TermLogError },

    #[snafu(display("Merging config: {}", source))]
    MergingConfig { source: config::ConfigError },

    #[snafu(display(
        "Setting default for config (key: {}, value: {}): {}",
        key,
        value,
        source
    ))]
    SetConfigDefault {
        key: &'static str,
        value: &'static str,
        source: config::ConfigError,
    },

    #[snafu(display("Getting value from config (key: {}): {}", key, source))]
    GetValueFromConfig {
        key: &'static str,
        source: config::ConfigError,
    },

    #[snafu(display("Converting config values to string: {}", source))]
    ConvertChannelsToString { source: config::ConfigError },

    #[snafu(display("Building UserConfig: {}", source))]
    BuildUserConfig { source: twitchchat::UserConfigError },

    #[snafu(display("Connecting to Twitch: {}", source))]
    ConnectToTwitch { source: std::io::Error },

    #[snafu(display("Waiting for IRCREADY from Twitch: {}", source))]
    WaitingForIrcReady { source: twitchchat::Error },

    #[snafu(display("Joining channel (name: {}): {}", name, source))]
    JoinChannel {
        name: String,
        source: twitchchat::Error,
    },

    #[snafu(display("Resolving twitch client: {}", source))]
    ResolvingTwitchClient { source: tokio::task::JoinError },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed)
        .context(InitTermLogger)?;

    let mut settings = config::Config::default();

    settings
        .merge(config::File::with_name("config"))
        .context(MergingConfig)?
        .merge(config::Environment::with_prefix("TWITCH_LOG"))
        .context(MergingConfig)?;

    settings
        .set_default("influxdb.url", "http://localhost:8086")
        .context(SetConfigDefault {
            key: "influxdb.url",
            value: "http:://localhost:8086",
        })?
        .set_default("influxdb.db", "twitch")
        .context(SetConfigDefault {
            key: "influxdb.db",
            value: "twitch",
        })?;

    let channels: Vec<String> = settings
        .get_array("twitch.channels")
        .context(GetValueFromConfig {
            key: "twitch.channels",
        })?
        .iter()
        .map(|v| v.clone().into_str())
        .collect::<Result<Vec<String>, _>>()
        .context(ConvertChannelsToString)?;

    let client = Client::new(
        settings
            .get_str("influxdb.url")
            .context(GetValueFromConfig {
                key: "influxdb.url",
            })?,
        settings
            .get_str("influxdb.db")
            .context(GetValueFromConfig { key: "influxdb.db" })?,
    );

    // make a user config, builder lets you configure it.
    let user_config = twitchchat::UserConfig::builder()
        .anonymous()
        .build()
        .context(BuildUserConfig)?;

    // get dispatcher to subscribe to events
    let dispatcher = twitchchat::Dispatcher::new();

    let mut privmsg = dispatcher.subscribe::<twitchchat::events::Privmsg>();
    // for join (when a user joins a channel)
    let mut join = dispatcher.subscribe::<twitchchat::events::Join>();
    // for part (when a user leaves a channel)
    let mut part = dispatcher.subscribe::<twitchchat::events::Part>();

    // make a new runner
    // control allows you to stop the runner, and gives you access to an async. encoder (writer)
    let (runner, mut control) =
        twitchchat::Runner::new(dispatcher.clone(), twitchchat::RateLimit::default());

    // the conn type is an tokio::io::{AsyncRead + AsyncWrite}
    let stream = twitchchat::connect_tls(&user_config)
        .await
        .context(ConnectToTwitch)?;

    // spawn the run off in another task so we don't block the current one.
    // you could just await on the future at the end of whatever block, but this is easier for this demonstration
    let handle = tokio::task::spawn(runner.run(stream));

    // we can block on the dispatcher for a specific event
    // if we call wait_for again for this event, it'll return the previous one
    info!("waiting for irc ready");
    let ready = dispatcher
        .wait_for::<twitchchat::events::IrcReady>()
        .await
        .context(WaitingForIrcReady)?;
    debug!("irc ready: nickname: {}", ready.nickname);

    // we can clone the writer and send it places
    let mut writer = control.writer().clone();

    // because we waited for IrcReady, we can confidently join channels
    info!("joining {} channels", channels.len());
    for channel in &channels {
        writer.join(channel).await.context(JoinChannel {
            name: channel.to_string(),
        })?;
    }

    // a fancy main loop without using tasks
    loop {
        tokio::select! {
            Some(join_msg) = join.next() => {
                info!("joined {}", join_msg.channel);
            }

            Some(part_msg) = part.next() => {
                info!("left {}", part_msg.channel);
            }

            Some(msg) = privmsg.next() => {
                debug!("PRIVMSG: [{}] {}: {}", msg.channel, msg.name, msg.data);

                let message = msg.data.clone().into_owned().escape_default().to_string();

                // This creates a query which writes a new measurement into a series called like
                // the channel
                let write_query = Query::write_query(Timestamp::Now, "privmsg")
                    .add_field("channel", msg.channel.clone().into_owned())
                    .add_field("name", msg.name.clone().into_owned())
                    .add_field("mod", msg.is_moderator())
                    .add_field("message", message);

                let write_query = match msg.display_name() {
                    Some(n) => write_query.add_tag("display_name", n.clone().into_owned().escape_default().to_string()),
                    None => write_query,
                };

                let write_query = match msg.bits() {
                    Some(b) => write_query.add_tag("bits", b),
                    None => write_query,
                };

                let write_query = match msg.color() {
                    Some(c) => write_query.add_tag("color", c.to_string()),
                    None => write_query,
                };

                let write_query = match msg.room_id() {
                    Some(id) => write_query.add_tag("room_id", id),
                    None =>  write_query,
                };

                let write_query = match msg.user_id() {
                    Some(id) => write_query.add_tag("user_id", id),
                    None => write_query,
                };

                // Submit the query to InfluxDB.
                match client.query(&write_query).await {
                    Ok(_) => {},
                    Err(e) => error!("Write to InfluxDB: {}", e),
                };
            }

            // when the 3 streams in this select are done this'll get hit
            else => { break }
        }
    }

    // await for the client to be done
    // unwrap the JoinHandle
    match handle.await.context(ResolvingTwitchClient)? {
        Ok(twitchchat::Status::Eof) => {
            info!("done!");
        }
        Ok(twitchchat::Status::Canceled) => {
            info!("client was stopped by user");
        }
        Err(err) => {
            error!("error: {}", err);
        }
    }

    dispatcher.clear_subscriptions_all();

    Ok(())
}
