#[macro_use]
extern crate log;
extern crate config;

use influxdb::{Client, Query, Timestamp};
use simplelog::*;
use tokio::stream::StreamExt as _;

#[tokio::main]
async fn main() {
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).unwrap();

    let mut settings = config::Config::default();

    settings
        .merge(config::File::with_name("config"))
        .unwrap()
        .merge(config::Environment::with_prefix("TWITCH_LOG"))
        .unwrap();

    settings
        .set_default("influxdb.url", "http://localhost:8086")
        .unwrap()
        .set_default("influxdb.db", "twitch")
        .unwrap();

    let channels = settings.get_array("twitch.channels").unwrap();

    let client = Client::new(
        settings.get_str("influxdb.url").unwrap(),
        settings.get_str("influxdb.db").unwrap(),
    );

    // make a user config, builder lets you configure it.
    let user_config = twitchchat::UserConfig::builder()
        .anonymous()
        .build()
        .unwrap();

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
    let stream = twitchchat::connect_tls(&user_config).await.unwrap();

    // spawn the run off in another task so we don't block the current one.
    // you could just await on the future at the end of whatever block, but this is easier for this demonstration
    let handle = tokio::task::spawn(runner.run(stream));

    // we can block on the dispatcher for a specific event
    // if we call wait_for again for this event, it'll return the previous one
    info!("waiting for irc ready");
    let ready = dispatcher
        .wait_for::<twitchchat::events::IrcReady>()
        .await
        .unwrap();
    debug!("irc ready: nickname: {}", ready.nickname);

    // we can clone the writer and send it places
    let mut writer = control.writer().clone();

    // because we waited for IrcReady, we can confidently join channels
    for channel in channels {
        writer.join(channel).await.unwrap();
    }

    // a fancy main loop without using tasks
    loop {
        tokio::select! {
            Some(join_msg) = join.next() => {
                info!("{} joined {}", join_msg.name, join_msg.channel);
            }

            Some(part_msg) = part.next() => {
                info!("{} left {}", part_msg.name, part_msg.channel);
            }

            Some(msg) = privmsg.next() => {
                info!("PRIVMSG: [{}] {}: {}", msg.channel, msg.name, msg.data);

                // This creates a query which writes a new measurement into a series called like
                // the channel
                let write_query = Query::write_query(Timestamp::Now, "privmsg")
                    .add_field("channel", msg.channel.clone().into_owned())
                    .add_field("name", msg.name.clone().into_owned())
                    .add_field("mod", msg.is_moderator())
                    .add_field("message", msg.data.clone().into_owned());

                let write_query = match msg.display_name() {
                    Some(n) => write_query.add_tag("display_name", n.clone().into_owned()),
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
    match handle.await.unwrap() {
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
}
