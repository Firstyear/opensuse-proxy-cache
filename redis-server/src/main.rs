#[macro_use]
extern crate tracing;

use structopt::StructOpt;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_util::codec::{FramedRead, FramedWrite};

use futures::SinkExt;
use futures::StreamExt;

use std::net;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use crate::tracing::Instrument;

use std::io::Read;
use std::sync::Arc;

use arc_disk_cache::ArcDiskCache;

mod codec;
mod parser;

use crate::codec::{RedisClientMsg, RedisCodec, RedisServerMsg};

pub(crate) type CacheT = ArcDiskCache<Vec<u8>, ()>;

async fn client_process<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    mut r: FramedRead<R, RedisCodec>,
    mut w: FramedWrite<W, RedisCodec>,
    client_address: net::SocketAddr,
    mut shutdown_rx: broadcast::Receiver<()>,
    cache: Arc<CacheT>,
) {
    info!(?client_address, "connect");

    'outer: loop {
        tokio::select! {
                Ok(()) = (&mut shutdown_rx).recv() => {
                    break;
                }
                res = r.next() => {
                    let rmsg =
                    match res {
                        None => {
                            info!(?client_address, "disconnect");
                            break;
                        }
                        Some(Err(e)) => {
                            error!(?e);
                            info!(?client_address, "disconnect");
                            break;
                        }
                        Some(Ok(rmsg)) => {
                            rmsg
                        }
                    };

                    match rmsg {
                        RedisClientMsg::Auth(_passwd) => {
                            debug!("Handling Auth");
                            if let Err(e) = w.send(RedisServerMsg::Ok).await {
                                error!(?e);
                                break;
                            }
                        }

                        RedisClientMsg::Info => {
                            debug!("Handling Info");
                            let stats = cache.view_stats();
                            let used_memory = stats.freq + stats.recent;
                            if let Err(e) = w.send(
                            RedisServerMsg::Info { used_memory }
                            ).await {
                                error!(?e);
                                break;
                            }
                        }
                        RedisClientMsg::ConfigGet(skey) => {
                            debug!("Handling Config Get");
                            let v = cache.view_stats().shared_max.to_string();
                            if let Err(e) = w.send(
                            RedisServerMsg::KvPair { k: skey, v }
                            ).await {
                                error!(?e);
                                break;
                            }
                        }

                        RedisClientMsg::Get(key) => {
                            debug!("Handling Get");
                            match cache.get(&key) {
                                Some(cobj) => {
                                    if let Err(e) = w.send(
                                        RedisServerMsg::DataHdr { sz: cobj.fhandle.amt }
                                    ).await {
                                        error!(?e);
                                        break;
                                    };

                                    let mut f = match cobj.fhandle.reopen() {
                                        Ok(f) => f,
                                        Err(e) => {
                                            error!(?e);
                                            break;
                                        }
                                    };
                                    let mut buffer = [0; 8192];

                                    'inner: loop {
                                        if let Ok(n) = f.read(&mut buffer) {
                                            if n == 0 {
                                                break 'inner;
                                            } else {
                                                let (slice, _) = buffer.split_at(n);
                                                if let Err(e) = w.send(
                                                    RedisServerMsg::DataChunk { slice }
                                                ).await {
                                                    error!(?e);
                                                    break 'outer;
                                                };

                                            }
                                        } else {
                                            info!(?client_address, "disconnect");
                                            break 'outer;
                                        }
                                    }

                                    if let Err(e) = w.send(
                                        RedisServerMsg::DataEof
                                    ).await {
                                        error!(?e);
                                        break;
                                    };
                                }
                                None => {
                                    if let Err(e) = w.send(
                                        RedisServerMsg::Null
                                    ).await {
                                        error!(?e);
                                        break;
                                    }
                                }
                            }
                        }
                        RedisClientMsg::Set(key, _dsz, fh) => {
                            debug!("Handling Set");
                            cache.insert(key, (), fh);
                            if let Err(e) = w.send(
                                RedisServerMsg::Null
                            ).await {
                                error!(?e);
                                break;
                            }
                        }
                        RedisClientMsg::ClientSetInfo(_name, _maybe_version) => {
                            debug!("Handling Client SetInfo");
                            if let Err(e) = w.send(RedisServerMsg::Ok).await {
                                error!(?e);
                                break;
                            }
                        }
                        RedisClientMsg::Disconnect => {
                            info!(?client_address, "disconnect");
                            break;
                        }
                    }
            }
        }
        .instrument(tracing::info_span!("client_request"));
    }
    trace!(?client_address, "client process stopped cleanly.");
}

async fn run_server(
    cache_size: usize,
    cache_path: PathBuf,
    durable_fs: bool,
    addr: net::SocketAddr,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    info!(%cache_size, ?cache_path, %addr, "Starting with parameters.");

    // Setup the cache here.
    let cache = match ArcDiskCache::new(cache_size, &cache_path, durable_fs) {
        Ok(l) => Arc::new(l),
        Err(err) => {
            error!(?err, "Could not create Arc Disk Cache");
            return;
        }
    };

    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Could not bind to redis server address {} -> {:?}", addr, e);
            return;
        }
    };

    let (tx, _rx) = broadcast::channel(1);

    trace!("Listening on {:?}", addr);

    loop {
        tokio::select! {
            Ok(()) = &mut shutdown_rx => {
                tx.send(())
                    .expect("Unable to broadcast shutdown!");
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((tcpstream, client_socket_addr)) => {
                        tcpstream.set_nodelay(true);
                        // Start the event
                        let (r, w) = tokio::io::split(tcpstream);
                        let r = FramedRead::new(r, RedisCodec::new(cache.clone()));
                        let w = FramedWrite::new(w, RedisCodec::new(cache.clone()));
                        let c_rx = tx.subscribe();
                        let c_cache = cache.clone();
                        // Let it rip.
                        tokio::spawn(client_process(r, w, client_socket_addr, c_rx, c_cache));
                    }
                    Err(e) => {
                        error!("TCP acceptor error, continuing -> {:?}", e);
                    }
                }
            }
        }
    }

    while tx.receiver_count() > 1 {
        trace!("Waiting for {} tasks", tx.receiver_count());
        sleep(Duration::from_millis(250)).await;
    }
}

#[derive(StructOpt)]
struct Config {
    #[structopt(default_value = "17179869184", env = "CACHE_SIZE")]
    /// Disk size for cache content in bytes. Defaults to 16GiB
    cache_size: usize,
    #[structopt(
        parse(from_os_str),
        default_value = "/var/cache/redis/",
        env = "CACHE_PATH"
    )]
    /// Path where cache content should be stored
    cache_path: PathBuf,
    #[structopt(default_value = "[::1]:6379", env = "BIND_ADDRESS", long = "addr")]
    /// Address to listen to for http
    bind_addr: String,
    #[structopt(short = "Z", long = "durable_fs", env = "DURABLE_FS")]
    /// Is this running on a consistent and checksummed fs? If yes, then we can skip
    /// internal crc32c sums on get().
    durable_fs: bool,
}

async fn do_main() {
    debug!("Starting");
    let Config {
        cache_size,
        cache_path,
        bind_addr,
        durable_fs,
    } = Config::from_args();

    let addr = match net::SocketAddr::from_str(&bind_addr) {
        Ok(a) => a,
        Err(e) => {
            error!(
                "Could not parse redis server address {} -> {:?}",
                bind_addr, e
            );
            return;
        }
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let mut handle = tokio::spawn(async move {
        run_server(cache_size, cache_path, durable_fs, addr, shutdown_rx).await;
    });

    tokio::select! {
        Some(()) = async move {
            let sigterm = tokio::signal::unix::SignalKind::terminate();
            tokio::signal::unix::signal(sigterm).unwrap().recv().await
        } => {}
        Ok(()) = tokio::signal::ctrl_c() => {
        }
        _ = &mut handle => {
            warn!("Server has unexpectedly stopped!");
            return;
        }
    }

    info!("Starting shutdown process ...");
    shutdown_tx
        .send(())
        .expect("Could not send shutdown signal!");
    // Ignore if there is an error from the handler on return.
    let _ = handle.await;
    info!("Server has stopped!");
}

#[tokio::main]
async fn main() {
    tracing_forest::worker_task()
        .set_global(true)
        .build_with(|layer: tracing_forest::ForestLayer<_, _>| {
            use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

            let filter_layer = EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap();

            Registry::default().with(filter_layer).with(layer)
        })
        .on(async { do_main().await })
        .await
}

#[cfg(test)]
mod tests {
    use crate::run_server;
    use tokio::sync::oneshot;

    use std::collections::HashMap;
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU16, Ordering};

    use redis::{cmd, InfoDict};

    static PORT_ALLOC: AtomicU16 = AtomicU16::new(19080);

    #[tokio::test]
    async fn server_it_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let port = PORT_ALLOC.fetch_add(1, Ordering::SeqCst);

        let localhost_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let addr = SocketAddr::new(localhost_v4, port);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let cache_path = PathBuf::from(format!(
            "{}/redis-test/{}/",
            option_env!("CARGO_TARGET_TMPDIR").unwrap_or("/tmp"),
            port
        ));
        let cache_size = 1048576;

        info!(?cache_path);
        fs::remove_dir_all(&cache_path);
        fs::create_dir_all(&cache_path).unwrap();

        let mut handle = tokio::spawn(async move {
            run_server(cache_size, cache_path, false, addr, shutdown_rx).await;
        });

        // Do the test
        let blocking_task = tokio::task::spawn_blocking(move || {
            let client = redis::Client::open(
                format!("redis://username:password@127.0.0.1:{}/", port).as_str(),
            )
            .expect("failed to launch redis client");

            let mut con = client.get_connection().expect("failed to get connection");

            let v: InfoDict = cmd("INFO").query(&mut con).expect("Failed to get info");

            let r = v.get::<u64>("used_memory");
            error!(?r, "used memory");

            let h: HashMap<String, usize> = cmd("CONFIG")
                .arg("GET")
                .arg("maxmemory")
                .query(&mut con)
                .expect("Failed to get config");

            let mm = h
                .get("maxmemory")
                .and_then(|&s| if s != 0 { Some(s as u64) } else { None })
                .expect("Failed to get maxmemory");

            let key = b"test_key";
            let d = b"test_data";

            let d1: Vec<u8> = cmd("GET")
                .arg(key)
                .query(&mut con)
                .expect("Failed to get key");

            let d2: Vec<u8> = cmd("SET")
                .arg(key)
                .arg(d)
                .query(&mut con)
                .expect("Failed to set key");

            let d3: Vec<u8> = cmd("GET")
                .arg(key)
                .query(&mut con)
                .expect("Failed to get key");

            assert!(d3 == d);
            info!("Success!");
        });

        blocking_task.await;

        shutdown_tx.send(());
        handle.await;
    }
}
