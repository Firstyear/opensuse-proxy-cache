use structopt::StructOpt;
use tracing_forest::prelude::*;

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

mod codec;
mod parser;

use crate::codec::{MemcacheClientMsg, MemcacheCodec, MemcacheServerMsg};

#[instrument(name = "redis-msg")]
async fn handle_msg(
    msg: Option<Result<MemcacheClientMsg, std::io::Error>>,
) -> Option<MemcacheServerMsg> {
    match msg {
        Some(Ok(MemcacheClientMsg::Version)) => {
            debug!("Handling Version");
            Some(MemcacheServerMsg::Version("redis-server 0.1.0".to_string()))
        }
        Some(Err(e)) => {
            error!(?e);
            None
        }
        None => None,
    }
}

async fn client_process<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    mut r: FramedRead<R, MemcacheCodec>,
    mut w: FramedWrite<W, MemcacheCodec>,
    client_address: net::SocketAddr,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    debug!(?client_address, "connect");

    loop {
        tokio::select! {
            Ok(()) = (&mut shutdown_rx).recv() => {
                break;
            }
            res = r.next() => {
                match handle_msg(res).await {
                    Some(rmsg) => {
                        if let Err(e) = w.send(rmsg).await {
                            error!(?e);
                            break;
                        }
                    }
                    None => {
                        debug!(?client_address, "disconnect");
                        break;
                    }
                }
            }
        }
    }
    trace!(?client_address, "client process stopped cleanly.");
}

async fn run_server(
    cache_size: usize,
    cache_path: PathBuf,
    addr: net::SocketAddr,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    info!(%cache_size, ?cache_path, %addr, "Starting with parameters.");

    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Could not bind to redis server address {} -> {:?}", addr, e);
            return;
        }
    };

    let (tx, rx) = broadcast::channel(1);

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
                        // Start the event
                        let (r, w) = tokio::io::split(tcpstream);
                        let r = FramedRead::new(r, MemcacheCodec);
                        let w = FramedWrite::new(w, MemcacheCodec);
                        let c_rx = tx.subscribe();
                        // Let it rip.
                        tokio::spawn(client_process(r, w, client_socket_addr, c_rx));
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
    #[structopt(default_value = "[::1]:8080", env = "BIND_ADDRESS", long = "addr")]
    /// Address to listen to for http
    bind_addr: String,
}

async fn do_main() {
    debug!("Starting");
    let Config {
        cache_size,
        cache_path,
        bind_addr,
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
        run_server(cache_size, cache_path, addr, shutdown_rx).await;
    });

    tokio::select! {
        Ok(()) = tokio::signal::ctrl_c() => {
            info!("Starting shutdown process ...");
            shutdown_tx.send(())
                .expect("Could not send shutdown signal!");
            handle.await;
            info!("Server has stopped!");
            return;
        }
        _ = &mut handle => {
            warn!("Server has unexpectedly stopped!");
            return;
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_forest::builder()
        .pretty()
        .with_writer(std::io::stdout)
        .async_layer()
        .with_env_filter()
        .on_main_future(do_main())
        .await
}

#[cfg(test)]
mod tests {
    use crate::run_server;
    use tokio::sync::oneshot;
    use tracing_forest::prelude::*;

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
            run_server(cache_size, cache_path, addr, shutdown_rx).await;
        });

        // Do the test
        let blocking_task = tokio::task::spawn_blocking(move || {
            let client = redis::Client::open(
                format!("redis://username:password@127.0.0.1:{}/", port).as_str()
            ).expect("failed to launch redis client");

            let mut con = client.get_connection()
                .expect("failed to get connection");

            let v: InfoDict = cmd("INFO").query(&mut con)
                .expect("Failed to get info");

            let r = v.get::<u64>("used_memory");
            error!(?r, "used memory");

            /*
            let h: HashMap<String, usize> = cmd("CONFIG")
                .arg("GET")
                .arg("maxmemory")
                .query_async(&mut c)
                .await?;
            Ok(h.get("maxmemory")
                .and_then(|&s| if s != 0 { Some(s as u64) } else { None }))

            cmd("GET").arg(key).query(con);

            cmd("SET").arg(key).arg(d).query(conn);
            */
        });

        blocking_task.await;

        shutdown_tx.send(());
        handle.await;
    }
}
