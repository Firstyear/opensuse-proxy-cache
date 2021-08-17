mod arc_disk_cache;
mod cache;
mod constants;

use async_std::fs::File;
use async_std::io::prelude::*;
use async_std::io::BufReader as AsyncBufReader;
use async_std::task::Context;
use async_std::task::Poll;
use pin_project_lite::pin_project;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Seek};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tide::log;
use tokio::signal;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use url::Url;

use crate::arc_disk_cache::*;
use crate::cache::*;
use crate::constants::*;

struct AppState {
    cache: Cache,
}

type BoxAsyncBufRead =
    Box<(dyn async_std::io::BufRead + Sync + Unpin + std::marker::Send + 'static)>;

impl AppState {
    fn new(capacity: usize, content_dir: &Path) -> Self {
        AppState {
            cache: Cache::new(capacity, content_dir),
        }
    }
}

pin_project! {
    struct CacheReader {
        dlos_reader: BoxAsyncBufRead,
        io_tx: Sender<Vec<u8>>,
    }
}

impl CacheReader {
    pub fn new(dlos_reader: BoxAsyncBufRead, io_tx: Sender<Vec<u8>>) -> Self {
        CacheReader { dlos_reader, io_tx }
    }
}

impl Read for CacheReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // self.dlos_reader.poll_read(ctx, buf)
        match Pin::new(&mut self.dlos_reader).poll_read(ctx, buf) {
            Poll::Ready(Ok(amt)) => {
                // We don't care if this errors - it won't be written to the cache so we'll
                // try again and correct it.
                let bytes = buf.split_at(amt).0.to_vec();

                if let Err(e) = self.io_tx.blocking_send(bytes) {
                    log::error!("poll_read blocking_send error -> {:?}", e);
                }

                // Write the content of the buffer here into the channel.
                log::debug!("cachereader amt -> {:?}", amt);
                Poll::Ready(Ok(amt))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
        // This is where we need to intercept and capture the bytes streamed.
    }
}

impl async_std::io::BufRead for CacheReader {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Result<&[u8], std::io::Error>> {
        let this = self.project();
        Pin::new(this.dlos_reader).poll_fill_buf(ctx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.dlos_reader).consume(amt)
    }
}

async fn setup_dl(url: Url) -> Result<(surf::Response, tide::ResponseBuilder), tide::Error> {
    // Allow redirects from download.opensuse.org to other locations.
    let client = surf::client().with(surf::middleware::Redirect::new(ALLOW_REDIRECTS));
    let req = surf::get(url);
    let dl_response = client.send(req).await.map_err(|e| {
        log::error!("{:?}", e);
        tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
    })?;

    let status = dl_response.status();
    let content = dl_response.content_type();
    let headers = dl_response.iter();
    // Setup to proxy
    let response = tide::Response::builder(status);

    // Feed in our headers.
    let response = dl_response
        .iter()
        .fold(response, |response, (hk, hv)| response.header(hk, hv));

    // Optionally add content type
    let response = if let Some(cnt) = content {
        response.content_type(cnt)
    } else {
        response
    };

    Ok((dl_response, response))
}

async fn stream(request: tide::Request<Arc<AppState>>, url: Url) -> tide::Result {
    log::info!("🍍  start stream ");
    let (mut dl_response, response) = setup_dl(url).await?;

    let body = dl_response.take_body();
    let reader = body.into_reader();
    let r_body = tide::Body::from_reader(reader, None);

    Ok(response.body(r_body).build())
}

fn write_file(
    mut io_rx: Receiver<Vec<u8>>,
    req_path: String,
    content: Option<tide::http::Mime>,
    mut headers: Vec<(String, String)>,
    request: tide::Request<Arc<AppState>>,
    dir: PathBuf,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
) {
    let mut amt = 0;

    let cnt_amt = headers
        .iter()
        .find_map(|(hv, hk)| {
            if hv == "content-length" {
                usize::from_str(hk).ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    /*
    if cnt_amt == 0 {
        return;
    }
    */

    // Create a tempfile.
    let file = match NamedTempFile::new_in(&dir) {
        Ok(t) => {
            log::info!("🥰  -> {:?}", t.path());
            t
        }
        Err(e) => {
            log::error!("{:?}", e);
            return;
        }
    };

    let mut buf_file = BufWriter::with_capacity(BUFFER_WRITE_PAGE, file);

    while let Some(bytes) = io_rx.blocking_recv() {
        // Path?
        buf_file.write(&bytes);
        amt += bytes.len();
    }

    // Check the content len is ok.
    // We have to check that amt >= cnt_amt (aka cnt_amt < amt)
    // because I think there is a bug in surf that sets the worng length. Wireshark
    // is showing all the lengths as 0.
    if amt == 0 || (cnt_amt != 0 && cnt_amt > amt) {
        log::info!(
            "transfer interuppted, ending - received: {} expect: {}",
            amt,
            cnt_amt
        );
        return;
    }

    if headers
        .iter_mut()
        .find_map(|(hv, hk)| {
            if hv == "content-length" {
                usize::from_str(hk).ok()
            } else {
                None
            }
        })
        .is_none()
    {
        headers.push(("content-length".into(), amt.to_string()))
    };

    let mut file = match buf_file.into_inner() {
        Ok(f) => f,
        Err(e) => {
            log::error!("error processing -> {}, {}", req_path, amt);
            return;
        }
    };

    // Now hash the file.
    file.seek(std::io::SeekFrom::Start(0));
    let mut buf_file = BufReader::with_capacity(BUFFER_READ_PAGE, file);
    let mut hasher = Sha256::new();
    loop {
        match buf_file.fill_buf() {
            Ok(buffer) => {
                let length = buffer.len();
                if length == 0 {
                    // We are done!
                    break;
                } else {
                    // we have content, proceed.
                    hasher.update(&buffer);
                    buf_file.consume(length);
                }
            }
            Err(e) => {
                log::error!("Bufreader error -> {}, {:?}", req_path, e);
                return;
            }
        }
    }
    // Finish the hash
    let hash = hasher.finalize();
    // hex str
    let hash_str = hex::encode(hash);
    log::info!("sha256 {}", hash_str);

    let file = buf_file.into_inner();
    let meta = CacheMeta {
        req_path,
        file,
        headers,
        content,
        amt,
        hash_str,
    };
    // Send the file + metadata to the main cache.
    if let Err(e) = submit_tx.blocking_send(meta) {
        log::error!("Failed to submit to cache channel -> {:?}", e);
    }
}

async fn miss(
    request: tide::Request<Arc<AppState>>,
    url: Url,
    req_path: String,
    dir: PathBuf,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
) -> tide::Result {
    log::info!("❄️   start miss ");
    let (mut dl_response, response) = setup_dl(url).await?;

    // May need to extract some hdrs and content type again from dl_response.
    let content = dl_response.content_type();
    log::info!("cnt -> {:?}", content);
    let headers: Vec<(String, String)> = dl_response
        .iter()
        .filter_map(|(hv, hk)| {
            if hv == "etag"
                || hv == "content-type"
                || hv == "content-length"
                || hv == "last-modified"
            {
                Some((hv.as_str().to_string(), hk.as_str().to_string()))
            } else {
                None
            }
        })
        .collect();
    log::info!("hdr -> {:?}", headers);

    // Create a bounded channel for sending the data to the writer.
    let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);

    // Setup a bg task for writing out the file. Needs the channel rx, the url,
    // and the request from tide to access the AppState. Also needs the hdrs
    // and content type
    let _ = tokio::task::spawn_blocking(move || {
        write_file(
            io_rx, req_path, content, headers, request, dir, submit_tx, cls,
        )
    });

    let body = dl_response.take_body();
    let reader = CacheReader::new(body.into_reader(), io_tx);
    let r_body = tide::Body::from_reader(reader, None);

    Ok(response.body(r_body).build())
}

async fn found(
    request: tide::Request<Arc<AppState>>,
    url: Url,
    obj: Arc<CacheObj>,
) -> tide::Result {
    // We have a hit, with our cache meta! Hooray!
    // Let's setup the response, and then stream from the file!

    let response = tide::Response::builder(tide::StatusCode::Ok);
    // headers
    // let response = meta.
    let response = obj.headers.iter().fold(response, |response, (hv, hk)| {
        response.header(hv.as_str(), hk.as_str())
    });

    let response = if let Some(cnt) = &obj.content {
        response.content_type(cnt.clone())
    } else {
        response
    };

    let n_file = File::open(&obj.path).await.map_err(|e| {
        log::error!("{:?}", e);
        tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
    })?;

    let reader = AsyncBufReader::with_capacity(BUFFER_READ_PAGE, n_file);

    let r_body = tide::Body::from_reader(reader, None);

    Ok(response.body(r_body).build())
}

async fn index_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    log::info!("================================================");
    let mut url = Url::parse("https://download.opensuse.org").expect("Invalid base url");
    // You can alternatelly go to downloadcontent.opensuse.org if you want from the primary mirror.
    let req_url = request.url();
    log::info!("req -> {:?}", req_url);
    let req_path = req_url.path();
    url.set_path(req_path);
    log::info!("dst -> {:?}", url);
    // Now we should check if we have req_path in our cache or not.
    let decision = request.state().cache.decision(req_path);
    let req_path = req_path.to_string();

    // Based on the decision, take an appropriate path.
    match decision {
        CacheDecision::Stream => stream(request, url).await,
        CacheDecision::FoundObj(meta) => found(request, url, meta).await,
        CacheDecision::MissObj(dir, submit_tx, cls) => {
            miss(request, url, req_path, dir, submit_tx, cls).await
        }
        CacheDecision::Refresh | CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::InternalServerError,
            "Invalid Request",
        )),
    }
}

#[tokio::main]
async fn main() {
    log::with_level(tide::log::LevelFilter::Info);

    let tdir = PathBuf::from_str("/private/tmp/osuse_cache").expect("Can't tdir path");

    log::info!("Using -> {:?}", tdir);

    let app_state = Arc::new(AppState::new(4096, &tdir));
    let mut app = tide::with_state(app_state);
    app.with(tide::log::LogMiddleware::new());
    app.at("").get(index_view);
    app.at("/").get(index_view);
    app.at("/*").get(index_view);

    let baddr = if cfg!(debug_assertions) {
        "[::]:8080"
    } else {
        "[::]:8080"
    };

    log::info!("Binding -> http://{}", baddr);

    tokio::spawn(async move {
        let _ = app.listen(baddr).await;
    });

    let _ = signal::ctrl_c().await;
    RUNNING.store(false, Ordering::Relaxed);
}
