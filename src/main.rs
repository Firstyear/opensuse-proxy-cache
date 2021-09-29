mod arc_disk_cache;
mod cache;
mod constants;

#[macro_use]
extern crate lazy_static;

use async_std::fs::File;
use async_std::io::prelude::*;
use async_std::io::BufReader as AsyncBufReader;
use async_std::task::Context;
use async_std::task::Poll;
use pin_project_lite::pin_project;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::{BufRead, BufReader, Seek};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use structopt::StructOpt;
use tempfile::NamedTempFile;
use tide::log;
use tide_openssl::TlsListener;
use tokio::signal;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use url::Url;

use crate::arc_disk_cache::*;
use crate::cache::*;
use crate::constants::*;

struct AppState {
    cache: Cache,
    client: surf::Client,
}

type BoxAsyncBufRead =
    Box<(dyn async_std::io::BufRead + Sync + Unpin + std::marker::Send + 'static)>;

impl AppState {
    fn new(
        capacity: usize,
        content_dir: &Path,
        clob: bool,
        mirror_chain: Option<Url>,
        client: surf::Client,
    ) -> Self {
        AppState {
            cache: Cache::new(capacity, content_dir, clob, mirror_chain),
            client,
        }
    }
}

macro_rules! filter_headers {
    (
        $resp:expr
    ) => {{
        let headers: BTreeMap<String, String> = $resp
            .iter()
            .filter_map(|(hv, hk)| {
                if hv == "etag"
                    || hv == "content-type"
                    || hv == "content-length"
                    || hv == "last-modified"
                    || hv == "expires"
                    || hv == "cache-control"
                {
                    Some((hv.as_str().to_string(), hk.as_str().to_string()))
                } else {
                    log::debug!("discarding -> {}: {}", hv.as_str(), hk.as_str());
                    None
                }
            })
            .collect();
        headers
    }};
}

pin_project! {
    struct CacheDownloader {
        dlos_reader: BoxAsyncBufRead,
        io_tx: Sender<Vec<u8>>,
    }
}

impl CacheDownloader {
    pub fn new(dlos_reader: BoxAsyncBufRead, io_tx: Sender<Vec<u8>>) -> Self {
        CacheDownloader { dlos_reader, io_tx }
    }
}

impl Read for CacheDownloader {
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
                // log::debug!("cachereader amt -> {:?}", amt);
                Poll::Ready(Ok(amt))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
        // This is where we need to intercept and capture the bytes streamed.
    }
}

impl async_std::io::BufRead for CacheDownloader {
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

pin_project! {
    struct CacheReader {
        dlos_reader: BoxAsyncBufRead,
        obj: Arc<CacheObj>,
    }
}

impl CacheReader {
    pub fn new(dlos_reader: BoxAsyncBufRead, obj: Arc<CacheObj>) -> Self {
        CacheReader { dlos_reader, obj }
    }
}

impl Read for CacheReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // self.dlos_reader.poll_read(ctx, buf)
        Pin::new(&mut self.dlos_reader).poll_read(ctx, buf)
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

struct ChannelWriter {
    io_tx: Sender<Vec<u8>>,
}

impl async_std::io::Write for ChannelWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<async_std::io::Result<usize>> {
        let bytes = buf.to_vec();
        match self.io_tx.try_send(bytes) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(TrySendError::Full(_)) => Poll::Pending,
            Err(TrySendError::Closed(_)) => {
                log::error!("poll_write channel -> unexpected close");
                Poll::Ready(Err(async_std::io::Error::new(
                    async_std::io::ErrorKind::Other,
                    "unexpected channel close",
                )))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<async_std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<async_std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn monitor_upstream(client: surf::Client, mirror_chain: Option<Url>) {
    log::info!("Spawning upstream monitor task ...");
    while RUNNING.load(Ordering::Relaxed) {
        let r = if let Some(mc_url) = mirror_chain.as_ref() {
            log::info!("upstream checking -> {}", mc_url.as_str());
            client
                .send(surf::head(mc_url))
                .await
                .map(|resp| {
                    log::info!("upstream check {} -> {:?}", mc_url.as_str(), resp.status());
                    resp.status() == surf::StatusCode::Ok
                })
                .unwrap_or_else(|e| {
                    log::error!("upstream check error {} -> {:?}", mc_url.as_str(), e);
                    false
                })
        } else {
            log::info!("upstream checking -> {:?}", DL_OS_URL.as_str());
            log::info!("upstream checking -> {:?}", MCS_OS_URL.as_str());
            client
                .send(surf::head(DL_OS_URL.as_str()))
                .await
                .map(|resp| {
                    log::info!(
                        "upstream check {} -> {:?}",
                        DL_OS_URL.as_str(),
                        resp.status()
                    );
                    resp.status() == surf::StatusCode::Ok
                        || resp.status() == surf::StatusCode::Forbidden
                })
                .unwrap_or_else(|e| {
                    log::error!("upstream check error {} -> {:?}", DL_OS_URL.as_str(), e);
                    false
                })
                && client
                    .send(surf::head(MCS_OS_URL.as_str()))
                    .await
                    .map(|resp| {
                        log::info!(
                            "upstream check {} -> {:?}",
                            MCS_OS_URL.as_str(),
                            resp.status()
                        );
                        resp.status() == surf::StatusCode::Ok
                            || resp.status() == surf::StatusCode::Forbidden
                    })
                    .unwrap_or_else(|e| {
                        log::error!("upstream check error {} -> {:?}", MCS_OS_URL.as_str(), e);
                        false
                    })
        };

        log::warn!("upstream online -> {}", r);
        UPSTREAM_ONLINE.store(r, Ordering::Relaxed);
        sleep(Duration::from_secs(120)).await;
    }
    log::info!("Stopping upstream monitor task.");
}

async fn setup_dl(
    url: Url,
    metadata: bool,
    client: &surf::Client,
) -> Result<
    (
        surf::Response,
        tide::ResponseBuilder,
        BTreeMap<String, String>,
    ),
    tide::Error,
> {
    log::debug!("setup_dl metadata {}, dst -> {:?}", metadata, url);
    let req = if metadata {
        surf::head(&url)
    } else {
        surf::get(&url)
    };
    let req = req
        .header("User-Agent", "opensuse-proxy-cache")
        .header("X-ZYpp-AnonymousId", "dd27909d-1c87-4640-b006-ef604d302f92");

    let dl_response = client.send(req).await.map_err(|e| {
        log::error!("dl_response error - {:?} -> {:?}", url, e);
        tide::Error::from_str(tide::StatusCode::BadGateway, "BadGateway")
    })?;

    let status = dl_response.status();
    let content = dl_response.content_type();
    // let headers = dl_response.iter();
    // filter the headers we send through.
    let headers = filter_headers!(dl_response);

    log::debug!("👉  orig headers -> {:?}", headers);
    // Setup to proxy
    let response = tide::Response::builder(status);

    // Feed in our headers.
    let response = headers.iter().fold(response, |response, (hk, hv)| {
        response.header(hk.as_str(), hv)
    });

    // Optionally add content type
    let response = if let Some(cnt) = content {
        response.content_type(cnt)
    } else {
        response
    };

    Ok((dl_response, response, headers))
}

async fn stream(request: tide::Request<Arc<AppState>>, url: Url, metadata: bool) -> tide::Result {
    if metadata {
        log::info!("🍍  start stream -> HEAD {}", url.as_str());
    } else {
        log::info!("🍍  start stream -> GET {}", url.as_str());
    }
    let (mut dl_response, response, _headers) =
        setup_dl(url, metadata, &request.state().client).await?;

    let r_body = if metadata {
        tide::Body::empty()
    } else {
        let body = dl_response.take_body();
        let reader = body.into_reader();
        tide::Body::from_reader(reader, None)
    };

    Ok(response.body(r_body).build())
}

fn write_file(
    mut io_rx: Receiver<Vec<u8>>,
    req_path: String,
    content: Option<tide::http::Mime>,
    mut headers: BTreeMap<String, String>,
    dir: PathBuf,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
) {
    let mut amt = 0;

    let cnt_amt = headers
        .remove("content-length")
        .and_then(|hk| usize::from_str(&hk).ok())
        .unwrap_or(0);

    // Create a tempfile.
    let file = match NamedTempFile::new_in(&dir) {
        Ok(t) => {
            log::debug!("🥰  -> {:?}", t.path());
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
        if let Err(e) = buf_file.write(&bytes) {
            log::error!("Error writing to tempfile -> {:?}", e);
            return;
        }
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

    if cnt_amt != 0 {
        // If zero, already removed above.
        headers.insert("content-length".to_string(), amt.to_string());
    }

    let mut file = match buf_file.into_inner() {
        Ok(f) => f,
        Err(e) => {
            log::error!("error processing -> {}, {} -> {:?}", req_path, amt, e);
            return;
        }
    };

    // Now hash the file.
    if let Err(e) = file.seek(std::io::SeekFrom::Start(0)) {
        log::error!("Unable to seek tempfile -> {:?}", e);
        return;
    };
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
    log::debug!("sha256 {}", hash_str);

    // event time
    let etime = time::OffsetDateTime::now_utc();
    let etag = headers.get("etag").cloned();

    let file = buf_file.into_inner();
    let meta = CacheMeta {
        req_path,
        etime,
        action: Action::Submit {
            file,
            headers,
            content,
            etag,
            amt,
            hash_str,
            cls,
        },
    };
    // Send the file + metadata to the main cache.
    if let Err(e) = submit_tx.blocking_send(meta) {
        log::error!("Failed to submit to cache channel -> {:?}", e);
    }
}

async fn missing() -> tide::Result {
    log::info!("👻  start force missing");

    let response = tide::Response::builder(tide::StatusCode::NotFound);
    let r_body = tide::Body::empty();

    Ok(response.body(r_body).build())
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
    let (mut dl_response, response, headers) =
        setup_dl(url, false, &request.state().client).await?;

    // May need to extract some hdrs and content type again from dl_response.
    let content = dl_response.content_type();
    log::debug!("cnt -> {:?}", content);
    log::info!("hdr -> {:?}", headers);

    let status = dl_response.status();

    // TODO: We need a way to send in meta - only notfounds.?

    let r_body = if status == surf::StatusCode::Ok {
        // Create a bounded channel for sending the data to the writer.
        let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);

        // Setup a bg task for writing out the file. Needs the channel rx, the url,
        // and the request from tide to access the AppState. Also needs the hdrs
        // and content type
        let _ = tokio::task::spawn_blocking(move || {
            write_file(io_rx, req_path, content, headers, dir, submit_tx, cls)
        });

        let body = dl_response.take_body();
        let reader = CacheDownloader::new(body.into_reader(), io_tx);
        tide::Body::from_reader(reader, None)
    } else if status == surf::StatusCode::NotFound {
        log::info!("👻  rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path,
                etime,
                action: Action::NotFound,
            })
            .await;
        tide::Body::empty()
    } else {
        log::error!(
            "Response returned {:?}, aborting miss to stream -> {}",
            status,
            req_path
        );
        let body = dl_response.take_body();
        let reader = body.into_reader();
        tide::Body::from_reader(reader, None)
    };

    Ok(response.body(r_body).build())
}

async fn found(obj: Arc<CacheObj>, metadata: bool, range: Option<&String>) -> tide::Result {
    // We have a hit, with our cache meta! Hooray!
    // Let's setup the response, and then stream from the file!
    let range = range.and_then(|sr| {
        if sr.starts_with("bytes=") {
            sr.strip_prefix("bytes=")
                .and_then(|v| v.split_once('-'))
                .and_then(|(range_str, _)| u64::from_str_radix(range_str, 10).ok())
        } else {
            None
        }
    });

    log::info!(
        "🔥  start found -> {:?} : range: {:?}",
        obj.fhandle.path,
        range
    );
    // Can we satisfy the range request, if any?

    let amt: u64 = obj.fhandle.amt as u64;

    let response = if metadata {
        tide::Response::builder(tide::StatusCode::Ok)
            .body(tide::Body::empty())
            .header("content-length", format!("{}", amt).as_str())
    } else {
        let mut n_file = File::open(&obj.fhandle.path).await.map_err(|e| {
            log::error!("{:?}", e);
            tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
        })?;

        let response = if let Some(start) = range {
            // If some clients already have the file, they'll send the byte range like this, so we
            // just ignore it and send the file again.
            if start == amt {
                tide::Response::builder(tide::StatusCode::Ok)
                    .header("content-length", format!("{}", amt).as_str())
            } else {
                if let Err(e) = n_file.seek(async_std::io::SeekFrom::Start(start)).await {
                    log::error!("Range not satisfiable -> {:?}", e);
                    return Err(tide::Error::from_str(
                        tide::StatusCode::RequestedRangeNotSatisfiable,
                        "RequestedRangeNotSatisfiable",
                    ));
                } else {
                    tide::Response::builder(tide::StatusCode::PartialContent)
                        .header(
                            "content-range",
                            format!("bytes {}-{}/{}", start, amt - 1, amt).as_str(),
                        )
                        .header("content-length", format!("{}", amt - start).as_str())
                }
            }
        } else {
            tide::Response::builder(tide::StatusCode::Ok)
                .header("content-length", format!("{}", amt).as_str())
        };

        let reader = CacheReader::new(
            Box::new(AsyncBufReader::with_capacity(BUFFER_READ_PAGE, n_file)),
            obj.clone(),
        );

        response.body(tide::Body::from_reader(reader, None))
    };

    // headers
    // let response = meta.
    let response = obj
        .headers
        .iter()
        .filter(|(hv, _)| hv.as_str() != "content-length")
        .fold(response, |response, (hv, hk)| {
            response.header(hv.as_str(), hk.as_str())
        });

    let response = if let Some(cnt) = &obj.fhandle.content {
        response.content_type(cnt.clone())
    } else {
        response
    };

    Ok(response.build())
}

async fn refresh(
    request: &tide::Request<Arc<AppState>>,
    url: Url,
    // req_path: String,
    // dir: PathBuf,
    // submit_tx: Sender<CacheMeta>,
    obj: &CacheObj,
) -> bool {
    log::info!("💸  start refresh ");
    // If we don't have an etag and/or last mod, treat as miss.
    // If we don't have a content-len we may have corrupt content,
    // so force the refresh.

    // First do a head request.
    let (_dl_response, _response, headers) =
        match setup_dl(url, false, &request.state().client).await {
            Ok(r) => r,
            Err(e) => {
                log::error!("dl error -> {:?}", e);
                // We need to proceed.
                return false;
            }
        };

    let etag: Option<String> = headers.get("etag").cloned();
    let content_len: Option<String> = headers.get("content-length").cloned();

    log::debug!("etag -> {:?}", etag);
    if content_len.is_some() && etag.is_some() && etag == obj.etag {
        // No need to refresh, continue.
        false
    } else {
        // No etag present from head request. Assume we need to refresh.
        true
    }
}

fn prefetch(
    request: &tide::Request<Arc<AppState>>,
    url: &Url,
    submit_tx: &Sender<CacheMeta>,
    dir: &PathBuf,
    prefetch_paths: Option<Vec<(String, Classification)>>,
) {
    if let Some(prefetch) = prefetch_paths {
        for (path, cls) in prefetch.into_iter() {
            let u = url.clone();
            let tx = submit_tx.clone();
            let c = request.state().client.clone();
            let dir = dir.clone();
            tokio::spawn(async move { prefetch_task(c, u, tx, path, dir, cls).await });
        }
    }
}

async fn prefetch_task(
    client: surf::Client,
    mut url: Url,
    submit_tx: Sender<CacheMeta>,
    req_path: String,
    dir: PathBuf,
    cls: Classification,
) {
    log::info!("🚅  start prefetch {}", req_path);

    url.set_path(&req_path);
    let req = surf::get(url);

    let mut dl_response = match client.send(req).await {
        Ok(d) => d,
        Err(e) => {
            log::error!("dl_response error - {:?}", e);
            return;
        }
    };

    let status = dl_response.status();
    if status == surf::StatusCode::NotFound {
        log::info!("👻  prefetch rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path,
                etime,
                action: Action::NotFound,
            })
            .await;
        return;
    } else if status != surf::StatusCode::Ok {
        log::error!("Response returned {:?}, aborting prefetch", status);
        return;
    }

    let content = dl_response.content_type();
    let headers = filter_headers!(dl_response);

    let body = dl_response.take_body();
    let mut byte_reader = body.into_reader();

    let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);
    let _ = tokio::task::spawn_blocking(move || {
        write_file(io_rx, req_path, content, headers, dir, submit_tx, cls)
    });

    // https://docs.rs/async-std/1.9.0/async_std/io/fn.copy.html
    // could change to this for large content.
    let mut channel_write = ChannelWriter { io_tx };

    if let Err(e) = async_std::io::copy(&mut byte_reader, &mut channel_write).await {
        log::error!("prefetch async_std::io::copy error -> {:?}", e);
    }
    // That's it!
}

async fn head_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    let req_url = request.url();
    log::debug!("req -> {:?}", req_url);

    let headers: BTreeMap<String, String> = request
        .iter()
        .map(|(hv, hk)| (hv.as_str().to_string(), hk.as_str().to_string()))
        .collect();
    log::info!("request_headers -> {:?}", headers);

    let req_path = req_url.path();
    // Now we should check if we have req_path in our cache or not.
    let decision = request.state().cache.decision(req_path, true);
    // Based on the decision, take an appropriate path. Generally with head reqs
    // we try to stream this if we don't have it, and we prefetch in the BG.
    match decision {
        CacheDecision::Stream(url) => stream(request, url, true).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, true, None).await,
        CacheDecision::Refresh(url, dir, submit_tx, _, prefetch_paths)
        | CacheDecision::MissObj(url, dir, submit_tx, _, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(&request, &url, &submit_tx, &dir, prefetch_paths);
            // Now we just stream.
            stream(request, url, true).await
        }
        CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::BadRequest,
            "Invalid Request",
        )),
    }
}

async fn get_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    let req_url = request.url();
    log::debug!("req -> {:?}", req_url);

    let headers: BTreeMap<String, String> = request
        .iter()
        .map(|(hv, hk)| (hv.as_str().to_string(), hk.as_str().to_string()))
        .collect();
    log::info!("request_headers -> {:?}", headers);

    let req_path = req_url.path();
    // Now we should check if we have req_path in our cache or not.
    let decision = request.state().cache.decision(req_path, false);
    let req_path = req_path.to_string();

    // Based on the decision, take an appropriate path.
    match decision {
        CacheDecision::Stream(url) => stream(request, url, false).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, false, headers.get("range")).await,
        CacheDecision::MissObj(url, dir, submit_tx, cls, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(&request, &url, &submit_tx, &dir, prefetch_paths);
            miss(request, url, req_path, dir, submit_tx, cls).await
        }
        CacheDecision::Refresh(url, dir, submit_tx, meta, prefetch_paths) => {
            // Do a head req - on any error, stream what we have if possible.
            // if head etag OR last update match, serve what we have.
            // else follow the miss path.
            log::debug!("prefetch {:?}", prefetch_paths);
            if refresh(&request, url.clone(), meta.as_ref()).await {
                log::info!("👉  refresh required");
                // Submit all our BG prefetch reqs
                prefetch(&request, &url, &submit_tx, &dir, prefetch_paths);
                miss(request, url, req_path, dir, submit_tx, meta.cls).await
            } else {
                log::info!("👉  cache valid");
                let etime = time::OffsetDateTime::now_utc();
                // If we can't submit, we are probably shutting down so just finish up cleanly.
                // That's why we ignore these errors.
                //
                // If this item is valid we can update all the related prefetch items.
                let _ = submit_tx
                    .send(CacheMeta {
                        req_path,
                        etime,
                        action: Action::Update,
                    })
                    .await;
                if let Some(pre) = prefetch_paths {
                    for p in pre.into_iter() {
                        let _ = submit_tx
                            .send(CacheMeta {
                                req_path: p.0,
                                etime,
                                action: Action::Update,
                            })
                            .await;
                    }
                }
                found(meta, false, headers.get("range")).await
            }
        }
        CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::BadRequest,
            "Invalid Request",
        )),
    }
}

#[derive(StructOpt)]
struct Config {
    #[structopt(short = "v", long = "verbose", env = "VERBOSE")]
    /// Enable verbose logging
    verbose: bool,
    #[structopt(default_value = "17179869184", env = "CACHE_SIZE")]
    /// Disk size for cache content in bytes. Defaults to 16GiB
    cache_size: usize,
    #[structopt(
        parse(from_os_str),
        default_value = "/tmp/osuse_cache",
        env = "CACHE_PATH"
    )]
    /// Path where cache content should be stored
    cache_path: PathBuf,
    #[structopt(short = "c", long = "cache_large_objects", env = "CACHE_LARGE_OBJECTS")]
    /// Should we cache large objects like ISO/vm images/boot images?
    cache_large_objects: bool,
    #[structopt(default_value = "[::]:8080", env = "BIND_ADDRESS", long = "addr")]
    /// Address to listen to for http
    bind_addr: String,
    #[structopt(env = "TLS_BIND_ADDRESS", long = "tlsaddr")]
    /// Address to listen to for https (optional)
    tls_bind_addr: Option<String>,
    #[structopt(env = "TLS_PEM_KEY", long = "tlskey")]
    /// Path to the TLS Key file in PEM format.
    tls_pem_key: Option<String>,
    #[structopt(env = "TLS_PEM_CHAIN", long = "tlschain")]
    /// Path to the TLS Chain file in PEM format.
    tls_pem_chain: Option<String>,
    #[structopt(env = "MIRROR_CHAIN", long = "mirrorchain")]
    /// Url to another proxy-cache instance to chain through.
    mirror_chain: Option<String>,
}

#[tokio::main]
async fn main() {
    let config = Config::from_args();

    if config.verbose {
        log::with_level(tide::log::LevelFilter::Info);
    } else {
        tracing_subscriber::fmt::init();
    }

    log::info!(
        "Using -> {:?} : {} bytes",
        config.cache_path,
        config.cache_size
    );

    let client: surf::Client = surf::Config::new()
        .set_tcp_no_delay(true)
        .set_timeout(None)
        .set_max_connections_per_host(10)
        .try_into()
        .map_err(|e| {
            log::error!("client builder error - {:?}", e);
            tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
        })
        .expect("Failed to build surf client");

    let client = client.with(surf::middleware::Redirect::new(ALLOW_REDIRECTS));

    let mirror_chain = config
        .mirror_chain
        .as_ref()
        .map(|s| Url::parse(s).expect("Invalid mirror_chain url"));

    let app_state = Arc::new(AppState::new(
        config.cache_size,
        &config.cache_path,
        config.cache_large_objects,
        mirror_chain.clone(),
        client.clone(),
    ));
    let mut app = tide::with_state(app_state);
    app.with(tide::log::LogMiddleware::new());
    app.at("").head(head_view).get(get_view);
    app.at("/").head(head_view).get(get_view);
    app.at("/*").head(head_view).get(get_view);

    // Need to add head reqs

    log::info!("Binding -> http://{}", config.bind_addr);
    let mut listener = tide::listener::ConcurrentListener::new();
    listener
        .add(&config.bind_addr)
        .expect("failed to build http listener");

    match (
        config.tls_bind_addr.as_ref(),
        config.tls_pem_key.as_ref(),
        config.tls_pem_chain.as_ref(),
    ) {
        (Some(tba), Some(tpk), Some(tpc)) => {
            log::info!("Binding -> https://{}", tba);

            let p_tpk = Path::new(tpk);
            let p_tpc = Path::new(tpc);

            if !p_tpk.exists() {
                log::error!("key does not exist -> {}", tpk);
            }

            if !p_tpc.exists() {
                log::error!("chain does not exist -> {}", tpc);
            }

            if !p_tpc.exists() || !p_tpk.exists() {
                return;
            }

            listener
                .add(
                    TlsListener::build()
                        .addrs(tba)
                        .cert(tpc)
                        .key(tpk)
                        .finish()
                        .expect("failed to build https listener"),
                )
                .expect("failed to add https listener");
        }
        (None, None, None) => {
            log::info!("TLS not configured");
        }
        _ => {
            log::error!("Inconsistent TLS config. Must specfiy tls_bind_addr, tls_pem_key and tls_pem_chain");
            return;
        }
    }

    RUNNING.store(true, Ordering::Relaxed);

    // spawn a task to monitor our upstream mirror.
    let _ = tokio::task::spawn(async move { monitor_upstream(client, mirror_chain).await });

    tokio::spawn(async move {
        let _ = app.listen(listener).await;
    });

    let _ = signal::ctrl_c().await;
    RUNNING.store(false, Ordering::Relaxed);
}
