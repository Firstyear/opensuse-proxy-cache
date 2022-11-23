// mod arc_disk_cache;
mod auth;
mod cache;
mod constants;

#[macro_use]
extern crate tracing;
use tracing::Instrument;

#[macro_use]
extern crate lazy_static;

use arc_disk_cache::CacheObj;
use async_std::fs::File;
use async_std::io::prelude::*;
use async_std::io::BufReader as AsyncBufReader;
use async_std::task::Context;
use async_std::task::Poll;
use bytes::{BufMut, Bytes, BytesMut};
use pin_project_lite::pin_project;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use structopt::StructOpt;
use tempfile::NamedTempFile;
use tide_openssl::TlsListener;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::mpsc::{
    channel, Receiver, Sender
};
use tokio::time::{sleep, Duration};
use url::Url;

use crate::cache::*;
use crate::constants::*;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

struct AppState {
    cache: Cache,
    client: surf::Client,
    oauth: Option<auth::BasicClient>,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState").finish()
    }
}

type BoxAsyncBufRead =
    Box<(dyn async_std::io::BufRead + Sync + Unpin + std::marker::Send + 'static)>;

impl AppState {
    fn new(
        capacity: usize,
        content_dir: &Path,
        clob: bool,
        durable_fs: bool,
        mirror_chain: Option<Url>,
        client: surf::Client,
        oauth: Option<auth::BasicClient>,
    ) -> Self {
        AppState {
            cache: Cache::new(capacity, content_dir, clob, durable_fs, mirror_chain),
            client,
            oauth,
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
                    debug!("discarding -> {}: {}", hv.as_str(), hk.as_str());
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
        io_send: bool,
        io_remaining: bool,
        io_tx: Sender<Bytes>,
        cl_send: bool,
        cl_remaining: bool,
        cl_tx: Sender<Bytes>,
        remaining: usize,

    }
}

impl CacheDownloader {
    pub fn new(
        dlos_reader: BoxAsyncBufRead,
        io_tx: Sender<Bytes>,
        cl_tx: Sender<Bytes>,
    ) -> Self {
        CacheDownloader {
            dlos_reader,
            io_send: true,
            io_remaining: false,
            io_tx,
            cl_send: true,
            cl_remaining: false,
            cl_tx,
            remaining: 0,
        }
    }
}

impl Read for CacheDownloader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {

        // Do we have any left over that didn't send?
        if self.cl_remaining || self.io_remaining {
            // The buffer will still be populated
            let bytes = BytesMut::from(buf.split_at(self.remaining).0);
            let bytes = bytes.freeze();

            if self.cl_send && self.cl_remaining {
                match self.cl_tx.try_send(bytes.clone()) {
                    Ok(_) => {
                        self.cl_remaining = false;
                    }
                    Err(TrySendError::Full(_)) => {
                        trace!("cl_tx is full, returning pending!");
                        return Poll::Pending;
                    }
                    Err(TrySendError::Closed(_)) => {
                        warn!("⚠️   poll_read cl_tx blocking_send error.");
                        warn!("⚠️   cl_rx has likely died. continuing to write to disk ...");
                        self.cl_send = false;
                    }
                }
            }

            if self.io_send && self.io_remaining {
                match self.io_tx.try_send(bytes) {
                    Ok(_) => {
                        self.io_remaining = false;
                    }
                    Err(TrySendError::Full(_)) => {
                        trace!("io_tx is full, returning pending!");
                        return Poll::Pending;
                    }
                    Err(TrySendError::Closed(_)) => {
                        error!("🚨  poll_read io_tx blocking_send error.");
                        error!("🚨  io_rx has likely died. continuing to stream ...");
                        self.io_send = false;
                    }
                }
            }
        }

        // From here the buffer is OVERWRITTEN
        match Pin::new(&mut self.dlos_reader).poll_read(ctx, buf) {
            Poll::Ready(Ok(amt)) => {
                if self.io_send || self.cl_send {
                    // Write the content of the buffer here into the channel.
                    let bytes = BytesMut::from(buf.split_at(amt).0);
                    let bytes = bytes.freeze();

                    // flag here that the io_tx hasn't yet seen this!
                    self.remaining = amt;
                    self.cl_remaining = true;
                    self.io_remaining = true;

                    // This is naughty!
                    if self.cl_send {
                        match self.cl_tx.try_send(bytes.clone()) {
                            Ok(_) => {
                                self.cl_remaining = false;
                            }
                            Err(TrySendError::Full(_)) => {
                                trace!("cl_tx is full, returning pending!");
                                return Poll::Pending;
                            }
                            Err(TrySendError::Closed(_)) => {
                                warn!("⚠️   poll_read cl_tx blocking_send error.");
                                warn!("⚠️   cl_rx has likely died. continuing to write to disk ...");
                                self.cl_send = false;
                            }
                        }
                    }

                    if self.io_send {
                        // We don't care if this errors - it won't be written to the cache so we'll
                        // try again and correct it.
                        match self.io_tx.try_send(bytes) {
                            Ok(_) => {
                                self.io_remaining = false;
                            }
                            Err(TrySendError::Full(_)) => {
                                trace!("io_tx is full, returning pending!");
                                return Poll::Pending;
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("🚨  poll_read io_tx blocking_send error.");
                                error!("🚨  io_rx has likely died. continuing to stream ...");
                                self.io_send = false;
                            }
                        }
                    }

                    self.remaining = 0;

                    // trace!("cachereader amt -> {:?}", amt);
                    Poll::Ready(Ok(amt))
                } else {
                    error!("Both IO and CL submission are closed.");
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "io and cl submission has closed",
                    )))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
        // This is where we need to intercept and capture the bytes streamed.
    }
}

pin_project! {
    struct ChannelReader {
        buf: Bytes,
        io_rx: Receiver<Bytes>,
    }
}

impl ChannelReader {
    pub fn new(io_rx: Receiver<Bytes>) -> Self {
        ChannelReader {
            buf: Bytes::new(),
            io_rx,
        }
    }
}

impl Read for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if !self.buf.is_empty() {
            let (bytes, mut rem) = if self.buf.len() > buf.len() {
                let rem = self.buf.split_off(buf.len());
                (self.buf.clone(), rem)
            } else {
                (self.buf.clone(), Bytes::new())
            };

            std::mem::swap(&mut self.buf, &mut rem);

            let (l, _) = buf.split_at_mut(bytes.len());
            l.copy_from_slice(&bytes);

            return Poll::Ready(Ok(bytes.len()));
        }

        match self.io_rx.try_recv() {
            Ok(mut bytes) => {
                let (bytes, mut rem) = if bytes.len() > buf.len() {
                    let rem = bytes.split_off(buf.len());
                    (bytes, rem)
                } else {
                    (bytes, Bytes::new())
                };

                std::mem::swap(&mut self.buf, &mut rem);

                let (l, _) = buf.split_at_mut(bytes.len());
                l.copy_from_slice(&bytes);
                Poll::Ready(Ok(bytes.len()))
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(Ok(0)),
            Err(TryRecvError::Empty) => {
                ctx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

pin_project! {
    struct CacheReader {
        dlos_reader: BoxAsyncBufRead,
        obj: CacheObj<String, Status>,
    }
}

impl CacheReader {
    pub fn new(dlos_reader: BoxAsyncBufRead, obj: CacheObj<String, Status>) -> Self {
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
    io_tx: Sender<Bytes>,
}

impl async_std::io::Write for ChannelWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<async_std::io::Result<usize>> {
        let bytes = BytesMut::from(buf);
        let bytes = bytes.freeze();
        match self.io_tx.try_send(bytes) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(TrySendError::Full(_)) => Poll::Pending,
            Err(TrySendError::Closed(_)) => {
            // Err(_) => {
                error!("poll_write channel -> unexpected close");
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
    info!(immediate = true, "Spawning upstream monitor task ...");

    while RUNNING.load(Ordering::Relaxed) {
        async {
            let r = if let Some(mc_url) = mirror_chain.as_ref() {
                info!("upstream checking -> {}", mc_url.as_str());
                client
                    .send(surf::head(mc_url))
                    .await
                    .map(|resp| {
                        info!("upstream check {} -> {:?}", mc_url.as_str(), resp.status());
                        resp.status() == surf::StatusCode::Ok
                    })
                    .unwrap_or_else(|e| {
                        error!("upstream check error {} -> {:?}", mc_url.as_str(), e);
                        false
                    })
            } else {
                info!("upstream checking -> {:?}", DL_OS_URL.as_str());
                info!("upstream checking -> {:?}", MCS_OS_URL.as_str());
                client
                    .send(surf::head(DL_OS_URL.as_str()))
                    .await
                    .map(|resp| {
                        info!(
                            "upstream check {} -> {:?}",
                            DL_OS_URL.as_str(),
                            resp.status()
                        );
                        resp.status() == surf::StatusCode::Ok
                            || resp.status() == surf::StatusCode::Forbidden
                    })
                    .unwrap_or_else(|e| {
                        error!("upstream check error {} -> {:?}", DL_OS_URL.as_str(), e);
                        false
                    })
                    && client
                        .send(surf::head(MCS_OS_URL.as_str()))
                        .await
                        .map(|resp| {
                            info!(
                                "upstream check {} -> {:?}",
                                MCS_OS_URL.as_str(),
                                resp.status()
                            );
                            resp.status() == surf::StatusCode::Ok
                                || resp.status() == surf::StatusCode::Forbidden
                        })
                        .unwrap_or_else(|e| {
                            error!("upstream check error {} -> {:?}", MCS_OS_URL.as_str(), e);
                            false
                        })
            };
            UPSTREAM_ONLINE.store(r, Ordering::Relaxed);
            warn!("upstream online -> {}", r);
        }
        .instrument(tracing::info_span!("monitor_upstream"))
        .await;

        for _n in 1..24 {
            if RUNNING.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(5)).await;
            } else {
                break;
            }
        }
    }
    info!(immediate = true, "Stopping upstream monitor task.");
}

#[instrument]
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
    debug!("setup_dl metadata {}, dst -> {:?}", metadata, url);
    let req = if metadata {
        surf::head(&url)
    } else {
        surf::get(&url)
    };
    let req = req
        .header("User-Agent", "opensuse-proxy-cache")
        .header("X-ZYpp-AnonymousId", "dd27909d-1c87-4640-b006-ef604d302f92");

    let dl_response = client.send(req).await.map_err(|e| {
        error!("dl_response error - {:?} -> {:?}", url, e);
        tide::Error::from_str(tide::StatusCode::BadGateway, "BadGateway")
    })?;

    let mut status = dl_response.status();
    if status == tide::StatusCode::Forbidden {
        status = tide::StatusCode::Ok;
    }

    let content = dl_response.content_type();
    // let headers = dl_response.iter();
    // filter the headers we send through.
    let headers = filter_headers!(dl_response);

    debug!("👉  orig headers -> {:?}", headers);
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
        info!("🍍  start stream -> HEAD {}", url.as_str());
    } else {
        info!("🍍  start stream -> GET {}", url.as_str());
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

#[instrument]
fn write_file(
    mut io_rx: Receiver<Bytes>,
    req_path: String,
    content: Option<tide::http::Mime>,
    mut headers: BTreeMap<String, String>,
    file: NamedTempFile,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
) {
    let mut amt = 0;

    // Do we have an etag? It has the content length in it.
    let etag_len = headers
        .get("etag")
        .and_then(|t| {
            ETAG_RE.captures(t).and_then(|caps| {
                let etcap = caps.name("len");
                etcap.map(|s| s.as_str()).and_then(|len_str| {
                    let r = usize::from_str_radix(len_str, 16).ok();
                    r
                })
            })
        })
        .unwrap_or(0);

    debug!("etag len -> {}", etag_len);

    let cnt_amt = headers
        .remove("content-length")
        .and_then(|hk| usize::from_str(&hk).ok())
        .unwrap_or(0);

    // If we have etag_len AND cnt_amt, do they match?
    // Or are both 0?

    if cnt_amt != 0 && etag_len != 0 && cnt_amt != etag_len {
        error!(
            "content-length and etag don't agree - {} != {}",
            cnt_amt, etag_len
        );
        warn!("Allowing to proceed for now ...");
        // return;
    }

    let mut buf_file = BufWriter::with_capacity(BUFFER_WRITE_PAGE, file);

    loop {
        match io_rx.try_recv() {
            Ok(bytes) => {
                // Path?
                if let Err(e) = buf_file.write(&bytes) {
                    error!("Error writing to tempfile -> {:?}", e);
                    return;
                }
                amt += bytes.len();
            }
            Err(TryRecvError::Empty) => {
                // pending
            },
            Err(TryRecvError::Disconnected) => {
                break;
            }
        }
    }

    // Check the content len is ok.
    // We have to check that amt >= cnt_amt (aka cnt_amt < amt)
    // because I think there is a bug in surf that sets the worng length. Wireshark
    // is showing all the lengths as 0.
    if amt == 0 || (cnt_amt != 0 && cnt_amt > amt) {
        warn!(
            "transfer interupted, ending - received: {} expect: {}",
            amt, cnt_amt
        );
        return;
    }

    if cnt_amt == 0 && etag_len > amt {
        warn!(
            "transfer MAY have been interupted - received: {} expect etag: {}",
            amt, etag_len
        );
    }

    info!(
        "final sizes - amt {} cnt_amt {} etag_len {}",
        amt, cnt_amt, etag_len
    );

    if cnt_amt != 0 {
        // If zero, already removed above.
        headers.insert("content-length".to_string(), amt.to_string());
    }

    let file = match buf_file.into_inner() {
        Ok(f) => f,
        Err(e) => {
            error!("error processing -> {}, {} -> {:?}", req_path, amt, e);
            return;
        }
    };

    // event time

    let etime = time::OffsetDateTime::now_utc();
    let etag = headers.get("etag").cloned();

    let meta = CacheMeta {
        req_path,
        etime,
        action: Action::Submit {
            file,
            headers,
            content,
            etag,
            cls,
        },
    };
    // Send the file + metadata to the main cache.
    if let Err(e) = submit_tx.try_send(meta) {
        error!("failed to submit to cache channel -> {:?}", e);
    }
}

#[instrument]
async fn missing() -> tide::Result {
    info!("👻  start force missing");

    let response = tide::Response::builder(tide::StatusCode::NotFound);
    let r_body = tide::Body::empty();

    Ok(response.body(r_body).build())
}

#[tracing::instrument]
async fn miss(
    request: tide::Request<Arc<AppState>>,
    url: Url,
    req_path: String,
    file: NamedTempFile,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
    range: Option<(u64, Option<u64>)>,
) -> tide::Result {
    info!("❄️   start miss ");
    debug!("range -> {:?}", range);

    if range.is_some() && matches!(range, Some((0, _))) {
        info!("🖕 Range requests are unsatisfiable on first DL, going to force a full DL");
    }

    let (mut dl_response, mut response, headers) =
        setup_dl(url, false, &request.state().client).await?;

    // May need to extract some hdrs and content type again from dl_response.
    let content = dl_response.content_type();
    debug!("cnt -> {:?}", content);
    debug!("hdr -> {:?}", headers);

    let status = dl_response.status();

    let r_body = if status == surf::StatusCode::Ok || status == surf::StatusCode::Forbidden {
        // Create a bounded channel for sending the data to the writer.
        let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);
        let (cl_tx, cl_rx) = channel(CHANNEL_MAX_OUTSTANDING);
        // let (io_tx, io_rx) = channel();
        // let (cl_tx, cl_rx) = channel();

        // Setup a bg task for writing out the file. Needs the channel rx, the url,
        // and the request from tide to access the AppState. Also needs the hdrs
        // and content type

        let _ = tokio::task::spawn_blocking(move || {
            write_file(io_rx, req_path, content, headers, file, submit_tx, cls)
        });

        let cl_reader = AsyncBufReader::new(ChannelReader::new(cl_rx));

        let body = dl_response.take_body();

        // TODO WILLIAM:
        // Change CacheDownloader to have TWO PARTs.

        // We can use spawn_blocking to actually drive the ios?

        // One is a task that drives to completion the DL.
        // This proxies to two channels, the io_tx, and the client body
        // Then the body reader can drop, but we internally drive the DL to complete.

        // This will prevent partial DL's so we can prime the cache correctly.

        let mut reader = CacheDownloader::new(body.into_reader(), io_tx, cl_tx);
        // This drives the reader to completion
        tokio::spawn(async move {
            let mut buf = vec![0; BUFFER_WRITE_PAGE];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
            debug!("read driver complete allocated {}", buf.capacity());
        });

        tide::Body::from_reader(cl_reader, None)
    } else if status == surf::StatusCode::NotFound {
        info!("👻  rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path,
                etime,
                action: Action::NotFound { cls },
            })
            .await;
        tide::Body::empty()
    } else {
        error!(
            "Response returned {:?}, aborting miss to stream -> {}",
            status, req_path
        );
        let body = dl_response.take_body();
        let reader = body.into_reader();
        tide::Body::from_reader(reader, None)
    };

    Ok(response.body(r_body).build())
}

#[instrument]
async fn found(
    obj: CacheObj<String, Status>,
    metadata: bool,
    range: Option<(u64, Option<u64>)>,
) -> tide::Result {
    info!(
        "🔥  start found -> {:?} : range: {:?}",
        obj.fhandle.path, range
    );
    // Can we satisfy the range request, if any?

    let amt: u64 = obj.fhandle.amt as u64;

    let response = if metadata {
        tide::Response::builder(tide::StatusCode::Ok)
            .body(tide::Body::empty())
            .header("content-length", format!("{}", amt).as_str())
    } else {
        let mut n_file = File::open(&obj.fhandle.path).await.map_err(|e| {
            error!("{:?}", e);
            tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
        })?;

        let (start, end) = match range {
            Some((start, None)) => {
                // If some clients already have the whole file, they'll send the byte range like this, so we
                // just ignore it and send the file again.
                if start == amt {
                    (0, amt)
                } else {
                    (start, amt)
                }
            }
            Some((start, Some(end))) => (start, end + 1),
            None => (0, amt),
        };

        // Sanity check!
        if end <= start || end > amt {
            error!("Range failed {} <= {} || {} > {}", end, start, end, amt);
            return Err(tide::Error::from_str(
                tide::StatusCode::RequestedRangeNotSatisfiable,
                "RequestedRangeNotSatisfiable",
            ));
        }

        if start != 0 {
            if let Err(e) = n_file.seek(async_std::io::SeekFrom::Start(start)).await {
                error!("Range start not satisfiable -> {:?}", e);
                return Err(tide::Error::from_str(
                    tide::StatusCode::RequestedRangeNotSatisfiable,
                    "RequestedRangeNotSatisfiable",
                ));
            }
        }

        // 0 - 1024, we want 1024 - 0 = 1024
        // 1024 - 2048, we want 2048 - 1024 = 1024
        let limit_bytes = end - start;
        let limit_file = n_file.take(limit_bytes);

        let mut n_file = CacheReader::new(
            Box::new(AsyncBufReader::with_capacity(BUFFER_READ_PAGE, limit_file)),
            obj.clone(),
        );

        if start == 0 && end == amt {
            assert!(limit_bytes == amt);
            tide::Response::builder(tide::StatusCode::Ok)
                .header("content-length", format!("{}", limit_bytes).as_str())
                .body(tide::Body::from_reader(n_file, None))
        } else {
            tide::Response::builder(tide::StatusCode::PartialContent)
                .header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end - 1, amt).as_str(),
                )
                .header("content-length", format!("{}", limit_bytes).as_str())
                .body(tide::Body::from_reader(n_file, None))
        }
    };

    // headers
    // let response = meta.
    let response = obj
        .userdata
        .headers
        .iter()
        .filter(|(hv, _)| hv.as_str() != "content-length")
        .fold(response, |response, (hv, hk)| {
            response.header(hv.as_str(), hk.as_str())
        });

    let response = if let Some(cnt) = obj
        .userdata
        .content
        .as_ref()
        .and_then(|s| tide::http::Mime::from_str(&s).ok())
    {
        response.content_type(cnt.clone())
    } else {
        response
    };

    Ok(response.build())
}

#[instrument]
async fn refresh(
    client: &surf::Client,
    url: Url,
    // req_path: String,
    // dir: PathBuf,
    // submit_tx: Sender<CacheMeta>,
    obj: &CacheObj<String, Status>,
) -> bool {
    info!("💸  start refresh ");
    // If we don't have an etag and/or last mod, treat as miss.
    // If we don't have a content-len we may have corrupt content,
    // so force the refresh.

    // First do a head request.
    let (_dl_response, _response, headers) = match setup_dl(url, true, &client).await {
        Ok(r) => r,
        Err(e) => {
            error!("dl error -> {:?}", e);
            // We need to proceed.
            return false;
        }
    };

    let etag: Option<String> = headers.get("etag").cloned();
    let content_len: Option<String> = headers.get("content-length").cloned();

    debug!("etag -> {:?} == {:?}", etag, obj.userdata.etag);
    if content_len.is_some() && etag.is_some() && etag == obj.userdata.etag {
        // No need to refresh, continue.
        false
    } else {
        // No etag present from head request. Assume we need to refresh.
        true
    }
}

#[instrument]
fn prefetch(
    request: &tide::Request<Arc<AppState>>,
    url: &Url,
    submit_tx: &Sender<CacheMeta>,
    prefetch_paths: Option<Vec<(String, NamedTempFile, Classification)>>,
) {
    if let Some(prefetch) = prefetch_paths {
        for (path, file, cls) in prefetch.into_iter() {
            let u = url.clone();
            let tx = submit_tx.clone();
            let c = request.state().client.clone();
            tokio::spawn(async move { prefetch_task(c, u, tx, path, file, cls).await });
        }
    }
}

#[instrument]
async fn prefetch_task(
    client: surf::Client,
    mut url: Url,
    submit_tx: Sender<CacheMeta>,
    req_path: String,
    file: NamedTempFile,
    cls: Classification,
) {
    info!("🚅  start prefetch {}", req_path);

    url.set_path(&req_path);
    let req = surf::get(url);

    let mut dl_response = match client.send(req).await {
        Ok(d) => d,
        Err(e) => {
            error!("dl_response error - {:?}", e);
            return;
        }
    };

    let status = dl_response.status();
    if status == surf::StatusCode::NotFound {
        info!("👻  prefetch rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path,
                etime,
                action: Action::NotFound { cls },
            })
            .await;
        return;
    } else if status != surf::StatusCode::Ok {
        error!("Response returned {:?}, aborting prefetch", status);
        return;
    }

    let content = dl_response.content_type();
    let headers = filter_headers!(dl_response);

    let body = dl_response.take_body();
    let mut byte_reader = body.into_reader();

    let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);
    // let (io_tx, io_rx) = unbounded_channel();
    let _ = tokio::task::spawn_blocking(move || {
        write_file(io_rx, req_path, content, headers, file, submit_tx, cls)
    });

    // https://docs.rs/async-std/1.9.0/async_std/io/fn.copy.html
    // could change to this for large content.
    let mut channel_write = ChannelWriter { io_tx };

    if let Err(e) = async_std::io::copy(&mut byte_reader, &mut channel_write).await {
        error!("prefetch async_std::io::copy error -> {:?}", e);
    }
    // That's it!
}

#[instrument]
fn async_refresh(
    request: &tide::Request<Arc<AppState>>,
    url: &Url,
    submit_tx: &Sender<CacheMeta>,
    file: NamedTempFile,
    obj: &CacheObj<String, Status>,
    prefetch_paths: Option<Vec<(String, NamedTempFile, Classification)>>,
) {
    let c = request.state().client.clone();
    let u = url.clone();
    let tx = submit_tx.clone();
    let obj = obj.clone();
    tokio::spawn(async move { async_refresh_task(c, u, tx, file, obj, prefetch_paths).await });
}

#[instrument]
async fn async_refresh_task(
    client: surf::Client,
    mut url: Url,
    submit_tx: Sender<CacheMeta>,
    file: NamedTempFile,
    obj: CacheObj<String, Status>,
    prefetch_paths: Option<Vec<(String, NamedTempFile, Classification)>>,
) {
    info!("🥺  start async refresh {}", obj.userdata.req_path);

    if !refresh(&client, url.clone(), &obj).await {
        info!(
            "🥰  async prefetch, content still valid {}",
            obj.userdata.req_path
        );
        let etime = time::OffsetDateTime::now_utc();
        // If we can't submit, we are probably shutting down so just finish up cleanly.
        // That's why we ignore these errors.
        //
        // If this item is valid we can update all the related prefetch items.
        let _ = submit_tx
            .send(CacheMeta {
                req_path: obj.userdata.req_path.clone(),
                etime,
                action: Action::Update,
            })
            .await;
        return;
    }

    // Okay, we know we need it now, so DL.
    info!(
        "😵  async refresh, need to refresh {}",
        obj.userdata.req_path
    );

    // spawn the prefetchers that go along with us.
    if let Some(prefetch) = prefetch_paths {
        for (path, file, cls) in prefetch.into_iter() {
            let u = url.clone();
            let tx = submit_tx.clone();
            let c = client.clone();
            tokio::spawn(async move { prefetch_task(c, u, tx, path, file, cls).await });
        }
    }

    url.set_path(&obj.userdata.req_path);
    let req = surf::get(url);

    let mut dl_response = match client.send(req).await {
        Ok(d) => d,
        Err(e) => {
            error!("dl_response error - {:?}", e);
            return;
        }
    };

    let status = dl_response.status();
    if status == surf::StatusCode::NotFound {
        info!("👻  async refresh rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path: obj.userdata.req_path.clone(),
                etime,
                action: Action::NotFound {
                    cls: obj.userdata.cls,
                },
            })
            .await;
        return;
    } else if status != surf::StatusCode::Ok {
        error!("Response returned {:?}, aborting async refresh", status);
        return;
    }

    let content = dl_response.content_type();
    let headers = filter_headers!(dl_response);

    let body = dl_response.take_body();
    let mut byte_reader = body.into_reader();

    let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);
    // let (io_tx, io_rx) = unbounded_channel();
    let _ = tokio::task::spawn_blocking(move || {
        write_file(
            io_rx,
            obj.userdata.req_path.clone(),
            content,
            headers,
            file,
            submit_tx,
            obj.userdata.cls,
        )
    });

    // https://docs.rs/async-std/1.9.0/async_std/io/fn.copy.html
    // could change to this for large content.
    let mut channel_write = ChannelWriter { io_tx };

    if let Err(e) = async_std::io::copy(&mut byte_reader, &mut channel_write).await {
        error!("prefetch async_std::io::copy error -> {:?}", e);
    }
    // That's it!
}

#[instrument]
async fn head_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    let req_url = request.url();
    debug!("req -> {:?}", req_url);

    let headers: BTreeMap<String, String> = request
        .iter()
        .map(|(hv, hk)| (hv.as_str().to_string(), hk.as_str().to_string()))
        .collect();
    info!("request_headers -> {:?}", headers);

    let req_path = req_url.path();

    // Req path sometimes has dup //, so we replace them.
    let req_path = req_path.replace("//", "/");

    // Now we should check if we have req_path in our cache or not.
    let decision = request.state().cache.decision(&req_path, true);
    // Based on the decision, take an appropriate path. Generally with head reqs
    // we try to stream this if we don't have it, and we prefetch in the BG.
    match decision {
        CacheDecision::Stream(url) => stream(request, url, true).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, true, None).await,
        CacheDecision::Refresh(url, _, submit_tx, _, prefetch_paths)
        | CacheDecision::MissObj(url, _, submit_tx, _, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(&request, &url, &submit_tx, prefetch_paths);
            // Now we just stream.
            stream(request, url, true).await
        }
        CacheDecision::AsyncRefresh(url, file, submit_tx, meta, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            async_refresh(&request, &url, &submit_tx, file, &meta, prefetch_paths);
            // Send our current head data.
            found(meta, true, None).await
        }
        CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::BadRequest,
            "Invalid Request",
        )),
    }
}

#[instrument]
async fn get_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    let req_url = request.url();
    debug!("req -> {:?}", req_url);

    let headers: BTreeMap<String, String> = request
        .iter()
        .map(|(hv, hk)| (hv.as_str().to_string(), hk.as_str().to_string()))
        .collect();
    info!("request_headers -> {:?}", headers);

    let req_path = req_url.path();
    // Now we should check if we have req_path in our cache or not.
    let decision = request.state().cache.decision(req_path, false);
    let req_path = req_path.to_string();
    // Req path sometimes has dup //, so we replace them.
    let req_path = req_path.replace("//", "/");

    // We have a hit, with our cache meta! Hooray!
    // Let's setup the response, and then stream from the file!
    let range = headers.get("range").and_then(|sr| {
        if sr.starts_with("bytes=") {
            sr.strip_prefix("bytes=")
                .and_then(|v| v.split_once('-'))
                .and_then(|(range_start, range_end)| {
                    let r_end = u64::from_str_radix(range_end, 10).ok();
                    u64::from_str_radix(range_start, 10)
                        .ok()
                        .map(|s| (s, r_end))
                })
        } else {
            None
        }
    });

    // Based on the decision, take an appropriate path.
    match decision {
        CacheDecision::Stream(url) => stream(request, url, false).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, false, range).await,
        CacheDecision::MissObj(url, dir, submit_tx, cls, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(&request, &url, &submit_tx, prefetch_paths);
            miss(request, url, req_path, dir, submit_tx, cls, range).await
        }
        CacheDecision::Refresh(url, file, submit_tx, meta, prefetch_paths) => {
            // Do a head req - on any error, stream what we have if possible.
            // if head etag OR last update match, serve what we have.
            // else follow the miss path.
            debug!("prefetch {:?}", prefetch_paths);
            if refresh(&request.state().client, url.clone(), &meta).await {
                info!("👉  refresh required");
                // Submit all our BG prefetch reqs
                prefetch(&request, &url, &submit_tx, prefetch_paths);
                miss(
                    request,
                    url,
                    req_path,
                    file,
                    submit_tx,
                    meta.userdata.cls,
                    range,
                )
                .await
            } else {
                info!("👉  cache valid");
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
                found(meta, false, range).await
            }
        }
        CacheDecision::AsyncRefresh(url, file, submit_tx, meta, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            async_refresh(&request, &url, &submit_tx, file, &meta, prefetch_paths);
            // Send our current cached data.
            found(meta, false, range).await
        }
        CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::BadRequest,
            "Invalid Request",
        )),
    }
}

#[instrument]
async fn admin_view(_request: tide::Request<Arc<AppState>>) -> tide::Result {
    let mut res = tide::Response::new(200);
    res.set_content_type("text/html;charset=utf-8");
    res.set_body(
        r#"
<!doctype html>
<html lang="en">

<head>
<meta charset="utf-8" />
<title>Mirror Cache</title>
<head>
    <meta charset="utf-8"/>
    <title>Mirror Cache</title>
    <script type="module" defer>
    const button = document.getElementById('clear-nxcache-btn');
    button.addEventListener('click', async _ => {
      try {
        const response = await fetch('/_admin/_clear_nxcache', {
          method: 'post',
          body: {
          }
        });
        console.log('Completed!', response);
      } catch(err) {
        console.error(`Error: ${err}`);
      }
    });
    </script>
</head>

<body>
<main>
    <button id="clear-nxcache-btn" class="" type="button">
      Clear NX Cache
    </button>
</main>
</body>

</html>
    "#,
    );

    Ok(res)
}

#[instrument]
async fn status_view(_request: tide::Request<Arc<AppState>>) -> tide::Result {
    let mut res = tide::Response::new(200);
    res.set_content_type("text/html;charset=utf-8");
    res.set_body("Ok");
    Ok(res)
}

#[instrument]
async fn admin_clear_nxcache_view(request: tide::Request<Arc<AppState>>) -> tide::Result {
    let etime = time::OffsetDateTime::now_utc();
    request.state().cache.clear_nxcache(etime);
    let mut res = tide::Response::new(200);
    res.set_content_type("text/html;charset=utf-8");
    Ok(res)
}

#[instrument]
async fn robots_view(_request: tide::Request<Arc<AppState>>) -> tide::Result {
    Ok(r#"
User-agent: *
Disallow: /
    "#
    .to_string()
    .into())
}

#[derive(StructOpt)]
struct Config {
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
    #[structopt(short = "Z", long = "durable_fs", env = "DURABLE_FS")]
    /// Is this running on a consistent and checksummed fs? If yes, then we can skip
    /// internal crc32c sums on get().
    durable_fs: bool,
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
    #[structopt(env = "ACME_CHALLENGE_DIR", long = "acmechallengedir")]
    /// Location to store acme challenges for lets encrypt if in use.
    acme_challenge_dir: Option<String>,

    #[structopt(env = "OAUTH2_CLIENT_ID", long = "oauth_client_id")]
    /// Oauth client id
    oauth_client_id: Option<String>,
    #[structopt(env = "OAUTH2_CLIENT_SECRET", long = "oauth_client_secret")]
    /// Oauth client secret
    oauth_client_secret: Option<String>,
    #[structopt(
        env = "OAUTH2_CLIENT_URL",
        default_value = "http://localhost:8080",
        long = "oauth_client_url"
    )]
    /// Oauth client url - this is the url of THIS server
    oauth_client_url: String,
    #[structopt(env = "OAUTH2_SERVER_URL", long = "oauth_server_url")]
    /// Oauth server url - the url of the authorisation provider
    oauth_server_url: Option<String>,
}

async fn do_main() {
    let config = Config::from_args();

    trace!("Trace working!");
    debug!("Debug working!");

    info!(
        "Using -> {:?} : {} bytes",
        config.cache_path, config.cache_size
    );

    let client: surf::Client = surf::Config::new()
        .set_tcp_no_delay(true)
        .set_timeout(None)
        .set_max_connections_per_host(10)
        .try_into()
        .map_err(|e| {
            error!("client builder error - {:?}", e);
            tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
        })
        .expect("Failed to build surf client");

    let client = client.with(surf::middleware::Redirect::new(ALLOW_REDIRECTS));

    let mirror_chain = config
        .mirror_chain
        .as_ref()
        .map(|s| Url::parse(s).expect("Invalid mirror_chain url"));

    let oauth = match (
        &config.oauth_client_id,
        &config.oauth_client_secret,
        &config.oauth_server_url,
    ) {
        (Some(o_client_id), Some(o_client_secret), Some(o_server_url)) => {
            Some(auth::configure_oauth(
                o_client_id,
                o_client_secret,
                &config.oauth_client_url,
                o_server_url,
            ))
        }
        _ => {
            warn!("⚠️  Oauth settings incomplete, or missing, admin UI is disabled!");
            None
        }
    };

    let app_state = Arc::new(AppState::new(
        config.cache_size,
        &config.cache_path,
        config.cache_large_objects,
        config.durable_fs,
        mirror_chain.clone(),
        client.clone(),
        oauth,
    ));
    let mut app = tide::with_state(app_state);
    app.at("robots.txt").get(robots_view);
    if let Some(acme_dir) = config.acme_challenge_dir.as_ref() {
        info!("Serving {} as /.well-known/acme-challenge", acme_dir);
        app.at("/.well-known/acme-challenge")
            .serve_dir(acme_dir)
            .expect("Failed to serve .well-known/acme-challenge directory");
    }

    let cookie_sig = StdRng::from_entropy().gen::<[u8; 32]>();
    let sessions =
        tide::sessions::SessionMiddleware::new(tide::sessions::MemoryStore::new(), &cookie_sig)
            .with_cookie_path("/")
            .with_same_site_policy(tide::http::cookies::SameSite::Lax)
            .with_cookie_name("opensuse-proxy-cache");
    app.with(sessions);

    let mut auth_app = app.at("/_admin");

    auth_app.with(auth::AuthMiddleware::new());
    auth_app.at("").get(admin_view);
    auth_app.at("/").get(admin_view);
    auth_app
        .at("/_clear_nxcache")
        .post(admin_clear_nxcache_view);

    // Need to be on an un-auth path.
    app.at("/oauth/login").get(auth::login_view);
    app.at("/oauth/response").get(auth::oauth_view);

    app.at("/_status").get(status_view);

    app.at("").head(head_view).get(get_view);
    app.at("/").head(head_view).get(get_view);
    app.at("/*").head(head_view).get(get_view);

    // Need to add head reqs

    info!("Binding -> http://{}", config.bind_addr);
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
            info!("Binding -> https://{}", tba);

            let p_tpk = Path::new(tpk);
            let p_tpc = Path::new(tpc);

            if !p_tpk.exists() {
                error!("key does not exist -> {}", tpk);
            }

            if !p_tpc.exists() {
                error!("chain does not exist -> {}", tpc);
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
            info!("TLS not configured");
        }
        _ => {
            error!("Inconsistent TLS config. Must specfiy tls_bind_addr, tls_pem_key and tls_pem_chain");
            return;
        }
    }

    RUNNING.store(true, Ordering::Relaxed);

    // spawn a task to monitor our upstream mirror.
    let handle = tokio::task::spawn(async move { monitor_upstream(client, mirror_chain).await });

    tokio::select! {
        Ok(()) = tokio::signal::ctrl_c() => {}
        Some(()) = async move {
            let sigterm = tokio::signal::unix::SignalKind::terminate();
            tokio::signal::unix::signal(sigterm).unwrap().recv().await
        } => {}
        _ = app.listen(listener) => {}
    }

    info!("Stopping ...");
    RUNNING.store(false, Ordering::Relaxed);
    let _ = handle.await;
    info!("Server has stopped!");
}

#[tokio::main]
async fn main() {
    let file_name = format!("/tmp/dhat/heap-{}.json", std::process::id());
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::builder()
        .trim_backtraces(Some(4))
        .file_name(file_name)
        .build();

    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let fmt_layer = tracing_forest::ForestLayer::default();

    // let fmt_layer = tracing_subscriber::fmt::layer()
    //     .with_target(true);

    Registry::default()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    do_main().await;
}
