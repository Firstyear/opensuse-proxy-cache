#[macro_use]
extern crate tracing;

mod cache;
mod constants;

use askama::Template;

use std::num::NonZeroUsize;
use std::time::Instant;
use tracing::Instrument;

use crate::cache::*;
use arc_disk_cache::CacheObj;

use crate::constants::*;

use clap::Parser;
use lru::LruCache;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use axum::{
    body::Body,
    extract,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::str::FromStr;

use tokio::sync::broadcast;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use bytes::Bytes;
use futures_util::stream::Stream;
use futures_util::task::{Context, Poll};
use pin_project_lite::pin_project;
use std::convert::TryInto;
use std::io::{BufWriter, Write};
use std::pin::Pin;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::{sleep, Duration};
use url::Url;

use tokio::fs::File;
use tokio::io::BufReader;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::StreamExt;
use tokio_util::io::InspectReader;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;

use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;

struct AppState {
    cache: Cache,
    client: reqwest::Client,
    // oauth: Option<auth::BasicClient>,
    prefetch_tx: Sender<PrefetchReq>,
    boot_origin: Url,
}

impl AppState {
    pub fn new(
        capacity: usize,
        content_dir: &Path,
        clob: bool,
        wonder_guard: bool,
        durable_fs: bool,
        mirror_chain: Option<Url>,
        client: reqwest::Client,
        prefetch_tx: Sender<PrefetchReq>,
        boot_origin: Url,
    ) -> std::io::Result<Self> {
        let cache = Cache::new(
            capacity,
            content_dir,
            clob,
            wonder_guard,
            durable_fs,
            mirror_chain,
        )?;

        Ok(AppState {
            cache,
            client,
            prefetch_tx,
            boot_origin,
        })
    }
}

#[instrument(skip_all)]
async fn head_view(
    headers: HeaderMap,
    extract::State(state): extract::State<Arc<AppState>>,
    extract::OriginalUri(req_uri): extract::OriginalUri,
) -> Response {
    let req_path = req_uri.path();
    let req_path = format!("/{}", req_path.replace("//", "/"));
    trace!("{:?}", req_path);
    info!("request_headers -> {:?}", headers);
    let decision = state.cache.decision(&req_path, true);
    // Based on the decision, take an appropriate path. Generally with head reqs
    // we try to stream this if we don't have it, and we prefetch in the BG.
    match decision {
        CacheDecision::Stream(url) => stream(state, url, true, None).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, true, None).await,
        CacheDecision::Refresh(url, _, submit_tx, _, prefetch_paths)
        | CacheDecision::MissObj(url, _, submit_tx, _, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(state.prefetch_tx.clone(), &url, &submit_tx, prefetch_paths);
            // Now we just stream.
            stream(state, url, true, None).await
        }
        CacheDecision::AsyncRefresh(url, file, submit_tx, meta, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            async_refresh(
                state.client.clone(),
                state.prefetch_tx.clone(),
                &url,
                &submit_tx,
                file,
                &meta,
                prefetch_paths,
            );
            // Send our current head data.
            found(meta, true, None).await
        }
        CacheDecision::Invalid => {
            (StatusCode::INTERNAL_SERVER_ERROR, "Invalid Request").into_response()
        }
    }
}

// https://github.com/tokio-rs/axum/discussions/608

#[instrument(skip_all)]
async fn get_view(
    headers: HeaderMap,
    extract::State(state): extract::State<Arc<AppState>>,
    extract::OriginalUri(req_uri): extract::OriginalUri,
) -> Response {
    let req_path = req_uri.path();
    let req_path = format!("/{}", req_path.replace("//", "/"));
    trace!("{:?}", req_path);
    info!("request_headers -> {:?}", headers);
    let decision = state.cache.decision(&req_path, false);
    // Req path sometimes has dup //, so we replace them.

    // We have a hit, with our cache meta! Hooray!
    // Let's setup the response, and then stream from the file!
    let range = headers
        .get("range")
        .and_then(|hv| hv.to_str().ok())
        .and_then(|sr| {
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
        CacheDecision::Stream(url) => stream(state, url, false, range).await,
        CacheDecision::NotFound => missing().await,
        CacheDecision::FoundObj(meta) => found(meta, false, range).await,
        CacheDecision::MissObj(url, file, submit_tx, cls, prefetch_paths) => {
            // Submit all our BG prefetch reqs
            prefetch(state.prefetch_tx.clone(), &url, &submit_tx, prefetch_paths);

            miss(state, url, req_path, file, submit_tx, cls, range).await
        }
        CacheDecision::Refresh(url, file, submit_tx, meta, prefetch_paths) => {
            // Do a head req - on any error, stream what we have if possible.
            // if head etag OR last update match, serve what we have.
            // else follow the miss path.
            debug!("prefetch {:?}", prefetch_paths);
            if refresh(&state.client, url.clone(), &meta).await {
                info!("👉  refresh required");
                // Submit all our BG prefetch reqs
                prefetch(state.prefetch_tx.clone(), &url, &submit_tx, prefetch_paths);

                miss(
                    state,
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
            async_refresh(
                state.client.clone(),
                state.prefetch_tx.clone(),
                &url,
                &submit_tx,
                file,
                &meta,
                prefetch_paths,
            );
            // Send our current cached data.
            found(meta, false, range).await
        }
        CacheDecision::Invalid => {
            (StatusCode::INTERNAL_SERVER_ERROR, "Invalid Request").into_response()
        }
    }
}

#[instrument(skip_all)]
fn async_refresh(
    client: reqwest::Client,
    prefetch_tx: Sender<PrefetchReq>,
    url: &Url,
    submit_tx: &Sender<CacheMeta>,
    file: NamedTempFile,
    obj: &CacheObj<String, Status>,
    prefetch_paths: Option<Vec<(String, NamedTempFile, Classification)>>,
) {
    let u = url.clone();
    let tx = submit_tx.clone();
    let obj = obj.clone();
    tokio::spawn(async move {
        async_refresh_task(client, prefetch_tx, u, tx, file, obj, prefetch_paths).await
    });
}

#[instrument(skip_all)]
async fn async_refresh_task(
    client: reqwest::Client,
    prefetch_tx: Sender<PrefetchReq>,
    url: Url,
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

    info!(
        "😵  async refresh, need to refresh {}",
        obj.userdata.req_path
    );

    prefetch(prefetch_tx.clone(), &url, &submit_tx, prefetch_paths);

    // Fetch our actual file too

    if let Err(_) = prefetch_tx
        .send(PrefetchReq {
            req_path: obj.userdata.req_path.clone(),
            url,
            submit_tx: submit_tx,
            file,
            cls: obj.userdata.cls.clone(),
        })
        .await
    {
        error!("Prefetch task may have died!");
    } else {
        debug!("Prefetch submitted");
    }
}

fn send_headers(range: Option<(u64, Option<u64>)>) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.append("user-agent", "opensuse-proxy-cache".try_into().unwrap());
    // h.append("x-ospc-uuid", tracing_forest::id().to_string());
    h.append(
        "x-zypp-anonymousid",
        "dd27909d-1c87-4640-b006-ef604d302f92".try_into().unwrap(),
    );

    if let Some((lower, maybe_upper)) = range {
        if let Some(upper) = maybe_upper {
            h.append(
                "range",
                format!("bytes={}-{}", lower, upper).try_into().unwrap(),
            );
        } else {
            h.append("range", format!("bytes={}-", lower).try_into().unwrap());
        }
    };

    h
}

fn filter_headers(headers: &HeaderMap, metadata: bool) -> HeaderMap {
    debug!(?headers);

    headers
        .iter()
        .filter_map(|(hv, hk)| {
            let hvs = hv.as_str();
            if hvs == "etag"
            || hvs == "accept-ranges"
            || hvs == "content-type"
            || hvs == "content-range"
            || hvs == "last-modified"
            || hvs == "expires"
            || hvs == "cache-control"
            // If it's metadata then nix the content-length else curl has a sad.
            || (hvs == "content-length" && !metadata)
            {
                Some((hv.clone(), hk.clone()))
            } else {
                debug!("discarding -> {}: {:?}", hvs, hk);
                None
            }
        })
        .collect()
}

#[instrument(skip_all)]
async fn stream(
    state: Arc<AppState>,
    url: Url,
    metadata: bool,
    range: Option<(u64, Option<u64>)>,
) -> Response {
    let send_headers = send_headers(range);

    let client_response = if metadata {
        info!("🍍  start stream -> HEAD {}", url.as_str());
        state.client.head(url).headers(send_headers).send().await
    } else {
        info!("🍍  start stream -> GET {}", url.as_str());
        state.client.get(url).headers(send_headers).send().await
    };

    // Handle this error properly. Shortcut return a 500?
    let client_response = match client_response {
        Ok(cr) => cr,
        Err(e) => {
            error!(?e, "Error handling client response");
            return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
        }
    };

    let headers = filter_headers(client_response.headers(), metadata);
    // Filter the headers
    let status = client_response.status();

    if metadata {
        (status, headers).into_response()
    } else {
        let stream = client_response.bytes_stream();
        let body = Body::from_stream(stream);
        (status, headers, body).into_response()
    }
}

#[instrument(skip_all)]
async fn miss(
    state: Arc<AppState>,
    url: Url,
    req_path: String,
    file: NamedTempFile,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
    range: Option<(u64, Option<u64>)>,
) -> Response {
    info!("❄️   start miss ");
    debug!("range -> {:?}", range);

    if range.is_some() {
        info!("Range request, submitting bg dl with rangestream");

        if let Err(_) = state
            .prefetch_tx
            .send(PrefetchReq {
                req_path,
                url: url.clone(),
                submit_tx,
                file,
                cls,
            })
            .await
        {
            error!("Prefetch task may have died!");
        }

        // Stream. metadata=false because we want the body.
        return stream(state, url, false, range).await;
    }

    // Not a range, go on.
    info!("Not a range request, as you were.");

    // Start the dl.
    let send_headers = send_headers(None);
    let client_response = state.client.get(url).headers(send_headers).send().await;

    let client_response = match client_response {
        Ok(cr) => cr,
        Err(e) => {
            error!(?e, "Error handling client response");
            return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
        }
    };

    let headers = filter_headers(client_response.headers(), false);
    // Filter the headers
    let status = client_response.status();

    if status == StatusCode::OK || status == StatusCode::FORBIDDEN {
        let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);

        let headers_clone = headers.clone();
        let _ = tokio::task::spawn_blocking(move || {
            write_file(io_rx, req_path, headers_clone, file, submit_tx, cls)
        });

        let stream = CacheDownloader::new(client_response.bytes_stream(), io_tx);
        let body = Body::from_stream(stream);
        (status, headers, body).into_response()
    } else if status == StatusCode::NOT_FOUND {
        info!("👻  rewrite -> NotFound");
        let etime = time::OffsetDateTime::now_utc();
        let _ = submit_tx
            .send(CacheMeta {
                req_path,
                etime,
                action: Action::NotFound { cls },
            })
            .await;

        // Send back the 404
        missing().await
    } else {
        error!(
            "Response returned {:?}, aborting miss to stream -> {}",
            status, req_path
        );
        let stream = client_response.bytes_stream();
        let body = Body::from_stream(stream);
        (status, headers, body).into_response()
    }
}

pin_project! {
    struct CacheDownloader<T>
        where T: Stream<Item = Result<Bytes, reqwest::Error>>
    {
        #[pin]
        dlos_reader: T,
        #[pin]
        io_send: bool,
        #[pin]
        io_tx: Sender<Bytes>,
    }
}

impl<T> CacheDownloader<T>
where
    T: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    pub fn new(dlos_reader: T, io_tx: Sender<Bytes>) -> Self {
        CacheDownloader {
            dlos_reader,
            io_send: true,
            io_tx,
        }
    }
}

impl<T> Stream for CacheDownloader<T>
where
    T: Stream<Item = Result<Bytes, reqwest::Error>>,
{
    type Item = Result<Bytes, reqwest::Error>;

    // Required method
    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.dlos_reader.poll_next(ctx) {
            Poll::Ready(Some(Ok(buf))) => {
                // We don't care if this errors - it won't be written to the cache so we'll
                // try again and correct it later
                if *this.io_send {
                    // Write the content of the buffer here into the channel.
                    let bytes = buf.clone();

                    if let Err(_e) = this.io_tx.try_send(bytes) {
                        error!("🚨  poll_read io_tx blocking_send error.");
                        error!(
                            "🚨  io_rx has likely died or is backlogged. continuing to stream ..."
                        );
                        *this.io_send = false;
                    }
                }

                Poll::Ready(Some(Ok(buf)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.dlos_reader.size_hint()
    }
}

#[instrument(skip_all)]
fn write_file(
    mut io_rx: Receiver<Bytes>,
    req_path: String,
    mut headers: HeaderMap,
    file: NamedTempFile,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
) {
    let mut amt = 0;

    let cnt_amt = headers
        .remove("content-length")
        .and_then(|hk| hk.to_str().ok().and_then(|i| usize::from_str(i).ok()))
        .unwrap_or(0);

    let etag_nginix_len = headers
        .get("etag")
        .and_then(|hk| {
            hk.to_str().ok().and_then(|t| {
                ETAG_NGINIX_RE.captures(t).and_then(|caps| {
                    let etcap = caps.name("len");
                    etcap.map(|s| s.as_str()).and_then(|len_str| {
                        let r = usize::from_str_radix(len_str, 16).ok();
                        r
                    })
                })
            })
        })
        .unwrap_or(0);

    let etag_apache_len = headers
        .get("etag")
        .and_then(|hk| {
            hk.to_str().ok().and_then(|t| {
                ETAG_APACHE_RE.captures(t).and_then(|caps| {
                    let etcap = caps.name("len");
                    etcap.map(|s| s.as_str()).and_then(|len_str| {
                        let r = usize::from_str_radix(len_str, 16).ok();
                        r
                    })
                })
            })
        })
        .unwrap_or(0);

    // At least *one* etag length has to make sense ...
    // Does this length make sense? Can we get an etag length?

    if cnt_amt != 0
        && ((etag_nginix_len != 0 && cnt_amt != etag_nginix_len)
            && (etag_apache_len != 0 && cnt_amt != etag_apache_len))
    {
        error!(
            "content-length and etag do not agree - {} != a {} && n {}",
            cnt_amt, etag_apache_len, etag_nginix_len
        );
        return;
    } else {
        info!(
            "content-length and etag agree - {} == a {} || n {}",
            cnt_amt, etag_apache_len, etag_nginix_len
        );
    };

    let mut buf_file = BufWriter::with_capacity(BUFFER_WRITE_PAGE, file);
    let mut count = 0;

    loop {
        match io_rx.try_recv() {
            Ok(bytes) => {
                // Path?
                if let Err(e) = buf_file.write(&bytes) {
                    error!("Error writing to tempfile -> {:?}", e);
                    return;
                }
                amt += bytes.len();
                if bytes.len() > 0 {
                    // We actually progressed.
                    if count >= 10 {
                        warn!("Download has become unstuck.");
                        eprintln!("Download has become unstuck.");
                    }
                    count = 0;
                }
            }
            Err(TryRecvError::Empty) => {
                // pending
                std::thread::sleep(std::time::Duration::from_millis(100));
                count += 1;
                if count >= 200 {
                    eprintln!("No activity in {}ms seconds, cancelling task.", count * 100);
                    error!("No activity in {}ms seconds, cancelling task.", count * 100);
                    return;
                } else if count == 10 {
                    warn!("Download may be stuck!!!");
                    eprintln!("Download may be stuck!!!");
                }
            }
            Err(TryRecvError::Disconnected) => {
                debug!("Channel closed, download may be complete.");
                break;
            }
        }
    }

    // Check the content len is ok.
    // We have to check that amt >= cnt_amt (aka cnt_amt < amt)
    if amt == 0 || (cnt_amt != 0 && cnt_amt > amt) {
        warn!(
            "transfer interupted, ending - received: {} expect: {}",
            amt, cnt_amt
        );
        return;
    }

    info!("final sizes - amt {} cnt_amt {}", amt, cnt_amt);

    if cnt_amt != 0 {
        // Header map overwrites content-length on insert.
        headers.insert("content-length", amt.into());
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

    // Don't touch etag! We need it to check if upstreams content is still
    // valid!

    let headers = headers
        .into_iter()
        .filter_map(|(k, v)| {
            if let Some(k) = k.map(|ik| ik.as_str().to_string()) {
                v.to_str().ok().map(|iv| (k, iv.to_string()))
            } else {
                None
            }
        })
        .collect();

    // TODO HERE
    // Now if the FILE is a repomd xml we need to parse it and indicate prefetch on
    // the sha-sum locations of the actual repodata.

    let meta = CacheMeta {
        req_path,
        etime,
        action: Action::Submit { file, headers, cls },
    };
    // Send the file + metadata to the main cache.
    if let Err(e) = submit_tx.try_send(meta) {
        error!("failed to submit to cache channel -> {:?}", e);
    }
}

#[instrument(skip_all)]
fn prefetch(
    prefetch_tx: Sender<PrefetchReq>,
    url: &Url,
    submit_tx: &Sender<CacheMeta>,
    prefetch_paths: Option<Vec<(String, NamedTempFile, Classification)>>,
) {
    if let Some(prefetch) = prefetch_paths {
        for (path, file, cls) in prefetch.into_iter() {
            if let Err(_) = prefetch_tx.try_send(PrefetchReq {
                req_path: path,
                url: url.clone(),
                submit_tx: submit_tx.clone(),
                file,
                cls,
            }) {
                error!("Prefetch task may have died!");
            }
        }
    }
}

#[instrument(skip_all)]
async fn prefetch_dl_task(
    client: reqwest::Client,
    mut url: Url,
    submit_tx: Sender<CacheMeta>,
    req_path: String,
    file: NamedTempFile,
    cls: Classification,
) {
    info!("🚅  start prefetch {}", req_path);

    let send_headers = send_headers(None);
    // Add the path to our base mirror url.
    url.set_path(&req_path);

    let client_response = client.get(url).headers(send_headers).send().await;

    let client_response = match client_response {
        Ok(cr) => cr,
        Err(e) => {
            error!(?e, "Error handling client response");
            return;
        }
    };

    let status = client_response.status();
    if status == StatusCode::NOT_FOUND {
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
    } else if status != StatusCode::OK {
        error!("Response returned {:?}, aborting prefetch", status);
        return;
    }

    let headers = filter_headers(client_response.headers(), false);

    let (io_tx, io_rx) = channel(CHANNEL_MAX_OUTSTANDING);
    let _ = tokio::task::spawn_blocking(move || {
        write_file(io_rx, req_path, headers, file, submit_tx, cls)
    });

    let mut byte_reader = InspectReader::new(
        StreamReader::new(
            client_response
                .bytes_stream()
                .map(|item| item.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
        ),
        move |bytes| {
            let b: Bytes = bytes.to_vec().into();
            let _ = io_tx.try_send(b);
        },
    );

    let mut sink = tokio::io::sink();

    if let Err(e) = tokio::io::copy(&mut byte_reader, &mut sink).await {
        error!("prefetch tokio::io::copy error -> {:?}", e);
    }
    // That's it!
}

#[instrument(skip_all)]
async fn found(
    obj: CacheObj<String, Status>,
    metadata: bool,
    range: Option<(u64, Option<u64>)>,
) -> Response {
    info!(
        "🔥  start found -> {:?} : range: {:?}",
        obj.fhandle.path, range
    );

    let amt: u64 = obj.fhandle.amt as u64;

    // rebuild headers.
    let mut headers = HeaderMap::new();

    obj.userdata.headers.iter().for_each(|(k, v)| {
        headers.insert(
            HeaderName::from_str(k.as_str()).unwrap(),
            HeaderValue::from_str(v.as_str()).unwrap(),
        );
    });

    let mut headers = filter_headers(&headers, metadata);

    if metadata {
        return (StatusCode::OK, headers).into_response();
    }

    // Not a head req - send the file!
    let mut n_file = match File::open(&obj.fhandle.path).await {
        Ok(f) => f,
        Err(e) => {
            error!("{:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

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
        return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    }

    if start != 0 {
        if let Err(e) = n_file.seek(std::io::SeekFrom::Start(start)).await {
            error!("Range start not satisfiable -> {:?}", e);
            return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
        }
    }

    // 0 - 1024, we want 1024 - 0 = 1024
    // 1024 - 2048, we want 2048 - 1024 = 1024
    let limit_bytes = end - start;

    // UPDATE HEADER WITH LIMIT_BYTES AS LEN
    headers.insert(
        "content-length",
        HeaderValue::from_str(format!("{}", limit_bytes).as_str()).unwrap(),
    );

    let limit_file = n_file.take(limit_bytes);

    let stream = Body::from_stream(ReaderStream::new(BufReader::with_capacity(
        BUFFER_READ_PAGE,
        limit_file,
    )));

    if start == 0 && end == amt {
        assert!(limit_bytes == amt);
        (StatusCode::OK, headers, stream).into_response()
    } else {
        headers.insert(
            "content-range",
            HeaderValue::from_str(format!("bytes {}-{}/{}", start, end - 1, amt).as_str()).unwrap(),
        );

        (StatusCode::PARTIAL_CONTENT, headers, stream).into_response()
    }
}

#[instrument(skip_all)]
async fn refresh(client: &reqwest::Client, url: Url, obj: &CacheObj<String, Status>) -> bool {
    info!("💸  start refresh ");
    // If we don't have an etag and/or last mod, treat as miss.
    // If we don't have a content-len we may have corrupt content,
    // so force the refresh.

    // First do a head request.
    let send_headers = send_headers(None);
    let client_response = client.head(url).headers(send_headers).send().await;

    let client_response = match client_response {
        Ok(cr) => cr,
        Err(e) => {
            error!(?e, "Error handling client response");
            // For now assume we can't proceed anyway
            return false;
        }
    };

    let etag: Option<&str> = client_response
        .headers()
        .get("etag")
        .and_then(|hv| hv.to_str().ok());
    let x_etag = obj.userdata.headers.get("etag");

    debug!("etag -> {:?} == {:?}", etag, x_etag);
    if etag.is_some() && etag == x_etag.map(|s| s.as_str()) {
        // No need to refresh, continue.
        info!("💸  refresh not required");
        false
    } else {
        // No etag present from head request. Assume we need to refresh.
        info!("💸  refresh is required");
        true
    }
}

#[instrument(skip_all)]
async fn missing() -> Response {
    info!("👻  start force missing");

    StatusCode::NOT_FOUND.into_response()
}

async fn monitor_upstream(
    client: reqwest::Client,
    mirror_chain: Option<Url>,
    mut rx: broadcast::Receiver<bool>,
) {
    info!(immediate = true, "Spawning upstream monitor task ...");

    loop {
        match rx.try_recv() {
            Err(broadcast::error::TryRecvError::Empty) => {
                async {
                    let r = if let Some(mc_url) = mirror_chain.as_ref() {
                        info!("upstream checking -> {}", mc_url.as_str());
                        client
                            .head(mc_url.as_str())
                            .timeout(std::time::Duration::from_secs(8))
                            .send()
                            .await
                            .map(|resp| {
                                info!("upstream check {} -> {:?}", mc_url.as_str(), resp.status());
                                resp.status() == StatusCode::OK
                                    || resp.status() == StatusCode::FORBIDDEN
                            })
                            .unwrap_or_else(|resp| {
                                info!(?resp);
                                info!(
                                    "upstream err check {} -> {:?}",
                                    mc_url.as_str(),
                                    resp.status()
                                );
                                resp.status() == Some(StatusCode::OK)
                                    || resp.status() == Some(StatusCode::FORBIDDEN)
                            })
                    } else {
                        info!("upstream checking -> {:?}", DL_OS_URL.as_str());
                        info!("upstream checking -> {:?}", MCS_OS_URL.as_str());
                        client
                            .head(DL_OS_URL.as_str())
                            .timeout(std::time::Duration::from_secs(8))
                            .send()
                            .await
                            .map(|resp| {
                                info!(
                                    "upstream check {} -> {:?}",
                                    DL_OS_URL.as_str(),
                                    resp.status()
                                );
                                resp.status() == StatusCode::OK
                                    || resp.status() == StatusCode::FORBIDDEN
                            })
                            .unwrap_or_else(|resp| {
                                info!(
                                    "upstream err check {} -> {:?}",
                                    DL_OS_URL.as_str(),
                                    resp.status()
                                );
                                resp.status() == Some(StatusCode::OK)
                                    || resp.status() == Some(StatusCode::FORBIDDEN)
                            })
                            && client
                                .head(MCS_OS_URL.as_str())
                                .timeout(std::time::Duration::from_secs(8))
                                .send()
                                .await
                                .map(|resp| {
                                    info!(
                                        "upstream check {} -> {:?}",
                                        MCS_OS_URL.as_str(),
                                        resp.status()
                                    );
                                    resp.status() == StatusCode::OK
                                        || resp.status() == StatusCode::FORBIDDEN
                                })
                                .unwrap_or_else(|resp| {
                                    info!(
                                        "upstream err check {} -> {:?}",
                                        MCS_OS_URL.as_str(),
                                        resp.status()
                                    );
                                    resp.status() == Some(StatusCode::OK)
                                        || resp.status() == Some(StatusCode::FORBIDDEN)
                                })
                    };
                    UPSTREAM_ONLINE.store(r, Ordering::Relaxed);
                    warn!("upstream online -> {}", r);
                }
                .instrument(tracing::info_span!("monitor_upstream"))
                .await;

                sleep(Duration::from_secs(5)).await;
            }
            _ => {
                break;
            }
        }
    }

    info!(immediate = true, "Stopping upstream monitor task.");
}

struct PrefetchReq {
    req_path: String,
    url: Url,
    file: NamedTempFile,
    submit_tx: Sender<CacheMeta>,
    cls: Classification,
}

async fn prefetch_task(
    state: Arc<AppState>,
    mut prefetch_rx: Receiver<PrefetchReq>,
    mut rx: broadcast::Receiver<bool>,
) {
    info!(immediate = true, "Spawning prefetch task ...");

    let mut req_cache = LruCache::new(NonZeroUsize::new(64).unwrap());

    while matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)) {
        async {
        tokio::select! {
            _ = sleep(Duration::from_secs(5)) => {
                // Do nothing, this is to make us loop and check the running state.
                info!("prefetch loop idle");
            }
            got = prefetch_rx.recv() => {
                match got {
                    Some(PrefetchReq {
                        req_path,
                        url,
                        file,
                        submit_tx,
                        cls
                    }) => {
                        trace!("received a prefetch operation");
                        let debounce_t = req_cache.get(&req_path)
                            .map(|inst: &Instant| inst.elapsed().as_secs())
                            .unwrap_or(DEBOUNCE + 1);
                        let debounce = debounce_t < DEBOUNCE;

                        if debounce {
                            info!(immediate = true, "Skipping debounce item {}", req_path);
                        } else {
                            prefetch_dl_task(state.client.clone(), url, submit_tx, req_path.clone(), file, cls).await;
                            // Sometimes if the dl is large, we can accidentally trigger a second dl because the cache
                            // hasn't finished crc32c yet. So we need a tiny cache to debounce repeat dl's.
                            req_cache.put(req_path, Instant::now());
                        }
                    }
                    None => {
                        // channels dead.
                        warn!("prefetch channel has died");
                        return;
                    }
                }
            }
        }
        }
        .instrument(tracing::info_span!("prefetch_task"))
        .await;
    }

    info!(immediate = true, "Stopping prefetch task.");
}

async fn ipxe_static(extract::Path(fname): extract::Path<PathBuf>) -> Response {
    let Some(rel_fname) = fname.file_name() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // Get the abs path.
    let abs_path = Path::new("/usr/share/ipxe").join(rel_fname);

    let n_file = match File::open(&abs_path).await {
        Ok(f) => f,
        Err(e) => {
            error!("{:?}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let stream = Body::from_stream(ReaderStream::new(BufReader::with_capacity(
        BUFFER_READ_PAGE,
        n_file,
    )));

    (StatusCode::OK, stream).into_response()
}

#[derive(Template)]
#[template(path = "ipxe.menu.html")]
struct IpxeMenuTemplate<'a> {
    mirror_uri: &'a str,
}

#[axum::debug_handler]
async fn ipxe_menu_view(
    headers: HeaderMap,
    extract::State(state): extract::State<Arc<AppState>>,
) -> Response {
    let menu = IpxeMenuTemplate {
        mirror_uri: state.boot_origin.as_str(),
    }
    .render()
    .unwrap();

    // error!("ipxe request_headers -> {:?}", headers);
    // ipxe request_headers -> {"connection": "keep-alive", "user-agent": "iPXE/1.21.1+git20231006.ff0f8604", "host": "172.24.11.130:8080"}

    // https://ipxe.org/cfg
    // https://ipxe.org/cmd/

    // set mirror-uri ${cwduri}

    menu.into_response()
}

async fn robots_view() -> Html<&'static str> {
    Html(
        r#"
User-agent: *
Disallow: /
"#,
    )
}

async fn status_view() -> Html<&'static str> {
    Html(r#"Ok"#)
}

#[derive(Debug, clap::Parser)]
#[clap(about = "OpenSUSE Caching Mirror Tool")]
struct Config {
    #[arg(short = 's', default_value = "17179869184", env = "CACHE_SIZE")]
    /// Disk size for cache content in bytes. Defaults to 16GiB
    cache_size: usize,
    #[arg(short = 'p', default_value = "/tmp/osuse_cache", env = "CACHE_PATH")]
    /// Path where cache content should be stored
    cache_path: PathBuf,
    #[arg(short = 'c', long = "cache_large_objects", env = "CACHE_LARGE_OBJECTS")]
    /// Should we cache large objects like ISO/vm images/boot images?
    cache_large_objects: bool,
    #[arg(short = 'w', long = "wonder_guard", env = "WONDER_GUARD")]
    /// Enables a bloom filter to prevent pre-emptive caching of one-hit-wonders
    wonder_guard: bool,
    #[arg(short = 'Z', long = "durable_fs", env = "DURABLE_FS")]
    /// Is this running on a consistent and checksummed fs? If yes, then we can skip
    /// internal crc32c sums on get().
    durable_fs: bool,
    #[arg(default_value = "[::]:8080", env = "BIND_ADDRESS", long = "addr")]
    /// Address to listen to for http
    bind_addr: String,

    #[arg(long = "boot-services", env = "BOOT_SERVICES")]
    /// Enable a tftp server for pxe boot services
    boot_services: bool,

    #[arg(
        env = "BOOT_ORIGIN",
        default_value = "http://localhost:8080",
        long = "boot_origin"
    )]
    /// The external URL of this server as seen by boot service clients
    boot_origin: Url,

    #[arg(env = "TLS_BIND_ADDRESS", long = "tlsaddr")]
    /// Address to listen to for https (optional)
    tls_bind_addr: Option<String>,
    #[arg(env = "TLS_PEM_KEY", long = "tlskey")]
    /// Path to the TLS Key file in PEM format.
    tls_pem_key: Option<String>,
    #[arg(env = "TLS_PEM_CHAIN", long = "tlschain")]
    /// Path to the TLS Chain file in PEM format.
    tls_pem_chain: Option<String>,
    #[arg(env = "MIRROR_CHAIN", long = "mirrorchain")]
    /// Url to another proxy-cache instance to chain through.
    mirror_chain: Option<String>,
    #[arg(env = "ACME_CHALLENGE_DIR", long = "acmechallengedir")]
    /// Location to store acme challenges for lets encrypt if in use.
    acme_challenge_dir: Option<String>,

    #[arg(env = "OAUTH2_CLIENT_ID", long = "oauth_client_id")]
    /// Oauth client id
    oauth_client_id: Option<String>,
    #[arg(env = "OAUTH2_CLIENT_SECRET", long = "oauth_client_secret")]
    /// Oauth client secret
    oauth_client_secret: Option<String>,
    #[arg(
        env = "OAUTH2_CLIENT_URL",
        default_value = "http://localhost:8080",
        long = "oauth_client_url"
    )]
    /// Oauth client url - this is the url of THIS server
    oauth_client_url: String,
    #[arg(env = "OAUTH2_SERVER_URL", long = "oauth_server_url")]
    /// Oauth server url - the url of the authorisation provider
    oauth_server_url: Option<String>,
}

async fn do_main() {
    let config = Config::parse();

    // This affects a bunch of things, may need to override in the upstream check.
    let timeout = std::time::Duration::from_secs(7200);

    let client = reqwest::ClientBuilder::new()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_proxy()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(ALLOW_REDIRECTS))
        .build()
        .expect("Unable to build client");

    trace!("Trace working!");
    debug!("Debug working!");

    let (tx, mut rx1) = broadcast::channel(1);
    let (prefetch_tx, prefetch_rx) = channel(2048);

    let mirror_chain = config
        .mirror_chain
        .as_ref()
        .map(|s| Url::parse(s).expect("Invalid mirror_chain url"));

    let app_state_res = AppState::new(
        config.cache_size,
        &config.cache_path,
        config.cache_large_objects,
        config.wonder_guard,
        config.durable_fs,
        mirror_chain.clone(),
        client.clone(),
        prefetch_tx,
        config.boot_origin.clone(),
    );

    let app_state = match app_state_res {
        Ok(state) => Arc::new(state),
        Err(err) => {
            error!(?err, "Unable to configure cache");
            return;
        }
    };

    let app = Router::new()
        .route("/", get(get_view).head(head_view))
        .route("/*req_path", get(get_view).head(head_view))
        .route("/_status", get(status_view))
        .route("/robots.txt", get(robots_view))
        .route("/menu.ipxe", get(ipxe_menu_view))
        .route("/ipxe/:fname", get(ipxe_static))
        .with_state(app_state.clone());

    // Later need to add acme well-known if needed.

    let svc = app
        // .into_make_service();
        .into_make_service_with_connect_info::<SocketAddr>();

    let tls_server_handle = match (
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

            let tls_addr = SocketAddr::from_str(&tba).expect("Invalid config bind address");

            let tls_svc = svc.clone();
            let mut tls_rx1 = tx.subscribe();

            let tls_config = RustlsConfig::from_pem_chain_file(p_tpc, p_tpk)
                .await
                .expect("Invalid TLS configuration");

            let server_handle = Handle::new();

            let server_fut = axum_server::bind_rustls(tls_addr, tls_config)
                .acceptor(NoDelayAcceptor::new())
                .handle(server_handle.clone())
                .serve(tls_svc);

            tokio::task::spawn(async move {
                let _ = tls_rx1.recv().await;
                server_handle.shutdown();
            });

            Some(tokio::task::spawn(async move {
                server_fut.await.unwrap();
                info!("TLS Server has stopped!");
            }))
        }
        (None, None, None) => {
            info!("TLS not configured");
            None
        }
        _ => {
            error!("Inconsistent TLS config. Must specfiy tls_bind_addr, tls_pem_key and tls_pem_chain");
            return;
        }
    };

    let addr = SocketAddr::from_str(&config.bind_addr).expect("Invalid config bind address");
    info!("Binding -> http://{}", config.bind_addr);

    let monitor_rx = tx.subscribe();
    let monitor_client = client.clone();
    let monitor_handle = tokio::task::spawn(async move {
        monitor_upstream(monitor_client, mirror_chain, monitor_rx).await
    });

    let prefetch_bcast_rx = tx.subscribe();
    let prefetch_app_state = app_state.clone();
    let prefetch_handle = tokio::task::spawn(async move {
        prefetch_task(prefetch_app_state, prefetch_rx, prefetch_bcast_rx).await
    });

    let server_handle = tokio::task::spawn(async move {
        tokio::select! {
            _ = rx1.recv() => {
                return
            }
            _ = axum_server::bind(addr)
                .acceptor(NoDelayAcceptor::new())
                .serve(svc) => {}
        }
        info!("Server has stopped!");
    });

    let mut boot_services_rx = tx.subscribe();

    let maybe_tftp_handle = if config.boot_services {
        let tftp_handle = tokio::task::spawn(async move {
            let tftpd = async_tftp::server::TftpServerBuilder::with_dir_ro("/usr/share/ipxe/")
                .expect("Unable to build tftp server")
                .build()
                .await
                .expect("Unable to build tftp server");
            info!("Starting TFTP");
            tokio::select! {
                _ = boot_services_rx.recv() => {
                    return
                }
                _ = tftpd.serve() => {}
            }
            info!("TFTP Server has stopped!");
        });
        Some(tftp_handle)
    } else {
        None
    };

    // Block for signals now

    tokio::select! {
        Ok(()) = tokio::signal::ctrl_c() => {}
        Some(()) = async move {
            let sigterm = tokio::signal::unix::SignalKind::terminate();
            tokio::signal::unix::signal(sigterm).unwrap().recv().await
        } => {}
    }

    info!("Stopping ...");
    tx.send(true).expect("Failed to signal workes to stop");

    let _ = server_handle.await;

    if let Some(tls_server_handle) = tls_server_handle {
        let _ = tls_server_handle;
    }

    let _ = monitor_handle.await;
    let _ = prefetch_handle.await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() {
    #[cfg(feature = "dhat-heap")]
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
    // let fmt_layer = tracing_subscriber::fmt::layer();
    // .with_target(true);

    // let console_layer = ConsoleLayer::builder().with_default_env().spawn();

    Registry::default()
        // .with(console_layer)
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    do_main().await;
}
