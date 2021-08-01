use async_std::io::prelude::*;
use async_std::task::Context;
use async_std::task::Poll;
use pin_project_lite::pin_project;
use std::io::{Write, BufWriter};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tempfile::{tempdir, TempDir, NamedTempFile};
use tide::log;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use url::Url;
use concread::arcache::ARCache;
use std::path::PathBuf;
use async_std::fs::File;
use async_std::io::BufReader;
use std::str::FromStr;


use std::os::unix::io::{FromRawFd, AsRawFd};

const ALLOW_REDIRECTS: u8 = 2;
const BUFFER_SIZE: usize = 4194304;

#[derive(Debug)]
struct CacheMeta {
    file: NamedTempFile,
    headers: Vec<(String, String)>,
    content: Option<tide::http::Mime>,
}

struct AppState {
    cache: ARCache<String, Arc<CacheMeta>>,
    content_dir: TempDir,
}

enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream,
    // We have this item, and can send from our cache.
    Found(Arc<CacheMeta>),
    // We don't have this item but we want it, so please dl it.
    Miss,
    // Can't proceed
    Invalid,
}

type BoxAsyncBufRead =
    Box<(dyn async_std::io::BufRead + Sync + Unpin + std::marker::Send + 'static)>;

impl Default for AppState {
    fn default() -> Self {
        AppState {
            cache: ARCache::new_size(4096, 0),
            content_dir: tempdir().expect("Cant create temp dir")
        }
    }
}

impl AppState {
    pub fn cache_decision(&self, req_path: &str) -> CacheDecision {
        log::info!("req -> {:?}", req_path);

        let path = Path::new(req_path);

        // If the path fails some validations, refuse to proceed.
        if !path.is_absolute() {
            log::error!("path not absolute");
            return CacheDecision::Invalid;
        }

        let fname = match path
            .file_name()
            .and_then(|f| f.to_str().map(str::to_string))
        {
            Some(fname) => fname,
            None => {
                log::error!("no valid filename found");
                return CacheDecision::Invalid;
            }
        };

        // Can't cache the following files.
        if fname == "repomed.xml"
            || fname == "repomd.xml"
            || fname == "repomd.xml.asc"
            || fname == "media"
            || fname == "repomd.xml.key"
        {
            return CacheDecision::Stream;
        }

        // Cache rpm.
        //
        // If it's in the repodata folder we CAN cache it because these have unique file names that
        // repomed will change on us when required. Generally these are xml.gz files.
        // a5ca84342048635bc688cde4d58177cedf845481332d86b8e4d6c70182970a86-filelists.xml.gz
        log::info!("🤔  -{:?}-", fname);
        if fname.ends_with("rpm")
            || fname.ends_with("primary.xml.gz")
            || fname.ends_with("filelists.xml.gz")
            || fname.ends_with("other.xml.gz")
            || fname.ends_with("updateinfo.xml.gz")
        {
            let mut rtxn = self.cache.read();
            match rtxn.get(req_path) {
                Some(meta) => {
                    log::info!("HIT");
                    return CacheDecision::Found(meta.clone());
                }
                None =>  {
                    log::info!("MISS");
                    return CacheDecision::Miss;
                }
            }
            // Do we have this in our cache? Use full path.
        }

        // We shouldn't cache anything else. We may split some types out in the future if we decide
        // to allow caching other content in the future.

        CacheDecision::Stream
    }
}

pin_project! {
    struct CacheReader {
        dlos_reader: BoxAsyncBufRead,
        io_tx: Sender<u8>,
    }
}

impl CacheReader {
    pub fn new(dlos_reader: BoxAsyncBufRead, io_tx: Sender<u8>) -> Self {
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
                let _ = buf
                    .split_at(amt)
                    .0
                    .iter()
                    .copied()
                    .try_for_each(|byte| self.io_tx.blocking_send(byte));

                // Write the content of the buffer here into the channel.
                log::debug!("amt -> {:?}", amt);
                Poll::Ready(Ok(amt))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
        // This is where we need to intercept and capture the bytes streamed.
    }
}

impl BufRead for CacheReader {
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

async fn write_file(
    mut io_rx: Receiver<u8>,
    req_path: String,
    content: Option<tide::http::Mime>,
    headers: Vec<(String, String)>,
    request: tide::Request<Arc<AppState>>
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

    if cnt_amt == 0 {
        return;
    }

    // Create a tempfile.
    let file = match NamedTempFile::new_in(&request.state().content_dir) {
        Ok(t) => {
            log::info!("🥰  -> {:?}", t.path());
            t
        }
        Err(e) => {
            log::error!("{:?}", e);
            return;
        }
    };

    let mut buf_file = BufWriter::new(file);

    while let Some(byte) = io_rx.recv().await {
        // Path?
        let x: [u8; 1] = [byte];
        buf_file.write(&x);
        amt += 1;
    }

    // Check the content len is ok.
    if cnt_amt != amt {
        log::info!("transfer interuppted, ending");
        return;
    }

    if let Ok(mut file) = buf_file.into_inner() {
        file.as_file().sync_all();
        let meta = Arc::new(CacheMeta {
            // Converts to an async file
            file,
            headers,
            content
        });
        // Send the file + metadata to the main cache.
        let mut rtxn = request.state().cache.read();
        log::info!("finished -> {}, {}", req_path, amt);
        rtxn.insert(req_path, meta);
    } else {
        log::error!("error processing -> {}, {}", req_path, amt);
    }

}

async fn miss(request: tide::Request<Arc<AppState>>, url: Url, req_path: String) -> tide::Result {
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
    let (io_tx, io_rx) = channel(BUFFER_SIZE);

    // Setup a bg task for writing out the file. Needs the channel rx, the url,
    // and the request from tide to access the AppState. Also needs the hdrs
    // and content type
    let _ =
        async_std::task::spawn(async move { write_file(io_rx, req_path, content, headers, request).await });

    let body = dl_response.take_body();
    let reader = CacheReader::new(body.into_reader(), io_tx);
    let r_body = tide::Body::from_reader(reader, None);

    Ok(response.body(r_body).build())
}

async fn found(request: tide::Request<Arc<AppState>>, url: Url, meta: Arc<CacheMeta>) -> tide::Result {
    // We have a hit, with our cache meta! Hooray!
    // Let's setup the response, and then stream from the file!

    let response = tide::Response::builder(tide::StatusCode::Ok);
    // headers
    // let response = meta.
    let response = meta.headers.iter().fold(response, |response, (hv, hk)|
        response.header(hv.as_str(), hk.as_str()));

    let response = if let Some(cnt) = &meta.content {
        response.content_type(cnt.clone())
    } else {
        response
    };

    let n_file = File::open(meta.file.path()).await
        .map_err(|e| {
            log::error!("{:?}", e);
            tide::Error::from_str(tide::StatusCode::InternalServerError, "InternalServerError")
        })?;

    let reader = BufReader::new(n_file);

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
    let decision = request.state().cache_decision(req_path);
    let req_path = req_path.to_string();

    // Based on the decision, take an appropriate path.
    match decision {
        CacheDecision::Stream => stream(request, url).await,
        CacheDecision::Found(meta) => found(request, url, meta).await,
        CacheDecision::Miss => miss(request, url, req_path).await,
        CacheDecision::Invalid => Err(tide::Error::from_str(
            tide::StatusCode::InternalServerError,
            "Invalid Request",
        )),
    }
}

#[tokio::main]
async fn main() {
    log::with_level(tide::log::LevelFilter::Info);

    let app_state = Arc::new(AppState::default());
    log::info!("Using -> {:?}", app_state.content_dir.path());
    let mut app = tide::with_state(app_state);
    app.with(tide::log::LogMiddleware::new());
    app.at("/").get(index_view);
    app.at("/*").get(index_view);

    let _ = app.listen("[::]:8081").await;
}
