use concread::arcache::{ARCache, ARCacheBuilder, CacheStats};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tracing_forest::prelude::*;

use serde::{Deserialize, Serialize};

use crate::cache::Classification;
use crate::RUNNING;

const PENDING_ADDS: usize = 8;

#[derive(Debug)]
pub enum Action {
    Submit {
        file: NamedTempFile,
        // These need to be extracted
        headers: BTreeMap<String, String>,
        content: Option<tide::http::Mime>,
        etag: Option<String>,
        amt: usize,
        hash_str: String,
        cls: Classification,
    },
    Update,
    NotFound,
}

#[derive(Debug)]
pub struct CacheMeta {
    // Clippy will whinge about variant sizes here.
    pub req_path: String,
    // Add the time this was added
    pub etime: OffsetDateTime,
    pub action: Action,
}

#[derive(Clone, Debug)]
pub struct CacheObj {
    pub req_path: String,
    pub fhandle: Arc<FileHandle>,
    pub headers: BTreeMap<String, String>,
    pub expiry: Option<OffsetDateTime>,
    pub etag: Option<String>,
    pub cls: Classification,
}

#[derive(Clone, Debug)]
pub struct FileHandle {
    pub meta_path: PathBuf,
    pub path: PathBuf,
    pub amt: usize,
    pub hash_str: String,
    pub content: Option<tide::http::Mime>,
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        // Always drop metadata on shutdown.
        if RUNNING.load(Ordering::Relaxed) {
            info!("🗑  remove fhandle -> {:?}", self.path);
            let _ = std::fs::remove_file(&self.meta_path);
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheObjMeta {
    pub req_path: String,
    pub headers: BTreeMap<String, String>,
    pub content: Option<String>,
    pub amt: usize,
    pub hash_str: String,
    pub expiry: Option<OffsetDateTime>,
    pub etag: Option<String>,
    pub cls: Classification,
}

#[derive(Debug, Clone)]
pub enum Status {
    Exist(Arc<CacheObj>),
    NotFound(OffsetDateTime),
}

pub struct ArcDiskCache {
    cache: Arc<ARCache<String, Status>>,
    pub submit_tx: Sender<CacheMeta>,
    pub content_dir: PathBuf,
}

fn persist_item(
    req_path: &String,
    etag: Option<String>,
    etime: OffsetDateTime,
    file: NamedTempFile,
    headers: BTreeMap<String, String>,
    content: Option<tide::http::Mime>,
    amt: usize,
    hash_str: String,
    cls: Classification,
    content_dir_buf: &Path,
) -> Option<Arc<CacheObj>> {
    let expiry = cls.expiry(etime);

    let path = content_dir_buf.join(&hash_str);
    let mut meta_str = hash_str.clone();
    meta_str.push_str(".meta");
    let meta_path = content_dir_buf.join(&meta_str);
    let m_file = File::create(&meta_path)
        .map(BufWriter::new)
        .map_err(|e| {
            error!("Failed to open metadata {:?}", e);
        })
        .ok()?;

    let objmeta = CacheObjMeta {
        req_path: req_path.clone(),
        headers: headers.clone(),
        content: content.as_ref().map(|mtype| mtype.to_string()),
        amt,
        hash_str: hash_str.clone(),
        expiry: expiry.clone(),
        etag: etag.clone(),
        cls,
    };

    serde_json::to_writer(m_file, &objmeta)
        .map_err(|e| {
            error!("Failed to write metadata {:?}", e);
        })
        .ok()?;

    info!("Persisted metadata for {:?} to {:?}", req_path, &meta_path);

    // Move it to the correct content dir loc named by hash.
    // Convert to a CacheObj with the hash name.
    file.persist(&path)
        .map_err(|e| {
            error!("Unable to persist {:?} -> {:?}", &path, e);
        })
        .ok()
        .map(|_n_file| {
            debug!("Persisted to {:?}", &path);

            Arc::new(CacheObj {
                req_path: req_path.clone(),
                fhandle: Arc::new(FileHandle {
                    meta_path,
                    path,
                    amt,
                    hash_str,
                    content,
                }),
                headers,
                expiry,
                etag,
                cls,
            })
        })
}

async fn cache_mgr(
    mut submit_rx: Receiver<CacheMeta>,
    cache: Arc<ARCache<String, Status>>,
    content_dir_buf: PathBuf,
) {
    // Wait on the channel, and when we get something proceed from there.
    while let Some(meta) = submit_rx.recv().await {
        debug!("✨ Cache Manager Got -> {:?}", meta);
        let mut wrtxn = cache.write();

        let CacheMeta {
            req_path,
            etime,
            action,
        } = meta;

        // What are we trying to do here?
        let item = wrtxn.get(&req_path);
        match (item, action) {
            (
                None,
                Action::Submit {
                    file,
                    headers,
                    content,
                    etag,
                    amt,
                    hash_str,
                    cls,
                },
            ) => {
                // Do the submit
                if let Some(obj) = persist_item(
                    &req_path,
                    etag,
                    etime,
                    file,
                    headers,
                    content,
                    amt,
                    hash_str,
                    cls,
                    &content_dir_buf,
                ) {
                    wrtxn.insert_sized(req_path, Status::Exist(obj), amt)
                }
            }
            (
                Some(Status::Exist(exist_meta)),
                Action::Submit {
                    file,
                    headers,
                    content,
                    etag,
                    amt,
                    hash_str,
                    cls,
                },
            ) => {
                // Is the hash different/same?
                if exist_meta.fhandle.hash_str == hash_str {
                    info!("Ignoring same file");
                    let pbuf = file.path().to_path_buf();
                    if let Err(e) = file.close() {
                        error!("Failed to remove temporary file: {:?} -> {:?}", pbuf, e);
                    } else {
                        info!("Removed {:?}", pbuf)
                    }
                } else {
                    // Do the submit.
                    if let Some(obj) = persist_item(
                        &req_path,
                        etag,
                        etime,
                        file,
                        headers,
                        content,
                        amt,
                        hash_str,
                        cls,
                        &content_dir_buf,
                    ) {
                        wrtxn.insert_sized(req_path, Status::Exist(obj), amt)
                    }
                }
            }
            (
                Some(Status::NotFound(_)),
                Action::Submit {
                    file,
                    headers,
                    content,
                    etag,
                    amt,
                    hash_str,
                    cls,
                },
            ) => {
                // Do the submit.
                if let Some(obj) = persist_item(
                    &req_path,
                    etag,
                    etime,
                    file,
                    headers,
                    content,
                    amt,
                    hash_str,
                    cls,
                    &content_dir_buf,
                ) {
                    wrtxn.insert_sized(req_path, Status::Exist(obj), amt)
                }
            }
            (Some(Status::Exist(exist_meta)), Action::Update) => {
                let mut obj: CacheObj = (*exist_meta.as_ref()).clone();
                obj.expiry = obj.cls.expiry(etime);
                let amt = obj.fhandle.amt;
                // Update it
                wrtxn.insert_sized(req_path, Status::Exist(Arc::new(obj)), amt)
            }
            (None, Action::Update) => {
                // Skip, it's been removed.
                info!("Skip update - cache has removed this item.");
            }
            (Some(Status::NotFound(_)), Action::Update) | (_, Action::NotFound) => {
                wrtxn.insert_sized(req_path, Status::NotFound(etime), 1)
            }
        }
        wrtxn.commit();
    }
}

async fn cache_stats(cache: Arc<ARCache<String, Status>>) {
    let zero = CacheStats::default();
    loop {
        let stats = cache.view_stats();
        warn!("cache stats - {:?}", (*stats).change_since(&zero));
        if cfg!(debug_assertions) {
            sleep(Duration::from_secs(5)).await;
        } else {
            sleep(Duration::from_secs(300)).await;
        }
    }
}

impl ArcDiskCache {
    pub fn new(capacity: usize, content_dir: &Path) -> Self {
        let cache = Arc::new(
            ARCacheBuilder::new()
                .set_size(capacity, 0)
                .set_watermark(0)
                .build()
                .expect("Invalid ARCache Parameters"),
        );
        let cache_mgr_clone = cache.clone();
        let content_dir_buf = content_dir.to_path_buf();
        let (submit_tx, submit_rx) = channel(PENDING_ADDS);
        // This launches our task too.
        let _ = tokio::task::spawn(async move {
            cache_mgr(submit_rx, cache_mgr_clone, content_dir_buf).await
        });

        // dump cache stats sometimes
        let cache_mgr_clone = cache.clone();
        let _ = tokio::task::spawn(async move { cache_stats(cache_mgr_clone).await });

        // Now for everything in content dir, look if we have valid metadata
        // and everything that isn't metadata.
        let mut entries = std::fs::read_dir(content_dir)
            .expect("unable to read content dir")
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .expect("Failed to access some dirents");

        entries.sort();

        let (meta, files): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|p| p.extension() == Some(std::ffi::OsStr::new("meta")));

        let meta_len = meta.len();
        info!("Will process {} metadata", meta_len);

        // Now we read each metadata in.
        let meta: Vec<(PathBuf, CacheObjMeta)> = meta
            .into_iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if i % 1000 == 0 {
                    info!("{} of {}", i, meta_len);
                }
                trace!(?p, "meta read");
                File::open(&p)
                    .ok()
                    .map(|f| BufReader::new(f))
                    .and_then(|rdr| serde_json::from_reader(rdr).ok())
                    .map(|m| (p.to_path_buf(), m))
            })
            .collect();

        let meta: Vec<CacheObj> = meta
            .into_iter()
            .filter_map(|(meta_path, m)| {
                let CacheObjMeta {
                    req_path,
                    headers,
                    content,
                    amt,
                    hash_str,
                    expiry,
                    etag,
                    cls,
                } = m;

                let path = content_dir.join(&hash_str);
                let content = content.and_then(|s| tide::http::Mime::from_str(&s).ok());

                if path.exists() {
                    Some(CacheObj {
                        req_path,
                        fhandle: Arc::new(FileHandle {
                            meta_path,
                            path,
                            amt,
                            hash_str,
                            content,
                        }),
                        headers,
                        expiry,
                        etag,
                        cls,
                    })
                } else {
                    None
                }
            })
            .collect();

        warn!("Found {:?} existing metadata", meta.len());

        // Now we prune any files that ARENT in our valid cache meta set.
        let mut files: BTreeSet<_> = files.into_iter().collect();
        meta.iter().for_each(|co| {
            files.remove(&co.fhandle.path);
        });

        files.iter().for_each(|p| {
            warn!("🗑  -> {:?}", p);
            let _ = std::fs::remove_file(p);
        });

        // Finally setup the cache.
        let mut wrtxn = cache.write();
        meta.into_iter().for_each(|co| {
            let req_path = co.req_path.clone();
            let amt = co.fhandle.amt;
            wrtxn.insert_sized(req_path, Status::Exist(Arc::new(co)), amt);
        });
        wrtxn.commit();

        // Reset the stats so that the import isn't present.
        cache.reset_stats();

        warn!("ArcDiskCache Ready!");

        // This launches our metadata sync task.
        ArcDiskCache {
            content_dir: content_dir.to_path_buf(),
            cache,
            submit_tx,
        }
    }

    pub fn get(&self, req_path: &str) -> Option<Status> {
        let mut rtxn = self.cache.read();
        rtxn.get(req_path).cloned()
    }

    /*
    pub fn submit(&self, meta: CacheMeta) {
        if let Err(e) = self.submit_tx.blocking_send(meta) {
            error!("Failed to submit to cache channel -> {:?}", e);
        }
    }
    */
}