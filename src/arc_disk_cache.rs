use concread::arcache::ARCache;
use std::collections::BTreeSet;
use std::fs::{read_dir, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tide::log;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

use serde::{Deserialize, Serialize};

use crate::RUNNING;

const PENDING_ADDS: usize = 8;

#[derive(Debug)]
pub struct CacheMeta {
    pub req_path: String,
    pub file: NamedTempFile,
    pub headers: Vec<(String, String)>,
    pub content: Option<tide::http::Mime>,
    pub amt: usize,
    pub hash_str: String,
}

#[derive(Debug)]
pub struct CacheObj {
    pub req_path: String,
    pub meta_path: PathBuf,
    pub path: PathBuf,
    pub headers: Vec<(String, String)>,
    pub content: Option<tide::http::Mime>,
    pub amt: usize,
    pub hash_str: String,
}

impl Drop for CacheObj {
    fn drop(&mut self) {
        if RUNNING.load(Ordering::Relaxed) {
            let _ = std::fs::remove_file(&self.meta_path);
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheObjMeta {
    pub req_path: String,
    pub headers: Vec<(String, String)>,
    pub content: Option<String>,
    pub amt: usize,
    pub hash_str: String,
}

pub struct ArcDiskCache {
    capacity: usize,
    cache: Arc<ARCache<String, Arc<CacheObj>>>,
    pub submit_tx: Sender<CacheMeta>,
    pub content_dir: PathBuf,
}

async fn cache_mgr(
    mut submit_rx: Receiver<CacheMeta>,
    cache: Arc<ARCache<String, Arc<CacheObj>>>,
    content_dir_buf: PathBuf,
) {
    // Wait on the channel, and when we get something proceed from there.
    while let Some(meta) = submit_rx.recv().await {
        log::info!("Got {:?}", meta);
        let mut wrtxn = cache.write();
        // Do we have it already?
        if wrtxn.contains_key(&meta.req_path) {
            log::info!("Skipping existing item");
            let CacheMeta { file, .. } = meta;
            let pbuf = file.path().to_path_buf();
            if let Err(e) = file.close() {
                log::error!("Failed to remove temporary file: {:?}", pbuf);
            } else {
                log::info!("Removed {:?}", pbuf)
            }
        } else {
            // Not present? Okay, lets get it ready to add.
            let CacheMeta {
                req_path,
                file,
                headers,
                content,
                amt,
                hash_str,
            } = meta;

            let path = content_dir_buf.join(&hash_str);
            let mut meta_str = hash_str.clone();
            meta_str.push_str(".meta");
            let meta_path = content_dir_buf.join(&meta_str);
            let m_file = match File::create(&meta_path).map(BufWriter::new) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("Failed to open metadata {:?}", e);
                    // Loop without commit.
                    continue;
                }
            };

            let objmeta = CacheObjMeta {
                req_path: req_path.clone(),
                headers: headers.clone(),
                content: content.as_ref().map(|mtype| mtype.to_string()),
                amt,
                hash_str: hash_str.clone(),
            };

            if let Err(e) = serde_json::to_writer(m_file, &objmeta) {
                log::error!("Failed to write metadata {:?}", e);
                continue;
            };

            log::info!("Persisted metadata to {:?}", &meta_path);

            // Move it to the correct content dir loc named by hash.
            // Convert to a CacheObj with the hash name.
            match file.persist(&path) {
                Ok(n_file) => {
                    log::info!("Persisted to {:?}", &path);

                    let obj = Arc::new(CacheObj {
                        req_path: req_path.clone(),
                        meta_path,
                        path,
                        headers,
                        content,
                        amt,
                        hash_str,
                    });
                    wrtxn.insert(req_path, obj);
                }
                Err(e) => {
                    log::error!("Unable to persist {:?}", &path);
                }
            }
        }
        wrtxn.commit();
    }
}

async fn cache_stats(cache: Arc<ARCache<String, Arc<CacheObj>>>) {
    loop {
        log::info!("{:?}", (*cache.view_stats()));
        sleep(Duration::from_secs(3600)).await;
    }
}

impl ArcDiskCache {
    pub fn new(capacity: usize, content_dir: &Path) -> Self {
        let cache = Arc::new(ARCache::new_size(4096, 0));
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

        log::debug!("{:?}", entries);

        let (meta, files): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|p| p.extension() == Some(std::ffi::OsStr::new("meta")));

        // Now we read each metadata in.
        let meta: Vec<(PathBuf, CacheObjMeta)> = meta
            .into_iter()
            .filter_map(|p| {
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
                } = m;
                let path = content_dir.join(&hash_str);
                let content = content.and_then(|s| tide::http::Mime::from_str(&s).ok());

                if path.exists() {
                    Some(CacheObj {
                        req_path,
                        meta_path,
                        path,
                        headers,
                        content,
                        amt,
                        hash_str,
                    })
                } else {
                    None
                }
            })
            .collect();

        log::warn!("Found {:?} existing metadata", meta.len());

        // Now we prune any files that ARENT in our valid cache meta set.
        let mut files: BTreeSet<_> = files.into_iter().collect();
        meta.iter().for_each(|co| {
            files.remove(&co.path);
        });

        files.iter().for_each(|p| {
            log::warn!("🗑  -> {:?}", p);
            let _ = std::fs::remove_file(p);
        });

        // Finally setup the cache.
        let mut wrtxn = cache.write();
        meta.into_iter().for_each(|co| {
            let req_path = co.req_path.clone();
            wrtxn.insert(req_path, Arc::new(co));
        });
        wrtxn.commit();

        // This launches our metadata sync task.
        ArcDiskCache {
            content_dir: content_dir.to_path_buf(),
            capacity,
            cache,
            submit_tx,
        }
    }

    pub fn get(&self, req_path: &str) -> Option<Arc<CacheObj>> {
        let mut rtxn = self.cache.read();
        rtxn.get(req_path).cloned()
    }

    /*
    pub fn submit(&self, meta: CacheMeta) {
        if let Err(e) = self.submit_tx.blocking_send(meta) {
            log::error!("Failed to submit to cache channel -> {:?}", e);
        }
    }
    */
}
