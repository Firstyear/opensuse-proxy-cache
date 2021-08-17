use crate::arc_disk_cache::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tide::log;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream,
    // We have this item, and can send from our cache.
    FoundObj(Arc<CacheObj>),
    // We don't have this item but we want it, so please dl it to this location
    // then notify this cache.
    MissObj(PathBuf, Sender<CacheMeta>, Classification),
    // Refresh
    Refresh,
    // Can't proceed, something is wrong.
    Invalid,
}

pub enum Classification {
    Metadata,
    Repository,
    Blob,
    Unknown,
}

pub struct Cache {
    pri_cache: ArcDiskCache,
}

impl Cache {
    pub fn new(capacity: usize, content_dir: &Path) -> Self {
        Cache {
            pri_cache: ArcDiskCache::new(capacity, content_dir),
        }
    }

    pub fn decision(&self, req_path: &str) -> CacheDecision {
        log::info!("req -> {:?}", req_path);

        let path = Path::new(req_path);

        // If the path fails some validations, refuse to proceed.
        if !path.is_absolute() {
            log::error!("path not absolute");
            return CacheDecision::Invalid;
        }

        let fname = path
            .file_name()
            .and_then(|f| f.to_str().map(str::to_string))
            .unwrap_or_else(|| "index.html".to_string());

        log::info!("🤔  {:?}", fname);

        match self.pri_cache.get(req_path) {
            Some(meta) => {
                // If we hit, we need to decide if this
                // is a found item or something that may need
                // a refresh.
                log::info!("HIT");
                return CacheDecision::FoundObj(meta);
            }
            None => {
                // If miss, we need to choose between stream and
                // miss.
                log::info!("MISS");

                match self.classify(&fname) {
                    Classification::Unknown => CacheDecision::Stream,
                    cls => CacheDecision::MissObj(
                        self.pri_cache.content_dir.clone(),
                        self.pri_cache.submit_tx.clone(),
                        cls,
                    ),
                }
            }
        }
    }

    fn classify(&self, fname: &str) -> Classification {
        if fname == "repomed.xml"
            || fname == "repomd.xml"
            || fname == "repomd.xml.asc"
            || fname == "media"
            || fname == "products"
            || fname == "repomd.xml.key"
        {
            log::info!("Classification::Metadata");
            Classification::Metadata
        } else if fname.ends_with("rpm")
            || fname.ends_with("primary.xml.gz")
            || fname.ends_with("filelists.xml.gz")
            || fname.ends_with("other.xml.gz")
            || fname.ends_with("updateinfo.xml.gz")
            || fname.ends_with("susedata.xml.gz")
            || fname.ends_with("appdata-icons.tar.gz")
            || fname.ends_with("appdata.xml.gz")
        {
            log::info!("Classification::Repository");
            Classification::Repository
        } else {
            log::info!("Classification::Unknown");
            Classification::Unknown
        }
    }
}
