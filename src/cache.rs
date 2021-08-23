use crate::arc_disk_cache::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tide::log;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;

use serde::{Deserialize, Serialize};

pub enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream,
    // We have this item, and can send from our cache.
    FoundObj(Arc<CacheObj>),
    // We don't have this item but we want it, so please dl it to this location
    // then notify this cache.
    MissObj(PathBuf, Sender<CacheMeta>, Classification),
    // Refresh
    Refresh(PathBuf, Sender<CacheMeta>, Arc<CacheObj>),
    // Can't proceed, something is wrong.
    Invalid,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum Classification {
    Metadata,
    Repository,
    Blob,
    Unknown,
}

impl Classification {
    pub fn expiry(&self, etime: OffsetDateTime) -> Option<OffsetDateTime> {
        match self {
            Classification::Metadata => Some(etime + time::Duration::minutes(2)),
            // Classification::Metadata => Some(etime),
            Classification::Repository => {
                // Content lives 4eva due to unique filenames
                None
            }
            Classification::Blob => Some(etime + time::Duration::minutes(20)),
            Classification::Unknown => Some(etime),
        }
    }
}

pub struct Cache {
    pri_cache: ArcDiskCache,
    clob: bool,
}

impl Cache {
    pub fn new(capacity: usize, content_dir: &Path, clob: bool) -> Self {
        Cache {
            pri_cache: ArcDiskCache::new(capacity, content_dir),
            clob,
        }
    }

    pub fn decision(&self, req_path: &str) -> CacheDecision {
        log::info!("🤔  contemplating req -> {:?}", req_path);

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

        match self.pri_cache.get(req_path) {
            Some(meta) => {
                // If we hit, we need to decide if this
                // is a found item or something that may need
                // a refresh.
                if let Some(exp) = meta.expiry {
                    if time::OffsetDateTime::now_utc() > exp {
                        log::debug!("EXPIRED");
                        return CacheDecision::Refresh(
                            self.pri_cache.content_dir.clone(),
                            self.pri_cache.submit_tx.clone(),
                            meta,
                        );
                    }
                }

                log::debug!("HIT");
                CacheDecision::FoundObj(meta)
            }
            None => {
                // If miss, we need to choose between stream and
                // miss.
                log::debug!("MISS");

                match (self.classify(&fname, req_path), self.clob) {
                    (Classification::Blob, false) | (Classification::Unknown, _) => {
                        CacheDecision::Stream
                    }
                    (cls, _) => CacheDecision::MissObj(
                        self.pri_cache.content_dir.clone(),
                        self.pri_cache.submit_tx.clone(),
                        cls,
                    ),
                }
            }
        }
    }

    fn classify(&self, fname: &str, req_path: &str) -> Classification {
        if fname == "repomed.xml"
            || fname == "repomd.xml"
            || fname == "media"
            || fname == "products"
            || fname == "repomd.xml.key"
            || fname == "content"
            || fname.ends_with("asc")
            || fname.ends_with("sha256")
            // Related to live boots of tumbleweed.
            || fname == "CHECKSUMS"
            || fname == "config"
            || fname == "bind"
            || fname == "control.xml"
            || fname == "license.tar.gz"
            || fname == "info.txt"
            || fname == "part.info"
            || fname == "README.BETA"
            || fname == "driverupdate"
        {
            log::info!("Classification::Metadata");
            Classification::Metadata
        } else if fname.ends_with("iso")
            || fname.ends_with("qcow2")
            || fname.ends_with("raw")
            || fname.ends_with("raw.xz")
            || fname.ends_with("tar.xz")
            // Related to live boots of tumbleweed.
            || fname == "linux"
            || fname == "initrd"
            || fname == "common"
            || fname == "root"
            || fname == "cracklib-dict-full.rpm"
            || fname.starts_with("yast2-trans")
        {
            log::info!("Classification::Blob");
            Classification::Blob
        } else if fname.ends_with("rpm")
            || fname.ends_with("primary.xml.gz")
            || fname.ends_with("filelists.xml.gz")
            || fname.ends_with("other.xml.gz")
            || fname.ends_with("updateinfo.xml.gz")
            || fname.ends_with("susedata.xml.gz")
            || fname.ends_with("appdata-icons.tar.gz")
            || fname.ends_with("appdata.xml.gz")
            || fname.ends_with("license.tar.gz")
        {
            log::info!("Classification::Repository");
            Classification::Repository
        } else {
            log::error!("⚠️  Classification::Unknown - {}", req_path);
            Classification::Unknown
        }
    }
}
