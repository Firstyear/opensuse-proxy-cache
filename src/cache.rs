use crate::arc_disk_cache::*;

use crate::constants::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tide::log;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use url::Url;

use serde::{Deserialize, Serialize};

pub enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream(Url),
    // We have this item, and can send from our cache.
    FoundObj(Arc<CacheObj>),
    // We don't have this item but we want it, so please dl it to this location
    // then notify this cache.
    MissObj(
        Url,
        PathBuf,
        Sender<CacheMeta>,
        Classification,
        Option<Vec<(String, Classification)>>,
    ),
    // Refresh - we can also prefetch some paths in the background.
    Refresh(
        Url,
        PathBuf,
        Sender<CacheMeta>,
        Arc<CacheObj>,
        Option<Vec<(String, Classification)>>,
    ),
    NotFound,
    // Can't proceed, something is wrong.
    Invalid,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum Classification {
    // The more major repos
    RepomdXmlSlow,
    // Stuff from obs
    RepomdXmlFast,
    // Metadata, related to repos. Do we need this?
    Metadata,
    // Large blobs that need a slower rate of refresh. Some proxies
    // may choose not to cache this at all.
    Blob,
    // Content that has inbuilt version strings, that we can
    // keep forever.
    Static,
    // Stuff that's not found.
    NotFound,
    // 🤔
    Unknown,
}

impl Classification {
    pub fn prefetch(&self, path: &Path) -> Option<Vec<(String, Classification)>> {
        match self {
            Classification::RepomdXmlSlow | Classification::RepomdXmlFast => {
                path.parent().and_then(|p| p.parent()).map(|p| {
                    vec![
                        (
                            p.join("media.1/media")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            Classification::Metadata,
                        ),
                        (
                            p.join("repodata/repomd.xml.asc")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            Classification::Metadata,
                        ),
                        (
                            p.join("repodata/repomd.xml.key")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            Classification::Metadata,
                        ),
                    ]
                })
            }
            _ => None,
        }
    }

    pub fn expiry(&self, etime: OffsetDateTime) -> Option<OffsetDateTime> {
        match self {
            Classification::RepomdXmlSlow | Classification::Metadata => {
                Some(etime + time::Duration::minutes(10))
            }
            Classification::RepomdXmlFast => Some(etime + time::Duration::minutes(2)),

            Classification::Blob => Some(etime + time::Duration::minutes(30)),
            // Content lives 4eva due to unique filenames
            Classification::Static => None,
            //
            Classification::NotFound => Some(etime + time::Duration::minutes(15)),
            // Always refresh
            Classification::Unknown => Some(etime),
        }
    }
}

pub struct Cache {
    pri_cache: ArcDiskCache,
    clob: bool,
    mirror_chain: Option<Url>,
}

impl Cache {
    pub fn new(capacity: usize, content_dir: &Path, clob: bool, mirror_chain: Option<Url>) -> Self {
        Cache {
            pri_cache: ArcDiskCache::new(capacity, content_dir),
            clob,
            mirror_chain,
        }
    }

    fn url(&self, cls: &Classification, req_path: &str) -> Url {
        let mut url = if let Some(m_url) = self.mirror_chain.as_ref() {
            m_url.clone()
        } else {
            match cls {
                Classification::RepomdXmlSlow
                | Classification::Metadata
                | Classification::RepomdXmlFast => MCS_OS_URL.clone(),
                Classification::Blob
                | Classification::Static
                | Classification::NotFound
                | Classification::Unknown => DL_OS_URL.clone(),
            }
        };

        url.set_path(req_path);
        url
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

        let cls = self.classify(&fname, req_path);

        match self.pri_cache.get(req_path) {
            Some(meta) => {
                // If we hit, we need to decide if this
                // is a found item or something that may need
                // a refresh.
                if let Some(exp) = meta.expiry {
                    if time::OffsetDateTime::now_utc() > exp {
                        log::debug!("EXPIRED");
                        return CacheDecision::Refresh(
                            self.url(&cls, req_path),
                            self.pri_cache.content_dir.clone(),
                            self.pri_cache.submit_tx.clone(),
                            meta,
                            cls.prefetch(&path),
                        );
                    }
                }

                if meta.cls == Classification::NotFound {
                    log::debug!("NOTFOUND");
                    return CacheDecision::NotFound;
                }

                log::debug!("HIT");
                CacheDecision::FoundObj(meta)
            }
            None => {
                // If miss, we need to choose between stream and
                // miss.
                log::debug!("MISS");

                match (cls, self.clob) {
                    (Classification::Blob, false) | (Classification::Unknown, _) => {
                        CacheDecision::Stream(self.url(&cls, req_path))
                    }
                    (cls, _) => CacheDecision::MissObj(
                        self.url(&cls, req_path),
                        self.pri_cache.content_dir.clone(),
                        self.pri_cache.submit_tx.clone(),
                        cls,
                        cls.prefetch(&path),
                    ),
                }
            }
        }
    }

    fn classify(&self, fname: &str, req_path: &str) -> Classification {
        if fname == "repomd.xml" {
            if req_path.starts_with("/repositories/") {
                // These are obs
                log::info!("Classification::RepomdXmlFast");
                Classification::RepomdXmlFast
            } else {
                log::info!("Classification::RepomdXmlSlow");
                Classification::RepomdXmlSlow
            }
        } else if fname == "media"
            || fname == "products"
            || fname == "repomd.xml.key"
            || fname.ends_with("asc")
            || fname.ends_with("sha256")
            // Related to live boots of tumbleweed.
            || fname == "add_on_products.xml"
            || fname == "add_on_products"
            || fname == "directory.yast"
            || fname == "CHECKSUMS"
            || fname == "config"
            || fname == "content"
            || fname == "bind"
            || fname == "control.xml"
            || fname == "autoinst.xml"
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
            Classification::Static
        } else {
            log::error!("⚠️  Classification::Unknown - {}", req_path);
            Classification::Unknown
        }
    }
}
