use crate::constants::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tracing::Instrument;
use url::Url;

use arc_disk_cache::{prelude::CacheStats, ArcDiskCache, CacheObj};

use serde::{Deserialize, Serialize};

const PENDING_ADDS: usize = 8;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Status {
    pub req_path: String,
    pub headers: BTreeMap<String, String>,
    //                  Soft            Hard
    pub expiry: Option<(OffsetDateTime, OffsetDateTime)>,
    pub content: Option<String>,
    pub etag: Option<String>,
    pub cls: Classification,
    pub nxtime: Option<OffsetDateTime>,
}

#[derive(Debug)]
pub struct CacheMeta {
    // Clippy will whinge about variant sizes here.
    pub req_path: String,
    // Add the time this was added
    pub etime: OffsetDateTime,
    pub action: Action,
}

/*
#[derive(Clone, Debug)]
pub struct CacheObj {
    pub req_path: String,
    pub fhandle: Arc<FileHandle>,
    pub headers: BTreeMap<String, String>,
    pub soft_expiry: Option<OffsetDateTime>,
    pub expiry: Option<OffsetDateTime>,
    pub etag: Option<String>,
    pub cls: Classification,
}
*/

#[derive(Debug)]
pub enum Action {
    Submit {
        file: NamedTempFile,
        // These need to be extracted
        headers: BTreeMap<String, String>,
        content: Option<tide::http::Mime>,
        etag: Option<String>,
        // amt: usize,
        // hash_str: String,
        cls: Classification,
    },
    Update,
    NotFound {
        cls: Classification,
    },
}

pub enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream(Url),
    // We have this item, and can send from our cache.
    FoundObj(CacheObj<String, Status>),
    // We don't have this item but we want it, so please dl it to this location
    // then notify this cache.
    MissObj(
        Url,
        NamedTempFile,
        Sender<CacheMeta>,
        Classification,
        Option<Vec<(String, NamedTempFile, Classification)>>,
    ),
    // Refresh - we can also prefetch some paths in the background.
    Refresh(
        Url,
        NamedTempFile,
        Sender<CacheMeta>,
        CacheObj<String, Status>,
        Option<Vec<(String, NamedTempFile, Classification)>>,
    ),
    // We found it, but we also want to refresh in the background.
    AsyncRefresh(
        Url,
        NamedTempFile,
        Sender<CacheMeta>,
        CacheObj<String, Status>,
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
    // Metadata, related to repos.
    Metadata,
    // Large blobs that need a slower rate of refresh. Some proxies
    // may choose not to cache this at all.
    Blob,
    // Content that has inbuilt version strings, that we can
    // keep forever.
    Static,
    // 🤔
    Unknown,
    // Spam ... ffs
    Spam,
}

impl Classification {
    fn prefetch(
        &self,
        path: &Path,
        pri_cache: &ArcDiskCache<String, Status>,
        complete: bool,
    ) -> Option<Vec<(String, NamedTempFile, Classification)>> {
        match self {
            Classification::RepomdXmlSlow | Classification::RepomdXmlFast => {
                path.parent().and_then(|p| p.parent()).map(|p| {
                    let mut v = vec![];
                    if let Some(temp_file) = pri_cache.new_tempfile() {
                        v.push((
                            p.join("media.1/media")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            temp_file,
                            Classification::Metadata,
                        ))
                    };

                    if let Some(temp_file) = pri_cache.new_tempfile() {
                        v.push((
                            p.join("repodata/repomd.xml.asc")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            temp_file,
                            Classification::Metadata,
                        ))
                    };

                    if let Some(temp_file) = pri_cache.new_tempfile() {
                        v.push((
                            p.join("repodata/repomd.xml.key")
                                .to_str()
                                .map(str::to_string)
                                .unwrap(),
                            temp_file,
                            Classification::Metadata,
                        ))
                    };
                    if complete {
                        if let Some(temp_file) = pri_cache.new_tempfile() {
                            v.push((
                                p.join("repodata/repomd.xml")
                                    .to_str()
                                    .map(str::to_string)
                                    .unwrap(),
                                temp_file,
                                Classification::Metadata,
                            ))
                        };
                    };
                    v
                })
            }
            /*
            Classification::Metadata => {

            }
            */
            _ => None,
        }
    }

    pub fn expiry(&self, etime: OffsetDateTime) -> Option<(OffsetDateTime, OffsetDateTime)> {
        match self {
            // The repomd.xml has to expire at the same time because else the bg refresh
            // works and causes a race. The client will be served the old repomd.xml, but
            // is sent a newer associated metadata
            Classification::RepomdXmlSlow => Some((
                etime + time::Duration::minutes(15),
                etime + time::Duration::minutes(15),
            )),
            Classification::RepomdXmlFast => Some((
                etime + time::Duration::minutes(2),
                etime + time::Duration::minutes(2),
            )),
            Classification::Metadata => Some((
                etime + time::Duration::minutes(15),
                etime + time::Duration::hours(24),
            )),
            Classification::Blob => Some((
                etime + time::Duration::hours(6),
                etime + time::Duration::hours(24),
            )),
            Classification::Static => Some((
                etime + time::Duration::hours(24),
                etime + time::Duration::hours(336),
            )),
            Classification::Unknown => Some((etime, etime + time::Duration::minutes(5))),
            Classification::Spam => None,
        }
    }
}

pub struct Cache {
    pri_cache: ArcDiskCache<String, Status>,
    clob: bool,
    mirror_chain: Option<Url>,
    pub submit_tx: Sender<CacheMeta>,
}

impl Cache {
    pub fn new(
        capacity: usize,
        content_dir: &Path,
        clob: bool,
        durable_fs: bool,
        mirror_chain: Option<Url>,
    ) -> Self {
        let pri_cache = ArcDiskCache::new(capacity, content_dir, durable_fs);
        let (submit_tx, submit_rx) = channel(PENDING_ADDS);
        let pri_cache_cln = pri_cache.clone();

        let _ = tokio::task::spawn(async move { cache_mgr(submit_rx, pri_cache_cln).await });

        let pri_cache_cln = pri_cache.clone();
        let _ = tokio::task::spawn(async move { cache_stats(pri_cache_cln).await });

        Cache {
            pri_cache,
            clob,
            mirror_chain,
            submit_tx,
        }
    }

    fn url(&self, cls: &Classification, req_path: &str) -> Url {
        let mut url = if let Some(m_url) = self.mirror_chain.as_ref() {
            m_url.clone()
        } else {
            match cls {
                Classification::RepomdXmlSlow
                | Classification::Metadata
                | Classification::RepomdXmlFast
                | Classification::Spam => MCS_OS_URL.clone(),
                Classification::Blob | Classification::Static | Classification::Unknown => {
                    DL_OS_URL.clone()
                }
            }
        };

        url.set_path(req_path);
        url
    }

    pub fn decision(&self, req_path: &str, head_req: bool) -> CacheDecision {
        let req_path = req_path.replace("//", "/");
        let req_path_trim = req_path.as_str();
        info!("🤔  contemplating req -> {:?}", req_path_trim);

        let path = Path::new(req_path_trim);

        // If the path fails some validations, refuse to proceed.
        if !path.is_absolute() {
            error!("path not absolute");
            return CacheDecision::Invalid;
        }

        let fname = if req_path_trim.ends_with("/") {
            "index.html".to_string()
        } else {
            path.file_name()
                .and_then(|f| f.to_str().map(str::to_string))
                .unwrap_or_else(|| "index.html".to_string())
        };

        debug!(" fname --> {:?}", fname);

        let cls = self.classify(&fname, req_path_trim);

        // Just go away.
        if cls == Classification::Spam {
            debug!("SPAM");
            return CacheDecision::NotFound;
        }

        let now = time::OffsetDateTime::now_utc();
        match self.pri_cache.get(req_path_trim) {
            Some(cache_obj) => {
                match &cache_obj.userdata.nxtime {
                    None => {
                        // If we hit, we need to decide if this
                        // is a found item or something that may need
                        // a refresh.
                        if let Some((softexp, hardexp)) = cache_obj.userdata.expiry {
                            debug!("now: {} - {} {}", now, softexp, hardexp);

                            let temp_file = match self.pri_cache.new_tempfile() {
                                Some(f) => f,
                                None => {
                                    error!("TEMP FILE COULD NOT BE CREATED - FORCE STREAM");
                                    return CacheDecision::Stream(self.url(&cls, req_path_trim));
                                }
                            };

                            if now > softexp && UPSTREAM_ONLINE.load(Ordering::Relaxed) {
                                if now > hardexp {
                                    debug!("EXPIRED INLINE REFRESH");
                                    return CacheDecision::Refresh(
                                        self.url(&cls, req_path_trim),
                                        temp_file,
                                        self.submit_tx.clone(),
                                        cache_obj,
                                        cls.prefetch(&path, &self.pri_cache, head_req),
                                    );
                                } else {
                                    debug!("EXPIRED ASYNC REFRESH");
                                    return CacheDecision::AsyncRefresh(
                                        self.url(&cls, req_path_trim),
                                        temp_file,
                                        self.submit_tx.clone(),
                                        cache_obj,
                                    );
                                }
                            }
                        }

                        debug!("HIT");
                        CacheDecision::FoundObj(cache_obj)
                    }
                    Some(etime) => {
                        // When we refresh this, we treat it as a MissObj, not a refresh.
                        if now > (*etime + time::Duration::minutes(5))
                            && UPSTREAM_ONLINE.load(Ordering::Relaxed)
                        {
                            debug!("NX EXPIRED");
                            let temp_file = match self.pri_cache.new_tempfile() {
                                Some(f) => f,
                                None => {
                                    error!("TEMP FILE COULD NOT BE CREATED - FORCE 404");
                                    return CacheDecision::NotFound;
                                }
                            };

                            return CacheDecision::MissObj(
                                self.url(&cls, req_path_trim),
                                temp_file,
                                self.submit_tx.clone(),
                                cls,
                                cls.prefetch(&path, &self.pri_cache, head_req),
                            );
                        }

                        debug!("NX VALID - force notfound to 404");
                        return CacheDecision::NotFound;
                    }
                }
            }
            None => {
                // NEED TO MOVE NX HERE

                // If miss, we need to choose between stream and
                // miss.
                debug!("MISS");

                if UPSTREAM_ONLINE.load(Ordering::Relaxed) {
                    match (cls, self.clob, self.pri_cache.new_tempfile()) {
                        (Classification::Blob, false, _) => {
                            CacheDecision::Stream(self.url(&cls, req_path_trim))
                        }
                        (cls, _, Some(temp_file)) => CacheDecision::MissObj(
                            self.url(&cls, req_path_trim),
                            temp_file,
                            self.submit_tx.clone(),
                            cls,
                            cls.prefetch(&path, &self.pri_cache, head_req),
                        ),
                        (cls, _, None) => {
                            error!("TEMP FILE COULD NOT BE CREATED - FORCE STREAM");
                            CacheDecision::Stream(self.url(&cls, req_path_trim))
                        }
                    }
                } else {
                    warn!("upstream offline - force miss to 404");
                    // If we are offline, just give a 404
                    CacheDecision::NotFound
                } // end upstream online
            }
        }
    }

    fn classify(&self, fname: &str, req_path: &str) -> Classification {
        if fname == "repomd.xml" {
            if req_path.starts_with("/repositories/") {
                // These are obs
                info!("Classification::RepomdXmlFast");
                Classification::RepomdXmlFast
            } else {
                info!("Classification::RepomdXmlSlow");
                Classification::RepomdXmlSlow
            }
        } else if fname == "media"
            || fname == "products"
            || fname == "repomd.xml.key"
            || fname == "ARCHIVES.gz"
            || fname.ends_with("asc")
            || fname.ends_with("sha256")
            || fname.ends_with("mirrorlist")
            || fname.ends_with("metalink")
            // Arch
            || fname.ends_with("Arch.key")
            || fname.ends_with("Arch.db")
            || fname.ends_with("Arch.db.tar.gz")
            || fname.ends_with("Arch.files")
            || fname.ends_with("Arch.files.tar.gz")
            || fname.ends_with(".sig")
            || fname.ends_with(".files")
            // Html
            || fname.ends_with("html")
            || fname.ends_with("js")
            || fname.ends_with("css")
            // Html assets - we make this metadata because else it's inconsistent between
            // MC and DL.O.O
            || fname.ends_with("svg")
            || fname.ends_with("png")
            || fname.ends_with("jpg")
            || fname.ends_with("gif")
            || fname.ends_with("ttf")
            || fname.ends_with("woff")
            || fname.ends_with("woff2")
            || fname == "favicon.ico"
            // --
            // Related to live boots of tumbleweed.
            // These are in metadata to get them to sync with the repo prefetch since
            // they can change aggressively.
            || fname == "config"
            // /tumbleweed/repo/oss/boot/x86_64/config is the first req.
            // All of these will come after.
            || fname == "add_on_products.xml"
            || fname == "add_on_products"
            || fname == "directory.yast"
            || fname == "CHECKSUMS"
            || fname == "content"
            || fname == "bind"
            || fname == "control.xml"
            || fname == "autoinst.xml"
            || fname == "license.tar.gz"
            || fname == "info.txt"
            || fname == "part.info"
            || fname == "README.BETA"
            || fname == "driverupdate"
            || fname == "linux"
            || fname == "initrd"
            || fname == "common"
            || fname == "root"
            || fname == "cracklib-dict-full.rpm"
            || fname.starts_with("yast2-trans")
        {
            info!("Classification::Metadata");
            Classification::Metadata
        } else if fname.ends_with("iso")
            || fname.ends_with("qcow2")
            || fname.ends_with("raw")
            || fname.ends_with("raw.xz")
            || fname.ends_with("tar.xz")
            // wsl
            || fname.ends_with("appx")
            // Random bits
            || fname.ends_with("txt")
        {
            info!("Classification::Blob");
            Classification::Blob
        } else if fname.ends_with("rpm")
            || fname.ends_with("deb")
            || fname.ends_with("primary.xml.gz")
            || fname.ends_with("suseinfo.xml.gz")
            || fname.ends_with("deltainfo.xml.gz")
            || fname.ends_with("filelists.xml.gz")
            || fname.ends_with("other.xml.gz")
            || fname.ends_with("updateinfo.xml.gz")
            || fname.ends_with("susedata.xml.gz")
            || fname.ends_with("appdata-icons.tar.gz")
            || fname.ends_with("app-icons.tar.gz")
            || fname.ends_with("appdata.xml.gz")
            || fname.ends_with("license.tar.gz")
            || fname.ends_with("pkg.tar.zst")
            || fname.ends_with("pkg.tar.zst.sig")
        {
            info!("Classification::Static");
            Classification::Static
        } else if fname.ends_with(".php") || fname.ends_with(".aspx") {
            error!("🥓  Classification::Spam - {}", req_path);
            Classification::Spam
        } else {
            error!("⚠️  Classification::Unknown - {}", req_path);
            Classification::Unknown
        }
    }
}

async fn cache_stats(pri_cache: ArcDiskCache<String, Status>) {
    let zero = CacheStats::default();
    loop {
        let stats = pri_cache.view_stats();
        warn!("cache stats - {:?}", stats.change_since(&zero));
        if cfg!(debug_assertions) {
            sleep(Duration::from_secs(5)).await;
        } else {
            sleep(Duration::from_secs(300)).await;
        }
    }
}

async fn cache_mgr(mut submit_rx: Receiver<CacheMeta>, pri_cache: ArcDiskCache<String, Status>) {
    // Wait on the channel, and when we get something proceed from there.
    while let Some(meta) = submit_rx.recv().await {
        async {
            info!(
                "✨ Cache Manager Got -> {:?} {} {:?}",
                meta.req_path, meta.etime, meta.action
            );

            let CacheMeta {
                req_path,
                etime,
                action,
            } = meta;

            // Req path sometimes has dup //, so we replace them.
            let req_path = req_path.replace("//", "/");

            match action {
                Action::Submit {
                    file,
                    headers,
                    content,
                    etag,
                    cls,
                } => {
                    let content = content.map(|m| m.to_string());
                    let expiry = cls.expiry(etime);
                    let key = req_path.clone();

                    pri_cache.insert(
                        key,
                        Status {
                            req_path,
                            headers,
                            expiry,
                            content,
                            etag,
                            cls,
                            nxtime: None,
                        },
                        file,
                    )
                }
                Action::Update => pri_cache.update_userdata(&req_path, |d: &mut Status| {
                    d.expiry = d.cls.expiry(etime);
                    if let Some(exp) = d.expiry.as_ref() {
                        debug!("⏰  expiry updated to soft {} hard {}", exp.0, exp.1);
                    }
                }),
                Action::NotFound { cls } => {
                    match pri_cache.new_tempfile() {
                        Some(file) => {
                            let key = req_path.clone();

                            pri_cache.insert(
                                key,
                                Status {
                                    req_path,
                                    headers: BTreeMap::default(),
                                    expiry: None,
                                    content: None,
                                    etag: None,
                                    cls,
                                    nxtime: Some(etime),
                                },
                                file,
                            )
                        }
                        None => {
                            error!("TEMP FILE COULD NOT BE CREATED - SKIP CACHING");
                        }
                    };
                }
            }
        }
        .instrument(tracing::info_span!("cache_mgr"))
        .await;
    }
    error!("CRITICAL: CACHE MANAGER STOPPED.");
}
