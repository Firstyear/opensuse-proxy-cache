use crate::backend::Backend;
use crate::constants::*;
use bloomfilter::Bloom;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};
use url::Url;

use arc_disk_cache::{ArcDiskCache, CacheObj};

use serde::{Deserialize, Serialize};

const PENDING_ADDS: usize = 8;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Status {
    #[serde(alias = "req_path")]
    pub cache_key: PathBuf,
    pub headers: BTreeMap<String, String>,
    //                  Soft            Hard
    pub expiry: Option<(OffsetDateTime, OffsetDateTime)>,
    pub cls: Classification,
    pub nxtime: Option<OffsetDateTime>,
}

#[derive(Debug)]
pub struct CacheMeta {
    // Clippy will whinge about variant sizes here.
    pub cache_key: PathBuf,
    // Add the time this was added
    pub etime: OffsetDateTime,
    pub action: Action,
}

#[derive(Debug)]
pub enum Action {
    Submit {
        file: NamedTempFile,
        // These need to be extracted
        headers: BTreeMap<String, String>,
        // amt: usize,
        // hash_str: String,
        cls: Classification,
    },
    Update,
    NotFound {
        cls: Classification,
    },
}

#[derive(Debug)]
pub struct PrefetchItem {
    pub fetch_url: Url,
    pub cache_key: PathBuf,
    pub tmp_file: NamedTempFile,
    pub cls: Classification,
}

pub enum CacheDecision {
    // We can't cache this, stream it from a remote.
    Stream(Url),
    // We have this item, and can send from our cache.
    FoundObj(CacheObj<PathBuf, Status>),
    // We don't have this item but we want it, so please dl it to this location
    // then notify this cache.
    MissObj {
        fetch_url: Url,
        cache_key: PathBuf,
        tmp_file: NamedTempFile,
        submit_tx: Sender<CacheMeta>,
        cls: Classification,
        prefetch_items: Option<Vec<PrefetchItem>>,
    },
    // Refresh - we can also prefetch some paths in the background.
    Refresh {
        fetch_url: Url,
        cache_key: PathBuf,
        tmp_file: NamedTempFile,
        submit_tx: Sender<CacheMeta>,
        meta: CacheObj<PathBuf, Status>,
        prefetch_items: Option<Vec<PrefetchItem>>,
    },
    // We found it, but we also want to refresh in the background.
    AsyncRefresh {
        fetch_url: Url,
        cache_key: PathBuf,
        tmp_file: NamedTempFile,
        submit_tx: Sender<CacheMeta>,
        meta: CacheObj<PathBuf, Status>,
        prefetch_items: Option<Vec<PrefetchItem>>,
    },
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

        context: &Backend,
        path: &Path,

        pri_cache: &ArcDiskCache<PathBuf, Status>,
        complete: bool,
    ) -> Option<Vec<PrefetchItem>> {
        match self {
            Classification::RepomdXmlSlow | Classification::RepomdXmlFast => {
                path.parent().and_then(|p| p.parent()).map(|p| {
                    let mut v = vec![];
                    if let Some(tmp_file) = pri_cache.new_tempfile() {
                        let cache_key = p.join("media.1/media");
                        let cls = Classification::Metadata;
                        let mut fetch_url = context.provider.clone();
                        fetch_url.set_path(&cache_key.to_string_lossy());
                        v.push(PrefetchItem {
                            fetch_url,
                            cache_key,
                            tmp_file,
                            cls,
                        })
                    };

                    if let Some(tmp_file) = pri_cache.new_tempfile() {
                        let cache_key = p.join("repodata/repomd.xml.asc");
                        let cls = Classification::Metadata;
                        let mut fetch_url = context.provider.clone();
                        fetch_url.set_path(&cache_key.to_string_lossy());
                        v.push(PrefetchItem {
                            fetch_url,
                            cache_key,
                            tmp_file,
                            cls,
                        })
                    };

                    if let Some(tmp_file) = pri_cache.new_tempfile() {
                        let cache_key = p.join("repodata/repomd.xml.key");
                        let cls = Classification::Metadata;
                        let mut fetch_url = context.provider.clone();
                        fetch_url.set_path(&cache_key.to_string_lossy());
                        v.push(PrefetchItem {
                            fetch_url,
                            cache_key,
                            tmp_file,
                            cls,
                        })
                    };

                    if let Some(tmp_file) = pri_cache.new_tempfile() {
                        let cache_key = p.join("repodata/repomd.xml");
                        let cls = Classification::Metadata;
                        let mut fetch_url = context.provider.clone();
                        fetch_url.set_path(&cache_key.to_string_lossy());
                        v.push(PrefetchItem {
                            fetch_url,
                            cache_key,
                            tmp_file,
                            cls,
                        })
                    }

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
            // We can now do async prefetching on bg refreshes so this keeps everything in sync.
            Classification::RepomdXmlSlow => Some((
                etime + time::Duration::minutes(10),
                etime + time::Duration::hours(180),
            )),
            Classification::RepomdXmlFast => Some((
                etime + time::Duration::minutes(1),
                etime + time::Duration::minutes(180),
            )),
            Classification::Metadata => Some((
                etime + time::Duration::minutes(15),
                etime + time::Duration::hours(24),
            )),
            Classification::Blob => Some((
                // etime + time::Duration::hours(2),
                etime + time::Duration::minutes(15),
                etime + time::Duration::hours(336),
            )),
            Classification::Static => Some((
                // Because OBS keeps publishing incorrect shit ...
                // etime + time::Duration::hours(2),
                etime + time::Duration::minutes(15),
                etime + time::Duration::hours(336),
            )),
            Classification::Unknown => Some((etime, etime + time::Duration::minutes(5))),
            Classification::Spam => None,
        }
    }
}

pub struct Cache {
    pri_cache: ArcDiskCache<PathBuf, Status>,
    bloom: Mutex<Bloom<Path>>,

    default_backend: Backend,
    backends: Vec<Backend>,

    pub submit_tx: Sender<CacheMeta>,
}

impl Cache {
    pub fn new(
        capacity: usize,
        content_dir: &Path,
        durable_fs: bool,
        default_backend: Backend,
        backends: Vec<Backend>,
    ) -> std::io::Result<Self> {
        let pri_cache = ArcDiskCache::new(capacity, content_dir, durable_fs)?;
        let (submit_tx, submit_rx) = channel(PENDING_ADDS);
        let pri_cache_cln = pri_cache.clone();

        let bloom = Mutex::new(Bloom::new_for_fp_rate(65536, 0.001));

        let _ = tokio::task::spawn_blocking(move || cache_mgr(submit_rx, pri_cache_cln));

        let pri_cache_cln = pri_cache.clone();
        let _ = tokio::task::spawn(async move { cache_stats(pri_cache_cln).await });

        Ok(Cache {
            pri_cache,
            bloom,
            default_backend,
            backends,
            submit_tx,
        })
    }

    pub fn monitor_backends(&self) -> impl Iterator<Item = &Backend> {
        std::iter::once(&self.default_backend)
            .chain(self.backends.iter())
            .filter(|backend| backend.check_upstream)
    }

    fn url(&self, cls: &Classification, req_path: &Path, context: &Backend) -> Url {
        let mut url = context.provider.clone();
        url.set_path(&req_path.to_string_lossy());
        debug!(?url);
        url
    }

    pub fn decision(&self, req_path: &Path, head_req: bool) -> CacheDecision {
        // let req_path_trim = req_path.replace("//", "/");
        let req_path_trim = req_path;

        info!("🤔  contemplating req -> {}", req_path_trim.display());

        // If the path fails some validations, refuse to proceed.
        if req_path_trim.is_relative() {
            error!("path not absolute");
            return CacheDecision::Invalid;
        }

        let fname = if req_path_trim.ends_with("/") {
            "index.html".to_string()
        } else {
            req_path_trim
                .file_name()
                .and_then(|f| f.to_str().map(str::to_string))
                .unwrap_or_else(|| "index.html".to_string())
        };

        debug!(" fname --> {:?}", fname);

        // This is where we now need to make choices about the prefix on the
        // req path.
        //
        // We need to see if a prefix matches?
        //
        // If it does, pull that backend. We then need to change the path from req_path
        // to rel_path.

        let context = self
            .backends
            .iter()
            .find(|backend| req_path.starts_with(&backend.prefix))
            .unwrap_or(&self.default_backend);

        let Ok(mut relative_path) = req_path_trim.strip_prefix(&context.prefix) else {
            error!("Unable to strip prefix, this is really weird...");
            return CacheDecision::NotFound;
        };

        let cache_key = req_path_trim.to_path_buf();

        info!(selected_context = ?context.prefix, ?relative_path);

        /*
        if relative_path.is_relative() {

        }
        */

        let upstream_online = context.online.load(Ordering::Relaxed);

        let cls = self.classify(&fname, &relative_path);

        // Just go away.
        if cls == Classification::Spam {
            debug!("SPAM");
            return CacheDecision::NotFound;
        }

        let now = time::OffsetDateTime::now_utc();
        //                       /--- Needs to be the *full* path.
        //                       v
        match self.pri_cache.get(&cache_key) {
            Some(cache_obj) => {
                // We assert this because the key is based on the requested path, not the normalised one?
                // But also, does this matter now that I use PathBuf?
                assert_eq!(cache_obj.key, cache_key);

                match &cache_obj.userdata.nxtime {
                    None => {
                        // If we hit, we need to decide if this
                        // is a found item or something that may need
                        // a refresh.
                        if let Some((softexp, hardexp)) = cache_obj.userdata.expiry {
                            debug!("now: {} - {} {}", now, softexp, hardexp);

                            let tmp_file = match self.pri_cache.new_tempfile() {
                                Some(f) => f,
                                None => {
                                    error!("TEMP FILE COULD NOT BE CREATED - FORCE STREAM");
                                    return CacheDecision::Stream(self.url(
                                        &cls,
                                        relative_path,
                                        context,
                                    ));
                                }
                            };

                            if now > softexp && upstream_online {
                                if now > hardexp {
                                    debug!("EXPIRED INLINE REFRESH");
                                    return CacheDecision::Refresh {
                                        fetch_url: self.url(&cls, relative_path, context),
                                        cache_key,
                                        tmp_file,
                                        submit_tx: self.submit_tx.clone(),
                                        meta: cache_obj,
                                        prefetch_items: cls.prefetch(
                                            context,
                                            &relative_path,
                                            &self.pri_cache,
                                            head_req,
                                        ),
                                    };
                                } else {
                                    debug!("EXPIRED ASYNC REFRESH");
                                    return CacheDecision::AsyncRefresh {
                                        fetch_url: self.url(&cls, relative_path, context),
                                        cache_key,
                                        tmp_file,
                                        submit_tx: self.submit_tx.clone(),
                                        meta: cache_obj,
                                        prefetch_items: cls.prefetch(
                                            context,
                                            &relative_path,
                                            &self.pri_cache,
                                            head_req,
                                        ),
                                    };
                                }
                            }
                        }

                        debug!("HIT");
                        CacheDecision::FoundObj(cache_obj)
                    }
                    Some(etime) => {
                        // When we refresh this, we treat it as a MissObj, not a refresh.
                        if &now > etime && upstream_online {
                            debug!("NX EXPIRED");
                            let tmp_file = match self.pri_cache.new_tempfile() {
                                Some(f) => f,
                                None => {
                                    error!("TEMP FILE COULD NOT BE CREATED - FORCE 404");
                                    return CacheDecision::NotFound;
                                }
                            };

                            return CacheDecision::MissObj {
                                fetch_url: self.url(&cls, relative_path, context),
                                cache_key,
                                tmp_file,
                                submit_tx: self.submit_tx.clone(),
                                cls,
                                prefetch_items: cls.prefetch(
                                    context,
                                    &relative_path,
                                    &self.pri_cache,
                                    head_req,
                                ),
                            };
                        }

                        debug!("NX VALID - force notfound to 404");
                        return CacheDecision::NotFound;
                    }
                }
            }
            None => {
                // NEED TO MOVE NX HERE

                // Is it in the bloom filter? We want to check if it's a "one hit wonder".
                let can_cache = if cls == Classification::Blob && !context.cache_large_objects {
                    // It's a blob, and cache large object is false
                    info!("cache_large_object=false - skip caching of blob item");
                    false
                } else if context.wonder_guard {
                    // Lets check it's in the wonder guard?
                    let x = {
                        let mut bguard = self.bloom.lock().unwrap();
                        bguard.check_and_set(&cache_key)
                    };
                    if !x {
                        info!("wonder_guard - skip caching of one hit item");
                    }
                    x
                } else {
                    // Yep, we can cache it as we aren't wonder guarding.
                    true
                };

                // If miss, we need to choose between stream and
                // miss.
                debug!("MISS");

                if upstream_online {
                    match (cls, can_cache, self.pri_cache.new_tempfile()) {
                        (_, false, _) => {
                            CacheDecision::Stream(self.url(&cls, relative_path, context))
                        }
                        (cls, _, Some(tmp_file)) => CacheDecision::MissObj {
                            fetch_url: self.url(&cls, relative_path, context),
                            cache_key,
                            tmp_file,
                            submit_tx: self.submit_tx.clone(),
                            cls,
                            prefetch_items: cls.prefetch(
                                context,
                                &relative_path,
                                &self.pri_cache,
                                head_req,
                            ),
                        },
                        (cls, _, None) => {
                            error!("TEMP FILE COULD NOT BE CREATED - FORCE STREAM");
                            CacheDecision::Stream(self.url(&cls, relative_path, context))
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

    fn classify(&self, fname: &str, req_path: &Path) -> Classification {
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
            || fname == "repoindex.xml"
            || fname == "repomd.xml.key"
            || fname == "ARCHIVES.gz"
            || fname.ends_with("asc")
            || fname.ends_with("sha256")
            || fname.ends_with("mirrorlist")
            || fname.ends_with("metalink")
            || fname.ends_with(".repo")
            // Arch
            || fname.ends_with("Arch.key")
            || fname.ends_with("Arch.db")
            || fname.ends_with("Arch.db.tar.gz")
            || fname.ends_with("Arch.files")
            || fname.ends_with("Arch.files.tar.gz")
            || fname.ends_with(".sig")
            || fname.ends_with(".files")
            // Deb
            || fname == "Packages"
            || fname == "Packages.gz"
            || fname == "Release"
            || fname == "Release.gpg"
            || fname == "Release.key"
            || fname == "Sources"
            || fname == "Sources.gz"
            || fname.ends_with(".dsc")
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
            // These are dirs that the mirror scanner likes to query.
            || req_path.ends_with("/repo")
            || req_path.ends_with("/15.4")
            || req_path.ends_with("/15.5")
            || req_path.ends_with("/15.6")
            || req_path.ends_with("/16.0")
            || req_path == "/history"
            || req_path == "/repositories"
            // FreeBSD
            || req_path.ends_with("/quarterly")
            || fname == "meta.conf"
            // Sadly fbsd pkg metadata has the same suffix as their pkgs :(
            || req_path.ends_with(".pkg")
        {
            info!("Classification::Metadata");
            Classification::Metadata
        } else if fname.ends_with("iso")
            || fname.ends_with("qcow2")
            || fname.ends_with("raw")
            || fname.ends_with("raw.xz")
            || fname.ends_with("raw.zst")
            || fname.ends_with("tar.xz")
            // looks to be used in some ubuntu repos? Not sure if metadata.
            || fname.ends_with("tar.gz")
            || fname.ends_with("tar.zst")
            || fname.ends_with("diff.gz")
            || fname.ends_with("diff.zst")
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
            || fname.ends_with("primary.xml.zst")
            || fname.ends_with("suseinfo.xml.gz")
            || fname.ends_with("suseinfo.xml.zst")
            || fname.ends_with("deltainfo.xml.gz")
            || fname.ends_with("deltainfo.xml.zst")
            || fname.ends_with("filelists.xml.gz")
            || fname.ends_with("filelists.xml.zst")
            || fname.ends_with("filelists-ext.xml.gz")
            || fname.ends_with("filelists-ext.xml.zst")
            || fname.ends_with("filelists.sqlite.bz2")
            || fname.ends_with("filelists.sqlite.gz")
            || fname.ends_with("filelists.sqlite.zst")
            || fname.ends_with("other.xml.gz")
            || fname.ends_with("other.xml.zst")
            || fname.ends_with("other.sqlite.bz2")
            || fname.ends_with("other.sqlite.gz")
            || fname.ends_with("other.sqlite.zst")
            || fname.ends_with("updateinfo.xml.gz")
            || fname.ends_with("updateinfo.xml.zst")
            || (fname.contains("susedata") && fname.ends_with(".xml.gz"))
            || (fname.contains("susedata") && fname.ends_with(".xml.zst"))
            || fname.ends_with("appdata-icons.tar.gz")
            || fname.ends_with("appdata-icons.tar.zst")
            || fname.ends_with("app-icons.tar.gz")
            || fname.ends_with("app-icons.tar.zst")
            || fname.ends_with("appdata.xml.gz")
            || fname.ends_with("appdata.xml.zst")
            || fname.ends_with("license.tar.gz")
            || fname.ends_with("license.tar.zst")
            || fname.ends_with("pkg.tar.zst")
            || fname.ends_with("pkg.tar.zst.sig")
            || fname.ends_with("patterns.xml.zst")
        {
            info!("Classification::Static");
            Classification::Static
        } else if fname == "login"
            || fname == "not.found"
            || fname.ends_with(".php")
            || fname.ends_with(".drpm")
            || fname.ends_with(".aspx")
            || fname.ends_with(".env")
        {
            error!("🥓  Classification::Spam - {}", req_path.display());
            Classification::Spam
        } else {
            error!("⚠️  Classification::Unknown - {}", req_path.display());
            Classification::Unknown
        }
    }

    pub fn clear_nxcache(&self, etime: OffsetDateTime) {
        warn!("NXCACHE CLEAR REQUESTED");
        self.pri_cache.update_all_userdata(
            |d: &Status| d.nxtime.is_some(),
            |d: &mut Status| {
                if d.nxtime.is_some() {
                    d.nxtime = Some(etime);
                }
            },
        )
    }
}

async fn cache_stats(pri_cache: ArcDiskCache<PathBuf, Status>) {
    // let zero = CacheStats::default();
    loop {
        let stats = pri_cache.view_stats();
        warn!("cache stats - {:?}", stats);
        // stats.change_since(&zero));
        if cfg!(debug_assertions) {
            sleep(Duration::from_secs(5)).await;
        } else {
            sleep(Duration::from_secs(300)).await;
        }
    }
}

fn cache_mgr(mut submit_rx: Receiver<CacheMeta>, pri_cache: ArcDiskCache<PathBuf, Status>) {
    // Wait on the channel, and when we get something proceed from there.
    while let Some(meta) = submit_rx.blocking_recv() {
        info!(
            "✨ Cache Manager Got -> {:?} {} {:?}",
            meta.cache_key, meta.etime, meta.action
        );

        let CacheMeta {
            cache_key,
            etime,
            action,
        } = meta;

        // Req path sometimes has dup //, so we replace them.
        // let req_path = req_path.replace("//", "/");

        match action {
            Action::Submit { file, headers, cls } => {
                let expiry = cls.expiry(etime);
                let key = cache_key.clone();

                pri_cache.insert(
                    key,
                    Status {
                        cache_key,
                        headers,
                        expiry,
                        cls,
                        nxtime: None,
                    },
                    file,
                )
            }
            Action::Update => pri_cache.update_userdata(&cache_key, |d: &mut Status| {
                d.expiry = d.cls.expiry(etime);
                if let Some(exp) = d.expiry.as_ref() {
                    debug!("⏰  expiry updated to soft {} hard {}", exp.0, exp.1);
                }
            }),
            Action::NotFound { cls } => {
                match pri_cache.new_tempfile() {
                    Some(file) => {
                        let key = cache_key.clone();

                        pri_cache.insert(
                            key,
                            Status {
                                cache_key,
                                headers: BTreeMap::default(),
                                expiry: None,
                                cls,
                                nxtime: Some(etime + time::Duration::minutes(1)),
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
    error!("CRITICAL: CACHE MANAGER STOPPED.");
}
