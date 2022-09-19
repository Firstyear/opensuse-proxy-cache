#[macro_use]
extern crate tracing;

use concread::arcache::stats::{ARCacheWriteStat, ReadCountStat};
use concread::arcache::{ARCache, ARCacheBuilder};
use concread::CowCell;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use tempfile::NamedTempFile;

use std::collections::BTreeSet;

use std::borrow::Borrow;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rand::prelude::*;

static CHECK_INLINE: usize = 536870912;

pub mod prelude {
    pub use tempfile::NamedTempFile;
}

#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub ops: u32,
    pub hits: u32,
    pub ratio: f64,

    // As of last write.
    pub p_weight: u64,
    pub freq: u64,
    pub recent: u64,
    pub shared_max: u64,
    pub all_seen_keys: u64,
}

impl CacheStats {
    fn update(&mut self, tstats: TraceStat) {
        self.p_weight = tstats.p_weight;
        self.shared_max = tstats.shared_max;
        self.freq = tstats.freq;
        self.recent = tstats.recent;
        self.all_seen_keys = tstats.all_seen_keys;
    }
}

#[derive(Debug, Default)]
pub struct TraceStat {
    /// The current cache weight between recent and frequent.
    pub p_weight: u64,

    /// The maximum number of items in the shared cache.
    pub shared_max: u64,
    /// The number of items in the frequent set at this point in time.
    pub freq: u64,
    /// The number of items in the recent set at this point in time.
    pub recent: u64,

    /// The number of total keys seen through the cache's lifetime.
    pub all_seen_keys: u64,
}

impl<K> ARCacheWriteStat<K> for TraceStat
where
    K: Debug,
{
    fn include(&mut self, k: &K) {
        tracing::debug!(?k, "arc-disk include");
    }

    fn include_haunted(&mut self, k: &K) {
        tracing::warn!(?k, "arc-disk include_haunted");
    }

    fn modify(&mut self, k: &K) {
        tracing::debug!(?k, "arc-disk modify");
    }

    fn ghost_frequent_revive(&mut self, k: &K) {
        tracing::warn!(?k, "arc-disk ghost_frequent_revive");
    }

    fn ghost_recent_revive(&mut self, k: &K) {
        tracing::warn!(?k, "arc-disk ghost_recent_revive");
    }

    fn evict_from_recent(&mut self, k: &K) {
        tracing::debug!(?k, "arc-disk evict_from_recent");
    }

    fn evict_from_frequent(&mut self, k: &K) {
        tracing::debug!(?k, "arc-disk evict_from_frequent");
    }

    fn p_weight(&mut self, p: u64) {
        self.p_weight = p;
    }

    fn shared_max(&mut self, i: u64) {
        self.shared_max = i;
    }

    fn freq(&mut self, i: u64) {
        self.freq = i;
    }

    fn recent(&mut self, i: u64) {
        self.recent = i;
    }

    fn all_seen_keys(&mut self, i: u64) {
        self.all_seen_keys = i;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheObjMeta<K, D> {
    pub key: K,
    pub key_str: String,
    pub crc: u32,
    pub userdata: D,
}

#[derive(Clone, Debug)]
pub struct CacheObj<K, D>
where
    K: Serialize
        + DeserializeOwned
        + AsRef<[u8]>
        + Hash
        + Eq
        + Ord
        + Clone
        + Debug
        + Sync
        + Send
        + 'static,
    D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static,
{
    pub key: K,
    pub fhandle: Arc<FileHandle>,
    pub userdata: D,
}

#[derive(Clone, Debug)]
pub struct FileHandle {
    pub key_str: String,
    pub meta_path: PathBuf,
    pub path: PathBuf,
    pub amt: usize,
    pub crc: u32,
    running: Arc<AtomicBool>,
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        if self.running.load(Ordering::Acquire) {
            info!("🗑  remove fhandle -> {:?}", self.path);
            let _ = std::fs::remove_file(&self.meta_path);
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl FileHandle {
    pub fn reopen(&self) -> Result<File, std::io::Error> {
        File::open(&self.path)
    }
}

#[instrument(level = "trace")]
fn crc32c_len(file: &mut File) -> Result<u32, ()> {
    file.seek(std::io::SeekFrom::Start(0)).map_err(|e| {
        error!("Unable to seek tempfile -> {:?}", e);
    })?;

    /*
    let amt = file.metadata().map(|m| m.len() as usize).map_err(|e| {
        error!("Unable to access metadata -> {:?}", e);
    })?;
    */

    let mut buf_file = BufReader::with_capacity(8192, file);
    let mut crc = 0;
    loop {
        match buf_file.fill_buf() {
            Ok(buffer) => {
                let length = buffer.len();
                if length == 0 {
                    // We are done!
                    break;
                } else {
                    // we have content, proceed.
                    crc = crc32c::crc32c_append(crc, &buffer);
                    buf_file.consume(length);
                }
            }
            Err(e) => {
                error!("Bufreader error -> {:?}", e);
                return Err(());
            }
        }
    }
    debug!("crc32c is: {:x}", crc);

    Ok(crc)
}

#[derive(Clone)]
pub struct ArcDiskCache<K, D>
where
    K: Serialize
        + DeserializeOwned
        + AsRef<[u8]>
        + Hash
        + Eq
        + Ord
        + Clone
        + Debug
        + Sync
        + Send
        + 'static,
    D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static,
{
    cache: Arc<ARCache<K, CacheObj<K, D>>>,
    stats: Arc<CowCell<CacheStats>>,
    pub content_dir: PathBuf,
    running: Arc<AtomicBool>,
    durable_fs: bool,
}

impl<K, D> Drop for ArcDiskCache<K, D>
where
    K: Serialize
        + DeserializeOwned
        + AsRef<[u8]>
        + Hash
        + Eq
        + Ord
        + Clone
        + Debug
        + Sync
        + Send
        + 'static,
    D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static,
{
    fn drop(&mut self) {
        trace!("ArcDiskCache - setting running to false");
        self.running.store(false, Ordering::Release);
    }
}

impl<K, D> ArcDiskCache<K, D>
where
    K: Serialize
        + DeserializeOwned
        + AsRef<[u8]>
        + Hash
        + Eq
        + Ord
        + Clone
        + Debug
        + Sync
        + Send
        + 'static,
    D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static,
{
    pub fn new(capacity: usize, content_dir: &Path, durable_fs: bool) -> Self {
        info!("capacity: {}  content_dir: {:?}", capacity, content_dir);

        let cache = Arc::new(
            ARCacheBuilder::new()
                .set_size(capacity, 0)
                .set_watermark(0)
                .set_reader_quiesce(false)
                .build()
                .expect("Invalid ARCache Parameters"),
        );

        let running = Arc::new(AtomicBool::new(true));

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
        let meta: Vec<(PathBuf, CacheObjMeta<K, D>)> = meta
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

        let meta: Vec<CacheObj<K, D>> = meta
            .into_iter()
            .enumerate()
            .filter_map(|(i, (meta_path, m))| {
                if i % 1000 == 0 {
                    info!("{} of {}", i, meta_len);
                }
                let CacheObjMeta {
                    key,
                    key_str,
                    crc,
                    userdata,
                } = m;

                let path = content_dir.join(&key_str);

                if !path.exists() {
                    return None;
                }

                let mut file = File::open(&path).ok()?;

                let amt = match file.metadata().map(|m| m.len() as usize) {
                    Ok(a) => a,
                    Err(e) => {
                        error!("Unable to access metadata -> {:?}", e);
                        return None;
                    }
                };

                if amt >= CHECK_INLINE {
                    // Check large files on startup ONLY
                    let crc_ck = crc32c_len(&mut file).ok()?;
                    if crc_ck != crc {
                        warn!("file potentially corrupted - {:?}", meta_path);
                        return None;
                    }
                }

                Some(CacheObj {
                    key,
                    userdata,
                    fhandle: Arc::new(FileHandle {
                        key_str,
                        meta_path,
                        path,
                        amt,
                        crc,
                        running: running.clone(),
                    }),
                })
            })
            .collect();

        info!("Found {:?} existing metadata", meta.len());

        // Now we prune any files that ARENT in our valid cache meta set.
        let mut files: BTreeSet<_> = files.into_iter().collect();
        meta.iter().for_each(|co| {
            files.remove(&co.fhandle.path);
        });

        files.iter().for_each(|p| {
            trace!("🗑  -> {:?}", p);
            let _ = std::fs::remove_file(p);
        });

        // Finally setup the cache.
        let mut wrtxn = cache.write();
        meta.into_iter().for_each(|co| {
            let key = co.key.clone();
            let amt = NonZeroUsize::new(co.fhandle.amt)
                .unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) });
            wrtxn.insert_sized(key, co, amt);
        });
        wrtxn.commit();

        let stats = Arc::new(CowCell::new(CacheStats::default()));

        debug!("ArcDiskCache Ready!");

        ArcDiskCache {
            content_dir: content_dir.to_path_buf(),
            cache,
            running,
            durable_fs,
            stats,
        }
    }

    pub fn get<Q: ?Sized>(&self, q: &Q) -> Option<CacheObj<K, D>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord,
    {
        let mut rtxn = self.cache.read();
        let maybe_obj = rtxn
            .get(q)
            .and_then(|obj| {
                let mut file = File::open(&obj.fhandle.path).ok()?;

                let amt = file
                    .metadata()
                    .map(|m| m.len() as usize)
                    .map_err(|e| {
                        error!("Unable to access metadata -> {:?}", e);
                    })
                    .ok()?;

                if !self.durable_fs {
                    if amt < CHECK_INLINE {
                        let crc_ck = crc32c_len(&mut file).ok()?;
                        if crc_ck != obj.fhandle.crc {
                            warn!("file potentially corrupted - {:?}", obj.fhandle.meta_path);
                            return None;
                        }
                    } else {
                        info!("Skipping crc check, file too large");
                    }
                }

                Some(obj)
            })
            .cloned();

        // We manually quiesce and finish for stat management here
        // In theory, this should only affect hit counts since evict / include
        // should only occur in a write with how this is setup
        let _ = rtxn.finish();

        let mut stat_guard = self.stats.write();
        (*stat_guard).ops += 1;
        if maybe_obj.is_some() {
            (*stat_guard).hits += 1;
        }
        (*stat_guard).ratio =
            (f64::from((*stat_guard).hits) / f64::from((*stat_guard).ops)) * 100.0;

        let stats = self.cache.try_quiesce_stats(TraceStat::default());
        (*stat_guard).update(stats);
        stat_guard.commit();

        maybe_obj
    }

    pub fn path(&self) -> &Path {
        &self.content_dir
    }

    pub fn view_stats(&self) -> CacheStats {
        let read_stats = self.stats.read();
        (*read_stats).clone()
    }

    pub fn insert_bytes(&self, k: K, d: D, bytes: &[u8]) -> () {
        let mut fh = match self.new_tempfile() {
            Some(fh) => fh,
            None => return,
        };

        if let Err(e) = fh.write(bytes) {
            error!(?e, "failed to write bytes to file");
            return;
        };

        if let Err(e) = fh.flush() {
            error!(?e, "failed to flush bytes to file");
            return;
        }

        self.insert(k, d, fh)
    }

    // Add an item?
    pub fn insert(&self, k: K, d: D, mut fh: NamedTempFile) -> () {
        let file = fh.as_file_mut();

        let amt = match file.metadata().map(|m| m.len() as usize) {
            Ok(a) => a,
            Err(e) => {
                error!("Unable to access metadata -> {:?}", e);
                return;
            }
        };

        let crc = match crc32c_len(file) {
            Ok(v) => v,
            Err(_) => return,
        };

        // Need to salt the file path so that we don't accidently collide.
        let mut rng = rand::thread_rng();
        let mut salt: [u8; 16] = [0; 16];
        rng.fill(&mut salt);

        let k_slice: &[u8] = k.as_ref();

        let mut adapted_k = Vec::with_capacity(16 + k_slice.len());
        adapted_k.extend_from_slice(k_slice);
        adapted_k.extend_from_slice(&salt);

        let key_str = base64::encode_config(&adapted_k, base64::URL_SAFE);
        let key_str = if key_str.len() > 160 {
            debug!("Needing to truncate filename due to excessive key length");
            let at = key_str.len() - 160;
            key_str.split_at(at).1.to_string()
        } else {
            key_str
        };

        let path = self.content_dir.join(&key_str);
        let mut meta_str = key_str.clone();
        meta_str.push_str(".meta");
        let meta_path = self.content_dir.join(&meta_str);

        info!("{:?}", path);
        info!("{:?}", meta_path);

        let objmeta = CacheObjMeta {
            key: k.clone(),
            key_str: key_str.clone(),
            crc,
            userdata: d.clone(),
        };

        if meta_path.exists() {
            warn!(
                immediate = true,
                "file collision detected, skipping write of {}", meta_str
            );
            return;
        }

        let m_file = match File::create(&meta_path).map(BufWriter::new) {
            Ok(f) => f,
            Err(e) => {
                error!(
                    immediate = true,
                    "CRITICAL! Failed to open metadata {:?}", e
                );
                return;
            }
        };

        if let Err(e) = serde_json::to_writer(m_file, &objmeta) {
            error!(
                immediate = true,
                "CRITICAL! Failed to write metadata {:?}", e
            );
            return;
        } else {
            info!("Persisted metadata for {:?}", &meta_path);

            if let Err(e) = fh.persist(&path) {
                error!(immediate = true, "CRITICAL! Failed to persist file {:?}", e);
                return;
            }
        }

        info!("Persisted data for {:?}", &path);

        // Can not fail from this point!
        let co = CacheObj {
            key: k.clone(),
            userdata: d,
            fhandle: Arc::new(FileHandle {
                key_str,
                meta_path,
                path,
                amt,
                crc,
                running: self.running.clone(),
            }),
        };

        let amt = NonZeroUsize::new(amt).unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) });

        let mut wrtxn = self.cache.write_stats(TraceStat::default());
        wrtxn.insert_sized(k, co, amt);
        debug!("commit");
        let stats = wrtxn.commit();

        let mut stat_guard = self.stats.write();
        (*stat_guard).update(stats);
        stat_guard.commit();
    }

    // Given key, update the ud.
    pub fn update_userdata<Q: ?Sized, F>(&self, q: &Q, mut func: F)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord,
        F: FnMut(&mut D),
    {
        let mut wrtxn = self.cache.write_stats(TraceStat::default());

        if let Some(mref) = wrtxn.get_mut(q, false) {
            func(&mut mref.userdata);

            let objmeta = CacheObjMeta {
                key: mref.key.clone(),
                key_str: mref.fhandle.key_str.clone(),
                crc: mref.fhandle.crc,
                userdata: mref.userdata.clone(),
            };

            // This will truncate the metadata if it does exist.
            let m_file = File::create(&mref.fhandle.meta_path)
                .map(BufWriter::new)
                .map_err(|e| {
                    error!("Failed to open metadata {:?}", e);
                })
                .unwrap();

            serde_json::to_writer(m_file, &objmeta)
                .map_err(|e| {
                    error!("Failed to write metadata {:?}", e);
                })
                .unwrap();

            info!("Persisted metadata for {:?}", &mref.fhandle.meta_path);
        }

        debug!("commit");
        let stats = wrtxn.commit();
        let mut stat_guard = self.stats.write();
        (*stat_guard).update(stats);
        stat_guard.commit();
    }

    pub fn update_all_userdata<F, C>(&self, check: C, mut func: F)
    where
        C: Fn(&D) -> bool,
        F: FnMut(&mut D),
    {
        let mut wrtxn = self.cache.write_stats(TraceStat::default());

        let keys: Vec<_> = wrtxn
            .iter()
            .filter_map(|(k, mref)| {
                if check(&mref.userdata) {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();

        for k in keys {
            if let Some(mref) = wrtxn.get_mut(&k, false) {
                func(&mut mref.userdata);

                let objmeta = CacheObjMeta {
                    key: mref.key.clone(),
                    key_str: mref.fhandle.key_str.clone(),
                    crc: mref.fhandle.crc,
                    userdata: mref.userdata.clone(),
                };

                // This will truncate the metadata if it does exist.
                let m_file = File::create(&mref.fhandle.meta_path)
                    .map(BufWriter::new)
                    .map_err(|e| {
                        error!("Failed to open metadata {:?}", e);
                    })
                    .unwrap();

                serde_json::to_writer(m_file, &objmeta)
                    .map_err(|e| {
                        error!("Failed to write metadata {:?}", e);
                    })
                    .unwrap();

                info!("Persisted metadata for {:?}", &mref.fhandle.meta_path);
            }
        }

        debug!("commit");
        let stats = wrtxn.commit();
        let mut stat_guard = self.stats.write();
        (*stat_guard).update(stats);
        stat_guard.commit();
    }

    // Remove a key
    pub fn remove(&self, k: K) {
        let mut wrtxn = self.cache.write_stats(TraceStat::default());
        let _ = wrtxn.remove(k);
        // This causes the handles to be dropped and binned.
        debug!("commit");
        let stats = wrtxn.commit();
        let mut stat_guard = self.stats.write();
        (*stat_guard).update(stats);
        stat_guard.commit();
    }

    //
    pub fn new_tempfile(&self) -> Option<NamedTempFile> {
        NamedTempFile::new_in(&self.content_dir)
            .map_err(|e| error!(?e))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::ArcDiskCache;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn disk_cache_test_basic() {
        let _ = tracing_subscriber::fmt::try_init();

        let dir = tempdir().expect("Failed to build tempdir");
        // Need a new temp dir
        let dc: ArcDiskCache<Vec<u8>, ()> = ArcDiskCache::new(1024, dir.path(), false);

        let mut fh = dc.new_tempfile().unwrap();
        let k = vec![0, 1, 2, 3, 4, 5];

        let file = fh.as_file_mut();
        file.write_all(b"Hello From Cache").unwrap();

        dc.insert(k, (), fh);
    }
}
