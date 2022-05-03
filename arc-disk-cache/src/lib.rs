#[macro_use]
extern crate tracing;

use concread::arcache::{ARCache, ARCacheBuilder, CacheStats};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use tempfile::NamedTempFile;

use std::collections::BTreeSet;

use std::borrow::Borrow;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, Seek};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub mod prelude {
    pub use concread::arcache::CacheStats;
    pub use tempfile::NamedTempFile;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheObjMeta<K, D> {
    pub key: K,
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
fn crc32c_len(file: &mut File) -> Result<(u32, usize), ()> {
    file.seek(std::io::SeekFrom::Start(0)).map_err(|e| {
        error!("Unable to seek tempfile -> {:?}", e);
    })?;

    let amt = file.metadata().map(|m| m.len() as usize).map_err(|e| {
        error!("Unable to access metadata -> {:?}", e);
    })?;

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

    Ok((crc, amt))
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
                let CacheObjMeta { key, crc, userdata } = m;

                let key_str = base64::encode(&key);

                let path = content_dir.join(&key_str);

                if !path.exists() {
                    return None;
                }

                let mut file = File::open(&path).ok()?;
                let (crc_ck, amt) = crc32c_len(&mut file).ok()?;

                if crc_ck != crc {
                    warn!("file potentially corrupted - {:?}", meta_path);
                    return None;
                }

                Some(CacheObj {
                    key,
                    userdata,
                    fhandle: Arc::new(FileHandle {
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

        // Reset the stats so that the import isn't present.
        cache.reset_stats();

        debug!("ArcDiskCache Ready!");

        ArcDiskCache {
            content_dir: content_dir.to_path_buf(),
            cache,
            running,
            durable_fs,
        }
    }

    pub fn get<Q: ?Sized>(&self, q: &Q) -> Option<CacheObj<K, D>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord,
    {
        let mut rtxn = self.cache.read();
        rtxn.get(q)
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
                    if amt < 536870912 {
                        let (crc_ck, _amt) = crc32c_len(&mut file).ok()?;
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
            .cloned()
    }

    pub fn view_stats(&self) -> CacheStats {
        (*self.cache.view_stats()).clone()
    }

    // Add an item?
    pub fn insert(&self, k: K, d: D, mut fh: NamedTempFile) -> () {
        let file = fh.as_file_mut();

        let (crc, amt) = match crc32c_len(file) {
            Ok(v) => v,
            Err(_) => return,
        };

        let key_str = base64::encode(&k);
        let path = self.content_dir.join(&key_str);
        let mut meta_str = key_str.clone();
        meta_str.push_str(".meta");
        let meta_path = self.content_dir.join(meta_str);

        let objmeta = CacheObjMeta {
            key: k.clone(),
            crc,
            userdata: d.clone(),
        };

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
                meta_path,
                path,
                amt,
                crc,
                running: self.running.clone(),
            }),
        };

        let amt = NonZeroUsize::new(amt).unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) });

        let mut wrtxn = self.cache.write();
        wrtxn.insert_sized(k, co, amt);
        debug!("commit");
        wrtxn.commit();
    }

    // Given key, update the ud.
    pub fn update_userdata<Q: ?Sized, F>(&self, q: &Q, mut func: F)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord,
        F: FnMut(&mut D),
    {
        let mut wrtxn = self.cache.write();

        if let Some(mref) = wrtxn.get_mut(q, false) {
            func(&mut mref.userdata);

            let objmeta = CacheObjMeta {
                key: mref.key.clone(),
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

            debug!("commit");
            wrtxn.commit();
        }
    }

    // Remove a key
    pub fn remove(&self, k: K) {
        let mut wrtxn = self.cache.write();
        let _ = wrtxn.remove(k);
        // This causes the handles to be dropped and binned.
        debug!("commit");
        wrtxn.commit();
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
