#[macro_use]
extern crate tracing;

use concread::arcache::{ARCache, ARCacheBuilder, CacheStats};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use std::collections::BTreeSet;
use std::collections::HashMap;

use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod prelude {
    pub use concread::arcache::CacheStats;
    pub use tempfile::NamedTempFile;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheObjMeta<D> {
    pub key: String,
    pub amt: usize,
    pub hash_str: String,
    // pub expiry: Option<OffsetDateTime>,
    pub userdata: D
}

#[derive(Clone, Debug)]
pub struct CacheObj<D>
    where D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static
{
    pub key: String,
    pub fhandle: Arc<FileHandle<D>>,
    // pub expiry: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub struct FileHandle<D>
    where D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static
{
    pub meta_path: PathBuf,
    pub path: PathBuf,
    pub amt: usize,
    pub hash_str: String,
    pub userdata: D
}

#[derive(Clone)]
pub struct ArcDiskCache<D>
    where D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static
{
    cache: Arc<ARCache<String, CacheObj<D>>>,
    pub content_dir: PathBuf,
}

impl<D> ArcDiskCache<D>
    where D: Serialize + DeserializeOwned + Clone + Debug + Sync + Send + 'static
{
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
        let meta: Vec<(PathBuf, CacheObjMeta<D>)> = meta
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

        let meta: Vec<CacheObj<D>> = meta
            .into_iter()
            .filter_map(|(meta_path, m)| {
                let CacheObjMeta {
                    key,
                    amt,
                    hash_str,
                    userdata,
                } = m;

                let path = content_dir.join(&hash_str);

                if path.exists() {
                    Some(CacheObj {
                        key,
                        fhandle: Arc::new(FileHandle {
                            meta_path,
                            path,
                            amt,
                            hash_str,
                            userdata,
                        }),
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
            let key = co.key.clone();
            let amt = co.fhandle.amt;
            wrtxn.insert_sized(key, co, amt);
        });
        wrtxn.commit();

        // Reset the stats so that the import isn't present.
        cache.reset_stats();

        warn!("ArcDiskCache Ready!");

        // This launches our metadata sync task.
        ArcDiskCache {
            content_dir: content_dir.to_path_buf(),
            cache,
        }
    }

    pub fn get(&self, req_path: &str) -> Option<CacheObj<D>> {
        let mut rtxn = self.cache.read();
        rtxn.get(req_path).cloned()
    }

    pub fn states(&self) -> CacheStats {
        (*self.cache.view_stats()).clone()
    }

    // Add an item?
    pub fn insert(&self) -> () {

    }

    // Remove a key
    pub fn remove(&self) -> () {
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
