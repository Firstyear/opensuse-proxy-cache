use std::sync::Arc;
use concread::hashmap::HashMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tide::log;
use std::time::Instant;

const PENDING_ADDS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemCacheObj {
    pub etag: String,
    // Time to refresh
    pub refresh: Instant,
    pub headers: Vec<(String, String)>,
    pub content: Option<tide::http::Mime>,
    pub blob: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MemCacheMeta {
    pub req_path: String,
    pub headers: Vec<(String, String)>,
    pub content: Option<tide::http::Mime>,
    pub blob: Vec<u8>,
    pub refresh: Instant,
}

async fn cache_mgr(
    map: Arc<HashMap<String, Arc<MemCacheObj>>>,
    mut submit_rx: Receiver<MemCacheMeta>,
) {
    // Wait on the channel, and when we get something proceed from there.
    while let Some(meta) = submit_rx.recv().await {
        log::error!("mem_cache_mgr got -> {:?}", meta.req_path);
        let MemCacheMeta {
            req_path, headers, content, blob, refresh
        } = meta;

        let etag = headers
            .iter()
            .find_map(|(hv, hk)| {
                if hv == "etag" {
                    Some(hk.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "".to_string());

        let obj = Arc::new(MemCacheObj {
            etag, refresh, headers, content, blob
        });

        let mut wrtxn = map.write();
        let _prev = wrtxn.insert(req_path, obj);
        wrtxn.commit();
    }
}

pub struct MemCache {
    map: Arc<HashMap<String, Arc<MemCacheObj>>>,
    pub submit_tx: Sender<MemCacheMeta>,
}

impl MemCache {
    pub fn new() -> MemCache {
        let map = Arc::new(HashMap::new());
        let map_cln = map.clone();
        let (submit_tx, submit_rx) = channel(PENDING_ADDS);
        // This launches our task too.
        let _ = tokio::task::spawn(async move {
            cache_mgr(map_cln, submit_rx).await
        });

        MemCache {
            map,
            submit_tx,
        }
    }

    pub fn get(&self, req_path: &str) -> Option<Arc<MemCacheObj>> {
        let rtxn = self.map.read();
        rtxn.get(req_path).cloned()
    }
}


