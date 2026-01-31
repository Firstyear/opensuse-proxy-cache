use std::sync::atomic::AtomicBool;
use url::Url;

#[derive(Debug)]
pub struct Backend {
    pub provider: Url,
    pub prefix: String,
    // type_: BackendType,
    pub check_upstream: bool,
    //
    pub cache_large_objects: bool,
    pub wonder_guard: bool,
    //
    // boot_services
    pub online: AtomicBool,
}
