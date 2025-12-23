use url::Url;

pub struct Backend {
    pub provider: Url,
    pub prefix: String,
    // type_: BackendType,
    pub check_upstream: bool,
    //
    pub cache_large_objects: bool,
    //
    // boot_services
}
