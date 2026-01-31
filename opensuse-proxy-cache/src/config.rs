use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Backend {
    pub url: Url,
    #[serde(default)]
    pub cache_large_objects: bool,
    #[serde(default)]
    pub wonder_guard: bool,
    #[serde(default)]
    pub check_upstream: bool,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub prefix: BTreeMap<String, Backend>,
}

impl Config {
    pub fn parse<P: AsRef<Path>>(config_path: P) -> Option<Config> {
        if !config_path.as_ref().exists() {
            return None;
        }

        let mut f = File::open(config_path.as_ref())
            .inspect_err(|err| error!(?err))
            .expect("unable to open file");

        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .inspect_err(|err| error!(?err))
            .expect("unable to open file");

        let config: Config = toml::from_str(contents.as_str())
            .inspect_err(|err| error!(?err))
            .expect("unable to open file");

        Some(config)
    }
}
