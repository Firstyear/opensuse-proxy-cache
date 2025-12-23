use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {}

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
