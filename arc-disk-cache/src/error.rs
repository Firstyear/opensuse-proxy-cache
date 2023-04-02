use thiserror::Error;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("unknown")]
    Unknown,
}
