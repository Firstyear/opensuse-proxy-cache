use std::sync::atomic::AtomicBool;

pub static RUNNING: AtomicBool = AtomicBool::new(true);
pub const ALLOW_REDIRECTS: u8 = 2;
// Should be about 16Mb worst case.
pub const CHANNEL_MAX_OUTSTANDING: usize = 2048;
pub const BUFFER_WRITE_PAGE: usize = 8192;
pub const BUFFER_READ_PAGE: usize = 4096;
