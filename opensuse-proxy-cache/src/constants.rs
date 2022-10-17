use regex::Regex;
use std::sync::atomic::AtomicBool;
use url::Url;

pub static RUNNING: AtomicBool = AtomicBool::new(false);
pub static UPSTREAM_ONLINE: AtomicBool = AtomicBool::new(false);
pub const ALLOW_REDIRECTS: u8 = 4;
// Should be about 16Mb worst case.
pub const CHANNEL_MAX_OUTSTANDING: usize = 2048;

pub const BUFFER_WRITE_PAGE: usize = 16 * 1024;
// Match zypper default range reqs
pub const BUFFER_READ_PAGE: usize = 128 * 1024;

// If we go to https we are booted to mirrorcache. If we use http we get the content
// that we want 😈
// You can alternately go to downloadcontent.opensuse.org if you want from the primary mirror.
// but that will likely break mirrorcache behaviour in the future.

lazy_static! {
    pub static ref DL_OS_URL: Url =
        Url::parse("http://downloadcontent2.opensuse.org").expect("Invalid base url");
    pub static ref MCS_OS_URL: Url =
        Url::parse("http://downloadcontent2.opensuse.org").expect("Invalid base url");
        // Url::parse("https://mirrorcache.opensuse.org").expect("Invalid base url");
    pub static ref ETAG_RE: Regex = {
        Regex::new("(?P<mtime>[a-fA-F0-9]+)-(?P<len>[a-fA-F0-9]+)").expect("Invalid etag regex")
    };
}
