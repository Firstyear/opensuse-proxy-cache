use crate::parser::*;
use crate::CacheT;
use bytes::{Buf, BufMut, BytesMut};
use nom::Err::Incomplete;
use std::io;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum RedisClientMsg {
    Auth(Vec<u8>),
    Info,
    Ping,
    Disconnect,
    ConfigGet(String),
    ClientSetInfo(String, Option<String>),
    Get(Vec<u8>),
    Set(Vec<u8>, usize, NamedTempFile),
}

#[allow(dead_code)]
pub enum RedisServerMsg<'a> {
    Ok,
    Error(String),
    // Info returns a single bulk string.
    Info { used_memory: u64 },
    KvPair { k: String, v: String },
    DataHdr { sz: usize },
    DataChunk { slice: &'a [u8] },
    DataEof,
    Null,
    Pong,
}

#[derive(Debug)]
enum DecodeState {
    Cmd,
    Set {
        fh: NamedTempFile,
        key: Vec<u8>,
        dsz: usize,
        rem: usize,
    },
}

pub struct RedisCodec {
    cache: Arc<CacheT>,
    d_state: DecodeState,
}

const MAX_CMD_LEN: usize = MAXIMUM_KEY_SIZE_BYTES + 256;

impl RedisCodec {
    pub fn new(cache: Arc<CacheT>) -> Self {
        RedisCodec {
            cache,
            d_state: DecodeState::Cmd,
        }
    }

    fn decode_cmd(&mut self, buf: &mut BytesMut) -> Result<Option<RedisClientMsg>, io::Error> {
        trace!("cap: {}: len: {}", buf.capacity(), buf.len());
        // trace!("buf_raw: {:?}", String::from_utf8(buf.to_vec()))

        let (_rem, (cmd, sz)) = match cmd_parser(buf.as_ref()) {
            Ok(r) => r,
            Err(Incomplete(_)) => {
                if buf.len() >= MAX_CMD_LEN {
                    return Err(io::Error::new(io::ErrorKind::Other, "Command too long"));
                } else {
                    debug!("Need more data");
                    return Ok(None);
                }
            }
            Err(e) => {
                error!(?e, "Malformed input");
                return Err(io::Error::new(io::ErrorKind::Other, "Malformed Input"));
            }
        };

        trace!(?cmd);

        let r = match cmd {
            Cmd::Wait => {
                buf.clear();
                trace!("WAIT cap: {}: len: {}", buf.capacity(), buf.len());
                None
            }
            Cmd::Auth(pw) => Some(RedisClientMsg::Auth(pw.to_vec())),
            Cmd::Get(key) => Some(RedisClientMsg::Get(key.to_vec())),
            Cmd::Set(key, dsz) => {
                // Okay, this is the fun one. We basicly need to setup for the next iter.
                let fh = self.cache.new_tempfile().ok_or_else(|| {
                    error!("Unable to allocate temp file");
                    io::Error::new(io::ErrorKind::Other, "Server Error")
                })?;
                self.d_state = DecodeState::Set {
                    fh,
                    key: key.to_vec(),
                    dsz: dsz as usize,
                    rem: dsz as usize,
                };
                buf.advance(sz);
                return self.process_set(buf);
            }
            Cmd::ConfigGet(key) => {
                let skey = String::from_utf8(key.to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid UTF8"))?;
                Some(RedisClientMsg::ConfigGet(skey))
            }

            Cmd::ClientSetInfo(name, None) => {
                let name = String::from_utf8(name.to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid UTF8"))?;
                Some(RedisClientMsg::ClientSetInfo(name, None))
            }
            Cmd::ClientSetInfo(name, Some(version)) => {
                let name = String::from_utf8(name.to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid UTF8"))?;
                let version = String::from_utf8(version.to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid UTF8"))?;
                Some(RedisClientMsg::ClientSetInfo(name, Some(version)))
            }

            Cmd::Info => Some(RedisClientMsg::Info),
            Cmd::Ping => Some(RedisClientMsg::Ping),
            Cmd::Disconnect => Some(RedisClientMsg::Disconnect),
        };

        if sz == buf.len() {
            buf.clear();
        } else {
            buf.advance(sz);
        }

        Ok(r)
    }

    fn process_set(&mut self, buf: &mut BytesMut) -> Result<Option<RedisClientMsg>, io::Error> {
        if let DecodeState::Set {
            fh,
            key: _,
            dsz: _,
            rem,
        } = &mut self.d_state
        {
            trace!("START PROCESS SET");
            trace!("cap: {}: len: {}", buf.capacity(), buf.len());
            // trace!("buf_raw: {:?}", String::from_utf8(buf.to_vec()));

            // How much is remaining?
            trace!("rem: {}", rem);

            if *rem > 0 {
                let r_buf = if *rem <= buf.len() {
                    // There could be excess bytes.
                    // Can finish and consume everything.
                    let (a, _) = buf.split_at(*rem);
                    a
                } else {
                    // Not enough to finish, just take as much as we can.
                    &buf
                };

                let wr_b = fh.write(r_buf).map_err(|e| {
                    error!(?e, "Failed to write to fh");
                    io::Error::new(io::ErrorKind::Other, "Server Error")
                })?;
                *rem -= wr_b;
                trace!("wrote: {} rem: {} buflen: {}", wr_b, rem, buf.len());

                if wr_b == buf.len() {
                    buf.clear();
                } else {
                    buf.advance(wr_b);
                }
            }

            let r = if *rem == 0 {
                // We don't need to read anything but we still need the crlf.
                match tag_eol(buf) {
                    Ok(_) => {
                        // We ignore the OK inners since this is the remaining / trailing bytes.
                        // Since we'll advance by the correct len here, we don't need to
                        // do anything else.
                        let wr_b = 2;
                        trace!("COMPLETE!!! {} {}", wr_b, buf.len());
                        if wr_b == buf.len() {
                            buf.clear();
                        } else {
                            buf.advance(wr_b);
                        };

                        // (2, Some(...))
                        let mut n_state = DecodeState::Cmd;
                        std::mem::swap(&mut n_state, &mut self.d_state);

                        if let DecodeState::Set {
                            fh,
                            key,
                            dsz,
                            rem: _,
                        } = n_state
                        {
                            Some(RedisClientMsg::Set(key, dsz, fh))
                        } else {
                            error!("Invalid state transition");
                            return Err(io::Error::new(io::ErrorKind::Other, "Server Error"));
                        }
                    }
                    Err(Incomplete(_)) => {
                        debug!("Need more data");
                        None
                    }
                    Err(e) => {
                        error!(?e, "Malformed input");
                        return Err(io::Error::new(io::ErrorKind::Other, "Malformed Input"));
                    }
                }
            } else {
                // Need more data!
                None
            };

            Ok(r)
        } else {
            error!("Invalid state transition");
            Err(io::Error::new(io::ErrorKind::Other, "Server Error"))
        }
    }
}

impl Decoder for RedisCodec {
    type Item = RedisClientMsg;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match &self.d_state {
            DecodeState::Cmd => self.decode_cmd(buf),
            DecodeState::Set { .. } => self.process_set(buf),
        }
    }
}

impl Encoder<RedisServerMsg<'_>> for RedisCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RedisServerMsg<'_>, buf: &mut BytesMut) -> io::Result<()> {
        // error!("wframe - cap: {}: len: {}", buf.capacity(), buf.len());
        match msg {
            RedisServerMsg::Ok => {
                buf.put(&b"+OK\r\n"[..]);
            }
            RedisServerMsg::Pong => {
                buf.put(&b"+PONG\r\n"[..]);
            }
            RedisServerMsg::Null => {
                buf.put(&b"$-1\r\n"[..]);
            }
            RedisServerMsg::Info { used_memory } => {
                // Build the raw buffer.
                let r = format!("# Memory\r\nused_memory:{}\r\n", used_memory);
                let b = r.as_bytes();
                let bs_hdr = format!("${}\r\n", b.len());
                buf.put_slice(bs_hdr.as_bytes());
                buf.put_slice(b);
                buf.put(&b"\r\n"[..]);

                debug!("buf {:?}", String::from_utf8(buf.to_vec()));
            }
            RedisServerMsg::KvPair { k, v } => {
                // Identify our response has 2 values.
                buf.put(&b"*2\r\n"[..]);
                // How long is the key?
                let kb = k.as_bytes();
                let kbs_hdr = format!("${}\r\n", k.len());
                buf.put_slice(kbs_hdr.as_bytes());
                buf.put_slice(kb);
                buf.put(&b"\r\n"[..]);
                // Add the value
                let vb = v.as_bytes();
                let vbs_hdr = format!("${}\r\n", v.len());
                buf.put_slice(vbs_hdr.as_bytes());
                buf.put_slice(vb);
                buf.put(&b"\r\n"[..]);
            }
            // We split this up into "parts".
            RedisServerMsg::DataHdr { sz } => {
                let kbs_hdr = format!("${}\r\n", sz);
                buf.put_slice(kbs_hdr.as_bytes());
            }
            RedisServerMsg::DataChunk { slice } => {
                // extend from slice will auto-resize.
                buf.extend_from_slice(slice)
            }
            RedisServerMsg::DataEof => {
                buf.put(&b"\r\n"[..]);
            }
            RedisServerMsg::Error(err) => {
                buf.put(&b"-SERVER_ERROR "[..]);
                buf.put_slice(err.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
        }

        Ok(())
    }
}
