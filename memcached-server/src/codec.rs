use std::io;
use bytes::{Buf, BytesMut, BufMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing_forest::prelude::*;
use crate::parser::*;

#[derive(Debug)]
pub enum MemcacheClientMsg {
    Version,
}

pub enum MemcacheServerMsg {
    Version(String),
    ServerError(String),
}

pub struct MemcacheCodec;

impl Decoder for MemcacheCodec {
    type Item = MemcacheClientMsg;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        error!("{}", buf.capacity());
        error!("{}", buf.len());
        // How many bytes to consume?
        error!("GOT {:?}", buf.to_vec());
        error!("{:?}", String::from_utf8(buf.to_vec()));

        let (rem, (cmd, sz)) = match cmd_parser(buf.as_ref()) {
            Ok(r) => {
                error!(?r);
                r
            }
            Err(e) => {
                error!(?e, "Malformed input");
                return Err(
                    io::Error::new(io::ErrorKind::Other, "Malformed Input")
                );
            }
        };

        // I always worry this isn't actually preventing the buffer from leaking .....
        if sz == buf.len() {
            buf.clear();
        } else {
            buf.advance(sz);
        }

        // std::thread::sleep(std::time::Duration::from_secs(2));

        match cmd {
            Cmd::Wait => return Ok(None),
            Cmd::Version => return Ok(Some(MemcacheClientMsg::Version)),
        }
    }
}

impl Encoder<MemcacheServerMsg> for MemcacheCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: MemcacheServerMsg, buf: &mut BytesMut) -> io::Result<()> {
        match msg {
            MemcacheServerMsg::Version(ver) => {
                buf.put(&b"VERSION "[..]);
                buf.put_slice(ver.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            MemcacheServerMsg::ServerError(err) => {
                buf.put(&b"SERVER_ERROR "[..]);
                buf.put_slice(err.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
        }

        Ok(())
    }
}
