use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::streaming::take_until;
use nom::combinator::eof;
use nom::IResult;

use tracing_forest::prelude::*;

#[derive(Debug)]
pub enum Cmd {
    Wait,
    Version,
    // Get
    // Set
    // Info
    // Config
}

// get
// set_noreply

fn wait_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    eof(input).map(|(a, _)| (a, (Cmd::Wait, 0)))
}

fn ver_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    tag("version")(input).map(|(a,_)|
            // Need to account for the crlf
            (a, (Cmd::Version, 9)))
}

fn auth_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    // GOT [42, 50, 13, 10, 36, 52, 13, 10, 65, 85, 84, 72, 13, 10, 36, 56, 13, 10, 112, 97, 115, 115, 119, 111, 114, 100, 13, 10]
    // Ok("*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n")
    // Ok("*2")
}

fn set_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    tag("set ")(input).map(|(a, b)| {
        eprintln!("{:?}", String::from_utf8(a.to_vec()).unwrap());
        // a 0 0 5
        // key ? ? datalen
        // set <key> <flags> <exptime> <bytes len>
        unimplemented!();
    })
}

fn line_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    let (rem, i) = take_until("\r\n")(input)?;
    debug!("{:?}", String::from_utf8(i.to_vec()));
    alt((auth_parser, ver_parser, set_parser))(i)
}

pub fn cmd_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    alt((wait_parser, line_parser))(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Err::Incomplete;

    //

    #[test]
    fn ver_test() {
        assert!(cmd_parser(b"version\r\n").is_ok());
        // No crlf
        assert!(cmd_parser(b"version").is_err());
        assert!(ver_parser(b"version").is_ok());
        assert!(ver_parser(b"versn").is_err());
    }

    #[test]
    fn cmd_test() {
        let r = cmd_parser(b"set ");
        eprintln!("{:?}", r);
        assert!(matches!(r, Err(Incomplete(_))));
    }

    #[test]
    fn set_test() {
        assert!(cmd_parser(b"set a 0 0 5\r\n").is_ok());
        // No crlf
        assert!(cmd_parser(b"set a 0 0 5").is_err());
        assert!(ver_parser(b"set key 0 0 128").is_ok());
        assert!(ver_parser(b"se a 0 0 0").is_err());
    }
}
