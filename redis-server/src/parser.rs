use nom::branch::alt;
use nom::bytes::streaming::tag;
use nom::bytes::streaming::{take, take_until};
use nom::combinator::eof;
use nom::IResult;

pub(crate) const MAXIMUM_KEY_SIZE_BYTES: usize = 1024;

#[derive(Debug, PartialEq)]
pub enum Cmd<'a> {
    Wait,
    Auth(&'a [u8]),
    Get(&'a [u8]),
    Set(&'a [u8], u32),
    ConfigGet(&'a [u8]),
    ClientSetInfo(&'a [u8], Option<&'a [u8]>),
    Info,
    Disconnect,
}

// get
// set_noreply

enum IType<'a> {
    // Array(usize),
    BulkString(&'a [u8]),
}

fn wait_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    eof(input).map(|(a, _)| (a, (Cmd::Wait, 0)))
}

fn line_parser(input: &[u8]) -> IResult<&[u8], (&[u8], usize)> {
    let (rem, ln) = take_until("\r\n")(input)?;
    // alt((auth_parser, ver_parser, set_parser))(i)

    let (rem, _) = take(2u32)(rem)?;

    // trace!("ln {:?}", String::from_utf8(ln.to_vec()));
    // trace!("rem {:?}", String::from_utf8(rem.to_vec()));

    Ok((rem, (ln, ln.len() + 2)))
}

fn bulkstrln_parser(input: &[u8]) -> IResult<&[u8], (u32, usize)> {
    let mut taken = 0;
    let (rem, (ln, sz)) = line_parser(input)?;
    taken += sz;

    let (strln, _tag) = tag(b"$")(ln)?;

    let a = unsafe { std::str::from_utf8_unchecked(&strln) };

    let strln = u32::from_str_radix(a, 10).expect("Invalid str");
    Ok((rem, (strln, taken)))
}

fn bulkstr_parser(input: &[u8]) -> IResult<&[u8], (IType<'_>, usize)> {
    let mut taken = 0;
    let (rem, (strln, sz)) = bulkstrln_parser(input)?;
    taken += sz;

    let (rem, tgt_bytes) = take(strln)(rem)?;

    // get rid of the \r\n
    let (rem, _) = tag(b"\r\n")(rem)?;

    taken += strln as usize + 2;

    Ok((rem, (IType::BulkString(tgt_bytes), taken)))
}

fn type_parser(input: &[u8]) -> IResult<&[u8], (IType<'_>, usize)> {
    alt((bulkstr_parser,))(input)
}

fn array4_parser(input: &[u8]) -> IResult<&[u8], (Cmd<'_>, usize)> {
    let mut taken = 0;

    let (rem, (ln, sz)) = line_parser(input)?;

    taken += sz;

    tag("*4")(ln)?;

    // What is the next value? That tells us if we should proceed now or not.
    let (rem, (itype1, sz)) = type_parser(rem)?;
    taken += sz;

    match itype1 {
        IType::BulkString(b"CLIENT") => {
            let (rem, (itype2, sz)) = type_parser(rem)?;
            taken += sz;
            let (rem, (itype3, sz)) = type_parser(rem)?;
            taken += sz;
            let (rem, (itype4, sz)) = type_parser(rem)?;
            taken += sz;

            trace!("array4_parser - taken {:?}", taken);

            match (itype2, itype3, itype4) {
                (IType::BulkString(b"SETINFO"), IType::BulkString(name), IType::BulkString(version)) => {

                    Ok((rem, (Cmd::ClientSetInfo(name, Some(version)), taken)))
                }
                _ => Ok((rem, (Cmd::Disconnect, taken))),
            }
        }
        _ => Ok((rem, (Cmd::Disconnect, taken))),
    }
}

fn array3_parser(input: &[u8]) -> IResult<&[u8], (Cmd<'_>, usize)> {
    let mut taken = 0;

    let (rem, (ln, sz)) = line_parser(input)?;

    taken += sz;

    tag("*3")(ln)?;

    // What is the next value? That tells us if we should proceed now or not.
    let (rem, (itype1, sz)) = type_parser(rem)?;
    taken += sz;

    match itype1 {
        IType::BulkString(b"CONFIG") => {
            let (rem, (itype2, sz)) = type_parser(rem)?;
            taken += sz;
            let (rem, (itype3, sz)) = type_parser(rem)?;
            taken += sz;

            trace!("array3_parser - taken {:?}", taken);

            match (itype2, itype3) {
                (IType::BulkString(b"GET"), IType::BulkString(key)) => {
                    Ok((rem, (Cmd::ConfigGet(key), taken)))
                }
                _ => Ok((rem, (Cmd::Disconnect, taken))),
            }
        }
        IType::BulkString(b"SET") => {
            let (rem, (itype2, sz)) = type_parser(rem)?;
            taken += sz;

            trace!("array3_parser - taken {:?}", taken);

            match itype2 {
                IType::BulkString(key) => {
                    let (rem, (strln, sz)) = bulkstrln_parser(rem)?;
                    taken += sz;
                    trace!("array3_parser - SET - taken {:?}", taken);

                    Ok((rem, (Cmd::Set(key, strln), taken)))
                } // _ => Ok((rem, (Cmd::Disconnect, taken))),
            }
        }
        _ => Ok((rem, (Cmd::Disconnect, taken))),
    }

    // do a "first value" parse, capable of reading if it's bulk or simple string.
    // once we know the first value, can understand the second.
}

fn array2_parser(input: &[u8]) -> IResult<&[u8], (Cmd<'_>, usize)> {
    // The *2 parser then checks the first element
    //     AUTH
    //         then it can stuff the second into the resp.
    //   etc
    //
    let mut taken = 0;

    let (rem, (ln, sz)) = line_parser(input)?;

    taken += sz;

    tag("*2")(ln)?;

    // What is the next value? That tells us if we should proceed now or not.
    let (rem, (itype1, sz)) = type_parser(rem)?;
    taken += sz;

    match itype1 {
        IType::BulkString(b"AUTH") => {
            let (rem, (itype2, sz)) = type_parser(rem)?;
            taken += sz;

            trace!("array2_parser - taken {:?}", taken);

            match itype2 {
                IType::BulkString(pw) => Ok((rem, (Cmd::Auth(pw), taken))),
                // _ => Ok((rem, (Cmd::Disconnect, taken))),
            }
        }
        IType::BulkString(b"GET") => {
            let (rem, (itype2, sz)) = type_parser(rem)?;
            taken += sz;

            trace!("array2_parser - taken {:?}", taken);

            match itype2 {
                IType::BulkString(k) => Ok((rem, (Cmd::Get(k), taken))),
                // _ => Ok((rem, (Cmd::Disconnect, taken))),
            }
        }
        _ => Ok((rem, (Cmd::Disconnect, taken))),
    }

    // do a "first value" parse, capable of reading if it's bulk or simple string.
    // once we know the first value, can understand the second.
}

fn array1_parser(input: &[u8]) -> IResult<&[u8], (Cmd<'_>, usize)> {
    let mut taken = 0;

    let (rem, (ln, sz)) = line_parser(input)?;

    taken += sz;

    tag("*1")(ln)?;

    // What is the next value? That tells us if we should proceed now or not.
    let (rem, (itype1, sz)) = type_parser(rem)?;
    taken += sz;

    match itype1 {
        IType::BulkString(b"INFO") => Ok((rem, (Cmd::Info, taken))),
        _ => Ok((rem, (Cmd::Disconnect, taken))),
    }
}

// For the set command we can just return the size and start of bytes?

pub fn cmd_parser(input: &[u8]) -> IResult<&[u8], (Cmd<'_>, usize)> {
    trace!(?input);
    alt((wait_parser, array1_parser, array2_parser, array3_parser, array4_parser))(input)
}

pub fn tag_eol(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag(b"\r\n")(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::Err::Incomplete;

    #[test]
    fn auth_test() {
        let _ = tracing_subscriber::fmt::try_init();
        assert!(
            cmd_parser(b"*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n")
                == Ok((b"", (Cmd::Auth(b"password"), 28)))
        );

        assert!(matches!(cmd_parser(b"*"), Err(Incomplete(_))));

        assert!(matches!(cmd_parser(b"*2\r"), Err(Incomplete(_))));

        assert!(matches!(cmd_parser(b"*2\r\n$"), Err(Incomplete(_))));

        assert!(matches!(
            cmd_parser(b"*2\r\n$4\r\nAUTH\r\n$8\r\npass"),
            Err(Incomplete(_))
        ));

        eprintln!("{:?}", cmd_parser(b"*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r"));

        assert!(matches!(
            cmd_parser(b"*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r"),
            Err(Incomplete(_))
        ));
    }

    #[test]
    fn config_get_test() {
        let _ = tracing_subscriber::fmt::try_init();
        assert!(
            cmd_parser(b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n")
                == Ok((b"", (Cmd::ConfigGet(b"maxmemory"), 40))) //
                                                                 // *2
                                                                 // $9
                                                                 // maxmemory
                                                                 // $10
                                                                 // 3758096384
        );
    }

    #[test]
    fn get_test() {
        let _ = tracing_subscriber::fmt::try_init();
        assert!(
            cmd_parser(b"*2\r\n$3\r\nGET\r\n$8\r\ntest_key\r\n")
                == Ok((b"", (Cmd::Get(b"test_key"), 27)))
        )

        // $-1

        // $1
        // a
    }

    #[test]
    fn set_test() {
        let _ = tracing_subscriber::fmt::try_init();
        assert!(
            cmd_parser(b"*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$9\r\ntest_data\r\n")
                == Ok((b"test_data\r\n", (Cmd::Set(b"test_key", 9), 31)))
        )

        // +OK
        //
    }

    #[test]
    fn cmd_test() {
        let r = cmd_parser(b"set ");
        eprintln!("{:?}", r);
        assert!(matches!(r, Err(Incomplete(_))));
    }
}
