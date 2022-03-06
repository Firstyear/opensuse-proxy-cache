
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::eof;

use tracing_forest::prelude::*;

#[derive(Debug)]
pub enum Cmd {
    Wait,
    Version
}

fn wait_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    eof(input)
        .map(|(a,_)|
            (a, (Cmd::Wait, 0))
        )
}

fn ver_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    tag("version\r\n")(input)
        .map(|(a,_)|
            (a, (Cmd::Version, 9))
        )
}

pub fn cmd_parser(input: &[u8]) -> IResult<&[u8], (Cmd, usize)> {
    alt((
        wait_parser,
        ver_parser,
    ))(input)
}




