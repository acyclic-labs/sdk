//! Encode Windows process arguments for the native executable and batch parsers.
use crate::Arg;
use std::ffi::OsStr;
use std::io;
use std::os::windows::ffi::OsStrExt as _;

const fn wide(byte: u8) -> u16 {
    byte as u16
}

const SEP: u16 = wide(b'\\');
const QUOTE: u16 = wide(b'"');

/// Rejects embedded NULs before a string is passed to a Windows process API.
pub fn ensure_no_nuls(text: &OsStr) -> io::Result<()> {
    if text.encode_wide().any(|unit| unit == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "nul byte found in provided data",
        ));
    }
    Ok(())
}

/// The command line for an executable, without its NUL: the program as
/// given, always quoted, then each argument as `CommandLineToArgvW` and the
/// C runtime parse it back.
pub fn make_command_line(argv0: &OsStr, args: &[Arg]) -> io::Result<Vec<u16>> {
    let mut line = vec![QUOTE];
    line.extend(argv0.encode_wide());
    line.push(QUOTE);
    for arg in args {
        line.push(wide(b' '));
        append_arg(&mut line, arg)?;
    }
    Ok(line)
}

fn append_arg(line: &mut Vec<u16>, arg: &Arg) -> io::Result<()> {
    let (text, escape) = match arg {
        Arg::Regular(text) => (text, true),
        Arg::Raw(text) => (text, false),
    };
    ensure_no_nuls(text)?;
    let bytes = text.as_encoded_bytes();
    let quote =
        escape && (bytes.is_empty() || bytes.iter().any(|byte| matches!(byte, b' ' | b'\t')));
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0_usize;
    for unit in text.encode_wide() {
        if escape {
            if unit == SEP {
                backslashes += 1;
            } else {
                if unit == QUOTE {
                    // n backslashes before a quote become 2n+1.
                    line.extend(std::iter::repeat_n(SEP, backslashes + 1));
                }
                backslashes = 0;
            }
        }
        line.push(unit);
    }
    if quote {
        // n backslashes before the closing quote become 2n.
        line.extend(std::iter::repeat_n(SEP, backslashes));
        line.push(QUOTE);
    }
    Ok(())
}

fn append_bat_arg(line: &mut Vec<u16>, text: &OsStr) -> io::Result<()> {
    ensure_no_nuls(text)?;
    let mut quote = text.is_empty() || text.encode_wide().last() == Some(SEP);
    const UNQUOTED: &str = r"#$*+-./:?@\_";
    for character in char::decode_utf16(text.encode_wide()).filter_map(Result::ok) {
        let ascii_needs_quotes = character.is_ascii()
            && !(character.is_ascii_alphanumeric() || UNQUOTED.contains(character));
        if ascii_needs_quotes || character.is_control() {
            quote = true;
        }
    }
    if quote {
        line.push(QUOTE);
    }
    let mut backslashes = 0_usize;
    for unit in text.encode_wide() {
        if unit == SEP {
            backslashes += 1;
        } else {
            if unit == QUOTE {
                // n backslashes before a quote become 2n, and the quote doubles.
                line.extend(std::iter::repeat_n(SEP, backslashes));
                line.push(QUOTE);
            } else if unit == wide(b'%') || unit == wide(b'\r') {
                // `%%cd:~,%` expands to nothing, so cmd never sees `%NAME%`.
                line.extend("%%cd:~,".encode_utf16());
            }
            backslashes = 0;
        }
        line.push(unit);
    }
    if quote {
        line.extend(std::iter::repeat_n(SEP, backslashes));
        line.push(QUOTE);
    }
    Ok(())
}

/// The command line that runs a batch script through `cmd.exe`, without its
/// NUL. `script` is the resolved script path, NUL-terminated or not.
pub fn make_bat_command_line(script: &[u16], args: &[Arg]) -> io::Result<Vec<u16>> {
    let mut line: Vec<u16> = "cmd.exe /e:ON /v:OFF /d /c \"".encode_utf16().collect();
    line.push(QUOTE);
    if script.contains(&QUOTE) || script.last() == Some(&SEP) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows file names may not contain `\"` or end with `\\`",
        ));
    }
    line.extend_from_slice(script.strip_suffix(&[0]).unwrap_or(script));
    line.push(QUOTE);
    for arg in args {
        line.push(wide(b' '));
        match arg {
            Arg::Regular(text) => {
                if text
                    .as_encoded_bytes()
                    .iter()
                    .any(|byte| matches!(byte, b'\r' | b'\n'))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "batch file arguments are invalid",
                    ));
                }
                append_bat_arg(&mut line, text)?;
            }
            Arg::Raw(_) => append_arg(&mut line, arg)?,
        }
    }
    line.push(QUOTE);
    Ok(line)
}
