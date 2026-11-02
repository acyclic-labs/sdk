//! Byte rules shared by Git exclusion publication and its fuzzer.
use std::io::{self, Read};
use std::path::{Component, Path};

/// Reads exactly the admitted length, using constant space even for opaque lines.
pub(super) fn contains(
    mut reader: impl Read,
    mut remaining: u64,
    pattern: &[u8],
) -> io::Result<(bool, bool)> {
    let mut buffer = [0; 8192];
    let (mut index, mut valid, mut carriage_return) = (0, true, false);
    let (mut start, mut negation, mut effective) = (true, false, false);
    let mut last = None;
    while remaining != 0 {
        let count = (&mut reader).take(remaining).read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Git exclusions changed during read",
            ));
        }
        remaining -= u64::try_from(count).map_err(io::Error::other)?;
        for byte in buffer.iter().take(count) {
            last = Some(*byte);
            if *byte == b'\n' {
                effective = settled(effective, negation, valid && index == pattern.len());
                (index, valid, carriage_return) = (0, true, false);
                (start, negation) = (true, false);
                continue;
            }
            if start {
                (start, negation) = (false, *byte == b'!');
            }
            if valid {
                if !carriage_return && pattern.get(index) == Some(byte) {
                    index += 1;
                } else if !carriage_return && index == pattern.len() && *byte == b'\r' {
                    carriage_return = true;
                } else {
                    valid = false;
                }
            }
        }
    }
    Ok((
        settled(effective, negation, valid && index == pattern.len()),
        last.is_none_or(|byte| byte == b'\n'),
    ))
}

fn settled(effective: bool, negation: bool, rule: bool) -> bool {
    !negation && (effective || rule)
}

pub(super) enum PatternError {
    NotRelative,
    Unrepresentable,
}

pub(super) fn pattern(path: &Path) -> Result<Vec<u8>, PatternError> {
    let mut result = Vec::new();
    for part in path.components() {
        let Component::Normal(part) = part else {
            return Err(PatternError::NotRelative);
        };
        #[cfg(unix)]
        let name = Some(std::os::unix::ffi::OsStrExt::as_bytes(part));
        #[cfg(not(unix))]
        let name = part.to_str().map(str::as_bytes);
        let name = name
            .filter(|name| !name.iter().any(|byte| matches!(byte, b'\r' | b'\n' | 0)))
            .ok_or(PatternError::Unrepresentable)?;
        result.push(b'/');
        for &byte in name {
            if matches!(byte, b'*' | b'?' | b'[' | b'\\' | b'!' | b'#')
                || byte.is_ascii_whitespace()
            {
                result.push(b'\\');
            }
            result.push(byte);
        }
    }
    Ok(result)
}
