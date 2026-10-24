use std::io::{Error, ErrorKind, Result};
use std::path::{Component, Path};

pub(super) fn contains(existing: &[u8], pattern: &str) -> bool {
    existing
        .split(|byte| *byte == b'\n')
        .any(|line| line.strip_suffix(b"\r").unwrap_or(line) == pattern.as_bytes())
}

pub(super) fn pattern(path: &Path) -> Result<String> {
    let mut result = String::new();
    for part in path.components() {
        let Component::Normal(part) = part else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mount exclusion must be relative",
            ));
        };
        let name = part
            .to_str()
            .filter(|name| !name.contains(['\r', '\n', '\0']))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "mount path cannot be represented in Git exclusions",
                )
            })?;
        result.push('/');
        for character in name.chars() {
            if matches!(character, '*' | '?' | '[' | '\\' | '!' | '#') || character.is_whitespace()
            {
                result.push('\\');
            }
            result.push(character);
        }
    }
    if result.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "empty mount exclusion"));
    }
    Ok(result)
}

/// Exercises opaque-byte exclusion matching and pattern encoding.
#[cfg(all(feature = "test-support", unix))]
pub(crate) fn fuzz_check(input: &[u8]) {
    use std::os::unix::ffi::OsStrExt;

    // Exercise the same opaque-byte line matcher used for existing excludes.
    // The independent oracle searches framed bytes instead of splitting lines.
    let mut framed = Vec::with_capacity(input.len() + 2);
    framed.push(b'\n');
    framed.extend_from_slice(input);
    framed.push(b'\n');
    let rule = "/.volumes/scratch";
    let expected = [
        b"\n/.volumes/scratch\n".as_slice(),
        b"\n/.volumes/scratch\r\n".as_slice(),
    ]
    .iter()
    .any(|needle| framed.windows(needle.len()).any(|bytes| bytes == *needle));
    assert_eq!(contains(input, rule), expected);
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(input));
    if let Ok(encoded) = pattern(path) {
        assert!(path.to_str().is_some());
        assert!(encoded.starts_with('/'));
        assert!(!encoded.contains(['\r', '\n', '\0']));
        assert!(!encoded.ends_with('/'));
    }
}
