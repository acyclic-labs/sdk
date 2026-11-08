use super::historical;
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Component, Path},
};

/// Check a corpus record containing package, version and archive bytes.
///
/// # Panics
/// Panics if the admitted result violates the independent contract checks.
pub fn check_input(input: &[u8]) -> Option<BTreeMap<String, Vec<u8>>> {
    let mut parts = input.splitn(3, |byte| *byte == b'\n');
    let package = std::str::from_utf8(parts.next()?).ok()?;
    let version = std::str::from_utf8(parts.next()?).ok()?;
    check_archive(parts.next()?, package, version).ok()
}

/// Compare every admitted member and original body with the maintained reader.
///
/// # Panics
/// Panics if the admitted result violates the independent contract checks.
pub fn check_archive(
    bytes: &[u8],
    package: &str,
    version: &str,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let result = historical::decode_archive_members(bytes, package, version);
    if let Ok(members) = &result {
        assert_eq!(check_members(bytes, package, version, members), Ok(()));
    }
    result
}

fn check_members(
    bytes: &[u8],
    package: &str,
    version: &str,
    members: &BTreeMap<String, Vec<u8>>,
) -> Result<(), String> {
    // Observe original entry bodies with the maintained format reader.
    // No copied SDK admission algorithm, limit reader or second member map.
    let reader: Box<dyn Read + '_> = if bytes.starts_with(&[0x1f, 0x8b]) {
        Box::new(flate2::read::MultiGzDecoder::new(bytes))
    } else {
        Box::new(bytes)
    };
    let mut archive = tar::Archive::new(reader);
    let directory = format!("{package}-{version}");
    let prefix = format!("{directory}/");
    let mut count = 0;
    for entry in archive
        .entries()
        .map_err(|error| format!("archive admission accepted invalid entries: {error}"))?
    {
        let mut entry = entry
            .map_err(|error| format!("archive admission accepted an invalid entry: {error}"))?;
        let kind = entry.header().entry_type();
        assert!(
            kind.is_file() || kind.is_dir(),
            "archive admission accepted a linked member"
        );
        let path = entry.path_bytes();
        let path = std::str::from_utf8(&path)
            .map_err(|error| format!("archive admission accepted invalid UTF-8: {error}"))?;
        if kind.is_dir() && path == directory {
            continue;
        }
        let relative = path
            .strip_prefix(&prefix)
            .ok_or_else(|| "archive admission lost package binding".to_owned())?;
        assert!(
            !relative.contains(['\\', ':'])
                && !Path::new(relative).components().any(|part| matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )),
            "archive admission accepted an unsafe path"
        );
        if kind.is_dir() {
            continue;
        }
        assert!(
            !relative.is_empty(),
            "archive admission accepted an empty member"
        );
        let name = relative.to_owned();
        let mut body = Vec::new();
        entry
            .read_to_end(&mut body)
            .map_err(|error| format!("archive admission accepted a truncated body: {error}"))?;
        assert!(
            members.get(&name) == Some(&body),
            "archive admission lost original member bytes"
        );
        count += 1;
    }
    assert_eq!(
        members.len(),
        count,
        "archive admission lost original inventory"
    );
    std::io::copy(&mut archive.into_inner(), &mut std::io::sink())
        .map_err(|error| format!("archive admission accepted an invalid trailer: {error}"))?;
    Ok(())
}
