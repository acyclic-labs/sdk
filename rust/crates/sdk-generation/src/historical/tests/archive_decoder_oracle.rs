// Retained byte-map decoder from signed4b16f06a52; small-fixture parity oracle.
use super::*;

pub(super) fn retained_archive_decoder(
    bytes: &[u8],
    package: &str,
    version: &str,
    limits: ArchiveLimits,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    validate_name(package)?;
    validate_version(version)?;
    if bytes.len() as u64 > limits.compressed_bytes {
        return Err("registry archive compressed-byte limit exceeded".into());
    }
    let reader: Box<dyn Read + '_> = if bytes.starts_with(&[0x1f, 0x8b]) {
        Box::new(flate2::read::MultiGzDecoder::new(bytes))
    } else {
        Box::new(bytes)
    };
    let mut reader = BufReader::new(ExpandedReader { reader, remaining: limits.expanded_bytes });
    if reader.fill_buf().map_err(|e| e.to_string())?.is_empty() {
        return Err("registry archive is empty".into());
    }
    let mut archive = tar::Archive::new(reader);
    let directory = format!("{package}-{version}");
    let prefix = format!("{directory}/");
    let mut result = BTreeMap::new();
    for (index, entry) in archive.entries().map_err(|e| e.to_string())?.enumerate() {
        if index >= limits.members {
            return Err("registry archive member-count limit exceeded".into());
        }
        let mut entry = entry.map_err(|e| e.to_string())?;
        if entry.size() > limits.member_bytes {
            return Err("registry archive member-byte limit exceeded".into());
        }
        let kind = entry.header().entry_type();
        if !kind.is_file() && !kind.is_dir() {
            return Err("registry archive contains unsupported linked source members".into());
        }
        let path = entry.path_bytes();
        let path = std::str::from_utf8(&path).map_err(|e| e.to_string())?;
        if kind.is_dir() && path == directory {
            continue;
        }
        let relative = path
            .strip_prefix(&prefix)
            .ok_or("registry archive member escapes package prefix")?;
        if relative.contains(['\\', ':'])
            || Path::new(relative).components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err("registry archive member escapes imported source".into());
        }
        if kind.is_dir() {
            continue;
        }
        if relative.is_empty() || result.contains_key(relative) {
            return Err("duplicate or empty registry archive member".into());
        }
        let relative = relative.to_owned();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
        result.insert(relative, bytes);
    }
    // Reading through the gzip trailer verifies the transport checksum too.
    std::io::copy(&mut archive.into_inner(), &mut std::io::sink()).map_err(|e| e.to_string())?;
    Ok(result)
}


struct ExpandedReader<R> {
    reader: R,
    remaining: u64,
}

impl<R: Read> Read for ExpandedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() { return Ok(0); }
        if self.remaining == 0 {
            let mut extra = [0];
            return match self.reader.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                    "registry archive expanded-byte limit exceeded")),
            };
        }
        let length = self.remaining.min(buffer.len() as u64) as usize;
        let read = self.reader.read(&mut buffer[..length])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}
