use super::*;

pub(crate) fn collect_physical_garbage(
    root: &Path,
    live_bodies: &BTreeSet<LocalBodyReference>,
    maximum_candidates: u64,
    maximum_object_bytes: u64,
    durability: LocalDurability,
) -> Result<LocalObjectsGarbageCollection, PhysicalError> {
    let mut candidates = 0_u64;
    let mut temporary = Vec::new();
    let segments = scan_segments(root, maximum_candidates, &mut candidates, &mut temporary)?;
    let live_segments = validate_referenced_segments(root, live_bodies, maximum_object_bytes)?;
    let mut report = LocalObjectsGarbageCollection {
        segments_examined: u64::try_from(segments.len()).unwrap_or(u64::MAX),
        ..LocalObjectsGarbageCollection::default()
    };
    let mut changed_parents = BTreeSet::new();
    for (id, path) in segments {
        if !live_segments.contains(&id) {
            fs::remove_file(&path)?;
            report.segments_removed = report.segments_removed.saturating_add(1);
            changed_parents.insert(root.join("segments"));
        }
    }
    for path in temporary {
        fs::remove_file(&path)?;
        report.temporary_files_removed = report.temporary_files_removed.saturating_add(1);
        if let Some(parent) = path.parent() {
            changed_parents.insert(parent.to_path_buf());
        }
    }
    for parent in changed_parents {
        sync_parent(&parent, durability)?;
    }
    Ok(report)
}

pub(super) fn scan_segments(
    root: &Path,
    maximum_candidates: u64,
    candidates: &mut u64,
    temporary: &mut Vec<PathBuf>,
) -> Result<Vec<([u8; 32], PathBuf)>, PhysicalError> {
    let mut entries = fs::read_dir(root.join("segments"))?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut files = Vec::new();
    for entry in entries {
        if !entry.file_type()?.is_file() {
            return Err(PhysicalError::Corrupt);
        }
        *candidates = candidates.checked_add(1).ok_or(PhysicalError::Invalid(
            "garbage-collection count overflowed",
        ))?;
        if *candidates > maximum_candidates {
            return Err(PhysicalError::Invalid(
                "garbage-collection candidate bound exceeded",
            ));
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or(PhysicalError::Corrupt)?;
        if name.starts_with('.') && name.ends_with(".tmp") {
            temporary.push(entry.path());
            continue;
        }
        let encoded = name
            .strip_suffix(".segment")
            .ok_or(PhysicalError::Corrupt)?;
        if encoded.len() != 64 || !encoded.bytes().all(is_lower_hex) {
            return Err(PhysicalError::Corrupt);
        }
        let mut id = [0_u8; 32];
        decode_lower_hex(encoded, &mut id)?;
        files.push((id, entry.path()));
    }
    Ok(files)
}

pub(super) fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

pub(super) fn decode_lower_hex(encoded: &str, output: &mut [u8]) -> Result<(), PhysicalError> {
    if encoded.len() != output.len().saturating_mul(2) {
        return Err(PhysicalError::Corrupt);
    }
    for (index, &[high, low]) in encoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let value = (hex_nibble(high)? << 4) | hex_nibble(low)?;
        *output.get_mut(index).ok_or(PhysicalError::Corrupt)? = value;
    }
    Ok(())
}

pub(super) fn hex_nibble(byte: u8) -> Result<u8, PhysicalError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(PhysicalError::Corrupt),
    }
}
