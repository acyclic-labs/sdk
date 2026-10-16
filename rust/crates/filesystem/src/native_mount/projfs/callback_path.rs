pub(super) fn components(wide: &[u16]) -> Option<Vec<Vec<u8>>> {
    fn separator(unit: &u16) -> bool {
        matches!(*unit, 47 | 92)
    }
    // Every Windows prefix and rooted path is rejected by the callback edge.
    if wide.first().is_some_and(separator)
        || (wide.get(1) == Some(&58) && matches!(wide.first(), Some(65..=90 | 97..=122)))
    {
        return None;
    }
    let parts = wide.split(separator);
    // Components preserves a leading CurDir but skips interior dot components.
    if parts.clone().next() == Some(&[46][..]) {
        return None;
    }
    let parts = parts.filter(|part| !part.is_empty() && *part != [46]);
    let mut count = 0usize;
    for part in parts.clone() {
        if part == [46, 46] {
            return None;
        }
        count = count.checked_add(1)?;
    }
    let mut components = Vec::with_capacity(count);
    for part in parts {
        let mut bytes = Vec::with_capacity(part.len().checked_mul(2)?);
        for unit in part {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        components.push(bytes);
    }
    Some(components)
}
