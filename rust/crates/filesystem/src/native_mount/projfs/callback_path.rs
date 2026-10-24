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

pub(super) fn wide_components(parts: &[Vec<u8>]) -> Option<impl Iterator<Item = u16> + Clone + '_> {
    for part in parts {
        if part.is_empty()
            || part.len() % 2 != 0
            || matches!(part.as_slice(), [46, 0] | [46, 0, 46, 0])
            || part
                .as_chunks::<2>()
                .0
                .iter()
                .any(|&bytes| matches!(u16::from_le_bytes(bytes), 0 | 47 | 58 | 92))
        {
            return None;
        }
    }
    Some(parts.iter().enumerate().flat_map(|(index, part)| {
        std::iter::once(92).take(usize::from(index != 0)).chain(
            part.as_chunks::<2>()
                .0
                .iter()
                .map(|&bytes| u16::from_le_bytes(bytes)),
        )
    }))
}
