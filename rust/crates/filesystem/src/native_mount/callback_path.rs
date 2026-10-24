pub(crate) fn components(wide: &[u16]) -> Option<Vec<Vec<u8>>> {
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

pub(crate) fn wide_components(parts: &[Vec<u8>]) -> Option<impl Iterator<Item = u16> + Clone + '_> {
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

/// Exercises callback path encoding and decoding without a native mount.
#[cfg(feature = "fuzzing")]
pub fn fuzz_check(input: &[u8]) {
    let raw_parts: Vec<_> = input
        .split(|byte| *byte == 0xff)
        .map(<[u8]>::to_vec)
        .collect();
    if let Some(wide) = wide_components(&raw_parts) {
        let wide: Vec<_> = wide.collect();
        let decoded = components(&wide);
        assert_eq!(decoded.as_ref(), Some(&raw_parts));
    }
    if input.len() % 2 != 0 {
        return;
    }
    let wide = input
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    if let Some(parts) = components(&wide) {
        assert_eq!(parts.capacity(), parts.len());
        let mut canonical = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            assert!(!part.is_empty());
            assert_eq!(part.capacity(), part.len());
            assert_eq!(part.len() % 2, 0);
            if index != 0 {
                canonical.push(92);
            }
            canonical.extend(
                part.chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]])),
            );
        }
        if let Some(direct) = wide_components(&parts) {
            assert_eq!(direct.collect::<Vec<_>>(), canonical);
        }
        assert_eq!(components(&canonical), Some(parts));
    }
}
