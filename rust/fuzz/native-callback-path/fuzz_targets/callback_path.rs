#![no_main]

#[path = "../../../crates/filesystem/src/native_mount/projfs/callback_path.rs"]
mod callback_path;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    let raw_parts: Vec<_> = input
        .split(|byte| *byte == 0xff)
        .map(<[u8]>::to_vec)
        .collect();
    if let Some(wide) = callback_path::wide_components(&raw_parts) {
        let wide: Vec<_> = wide.collect();
        let decoded = callback_path::components(&wide);
        assert_eq!(decoded.as_ref(), Some(&raw_parts));
    }
    if input.len() % 2 != 0 {
        return;
    }
    let wide = input
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    if let Some(parts) = callback_path::components(&wide) {
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
        if let Some(direct) = callback_path::wide_components(&parts) {
            assert_eq!(direct.collect::<Vec<_>>(), canonical);
        }
        assert_eq!(callback_path::components(&canonical), Some(parts));
    }
});
