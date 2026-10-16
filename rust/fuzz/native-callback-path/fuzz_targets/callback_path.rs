#![no_main]

#[path = "../../../crates/filesystem/src/native_mount/projfs/callback_path.rs"]
mod callback_path;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
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
        assert_eq!(callback_path::components(&canonical), Some(parts));
    }
});
