#![no_main]
//! Check the production callback path codecs against independent component admission.

#[path = "../../../crates/filesystem/src/native_mount/projfs/callback_path.rs"]
mod callback_path;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    // Construct an admitted component independently of either codec. The
    // leading underscore excludes dot/parent components; forbidden filename
    // units are replaced, while every other UTF-16 unit stays unchanged.
    let admitted: Vec<_> = std::iter::once(95)
        .chain(
            input
                .as_chunks::<2>()
                .0
                .iter()
                .map(|&pair| match u16::from_le_bytes(pair) {
                    0 | 47 | 58 | 92 => 95,
                    unit => unit,
                }),
        )
        .collect();
    let expected = vec![
        admitted
            .iter()
            .flat_map(|unit| unit.to_le_bytes())
            .collect(),
    ];
    assert_eq!(
        callback_path::components(&admitted),
        Some(expected.clone()),
        "valid callback component must be admitted unchanged"
    );
    assert_eq!(
        callback_path::wide_components(&expected).map(|wide| wide.collect::<Vec<_>>()),
        Some(admitted),
        "valid callback component must be encodable"
    );

    let raw_parts: Vec<_> = input
        .split(|byte| *byte == 0xff)
        .map(<[u8]>::to_vec)
        .collect();
    if let Some(wide) = callback_path::wide_components(&raw_parts) {
        let wide: Vec<_> = wide.collect();
        let decoded = callback_path::components(&wide);
        assert_eq!(decoded.as_ref(), Some(&raw_parts));
    }
    if !input.len().is_multiple_of(2) {
        return;
    }
    let wide = input
        .as_chunks::<2>()
        .0
        .iter()
        .map(|&pair| u16::from_le_bytes(pair))
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
                part.as_chunks::<2>()
                    .0
                    .iter()
                    .map(|&pair| u16::from_le_bytes(pair)),
            );
        }
        if let Some(direct) = callback_path::wide_components(&parts) {
            assert_eq!(direct.collect::<Vec<_>>(), canonical);
        }
        assert_eq!(callback_path::components(&canonical), Some(parts));
    }
});
