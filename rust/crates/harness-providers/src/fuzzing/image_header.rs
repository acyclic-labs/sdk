use acyclic_harness::model::{ImageHeader, ImageInput};

fn policy() -> ImageInput {
    ImageInput {
        maximum_bytes: 4096,
        maximum_total_bytes: 8192,
        maximum_images: 4,
        maximum_dimension: 4096,
        maximum_tokens: 1024,
    }
}

fn check(bytes: &[u8], limits: &ImageInput, parsed: Option<ImageHeader>) {
    if let Some(header) = parsed {
        assert!(!bytes.is_empty() && bytes.len() as u64 <= limits.maximum_bytes);
        assert!(header.width > 0 && header.height > 0);
        assert!(
            header.width <= limits.maximum_dimension as usize
                && header.height <= limits.maximum_dimension as usize,
            "dimension admission"
        );
        let encoded = serde_json::to_vec(&header).unwrap();
        assert_eq!(
            header,
            serde_json::from_slice::<ImageHeader>(&encoded).unwrap()
        );
        let mime = header.format.media().0;
        assert_eq!(limits.parse(bytes, Some(mime)).unwrap(), header);
        assert!(limits.parse(bytes, Some("text/plain")).is_err());
    }
}

/// Exercises the same bounded image parser used by host and provider admission.
pub fn assert_image_header(bytes: &[u8]) {
    let limits = policy();
    check(bytes, &limits, limits.parse(bytes, None).ok());
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    const PNG: &[u8] = include_bytes!("../../fuzz/corpus/image_header/one.png");

    #[test]
    fn image_header_checked_corpus_and_every_truncated_prefix() {
        let header = policy().parse(PNG, Some("image/png")).unwrap();
        assert_eq!((header.width, header.height), (1, 1));
        for seed in [
            PNG,
            include_bytes!("../../fuzz/corpus/image_header/truncated-jpeg").as_slice(),
            include_bytes!("../../fuzz/corpus/image_header/truncated-webp").as_slice(),
            include_bytes!("../../fuzz/corpus/image_header/truncated-gif").as_slice(),
            include_bytes!("../../fuzz/corpus/image_header/truncated-heif").as_slice(),
            include_bytes!("../../fuzz/corpus/image_header/zero-dimensions").as_slice(),
            include_bytes!("../../fuzz/corpus/image_header/malformed").as_slice(),
        ] {
            for end in 0..=seed.len() {
                assert_image_header(&seed[..end]);
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn image_header_arbitrary_bytes_are_total(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            assert_image_header(&bytes);
        }
        #[test]
        fn image_header_dimension_round_trip_shrinks(width in 0u32..8192, height in 0u32..8192,
            trailing in proptest::collection::vec(any::<u8>(), 0..64)) {
            let mut bytes = PNG.to_vec();
            bytes[16..20].copy_from_slice(&width.to_be_bytes());
            bytes[20..24].copy_from_slice(&height.to_be_bytes());
            bytes.extend(trailing);
            let limits = policy();
            let parsed = limits.parse(&bytes, None);
            prop_assert_eq!(parsed.is_ok(), width > 0 && height > 0 && width <= 4096 && height <= 4096);
            if let Ok(header) = parsed {
                prop_assert_eq!((header.width, header.height), (width as usize, height as usize));
                check(&bytes, &limits, Some(header));
            }
        }
    }

    #[test]
    #[should_panic(expected = "dimension admission")]
    fn image_header_missing_dimension_bound_negative_control() {
        let mut bytes = PNG.to_vec();
        bytes[16..20].copy_from_slice(&5000u32.to_be_bytes());
        let mut loose = policy();
        loose.maximum_dimension = 8192;
        check(&bytes, &policy(), loose.parse(&bytes, None).ok());
    }
}
