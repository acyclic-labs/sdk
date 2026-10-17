use super::*;

fn marker(file: &FileRef) -> String {
    crate::files::FileText::default()
        .project(file, &FileProjectionPolicy::Reference)
        .unwrap()
}

fn restore(marker: &str) -> FileRef {
    serde_json::from_str(
        marker
            .strip_prefix("[file: ")
            .unwrap()
            .strip_suffix(']')
            .unwrap(),
    )
    .unwrap()
}

fn assert_identity(first: &FileRef, first_marker: &str, second: &FileRef, second_marker: &str) {
    assert_ne!(first, second);
    assert_ne!(
        first_marker, second_marker,
        "reference projection erased file identity"
    );
    assert_eq!(&restore(first_marker), first);
    assert_eq!(&restore(second_marker), second);
}

fn contains(value: &Value, marker: &str) -> bool {
    match value {
        Value::String(text) => text.contains(marker),
        Value::Array(values) => values.iter().any(|value| contains(value, marker)),
        Value::Object(values) => values.values().any(|value| contains(value, marker)),
        _ => false,
    }
}

fn changed(file: &FileRef, field: &str, value: Value) -> FileRef {
    let mut reference = serde_json::to_value(file).unwrap();
    if field == "volume" {
        reference["volume"]["id"] = value;
    } else {
        reference[field] = value;
    }
    serde_json::from_value(reference).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn reference_identity_survives_every_provider_projection(
        text in ".{0,64}", path in "[a-zα-ω]{1,16}", revision in any::<u64>(),
    ) {
        let original = file(text.as_bytes(), "text/plain").unwrap();
        let original_marker = marker(&original);
        for different in [
            changed(&original, "path", json!(format!("{path}/report.txt"))),
            changed(&original, "version", json!(format!("revision-{revision}"))),
            changed(&original, "volume", json!(format!("other-{path}"))),
        ] {
            let other_marker = marker(&different);
            assert_identity(&original, &original_marker, &different, &other_marker);
            for (adapter, _) in fixtures() {
                let (provider, _) = provider(adapter, 200);
                let request = with_parts(&provider, [original.clone(), different.clone()].into_iter()
                    .map(|file| ModelContentPart::File { file, policy: FileProjectionPolicy::Reference }).collect()).unwrap();
                let body = crate::projection::body_with_files(&provider, request.request(), &mut crate::files::FileText::default()).unwrap();
                prop_assert!(contains(&body, &original_marker));
                prop_assert!(contains(&body, &other_marker));
            }
        }
    }
}

#[test]
#[should_panic(expected = "reference projection erased file identity")]
fn negative_control_detects_the_former_lossy_marker() {
    let original = file(b"same contents", "text/plain").unwrap();
    let different = changed(&original, "path", json!("b/report.txt"));
    // All the former marker's inputs are equal: display name, type, digest, size.
    assert_eq!(original.display_name(), different.display_name());
    assert_eq!(original.descriptor(), different.descriptor());
    let old_marker = format!(
        "[file: {}; {}; sha256={:?}; bytes={}]",
        original.display_name(),
        original.descriptor().media_type(),
        original.descriptor().sha256(),
        original.descriptor().byte_length()
    );
    assert_identity(&original, &old_marker, &different, &old_marker);
}

#[test]
fn checked_in_reference_seed_round_trips_its_full_identity() {
    let seed = include_bytes!("../fuzz/corpus/provider_stream/reference.json");
    let file: FileRef = serde_json::from_slice(&seed[1..]).expect("accepted FileRef seed");
    assert_eq!(file.path(), "a/report.txt");
    assert_eq!(restore(&marker(&file)), file);
    crate::fuzzing::assert_stream_invariants(seed);
}
