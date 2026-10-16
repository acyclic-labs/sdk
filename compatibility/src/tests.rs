use super::*;
use proptest::prelude::*;
use prost::Message;
use prost_types::{
    FileDescriptorProto, FileDescriptorSet, SourceCodeInfo, source_code_info::Location,
};

#[test]
fn every_compiled_descriptor_is_byte_identical_to_its_wire_baseline()
-> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("missing workspace root")?;
    let output = tempfile::tempdir()?;
    for (family, baseline, protos) in FAMILIES {
        let destination = output.path().join(baseline);
        descriptor(&root.join("rust/crates").join(family), protos, &destination)?;
        let actual = std::fs::read(&destination)?;
        let expected = std::fs::read(root.join("compatibility/descriptors").join(baseline))?;
        assert!(
            actual == expected,
            "{family} protocol identity changed: actual {} expected {} first difference {:?}",
            actual.len(),
            expected.len(),
            actual.iter().zip(&expected).position(|(a, b)| a != b)
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Canonicalization is idempotent, independent of source-location emission
    // order, and preserves wire field numbers. JSON/options are not reinterpreted.
    #[test]
    fn canonical_descriptor_roundtrip(
        paths in prop::collection::vec(prop::collection::vec(0..32_i32, 0..8), 0..32),
        field_number in 1..536_870_911_i32,
    ) {
        let mut set = FileDescriptorSet { file: vec![FileDescriptorProto {
            name: Some("sample.proto".into()),
            package: Some("sample".into()),
            syntax: Some("proto3".into()),
            message_type: vec![prost_types::DescriptorProto {
                name: Some("Message".into()),
                field: vec![prost_types::FieldDescriptorProto {
                    name: Some("value".into()), number: Some(field_number),
                    r#type: Some(9), label: Some(1), ..Default::default()
                }], ..Default::default()
            }],
            source_code_info: Some(SourceCodeInfo {
                location: paths.into_iter().zip(0..32_i32).map(|(path, line)| Location {
                    path, span: vec![line, 0, 8],
                    trailing_comments: Some(" trailing\n".into()), ..Default::default()
                }).collect(),
            }), ..Default::default()
        }] };
        let roots = &["sample.proto"];
        let canonical = canonicalize(&set.encode_to_vec(), roots).map_err(|error| TestCaseError::fail(error.to_string()))?;
        set.file.first_mut().and_then(|file| file.source_code_info.as_mut()).ok_or_else(|| std::io::Error::other("missing source info"))?.location.reverse();
        prop_assert_eq!(&canonical, &canonicalize(&set.encode_to_vec(), roots).map_err(|error| TestCaseError::fail(error.to_string()))?);
        prop_assert_eq!(&canonical, &canonicalize(&canonical, roots).map_err(|error| TestCaseError::fail(error.to_string()))?);
        let decoded = FileDescriptorSet::decode(canonical.as_slice())?;
        prop_assert_eq!(decoded.file.first().and_then(|file| file.message_type.first()).and_then(|message| message.field.first()).and_then(|field| field.number), Some(field_number));
    }
}
