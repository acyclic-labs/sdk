//! Independent JSON scalar expectations over the actual protobuf descriptor.
use acyclic_protobuf_json::{Error, decode, encode};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use prost::Message;
use prost_reflect::{DescriptorPool, MessageDescriptor};

fn descriptor() -> Result<MessageDescriptor, TestCaseError> {
    DescriptorPool::global()
        .get_message_by_name("google.protobuf.UninterpretedOption")
        .ok_or_else(|| TestCaseError::fail("standard protobuf descriptor is absent"))
}

proptest! {
    #[test]
    fn every_nonzero_uint64_keeps_its_decimal_string_and_exact_wire(value in 1_u64..=u64::MAX) {
        // Standard descriptor.proto: positive_int_value is uint64 field 4.
        let mut wire = vec![32];
        prost::encoding::encode_varint(value, &mut wire);
        let descriptor = descriptor()?;
        let json = encode(descriptor.clone(), &wire)
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        let expected = format!("{{\"positiveIntValue\":\"{value}\"}}");
        prop_assert_eq!(json.as_slice(), expected.as_bytes());
        let actual = decode(descriptor, &json, json.len())
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        prop_assert_eq!(actual.encode_to_vec(), wire);
    }
}

#[test]
fn bytes_use_base64_and_each_refusal_stage_stays_distinct() -> Result<(), TestCaseError> {
    let descriptor = descriptor()?;
    // Standard descriptor.proto: string_value is bytes field 7.
    let wire = [58, 2, 0, 255];
    let json = encode(descriptor.clone(), &wire)
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
    assert_eq!(json, br#"{"stringValue":"AP8="}"#);
    let actual = decode(descriptor.clone(), &json, json.len())
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
    assert_eq!(actual.encode_to_vec(), wire);
    assert_eq!(
        decode(descriptor.clone(), b"{}", 1).err(),
        Some(Error::Limit)
    );
    assert_eq!(
        decode(descriptor.clone(), br#"{"unknown":true}"#, 64).err(),
        Some(Error::Json)
    );
    assert_eq!(
        decode(descriptor.clone(), br#"{"positiveIntValue":"-1"}"#, 64).err(),
        Some(Error::Json)
    );
    assert_eq!(
        decode(descriptor.clone(), b"{} {}", 64).err(),
        Some(Error::Trailing)
    );
    assert_eq!(encode(descriptor, &[128]).err(), Some(Error::Wire));
    Ok(())
}
