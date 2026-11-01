//! Differential coverage of the public Objects JSON boundary and shared codec.
use acyclic_objects::{Error, json, wire::ErrorCode};
use prost::Message;
use prost_reflect::MessageDescriptor;

fn name(descriptor: &MessageDescriptor) -> Option<&str> {
    descriptor.full_name().strip_prefix("acyclic.objects.v1.")
}

fn value(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("actual protobuf JSON writer output")
}

fn quota<T>(result: Result<T, Error>) {
    assert_eq!(result.err(), Some(ErrorCode::QuotaExceeded.into()));
}

pub(super) fn binary(descriptor: &MessageDescriptor, bytes: &[u8]) {
    let Some(name) = name(descriptor) else {
        return;
    };
    let actual = json::encode_binary(name, bytes, None);
    let shared = acyclic_protobuf_json::encode(descriptor.clone(), bytes);
    match (actual, shared) {
        (Ok(actual), Ok(shared)) => assert_eq!(value(&actual), value(&shared)),
        (Err(actual), Err(_)) => assert_eq!(actual, ErrorCode::InvalidArgument.into()),
        _ => panic!("Objects and shared wire admission disagree"),
    }
    if let Some(maximum) = bytes.len().checked_sub(1) {
        quota(json::encode_binary(name, bytes, Some(maximum)));
    }
}

pub(super) fn json(descriptor: &MessageDescriptor, bytes: &[u8]) {
    let Some(name) = name(descriptor) else {
        return;
    };
    let actual = json::decode_binary(name, bytes, None);
    let shared = acyclic_protobuf_json::decode(descriptor.clone(), bytes, bytes.len());
    match (actual, shared) {
        (Ok(actual), Ok(shared)) => {
            let actual_json = json::encode_binary(name, &actual, None).unwrap();
            let shared_json =
                acyclic_protobuf_json::encode(descriptor.clone(), &shared.encode_to_vec()).unwrap();
            assert_eq!(value(&actual_json), value(&shared_json));
            let repeated = json::decode_binary(name, &actual_json, None).unwrap();
            assert_eq!(
                value(&json::encode_binary(name, &repeated, None).unwrap()),
                value(&actual_json)
            );
            let mut trailing = bytes.to_vec();
            trailing.extend_from_slice(b" false");
            assert_eq!(
                json::decode_binary(name, &trailing, None).unwrap_err(),
                ErrorCode::InvalidArgument.into()
            );
        }
        (Err(actual), Err(_)) => assert_eq!(actual, ErrorCode::InvalidArgument.into()),
        _ => panic!("Objects and shared JSON admission disagree"),
    }
    if let Some(maximum) = bytes.len().checked_sub(1) {
        quota(json::decode_binary(name, bytes, Some(maximum)));
    }
}

pub(super) fn typed<T: Message + Default>(descriptor: &MessageDescriptor, message: &T) {
    let Some(name) = name(descriptor) else {
        return;
    };
    let encoded = json::encode(name, message).expect("typed Objects message encodes");
    let restored: T = json::decode(name, &encoded, None).expect("typed Objects JSON decodes");
    assert_eq!(
        value(&json::encode(name, &restored).unwrap()),
        value(&encoded)
    );
    if let Some(maximum) = encoded.len().checked_sub(1) {
        quota(json::decode::<T>(name, &encoded, maximum));
    }
}
