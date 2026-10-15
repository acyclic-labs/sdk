#![no_main]
use acyclic_auth::{
    authority::{decode_block, decode_capability},
    contract::{BlockProto, CapabilityProto},
    decode_component, encode_component,
};
use libfuzzer_sys::fuzz_target;
use prost::Message;
fuzz_target!(|bytes: &[u8]| {
    if let Ok(value) = decode_capability(bytes) {
        assert_eq!(
            decode_capability(&CapabilityProto::from(value.clone()).encode_to_vec()),
            Ok(value)
        );
    }
    if let Ok(value) = decode_block(bytes) {
        assert_eq!(
            decode_block(&BlockProto::from(value.clone()).encode_to_vec()),
            Ok(value)
        );
    }
    assert_eq!(
        decode_component(&encode_component(bytes)),
        Ok(bytes.to_vec())
    );
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(decoded) = decode_component(text) {
            assert_eq!(encode_component(&decoded), text);
        }
    }
});
