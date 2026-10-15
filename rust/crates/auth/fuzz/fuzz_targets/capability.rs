#![no_main]
use acyclic_auth::{
    authority::{decode_block, decode_capability},
    contract::{
        BlockProto, CapabilityProto, RecordAttributes, RecordAttributesProto, SocketHostRule,
        SocketHostRuleProto,
    },
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
    if let Ok(value) = RecordAttributes::decode(bytes) {
        assert!(
            value
                .as_slice()
                .iter()
                .enumerate()
                .all(|(i, a)| value.as_slice()[..i].iter().all(|b| a.name != b.name))
        );
        assert_eq!(
            RecordAttributes::decode(&RecordAttributesProto::from(value.clone()).encode_to_vec()),
            Ok(value)
        );
    }
    assert_eq!(
        decode_component(&encode_component(bytes)),
        Ok(bytes.to_vec())
    );
    if let Ok(text) = std::str::from_utf8(bytes) {
        let wire = SocketHostRuleProto {
            host: text.to_owned(),
            ports: vec![],
        };
        if let Ok(value) = SocketHostRule::try_from(wire) {
            let bytes = SocketHostRuleProto::from(value.clone()).encode_to_vec();
            assert_eq!(
                SocketHostRule::try_from(SocketHostRuleProto::decode(bytes.as_slice()).unwrap()),
                Ok(value)
            );
        }
        if let Ok(decoded) = decode_component(text) {
            assert_eq!(encode_component(&decoded), text);
        }
    }
});
