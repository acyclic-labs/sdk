#![no_main]
use acyclic_auth::{
    authority::{
        Deny, PolicyDecision, Request, Target, decode_block, decode_capability, decode_caveat,
        permits,
    },
    contract::{
        BlockProto, CapabilityProto, CaveatProto, Limits, RecordAttributes, RecordAttributesProto,
        SocketHostRule, SocketHostRuleProto,
    },
    decode_component, encode_component,
    socket::valid_dns_name,
    valid_resource,
};
use libfuzzer_sys::fuzz_target;
use prost::Message;
fuzz_target!(|bytes: &[u8]| {
    if let Ok(value) = decode_caveat(bytes) {
        assert_eq!(CaveatProto::from(value).encode_to_vec(), bytes);
    }
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
        let attributes = RecordAttributes::default();
        let usage = Limits {
            rps: None,
            burst: None,
            bytes_per_s: None,
            channels: None,
            concurrency: None,
            bytes: None,
            spend_micros: None,
            tokens: None,
        };
        let mut request = Request {
            target: Target::Resource {
                action: "fs/read",
                resource: text,
            },
            audience: "workers:/app",
            holder: None,
            now_unix_seconds: 2,
            keys: &[],
            attributes: &attributes,
            usage: &usage,
            cpu_millis: 0,
            memory_bytes: 0,
        };
        if !valid_resource(text) {
            assert_eq!(
                permits(&[], &request, PolicyDecision::Allow),
                Err(Deny::NonCanonicalResource)
            );
            request.target = Target::Resource {
                action: "fs/read",
                resource: "fs:/a",
            };
            request.audience = text;
            assert_eq!(
                permits(&[], &request, PolicyDecision::Allow),
                Err(Deny::NonCanonicalAudience)
            );
        }
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
    let numeric = format!(
        "0x{:x}.{}",
        bytes.first().copied().unwrap_or(0),
        bytes.get(1).copied().unwrap_or(0)
    );
    assert!(!valid_dns_name(&numeric));
    assert!(
        SocketHostRule::try_from(SocketHostRuleProto {
            host: numeric,
            ports: vec![]
        })
        .is_err()
    );
});
