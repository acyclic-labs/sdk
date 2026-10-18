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
pub use acyclic_fuzz_support as fuzz_support;
use prost::Message;
fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(value) = fuzz_support::FuzzInput::parse(fuzz_input, decode_caveat) {
        assert_eq!(CaveatProto::from(value).encode_to_vec(), bytes);
    }
    if let Some(value) = fuzz_support::FuzzInput::parse(fuzz_input, decode_capability) {
        let _ = fuzz_support::round_trip(
            &(value),
            decode_capability(&CapabilityProto::from(value.clone()).encode_to_vec()),
        );
    }
    if let Some(value) = fuzz_support::FuzzInput::parse(fuzz_input, decode_block) {
        let _ = fuzz_support::round_trip(
            &(value),
            decode_block(&BlockProto::from(value.clone()).encode_to_vec()),
        );
    }
    if let Some(value) = fuzz_support::FuzzInput::parse(fuzz_input, RecordAttributes::decode) {
        assert!(
            value
                .as_slice()
                .iter()
                .enumerate()
                .all(|(i, a)| value.as_slice()[..i].iter().all(|b| a.name != b.name))
        );
        let _ = fuzz_support::round_trip(
            &(value),
            RecordAttributes::decode(&RecordAttributesProto::from(value.clone()).encode_to_vec()),
        );
    }
    let _ = fuzz_support::round_trip(
        &(bytes.to_vec()),
        decode_component(&encode_component(bytes)),
    );
    if let Some(text) = fuzz_input.text() {
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
        if let Some(value) = fuzz_support::FuzzInput::parse_text(fuzz_input, |text| {
            SocketHostRule::try_from(SocketHostRuleProto {
                host: text.to_owned(),
                ports: vec![],
            })
        }) {
            let bytes = SocketHostRuleProto::from(value.clone()).encode_to_vec();
            let _ = fuzz_support::round_trip(
                &(value),
                SocketHostRule::try_from(SocketHostRuleProto::decode(bytes.as_slice()).unwrap()),
            );
        }
        if let Some(decoded) = fuzz_support::FuzzInput::parse_text(fuzz_input, decode_component) {
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
