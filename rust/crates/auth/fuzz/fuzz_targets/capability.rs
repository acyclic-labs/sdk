#![no_main]
use acyclic_auth::{
    authority::{
        Deny, PolicyDecision, Request, Target, decode_block, decode_capability, decode_caveat,
        permits,
    },
    component_matches,
    contract::{
        BlockProto, CapabilityProto, CaveatProto, IssuanceReplyProto, IssuanceRequestProto, Limits,
        RecordAttributes, RecordAttributesProto, SocketHostRule, SocketHostRuleProto,
    },
    decode_component, encode_component,
    socket::valid_dns_name,
    trust::issuance::{Reply as IssuanceReply, Request as IssuanceRequest},
    valid_resource,
};
use libfuzzer_sys::fuzz_target;
use prost::Message;
fuzz_target!(|bytes: &[u8]| {
    if let Ok(request) = IssuanceRequest::decode(bytes) {
        assert_eq!(
            request.wire(),
            bytes,
            "issuance request must retain canonical bytes"
        );
        let mut repeated = IssuanceRequestProto {
            version: 1,
            ..Default::default()
        }
        .encode_to_vec();
        repeated.extend_from_slice(bytes);
        assert!(
            IssuanceRequest::decode(&repeated).is_err(),
            "duplicate issuance request version must refuse"
        );
    }
    if let Ok(reply) = IssuanceReply::decode(bytes) {
        assert_eq!(
            reply.into_wire(),
            bytes,
            "issuance reply must retain canonical bytes"
        );
        let mut repeated = IssuanceReplyProto {
            version: 1,
            ..Default::default()
        }
        .encode_to_vec();
        repeated.extend_from_slice(bytes);
        assert!(
            IssuanceReply::decode(&repeated).is_err(),
            "duplicate issuance reply version must refuse"
        );
    }
    if let Ok(value) = decode_caveat(bytes) {
        assert_eq!(CaveatProto::from(value).encode_to_vec(), bytes);
    }
    if let Ok(value) = decode_capability(bytes) {
        assert_eq!(
            decode_capability(&CapabilityProto::from(value.clone()).encode_canonical()),
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
    let encoded = encode_component(bytes);
    assert_eq!(decode_component(&encoded), Ok(bytes.to_vec()));
    assert!(component_matches(&encoded, bytes));
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
                permits(
                    &[],
                    &request,
                    PolicyDecision::Allow,
                    acyclic_auth::authority::Scope::Exact
                ),
                Err(Deny::NonCanonicalResource)
            );
            request.target = Target::Resource {
                action: "fs/read",
                resource: "fs:/a",
            };
            request.audience = text;
            assert_eq!(
                permits(
                    &[],
                    &request,
                    PolicyDecision::Allow,
                    acyclic_auth::authority::Scope::Exact
                ),
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
        let decoded = decode_component(text);
        assert_eq!(
            component_matches(text, bytes),
            decoded.as_ref().is_ok_and(|decoded| decoded == bytes)
        );
        if let Ok(decoded) = decoded {
            assert_eq!(encode_component(&decoded), text);
            assert!(component_matches(text, &decoded));
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
