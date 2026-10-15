//! Shrinking properties for the contract codec and canonical resource grammar.
use acyclic_auth::{contract::*, valid_action, valid_resource};
use proptest::prelude::*;
use prost::Message;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Scalar repetitions, optional zero limits and all oneof cases preserve
    // their meaning under semantic -> protobuf -> bytes -> semantic ingress.
    #[test]
    fn contract_round_trip(
        lineage in prop::collection::vec("[a-z]{1,12}", 0..8),
        alternatives in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 0..8),
        limit in prop::option::of(any::<u64>()),
        action in prop::sample::select(acyclic_auth::FS_ACTIONS),
    ) {
        let principal = Principal { kind: PrincipalKind::Agent, id: "agent".into(), lineage,
            groups: vec![], roles: vec![], attributes: vec![] };
        let grant = Grant { kind: GrantKind::Resource(ResourceGrant {
            resource: ResourcePattern { name: "fs:/v/".into(), prefix: true },
            actions: vec![format!("fs/{action}")],
        }) };
        let filter = Filter { keys: vec![KeyRange { start: vec![0], end: vec![] }],
            attributes: vec![AttributeMatch { name: "author".into(), any_of: alternatives, subject_id: true, subject_lineage: false }] };
        let limits = Limits { rps: limit, burst: Some(0), bytes_per_s: None, channels: None,
            concurrency: None, bytes: None, spend_micros: None, tokens: None };
        let execution = Grant { kind: GrantKind::Execution(ExecutionGrant {
            grant: ResourceGrant { resource: ResourcePattern { name: "execution:/".into(), prefix: true }, actions: vec!["execution/invoke".into()] },
            cpu_millis: limit, memory_bytes: limit,
        }) };
        let billing = Grant { kind: GrantKind::Billing(BillingGrant {
            grant: ResourceGrant { resource: ResourcePattern { name: "billing:/".into(), prefix: true }, actions: vec!["billing/read".into()] }, spend_micros: limit,
        }) };
        let websocket = Grant { kind: GrantKind::WebsocketConnect(WebsocketConnectGrant {
            rules: vec![SocketHostRule { host: "example.com".into(), ports: vec![SocketPortRange { first: 443, last: 443 }] }],
        }) };
        let cases = vec![CaveatKind::Grants(GrantSet { grants: vec![grant, execution, billing, websocket] }),
            CaveatKind::ExpiresAt(1), CaveatKind::NotBefore(0),
            CaveatKind::Audience("workers:/bot".into()), CaveatKind::Holder(Holder { account: None }),
            CaveatKind::Subject(principal), CaveatKind::Filter(filter),
            CaveatKind::Pin(Pin { name: "author".into(), value: PinValue::SubjectId(true) }),
            CaveatKind::Limits(limits), CaveatKind::MaxDepth(0)];
        let block = Block { account_bearer: Some("certified-bearer".into()),
            caveats: cases.into_iter().map(|kind| Caveat { kind }).collect(),
            next_public_key: vec![1; 32] };
        let wire: BlockProto = block.clone().into();
        let bytes = wire.encode_to_vec();
        let decoded = BlockProto::decode(bytes.as_slice()).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(Block::try_from(decoded), Ok(block));
        let capability = Capability { blocks: vec![SignedBlock { payload: bytes, signature: vec![1; 64] }],
            proof: TipProof::Secret(vec![2; 32]) };
        let wire: CapabilityProto = capability.clone().into();
        let decoded = CapabilityProto::decode(wire.encode_to_vec().as_slice())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(Capability::try_from(decoded), Ok(capability));
        let capability = Capability { blocks: vec![SignedBlock { payload: vec![1], signature: vec![1; 64] }],
            proof: TipProof::Sealed(vec![2; 64]) };
        let wire: CapabilityProto = capability.clone().into();
        let decoded = CapabilityProto::decode(wire.encode_to_vec().as_slice())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(Capability::try_from(decoded), Ok(capability));
    }

    // Untrusted protobuf either fails closed at ingress or produces a value
    // whose canonical re-encoding is stable. No arbitrary bytes may panic.
    #[test]
    fn arbitrary_capability_bytes(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(wire) = CapabilityProto::decode(bytes.as_slice())
            && let Ok(value) = Capability::try_from(wire) {
            let wire: CapabilityProto = value.clone().into();
            let decoded = CapabilityProto::decode(wire.encode_to_vec().as_slice())
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(Capability::try_from(decoded), Ok(value));
        }
    }

    #[test]
    fn canonical_resource_grammar_rejects_path_aliases(name in "[a-z]{1,30}") {
        prop_assert!(valid_resource(&format!("fs:/v/{name}")), "canonical name");
        for alias in [format!("fs:/v/../{name}"), format!("fs:/v/./{name}"), format!("fs:/v//{name}"), format!("fs:/v\\{name}")] {
            prop_assert!(!valid_resource(&alias));
        }
        prop_assert!(!valid_action(&format!("fs/{name}/read")), "action has an extra segment");
    }

    // Wire admission accepts exactly the defined key/signature/proof sizes,
    // and exactly ordered nonempty intervals or an unbounded endpoint.
    #[test]
    fn wire_admission_enforces_shapes(
        len in 0..80usize,
        start in prop::collection::vec(any::<u8>(), 0..16),
        end in prop::collection::vec(any::<u8>(), 0..16),
        first in any::<u32>(), last in any::<u32>(),
    ) {
        let block = BlockProto { account_bearer: None, caveats: vec![], next_public_key: vec![1; len] };
        prop_assert_eq!(Block::try_from(block).is_ok(), len == 32);
        let signed = SignedBlockProto { payload: vec![1], signature: vec![1; len] };
        prop_assert_eq!(SignedBlock::try_from(signed).is_ok(), len == 64);
        let range = KeyRangeProto { start: start.clone(), end: end.clone() };
        prop_assert_eq!(KeyRange::try_from(range).is_ok(), end.is_empty() || start < end);
        let port = SocketPortRangeProto { first, last };
        prop_assert_eq!(SocketPortRange::try_from(port).is_ok(), first > 0 && first <= last && last <= 65535);
        let capability = CapabilityProto {
            blocks: vec![SignedBlockProto { payload: vec![1], signature: vec![1; 64] }],
            proof: Some(TipProofProto::Secret(vec![1; len])),
        };
        prop_assert_eq!(Capability::try_from(capability).is_ok(), len == 32);
    }
}
