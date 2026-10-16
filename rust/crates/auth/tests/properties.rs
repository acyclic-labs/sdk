//! Shrinking properties exercise the actual contract append and decision algebra.
mod common;
use acyclic_auth::{authority::*, contract::*, *};
use common::*;
use proptest::prelude::*;
use prost::Message;

fn caveat() -> impl Strategy<Value = Caveat> {
    prop_oneof![
        ("[a-z]{1,5}", any::<bool>()).prop_map(|(n, p)| CaveatKind::Grants(grants(
            &format!("fs:/{n}"),
            p,
            "fs/read"
        ))),
        (0..1200u64).prop_map(CaveatKind::ExpiresAtUnixSeconds),
        (0..1200u64).prop_map(CaveatKind::NotBeforeUnixSeconds),
        "[a-z]{1,5}".prop_map(|n| CaveatKind::Audience(ResourcePattern {
            name: format!("workers:/{n}"),
            prefix: false
        })),
        prop::option::of("[a-z]{1,5}").prop_map(|account| CaveatKind::Holder(Holder { account })),
        prop::collection::vec("[a-z]{1,5}", 1..4)
            .prop_map(|ids| CaveatKind::LineageAppend(LineageAppend { ids })),
        (any::<u8>(), any::<bool>()).prop_map(|(b, lineage)| CaveatKind::Filter(Filter {
            keys: vec![KeyRange {
                start: vec![b],
                end: vec![]
            }],
            attributes: vec![AttributeMatch {
                name: "author".into(),
                any_of: vec![vec![b]],
                subjects: vec![SubjectReference {
                    kind: if lineage {
                        SubjectReferenceKind::Lineage(SubjectLineage {})
                    } else {
                        SubjectReferenceKind::Id(SubjectId {})
                    }
                }]
            }],
        })),
        prop_oneof![
            Just(PinValue::SubjectId(SubjectId {})),
            "[a-z]{1,5}".prop_map(|n| PinValue::Literal(n.into_bytes()))
        ]
        .prop_map(|value| CaveatKind::Pin(Pin {
            name: "author".into(),
            value
        })),
        prop::option::of(0..10u64).prop_map(|n| CaveatKind::Limits(limits(n))),
        (0..8u32).prop_map(CaveatKind::MaxDepth),
        "[a-z]{0,5}".prop_map(CaveatKind::Label),
    ]
    .prop_map(|kind| Caveat { kind })
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Adding ANY generated conjunct or acting lineage cannot make a denied
    // request pass; owner denial is absorbing for every prefix and descendant.
    #[test]
    fn attenuation_never_widens_and_deny_wins(
        before in prop::collection::vec(caveat(),0..8),
        after in prop::collection::vec(caveat(),0..8),
        name in "[a-z]{1,5}", author in "[a-z]{1,5}", now in 0..1200u64,
        used in 0..12u64, key in any::<u8>(),
    ) {
        let mut root = root(); root.caveats = before;
        let parent = vec![root];
        let usage = limits(Some(used));
        let attrs = vec![Attribute { name:"author".into(),value:author.into_bytes() }];
        let keys=vec![vec![key]];
        let resource=format!("fs:/{name}"); let audience=format!("workers:/{name}");
        let r=Request { target:Target::Resource {action:"fs/read",resource:&resource},audience:&audience,holder:Some(&name),now_unix_seconds:now,keys:&keys,attributes:&RecordAttributes::try_new(attrs).unwrap(),usage:&usage,cpu_millis:used,memory_bytes:used };
        prop_assert!(!permits(&parent,&r,PolicyDecision::Deny).is_ok());
        if let Ok(child)=attenuate(&parent,after,vec![6;32]) {
            prop_assert!(!permits(&child,&r,PolicyDecision::Allow).is_ok() || permits(&parent,&r,PolicyDecision::Allow).is_ok());
            prop_assert!(!permits(&child,&r,PolicyDecision::Deny).is_ok());
            prop_assert_eq!(&child[..parent.len()],parent.as_slice());
        }
    }
    #[test]
    fn all_caveats_round_trip(caveats in prop::collection::vec(caveat(),0..16)) {
        for caveat in &caveats {
            let bytes = CaveatProto::from(caveat.clone()).encode_to_vec();
            prop_assert_eq!(decode_caveat(&bytes), Ok(caveat.clone()));
            let duplicate = [bytes.as_slice(), bytes.as_slice()].concat();
            prop_assert!(decode_caveat(&duplicate).is_err());
            // Removing canonical admission accepts the same duplicate oneof.
            prop_assert!(CaveatProto::decode(duplicate.as_slice()).is_ok());
        }
        let mut block=root();block.caveats=caveats;
        let wire:BlockProto=block.clone().into();let bytes=wire.encode_to_vec();
        prop_assert_eq!(decode_block(&bytes),Ok(block));
        for proof in [TipProof::Secret(vec![2;32]),TipProof::Sealed(vec![2;64])] {
            let cap=Capability {version:VERSION,blocks:vec![SignedBlock {payload:bytes.clone(),signature:vec![3;64]}],proof};
            let wire:CapabilityProto=cap.clone().into();
            prop_assert_eq!(decode_capability(&wire.encode_to_vec()),Ok(cap));
        }
    }
    #[test]
    fn arbitrary_wire_round_trip(bytes in prop::collection::vec(any::<u8>(),0..2048)) {
        if let Ok(cap)=decode_capability(&bytes) {
            let wire:CapabilityProto=cap.into();prop_assert_eq!(wire.encode_to_vec(),bytes.clone());
        }
        if let Ok(block)=decode_block(&bytes) {
            let wire:BlockProto=block.into();prop_assert_eq!(wire.encode_to_vec(),bytes);
        }
    }
    // Encoding is a left inverse on arbitrary bytes and injective on pairs,
    // including invalid UTF-8, dots, slashes, query punctuation and percent.
    #[test]
    fn opaque_keys_round_trip_without_collisions(a in prop::collection::vec(any::<u8>(),0..64), b in prop::collection::vec(any::<u8>(),0..64), bucket in prop::collection::vec(any::<u8>(),1..16), other_bucket in prop::collection::vec(any::<u8>(),1..16)) {
        prop_assert_eq!(decode_component(&encode_component(&a)),Ok(a.clone()));
        prop_assert_eq!(objects_resource(&bucket,&a)==objects_resource(&other_bucket,&b),bucket==other_bucket && a==b);
        prop_assert!(valid_resource(&objects_resource(&bucket,&a)));
    }
    #[test]
    fn component_prefix_is_preserved(a in prop::collection::vec(any::<u8>(),0..32), b in prop::collection::vec(any::<u8>(),0..32)) {
        let mut whole=a.clone();whole.extend(b);
        prop_assert!(encode_component(&whole).starts_with(&encode_component(&a)));
    }
    #[test]
    fn max_depth_is_absolute_and_sealing_costs_no_block(max in 0..6u32, ceilings in prop::collection::vec(0..8u32,0..8)) {
        let len=ceilings.len();let effective=ceilings.iter().copied().fold(max,u32::min);
        let mut blocks=vec![root()]; blocks[0].caveats.push(Caveat {kind:CaveatKind::MaxDepth(max)});
        for max in ceilings { blocks.push(Block {certificates:None,caveats:vec![Caveat {kind:CaveatKind::MaxDepth(max)}],next_public_key:vec![6;32]}); }
        prop_assert_eq!(valid_chain(&blocks),len<=effective as usize);
        let signed=blocks.into_iter().map(|b| SignedBlock {payload:BlockProto::from(b).encode_to_vec(),signature:vec![1;64]}).collect::<Vec<_>>();
        for proof in [TipProof::Secret(vec![2;32]),TipProof::Sealed(vec![2;64])] {
            let cap=Capability {version:VERSION,blocks:signed.clone(),proof};
            prop_assert_eq!(decode_capability(&CapabilityProto::from(cap).encode_to_vec()).is_ok(),len<=effective as usize);
        }
    }
    #[test]
    fn validity_has_explicit_units_and_thirty_day_bound(start in 0..253_402_300_799u64, duration in 0..4_000_000u64) {
        let expiry=start+duration;
        prop_assert_eq!(Validity::try_from(ValidityProto {not_before_unix_seconds:start,expires_at_unix_seconds:expiry}).is_ok(), duration>0 && duration<=30*86400 && expiry<=253_402_300_799);
    }
    // No protocol step accepts a bearer or signing secret as root input. The
    // complete certificate projection is preserved on every offline append;
    // legacy block-zero JWT bytes are rejected at canonical ingress.
    #[test]
    fn attenuation_exposes_only_public_certificates(suffix in "[A-Za-z0-9_-]{1,64}", caveats in prop::collection::vec(caveat(),0..8)) {
        let parent=vec![root()];
        if let Ok(child)=attenuate(&parent,caveats,vec![6;32]) {
            prop_assert_eq!(&child[0].certificates,&parent[0].certificates);
            prop_assert!(child[1..].iter().all(|b| b.certificates.is_none()));
            let root_bytes=BlockProto::from(child[0].clone()).encode_to_vec();
            prop_assert!(!root_bytes.windows(b"acyclic-account+jwt".len()).any(|w| w==b"acyclic-account+jwt"));
        }
        let bearer=format!("eyJ0eXAiOiJhY3ljbGljLWFjY291bnQrand0In0.{suffix}.{suffix}");
        let mut legacy=vec![10]; prost::encoding::encode_varint(bearer.len() as u64,&mut legacy); legacy.extend(bearer.bytes()); legacy.extend([26,32]); legacy.extend([1;32]);
        prop_assert!(decode_block(&legacy).is_err());
    }
    // Positive witnesses prevent the general monotonicity property from being
    // satisfied only by already-denied parents. Old references stay frozen.
    #[test]
    fn lineage_cannot_expand_an_ancestor_filter(child_id in "[a-z]{1,12}") {
        let mut root=root();
        root.caveats.push(Caveat { kind:CaveatKind::Filter(Filter { keys:vec![], attributes:vec![AttributeMatch { name:"author".into(),any_of:vec![],subjects:vec![SubjectReference {kind:SubjectReferenceKind::Lineage(SubjectLineage {})}] }] }) });
        let parent=vec![root];let child=attenuate(&parent,vec![Caveat {kind:CaveatKind::LineageAppend(LineageAppend {ids:vec![child_id.clone()]})}],vec![6;32]).unwrap();
        let usage=limits(None);
        for author in ["issuer",child_id.as_str()] {
            let attrs=vec![Attribute {name:"author".into(),value:author.as_bytes().to_vec()}];
            let r=Request { target:Target::Resource {action:"fs/read",resource:"fs:/v"},audience:"workers:/app",holder:None,now_unix_seconds:2,keys:&[],attributes:&RecordAttributes::try_new(attrs).unwrap(),usage:&usage,cpu_millis:0,memory_bytes:0 };
            prop_assert_eq!(permits(&parent,&r,PolicyDecision::Allow).is_ok(),author=="issuer");
            prop_assert_eq!(permits(&child,&r,PolicyDecision::Allow).is_ok(),author=="issuer");
        }
    }
    #[test]
    fn pins_bind_acting_identity_at_declaration(child_id in "[a-z]{1,12}", used in 0..10u64, ceiling in 0..10u64) {
        let parent=vec![root()];
        let child=attenuate(&parent,vec![
            Caveat {kind:CaveatKind::LineageAppend(LineageAppend {ids:vec![child_id.clone()]})},
            Caveat {kind:CaveatKind::Pin(Pin {name:"author".into(),value:PinValue::SubjectId(SubjectId {})})},
            Caveat {kind:CaveatKind::Limits(limits(Some(ceiling)))}],vec![6;32]).unwrap();
        let attrs=vec![Attribute {name:"author".into(),value:child_id.into_bytes()}];let usage=limits(Some(used));
        let r=Request {target:Target::Resource {action:"fs/read",resource:"fs:/v"},audience:"workers:/app",holder:None,now_unix_seconds:2,keys:&[],attributes:&RecordAttributes::try_new(attrs).unwrap(),usage:&usage,cpu_millis:0,memory_bytes:0};
        prop_assert!(permits(&parent,&r,PolicyDecision::Allow).is_ok());
        prop_assert_eq!(permits(&child,&r,PolicyDecision::Allow).is_ok(),used<=ceiling);
        prop_assert!(!permits(&child,&r,PolicyDecision::Deny).is_ok());
    }
    #[test]
    fn root_certificates_and_versions_cannot_be_reintroduced(version in any::<u32>()) {
        let payload=BlockProto::from(root()).encode_to_vec();
        let signed=SignedBlock {payload:payload.clone(),signature:vec![1;64]};
        let cap=Capability {version,blocks:vec![signed.clone()],proof:TipProof::Secret(vec![2;32])};
        let mut bytes=CapabilityProto::from(cap).encode_to_vec();
        let parsed=decode_capability(&bytes);
        if version==VERSION {prop_assert!(parsed.is_ok());} else {prop_assert_eq!(parsed,Err(Error::UnsupportedVersion(version)));
            bytes.extend([0x50,1]);prop_assert_eq!(decode_capability(&bytes),Err(Error::UnsupportedVersion(version)));}
        let cap=Capability {version:VERSION,blocks:vec![signed.clone(),signed],proof:TipProof::Secret(vec![2;32])};
        prop_assert!(decode_capability(&CapabilityProto::from(cap).encode_to_vec()).is_err());
    }

    #[test]
    fn certificate_ancestry_is_complete(parent_id in "[a-z]{1,12}", wrong_id in "[a-z]{1,12}", revoked in prop::collection::vec(any::<u8>(),32), expiry in 2..1001u64) {
        let mut chain=root().certificates.unwrap();
        chain.keys[0].key.key_id=parent_id.clone();
        chain.keys[0].key.parent_revocation_ids=vec![revoked];
        let mut child=chain.keys[0].clone();child.key.key_id="child-key".into();
        child.key.parent_key_id=Some(parent_id.clone());child.key.validity.expires_at_unix_seconds=expiry;
        chain.keys.push(child);
        let wire:CertificateChainProto=chain.clone().into();
        prop_assert_eq!(CertificateChain::try_from(wire),Ok(chain.clone()));
        chain.keys[1].key.parent_key_id=Some(wrong_id.clone());
        prop_assert_eq!(CertificateChain::try_from(CertificateChainProto::from(chain.clone())).is_ok(),wrong_id==parent_id);
        chain.keys[1].key.parent_key_id=Some(parent_id);
        chain.keys[1].key.parent_revocation_ids.clear();
        prop_assert!(CertificateChain::try_from(CertificateChainProto::from(chain)).is_err());
    }
    #[test]
    fn typed_grants_round_trip(cpu in prop::option::of(any::<u64>()), memory in prop::option::of(any::<u64>()), spend in prop::option::of(any::<u64>())) {
        let resource=ResourceGrant {resource:ResourcePattern {name:"execution:/".into(),prefix:true},actions:vec!["execution/invoke".into()]};
        let set=GrantSet {grants:vec![
            Grant {kind:GrantKind::Resource(resource.clone())},
            Grant {kind:GrantKind::Execution(ExecutionGrant {grant:resource.clone(),cpu_millis:cpu,memory_bytes:memory})},
            Grant {kind:GrantKind::Billing(BillingGrant {grant:ResourceGrant {resource:ResourcePattern {name:"billing:/".into(),prefix:true},actions:vec!["billing/read".into()]},spend_micros:spend})},
            Grant {kind:GrantKind::WebsocketConnect(WebsocketConnectGrant {rules:vec![SocketHostRule {host:"example.com".into(),ports:vec![SocketPortRange {first:443,last:443}]}]})} ]};
        let bytes=GrantSetProto::from(set.clone()).encode_to_vec();
        prop_assert_eq!(GrantSet::try_from(GrantSetProto::decode(bytes.as_slice()).unwrap()),Ok(set));
    }

}
