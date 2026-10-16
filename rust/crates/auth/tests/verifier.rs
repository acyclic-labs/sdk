#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{authority::PolicyDecision, contract::*, verify::*};
use proptest::prelude::*;
use prost::Message;
use signed::*;

fn limits(n: Option<u64>) -> Limits {
    Limits { rps: n, burst: n, bytes_per_s: n, channels: n, concurrency: n, bytes: n, spend_micros: n, tokens: n }
}
fn caveat() -> impl Strategy<Value = Caveat> {
    prop_oneof![
        (0..1200u64).prop_map(CaveatKind::ExpiresAtUnixSeconds),
        (0..1200u64).prop_map(CaveatKind::NotBeforeUnixSeconds),
        prop::option::of(0..10u64).prop_map(|n| CaveatKind::Limits(limits(n))),
        "[a-z]{1,5}".prop_map(|name| CaveatKind::Audience(ResourcePattern { name: format!("workers:/{name}"), prefix: false })),
        any::<bool>().prop_map(|allow| CaveatKind::Grants(GrantSet { grants: if allow { vec![Grant { kind: GrantKind::Resource(ResourceGrant { resource: ResourcePattern { name: "fs:/public".into(), prefix: true }, actions: vec!["fs/read".into()] }) }] } else { vec![] } })),
        "[a-z]{1,5}".prop_map(|account| CaveatKind::Holder(Holder { account: Some(account) })),
        "[a-z]{1,5}".prop_map(|id| CaveatKind::LineageAppend(LineageAppend { ids: vec![id] })),
        "[a-z]{1,5}".prop_map(|value| CaveatKind::Pin(Pin { name: "author".into(), value: PinValue::Literal(value.into_bytes()) })),
        any::<bool>().prop_map(|empty| CaveatKind::Filter(Filter { keys: vec![], attributes: if empty { vec![] } else { vec![AttributeMatch { name: "author".into(), any_of: vec![b"issuer".to_vec()], subjects: vec![] }] } })),
        (2..8u32).prop_map(CaveatKind::MaxDepth),
        "[a-z]{0,5}".prop_map(CaveatKind::Label),
    ].prop_map(|kind| Caveat { kind })
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn signed_attenuation_never_widens_and_owner_deny_wins(
        before in prop::collection::vec(caveat(), 0..4),
        after in prop::collection::vec(caveat(), 0..4),
        name in "[a-z]{1,6}", used in 0..12u64,
    ) {
        let trust = Snapshot::default();
        let parent = verify(&root(|_| {}), &trust, NOW).unwrap().attenuate(before, &[5;32]).unwrap();
        let child = parent.attenuate(after, &[6;32]).unwrap();
        let attrs = RecordAttributes::try_new(vec![]).unwrap();
        let usage = limits(Some(used));
        let resource = format!("fs:/{name}");
        let request = request(&resource, &attrs, &usage);
        let parent_allows = parent.authorize(&trust, &request, PolicyDecision::Allow).is_ok();
        let child_allows = child.authorize(&trust, &request, PolicyDecision::Allow).is_ok();
        prop_assert!(!child_allows || parent_allows);
        prop_assert!(child.authorize(&trust, &request, PolicyDecision::Deny).is_err());
        let encoded = child.encode();
        match verify(&encoded, &trust, NOW) {
            Ok(decoded) => {
                let reencoded = decoded.encode();
                prop_assert_eq!(reencoded.as_slice(), encoded.as_slice());
                prop_assert_eq!(decoded.authorize(&trust, &request, PolicyDecision::Allow).is_ok(), child_allows);
            }
            Err(VerifyError::Time) => prop_assert!(!child_allows),
            Err(error) => prop_assert!(false, "unexpected error: {error}"),
        }
    }
    #[test]
    fn any_changed_signed_byte_fails(index in any::<usize>(), bit in 0..8u8) {
        let trust = Snapshot::default();
        let original = verify(&root(|_| {}), &trust, NOW).unwrap().attenuate(vec![], &[5;32]).unwrap().encode();
        let mut changed = original.to_vec();
        let index = index % changed.len(); changed[index] ^= 1 << bit;
        prop_assert!(verify(&changed, &trust, NOW).is_err());
    }
    #[test]
    fn signature_checks_detect_what_structural_admission_cannot(index in 0..64usize, bit in 0..8u8) {
        let trust = Snapshot::default();
        let bytes = root(|_| {});
        let mut wire = CapabilityProto::decode(bytes.as_slice()).unwrap();
        wire.blocks[0].signature[index] ^= 1 << bit;
        let changed = wire.encode_to_vec();
        // Negative control: replacing authenticated verification with structural
        // admission accepts every generated forgery. The signature property fails.
        prop_assert!(acyclic_auth::authority::decode_capability(&changed).is_ok());
        prop_assert!(verify(&changed, &trust, NOW).is_err());
    }
    #[test]
    fn revoking_block_k_invalidates_k_and_every_descendant(k in 0..4usize) {
        let mut trust = Snapshot::default();
        let mut chain = vec![verify(&root(|_| {}), &trust, NOW).unwrap()];
        for seed in 5..8u8 { chain.push(chain.last().unwrap().attenuate(vec![], &[seed;32]).unwrap()); }
        trust.revoked_blocks.push(chain[k].block_ids()[k]);
        for (index, cap) in chain.iter().enumerate() {
            prop_assert_eq!(cap.check(&trust, NOW).is_ok(), index < k);
            prop_assert_eq!(verify(&cap.encode(), &trust, NOW).is_ok(), index < k);
        }
    }
    #[test]
    fn sealed_round_trip_preserves_authority_and_removes_append_secret(labels in prop::collection::vec("[a-z]{0,8}",0..5)) {
        let trust = Snapshot::default();
        let caveats = labels.into_iter().map(|label| Caveat { kind:CaveatKind::Label(label) }).collect();
        let child = verify(&root(|_| {}), &trust, NOW).unwrap().attenuate(caveats, &[5;32]).unwrap().seal().unwrap();
        let bytes = child.encode();
        let wire = CapabilityProto::decode(bytes.as_slice()).unwrap();
        prop_assert!(matches!(wire.proof, Some(TipProofProto::Sealed(_))));
        let decoded = verify(&bytes, &trust, NOW).unwrap();
        prop_assert_eq!(decoded.block_ids(), child.block_ids());
        prop_assert!(matches!(decoded.attenuate(vec![], &[6;32]), Err(VerifyError::Sealed)));
        let attrs = RecordAttributes::try_new(vec![]).unwrap(); let usage = limits(None);
        prop_assert!(decoded.authorize(&trust, &request("fs:/public", &attrs, &usage), PolicyDecision::Allow).is_ok());
    }
    #[test]
    fn ancestor_or_trusted_root_key_cannot_be_reused(seed in 1..6u8) {
        let mut trust = Snapshot::default();
        trust.issuers.push(key(5).verifying_key().to_bytes());
        let cap = verify(&root(|_| {}), &trust, NOW).unwrap();
        prop_assert!(matches!(cap.attenuate(vec![], &[seed;32]), Err(VerifyError::RepeatedKey)));
    }
    #[test]
    fn cached_admission_allocates_nothing_per_caveat(repetitions in 0..32usize) {
        let trust = Snapshot::default();
        let clauses = vec![
            CaveatKind::Grants(GrantSet { grants: vec![Grant { kind: GrantKind::Resource(ResourceGrant { resource: ResourcePattern { name: "fs:/public".into(), prefix: true }, actions: vec!["fs/read".into()] }) }] }),
            CaveatKind::ExpiresAtUnixSeconds(900), CaveatKind::NotBeforeUnixSeconds(1),
            CaveatKind::Audience(ResourcePattern { name: "workers:/test".into(), prefix: false }),
            CaveatKind::Holder(Holder { account: Some("account".into()) }),
            CaveatKind::Filter(Filter { keys: vec![], attributes: vec![AttributeMatch { name: "author".into(), any_of: vec![b"issuer".to_vec()], subjects: vec![] }] }),
            CaveatKind::Pin(Pin { name: "author".into(), value: PinValue::Literal(b"issuer".to_vec()) }),
            CaveatKind::Limits(limits(None)), CaveatKind::MaxDepth(1),
            CaveatKind::Label("test".into()), CaveatKind::LineageAppend(LineageAppend { ids: vec!["child".into()] }),
        ];
        let caveats = (0..repetitions).flat_map(|_| clauses.iter().cloned().map(|kind| Caveat { kind })).collect();
        let capability = verify(&root(|_| {}), &trust, NOW).unwrap().attenuate(caveats, &[5;32]).unwrap();
        let attributes = RecordAttributes::try_new(vec![Attribute { name: "author".into(), value: b"issuer".to_vec() }]).unwrap();
        let usage = limits(None); let request = request("fs:/public", &attributes, &usage);
        let mut admitted = false;
        let measured = allocation_counter::measure(|| { admitted = capability.authorize(&trust, &request, PolicyDecision::Allow).is_ok(); });
        prop_assert!(admitted);
        prop_assert_eq!(measured.count_total, 0);
        // Negative control for the measurement itself: the former resource
        // decoder allocates a discarded component buffer during validation.
        let broken = allocation_counter::measure(|| { std::hint::black_box(acyclic_auth::decode_component("public").unwrap()); });
        prop_assert!(broken.count_total > 0);
    }
    #[test]
    fn effective_limits_are_dimensionwise_minima_and_lineage_is_append_only(
        parent_limit in prop::option::of(any::<u64>()),
        child_limit in prop::option::of(any::<u64>()),
        ids in prop::collection::vec("[a-z]{1,8}",1..6),
    ) {
        let trust = Snapshot::default();
        let parent = verify(&root(|_| {}), &trust, NOW).unwrap().attenuate(vec![Caveat { kind: CaveatKind::Limits(limits(parent_limit)) }], &[5;32]).unwrap();
        let child = parent.attenuate(vec![Caveat { kind: CaveatKind::Limits(limits(child_limit)) }, Caveat { kind: CaveatKind::LineageAppend(LineageAppend { ids: ids.clone() }) }], &[6;32]).unwrap();
        let minimum = match (parent_limit, child_limit) { (Some(a),Some(b)) => Some(a.min(b)), (a,b) => a.or(b) };
        let expected = limits(minimum);
        prop_assert_eq!(child.effective().limits(), &expected);
        prop_assert_eq!(&child.effective().principal().lineage, &ids);
        prop_assert_eq!(&child.effective().principal().groups, &parent.effective().principal().groups);
        prop_assert_eq!(&child.effective().principal().roles, &parent.effective().principal().roles);
        prop_assert_eq!(&child.effective().principal().attributes, &parent.effective().principal().attributes);
    }
}

#[test]
fn effective_filters_keep_the_declaring_subject() {
    let trust = Snapshot::default();
    let bytes = root(|block| block.caveats.push(Caveat { kind: CaveatKind::Filter(Filter { keys: vec![], attributes: vec![AttributeMatch { name: "author".into(), any_of: vec![], subjects: vec![SubjectReference { kind: SubjectReferenceKind::Id(SubjectId {}) }] }] }) }));
    let parent = verify(&bytes, &trust, NOW).unwrap();
    let child = parent.attenuate(vec![Caveat { kind: CaveatKind::LineageAppend(LineageAppend { ids: vec!["child".into()] }) }, Caveat { kind: CaveatKind::Filter(Filter { keys: vec![], attributes: vec![] }) }], &[5;32]).unwrap();
    let effective = child.effective();
    let filters = effective.filters().collect::<Vec<_>>();
    assert_eq!(filters[0].subject_id(), "issuer");
    assert_eq!(filters[0].lineage().collect::<Vec<_>>(), ["issuer"]);
    assert_eq!(filters[1].subject_id(), "child");
    assert_eq!(filters[1].lineage().collect::<Vec<_>>(), ["issuer", "child"]);
    let attributes = RecordAttributes::try_new(vec![Attribute { name: "author".into(), value: b"issuer".to_vec() }]).unwrap();
    let usage = limits(None);
    child.authorize(&trust, &request("fs:/public", &attributes, &usage), PolicyDecision::Allow).unwrap();
}

#[test]
fn cached_admission_observes_current_trust_epochs_and_parent_revocations() {
    let bytes = root(|block| block.certificates.as_mut().unwrap().keys[0].key.parent_revocation_ids.push(vec![9;32]));
    let mut trust = Snapshot::default();
    let cap = verify(&bytes, &trust, NOW).unwrap();
    trust.environments.push(key(8).verifying_key().to_bytes());
    cap.check(&trust, NOW).unwrap();
    trust.environments.remove(0);
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Trust));
    trust = Snapshot::default(); trust.issuers.clear();
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Trust));
    trust = Snapshot::default(); trust.minimum_epoch = 2;
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Revoked));
    trust = Snapshot::default(); trust.revoked_keys.push("key".into());
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Revoked));
    trust = Snapshot::default(); trust.revoked_blocks.push([9;32]);
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Revoked));
    trust = Snapshot::default(); trust.available = false;
    assert_eq!(cap.check(&trust, NOW), Err(VerifyError::Trust));
}

#[test]
fn every_signature_precedes_semantic_claim_admission() {
    // Fully signed, structurally decodable claims with a duplicate attribute.
    let bytes = root(|block| {
        let principal = &mut block.certificates.as_mut().unwrap().keys[0].key.principal;
        principal.attributes = vec![Attribute { name: "role".into(), value: b"one".to_vec() }, Attribute { name: "role".into(), value: b"two".to_vec() }];
    });
    let trust = Snapshot::default();
    assert!(matches!(verify(&bytes, &trust, NOW), Err(VerifyError::Malformed)));
    let mut wire = CapabilityProto::decode(bytes.as_slice()).unwrap();
    wire.blocks[0].signature[0] ^= 1;
    assert!(matches!(verify(&wire.encode_to_vec(), &trust, NOW), Err(VerifyError::Signature)));
}

#[test]
fn cached_clock_cannot_predate_birth_and_validity_is_half_open() {
    let bytes = root(|block| block.certificates.as_mut().unwrap().birth.birth.issued_at_unix_seconds = 50);
    let trust = Snapshot::default();
    let cap = verify(&bytes, &trust, NOW).unwrap();
    assert_eq!(cap.check(&trust, 49), Err(VerifyError::Time));
    cap.check(&trust, 50).unwrap();
    cap.check(&trust, 999).unwrap();
    assert_eq!(cap.check(&trust, 1000), Err(VerifyError::Time));
}

#[test]
fn signed_parser_seed_corpus() {
    let trust = Snapshot::default();
    let bytes = root(|_| {});
    let root = verify(&bytes, &trust, NOW).unwrap();
    let child = root.attenuate(vec![Caveat { kind: CaveatKind::ExpiresAtUnixSeconds(900) }], &[5;32]).unwrap();
    let cases = [("root", root.encode()), ("attenuated", child.encode()), ("sealed", child.seal().unwrap().encode())];
    for (name, bytes) in cases {
        let parsed = verify(&bytes, &trust, NOW).unwrap();
        assert_eq!(parsed.encode().as_slice(), bytes.as_slice());
        // Explicit corpus regeneration shares the same fully signed fixtures.
        if let Some(directory) = std::env::var_os("AUTH_CORPUS_OUTPUT") {
            let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").join(directory);
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(std::path::Path::new(&directory).join(name), bytes.as_slice()).unwrap();
        }
    }
}
