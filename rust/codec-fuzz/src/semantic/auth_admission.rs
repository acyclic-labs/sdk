//! Historical authentication never substitutes for exact request authorization.
use acyclic_auth::{
    admission::{HistoryVerifier, RuntimeAdmission},
    authority::{self, Target},
    contract::*,
    testing::{self, NOW, Snapshot},
    verify::{self, CertifiedSigner, VerifyError},
};
use prost::Message;

const MESSAGE: &str = "acyclic.auth.v1.AdmittedCapability";
const RESOURCE: &str = "fs:/codec/file";
const PAYLOAD: [u8; 32] = [7; 32];

pub(super) fn check(name: &str, bytes: &[u8]) {
    if name != MESSAGE {
        return;
    }
    let Ok(wire) = AdmittedCapabilityProto::decode(bytes) else {
        return;
    };
    let Ok(admitted) = AdmittedCapability::try_from(wire) else {
        return;
    };
    let trust = Snapshot::default();
    let mut verifier = HistoryVerifier::default();
    // This caller clock reaches either historical parser for arbitrary input;
    // it does not attest that an untrusted supplied timestamp is authentic.
    let now = NOW.max(admitted.admitted_at_unix_seconds);
    let Ok(original) = verifier.authenticate(&admitted, &trust, now) else {
        return;
    };
    let canonical = AdmittedCapabilityProto::from(admitted.clone()).encode_to_vec();
    let normalized = AdmittedCapability::try_from(
        AdmittedCapabilityProto::decode(canonical.as_slice()).unwrap(),
    )
    .unwrap();
    let repeated = verifier
        .authenticate(&normalized, &trust, now)
        .expect("warmed authentication preserves the original evidence");
    assert_eq!(
        repeated.workload().block_ids(),
        original.workload().block_ids()
    );
    assert!(original.workload().is_sealed());
    for receipt in [false, true] {
        let mut forged = admitted.clone();
        let evidence = if receipt {
            &mut forged.receipt
        } else {
            &mut forged.workload
        };
        evidence[0] ^= 1;
        assert!(verifier.authenticate(&forged, &trust, now).is_err());
    }
    if let Some(before) = admitted.admitted_at_unix_seconds.checked_sub(1) {
        assert!(matches!(
            verifier.authenticate(&admitted, &trust, before),
            Err(VerifyError::Time)
        ));
    }
    let revoked = Snapshot {
        revoked_blocks: vec![original.workload().block_ids()[0]],
        ..Default::default()
    };
    assert!(matches!(
        verifier.authenticate(&admitted, &revoked, now),
        Err(VerifyError::Revoked)
    ));

    let attributes = RecordAttributes::try_new(Vec::new()).unwrap();
    let usage = testing::limits(None);
    let request = authority::Request {
        now_unix_seconds: now,
        ..testing::request(RESOURCE, &attributes, &usage)
    };
    let allowed = original.check_request(&trust, &request, PAYLOAD).is_ok();
    assert_eq!(
        admitted.verify_history(&trust, &request, PAYLOAD).is_ok(),
        allowed,
    );
    assert!(original.check_request(&revoked, &request, PAYLOAD).is_err());
}

pub(super) fn seeds()
-> Result<Vec<(&'static str, &'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let encoded = testing::root(|block| {
        block.certificates.as_mut().unwrap().keys[0]
            .key
            .grants
            .grants
            .push(Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "harness:/admissions".into(),
                        prefix: true,
                    },
                    actions: vec!["harness/admit".into()],
                }),
            });
    });
    let root = authority::decode_capability(&encoded)?;
    let certificates = authority::decode_block(&root.blocks[0].payload)?
        .certificates
        .ok_or(VerifyError::Malformed)?;
    let signer = CertifiedSigner::new(certificates, testing::key(3))?;
    let trust = Snapshot::default();
    let workload = verify::verify(&encoded, &trust, NOW)?;
    let attributes = RecordAttributes::try_new(Vec::new())?;
    let usage = testing::limits(None);
    let request = testing::request(RESOURCE, &attributes, &usage);
    let runtime = RuntimeAdmission::new(&signer, &trust, NOW);
    let mut outputs = Vec::new();
    for (name, payload, entropy) in [
        ("historical-admission", PAYLOAD, [8; 32]),
        ("historical-foreign-payload", [8; 32], [9; 32]),
    ] {
        let admitted = runtime.record(&workload, &request, payload, &entropy)?;
        admitted.verify_history(&trust, &request, payload)?;
        let original = admitted.authenticate_history(&trust, NOW)?;
        let mut foreign_payload = payload;
        foreign_payload[0] ^= 1;
        assert!(
            original
                .check_request(&trust, &request, foreign_payload)
                .is_err()
        );
        for (action, resource, audience) in [
            ("fs/write", RESOURCE, request.audience),
            ("fs/read", "fs:/codec/foreign", request.audience),
            ("fs/read", RESOURCE, "workers:/foreign"),
        ] {
            let foreign = authority::Request {
                target: Target::Resource { action, resource },
                audience,
                ..request
            };
            assert!(original.check_request(&trust, &foreign, payload).is_err());
        }
        original.check_request(&trust, &request, payload)?;
        assert_eq!(
            admitted.verify_history(&trust, &request, PAYLOAD).is_ok(),
            payload == PAYLOAD,
        );
        let bytes = AdmittedCapabilityProto::from(admitted).encode_to_vec();
        check(MESSAGE, &bytes);
        outputs.push((MESSAGE, name, bytes));
    }
    Ok(outputs)
}
