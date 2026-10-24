use super::*;
use std::sync::Arc;

#[test]
fn runtime_receipts_share_one_restricted_certificate_until_its_expiry() -> Result<()> {
    let keys = RuntimeKeys::new(&[93; 32]);
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            let first = keys.receipt_signer("owner", 100)?;
            for timestamp in 100..108 {
                assert!(Arc::ptr_eq(
                    &first,
                    &keys.receipt_signer("owner", timestamp)?
                ));
            }
            Ok(())
        },
    );
    result?;
    let signatures: Vec<CertificateSignature> = signatures;
    assert_eq!(signatures.len(), 2, "birth and leaf must be signed once");
    let (result, former) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            for timestamp in 100..108 {
                signer("owner", &keys, timestamp, None, None)?;
            }
            Ok(())
        },
    );
    result?;
    let former: Vec<CertificateSignature> = former;
    assert_eq!(
        former.len(),
        16,
        "execute the former per-record certification"
    );

    let first = keys.receipt_signer("owner", 100)?;
    assert!(Arc::ptr_eq(
        &first,
        &keys.receipt_signer("owner", 100 + CERTIFICATE_SECONDS - 1)?
    ));
    let trust = trust("owner", &keys);
    // A contradictory caveat can be minted but cannot widen certificate grants.
    // Exercise the real authorization boundary, including the former wide signer.
    let name = "harness:/tasks/forged";
    let request = Request {
        target: Target::Resource {
            action: ACTION,
            resource: name,
        },
        audience: name,
        holder: Some("owner"),
        now_unix_seconds: 101,
        keys: &[],
        attributes: &RecordAttributes::default(),
        usage: &usage(),
        cpu_millis: 0,
        memory_bytes: 0,
    };
    let execution = |owner: &CertifiedSigner| {
        owner
            .mint(
                vec![Caveat {
                    kind: CaveatKind::Grants(grant(name)),
                }],
                SigningKey::from_bytes(&[94; 32]),
                &trust,
                101,
            )?
            .authorize(&trust, &request, PolicyDecision::Allow)
    };
    assert!(execution(&first).is_err());
    assert!(execution(&signer("owner", &keys, 100, Some(name), None)?).is_ok());
    let old = Arc::downgrade(&first);
    let next = keys.receipt_signer("owner", 100 + CERTIFICATE_SECONDS)?;
    assert!(!Arc::ptr_eq(&first, &next));
    assert!(
        first
            .mint(
                Vec::new(),
                SigningKey::from_bytes(&[94; 32]),
                &trust,
                100 + CERTIFICATE_SECONDS
            )
            .is_err()
    );
    drop(first);
    assert!(
        old.upgrade().is_none(),
        "replacement retained the old signing owner"
    );
    // A backwards clock and changed account cannot reuse a differently bound owner.
    let earlier = keys.receipt_signer("owner", 99)?;
    assert!(!Arc::ptr_eq(&next, &earlier));
    let foreign = keys.receipt_signer("foreign", 99)?;
    assert!(!Arc::ptr_eq(&earlier, &foreign));
    assert!(
        foreign
            .mint(Vec::new(), SigningKey::from_bytes(&[94; 32]), &trust, 100)
            .is_err()
    );
    Ok(())
}

#[test]
fn retained_receipt_signer_preserves_exact_admission_bindings() -> Result<()> {
    let issuer = AuthorityIssuer::new(
        "owner",
        [95; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "receipt-root".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.issue(
        "actor",
        &Capabilities::new(["event:append"]),
        None,
        None,
        &mut None,
    )?;
    let mut retained = Vec::new();
    for byte in 0..8 {
        let payload = [byte; 32];
        let receipt =
            verifier
                .admission
                .record("owner", &issuer.audience, &scope, payload, &mut None)?;
        verify_record(
            &verifier,
            &receipt,
            payload,
            None,
            &["event:append"],
            &mut None,
        )?;
        assert!(verify_record(&verifier, &receipt, [byte + 1; 32], None, &[], &mut None).is_err());
        assert!(
            verify_record(
                &verifier,
                &receipt,
                payload,
                None,
                &["effect:plan"],
                &mut None
            )
            .is_err()
        );
        retained.push((receipt, payload));
    }
    let mut foreign = verifier.copy(&mut None)?;
    foreign.audience.id.push_str("-foreign");
    for (receipt, payload) in &retained {
        assert!(verify_record(&foreign, receipt, *payload, None, &[], &mut None).is_err());
    }
    issuer.revoke_workload(&scope)?;
    for (receipt, payload) in &retained {
        assert!(verify_record(&verifier, receipt, *payload, None, &[], &mut None).is_err());
    }
    assert!(
        verifier
            .admission
            .record("owner", &issuer.audience, &scope, [9; 32], &mut None)
            .is_err()
    );
    Ok(())
}
