//! Key preparation belongs to the runtime lifetime, not each signed message.
use super::*;

#[test]
fn issuance_receipts_and_metadata_share_runtime_prepared_keys() -> Result<()> {
    let (result, preparations) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            let issuer = AuthorityRoot::new(
                "prepared-runtime",
                [89; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "prepared-task".into(),
                },
            );
            let copy = issuer.copy(&mut None)?;
            let verifier = copy.verifier(&mut None)?;
            for byte in 0..8 {
                let scope = copy.bootstrap_scope(
                    "actor",
                    &Capabilities::new(["event:append"]),
                    None,
                    &mut None,
                )?;
                verifier.verify(&scope, &mut None)?;
                let payload = [byte; 32];
                let receipt = verifier.admission.record(
                    "prepared-runtime",
                    &issuer.audience,
                    &scope,
                    payload,
                    &mut None,
                )?;
                workload::verify_record(&verifier, &receipt, payload, None, &[], &mut None)?;
                let proof = verifier.canonical_proof(b"prepared-metadata", &payload, &mut None)?;
                verifier.verify_canonical_proof(
                    b"prepared-metadata",
                    &payload,
                    &proof,
                    &mut None,
                )?;
                assert!(
                    verifier
                        .verify_canonical_proof(b"foreign-metadata", &payload, &proof, &mut None,)
                        .is_err()
                );
            }
            Ok(())
        },
    );
    result?;
    // Four domain-separated runtime keys, regardless of messages, receipts,
    // verifier copies or metadata signatures. All signatures above are real.
    let preparations: Vec<KeyPreparation> = preparations;
    assert_eq!(preparations.len(), 4, "runtime keys were prepared again");
    Ok(())
}

#[test]
fn admitted_history_entry_points_authenticate_each_chain_once() -> Result<()> {
    #[derive(Clone, Copy, Debug)]
    enum Entry {
        Record,
        Event,
        Live,
        Effect,
    }
    let issuer = AuthorityRoot::new(
        "single-authentication",
        [93; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "single-authentication-task".into(),
        },
    );
    let copy = issuer.copy(&mut None)?;
    let verifier = copy.verifier(&mut None)?;
    let scope = copy.bootstrap_scope(
        "actor",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let mut event = Event {
        revision: 1,
        operation_id: OperationId::from_bytes([94; 16]),
        intent_digest: [95; 32],
        scope: RecordedScope::from_wire(
            scope.id.clone(),
            scope.capabilities.clone(),
            scope.issuer.clone(),
            scope.agent,
        ),
        attestation: vec![],
        causal_parent: None,
        payload: EventPayload::LifecycleTransitioned {
            from: LifecycleState::Pending,
            to: LifecycleState::Active,
            reason: None,
        },
    };
    event.attestation = verifier.attest_event(&scope, &event, &mut None)?;
    let digest = verifier.event_digest(&event, &mut None)?;
    let mut observation = EffectAttestation {
        provider: "fixture-effect".into(),
        effect_id: EffectId::from_bytes([96; 16]),
        attempt_id: EffectAttemptId::from_bytes([97; 16]),
        request_digest: [98; 32],
        guarantee: EffectGuarantee::AtMostOnce,
        result_schema_digest: [99; 32],
        status: EffectStatus::Indeterminate,
        proof: vec![],
    };
    let effect_scope = copy.bootstrap_scope(
        "effect-observation",
        &Capabilities::new([
            "effect:run".to_owned(),
            capability::effect_provider(&observation.provider),
        ]),
        None,
        &mut None,
    )?;
    let effect_digest = effect_attestation_digest(&issuer.audience, &observation, &mut None)?;
    observation.proof = verifier.admission.record(
        &issuer.id,
        &issuer.audience,
        &effect_scope,
        effect_digest,
        &mut None,
    )?;
    for entry in [Entry::Record, Entry::Event, Entry::Live, Entry::Effect] {
        let (verified, signatures) = acyclic_test_support::differential::observe(
            || (),
            |()| match entry {
                Entry::Record => workload::verify_record(
                    &verifier,
                    &event.attestation,
                    digest,
                    Some(&event.scope),
                    &["event:append"],
                    &mut None,
                )
                .map(|_| ()),
                Entry::Event => verifier.verify_event(&event, &mut None),
                Entry::Live => workload::live_record(&verifier, &event, &mut None).map(|_| ()),
                Entry::Effect => verifier.verify_effect(&observation, &mut None),
            },
        );
        verified?;
        let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
        // Workload: Birth, certified key, two blocks and seal. Receipt:
        // Birth, certified key, one block and seal. Scope inspection and
        // current-trust rechecks never repeat those nine real signatures.
        assert_eq!(
            signatures.len(),
            9,
            "duplicate authentication through {entry:?}"
        );
    }
    Ok(())
}
