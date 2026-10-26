use super::*;
fn fixture() -> AuthorityRoot {
    AuthorityRoot::new(
        "owner",
        [29; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "root".into(),
        },
    )
}
#[test]
fn workload_scopes_verify_ed25519_ancestry_and_reject_forgery_expiry_and_wrong_audience()
-> Result<()> {
    let issuer = fixture();
    let root = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["model:generate", "tool:call:read"]),
        None,
        &mut None,
    )?;
    let child = issuer.attenuate(&root, "child", &Capabilities::new(["tool:call:read"]))?;
    let verifier = issuer.verifier(&mut None)?;
    let verified = verify(&verifier, &issuer.audience, &child, now(), &mut None)?;
    assert_eq!(verified.block_ids().len(), 2);
    let mut forged = child.clone();
    forged.capabilities = root.capabilities.clone();
    assert!(verifier.verify(&forged, &mut None).is_err());
    let mut damaged = child.clone();
    damaged.workload[20] ^= 1;
    assert!(verifier.verify(&damaged, &mut None).is_err());
    assert!(
        verify(
            &verifier,
            &issuer.audience,
            &root,
            verified.effective().expires_at_unix_seconds(),
            &mut None
        )
        .is_err()
    );
    let other = Authority {
        kind: AggregateKind::Task,
        id: "neighbor".into(),
    };
    assert!(verify(&verifier, &other, &child, now(), &mut None).is_err());
    // A holder can sign an append, but cannot add authority to the authenticated claims.
    let mut widened = child.clone();
    widened.capabilities = root.capabilities.clone();
    widened.parent_proof = Some(*child.proof);
    let forged_chain = verified
        .attenuate(vec![claims(&widened, 2)?], &entropy())
        .map_err(unauthorized)?;
    retain(&mut widened, &forged_chain, &mut None)?;
    assert!(verifier.verify(&widened, &mut None).is_err());
    // Empty and multi-claim appends must not impersonate successive workload
    // delegations even when the total pin and block counts happen to agree.
    let skipped = verified
        .attenuate(vec![], &entropy())
        .map_err(unauthorized)?;
    let mut misplaced = child.clone();
    misplaced.parent_proof = Some(*skipped.block_ids().last().unwrap());
    let mut earlier = child.clone();
    earlier.parent_proof = Some(*child.proof);
    let chain = skipped
        .attenuate(
            vec![claims(&earlier, 2)?, claims(&misplaced, 3)?],
            &entropy(),
        )
        .map_err(unauthorized)?;
    retain(&mut misplaced, &chain, &mut None)?;
    assert!(verifier.verify(&misplaced, &mut None).is_err());
    Ok(())
}

#[test]
fn workload_holders_attenuate_with_public_trust_and_revocation_invalidates_descendants()
-> Result<()> {
    let issuer = fixture();
    let audience = issuer.audience.clone();
    let mut trust = issuer.public_workload_trust()?;
    let parent = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new(["tool:call:read", "tool:call:write"]),
        None,
        &mut None,
    )?;
    drop(issuer);
    let child = parent.attenuate_workload(
        "reader",
        &Capabilities::new(["tool:call:read"]),
        &trust,
        &audience,
        now(),
    )?;
    child.verify_workload(&trust, &audience, now(), &mut None)?;
    assert!(
        child
            .attenuate_workload(
                "writer",
                &Capabilities::new(["tool:call:write"]),
                &trust,
                &audience,
                now()
            )
            .is_err()
    );
    trust.revoked_block_ids.insert(*parent.proof);
    assert!(
        child
            .verify_workload(&trust, &audience, now(), &mut None)
            .is_err()
    );
    assert!(
        parent
            .attenuate_workload(
                "reader-again",
                &Capabilities::new(["tool:call:read"]),
                &trust,
                &audience,
                now()
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn an_offline_deadline_cannot_be_widened_by_a_later_child() -> Result<()> {
    let runtime = fixture();
    let now = now();
    let root = runtime.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let bounded = root.with_workload_expiry(
        now + 2,
        &runtime.public_workload_trust()?,
        runtime.audience(),
        now,
    )?;
    let widened = bounded.with_workload_expiry(
        now + 100,
        &runtime.public_workload_trust()?,
        runtime.audience(),
        now,
    )?;
    widened.verify_workload(
        &runtime.public_workload_trust()?,
        runtime.audience(),
        now + 1,
        &mut None,
    )?;
    assert!(
        widened
            .verify_workload(
                &runtime.public_workload_trust()?,
                runtime.audience(),
                now + 2,
                &mut None
            )
            .is_err()
    );
    Ok(())
}

fn expired_historical_event(issuer: &AuthorityRoot) -> Result<Event> {
    let admitted_at = 100;
    let name = resource(&issuer.audience)?;
    let trust = issuer.public_workload_trust()?;
    let signer = signer(
        &issuer.id,
        &issuer.runtime.workload,
        admitted_at,
        &name,
        None,
    )?;
    let mut scope = Scope {
        id: "expired-history".into(),
        capabilities: Capabilities::new(["event:append"]),
        agent: None,
        issuer: issuer.id.clone(),
        parent_proof: None,
        proof: Box::new([0; 32]),
        workload: vec![],
    };
    let capability = signer
        .mint(
            vec![
                claims(&scope, 0)?,
                Caveat {
                    kind: CaveatKind::Grants(grant(&name)),
                },
                Caveat {
                    kind: CaveatKind::Audience(ResourcePattern {
                        name: name.clone(),
                        prefix: false,
                    }),
                },
            ],
            SigningKey::from_bytes(&[101; 32]),
            &trust,
            admitted_at,
        )
        .map_err(unauthorized)?;
    retain(&mut scope, &capability, &mut None)?;
    let attributes = RecordAttributes::try_new(vec![Attribute {
        name: format!("{CLAIM}0"),
        value: serde_json::to_vec(&Claims::from_scope(&scope)).map_err(unauthorized)?,
    }])
    .map_err(unauthorized)?;
    let mut event = Event {
        revision: 1,
        operation_id: OperationId::from_bytes([102; 16]),
        intent_digest: [103; 32],
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
    let verifier = issuer.verifier(&mut None)?;
    let payload = verifier.event_digest(&event, &mut None)?;
    let admitted = acyclic_auth::admission::RuntimeAdmission::new(&signer, &trust, admitted_at)
        .record(
            &capability,
            &Request {
                target: Target::Resource {
                    action: ACTION,
                    resource: &name,
                },
                audience: &name,
                holder: Some(&issuer.id),
                now_unix_seconds: admitted_at,
                keys: &[],
                attributes: &attributes,
                usage: &usage(),
                cpu_millis: 0,
                memory_bytes: 0,
            },
            payload,
            &[104; 32],
        )
        .map_err(unauthorized)?;
    event.attestation = AdmittedCapabilityProto::from(admitted).encode_to_vec();
    Ok(event)
}

#[test]
fn expired_history_remains_verifiable_but_cannot_become_live_authority() -> Result<()> {
    let issuer = fixture();
    let event = expired_historical_event(&issuer)?;
    let verifier = issuer.verifier(&mut None)?;
    assert!(now() >= 100 + CERTIFICATE_SECONDS);
    verifier.verify_event(&event, &mut None)?;
    assert!(
        live_record(&verifier, &event, &mut None).is_err(),
        "historical receipt resurrected expired live workload authority",
    );
    Ok(())
}
