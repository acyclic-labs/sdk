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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn live_and_historical_authentication_compare_funded_exact_wire_without_rehashing() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let encoded =
        verifier
            .admission
            .record("owner", issuer.audience(), &scope, [7; 32], &mut None)?;
    verifier.verify(&scope, &mut None)?;
    verify_record(&verifier, &encoded, [7; 32], None, &[], &mut None)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    for _ in 0..3 {
        let before = work.receipt();
        verifier.verify(&scope, &mut Some(&mut io.history_io()))?;
        let live = work.receipt();
        assert_eq!(
            live.bytes_hashed, before.bytes_hashed,
            "retained live authentication rehashed its wire"
        );
        assert!(live.items_examined >= before.items_examined + scope.workload.len() as u64);
        verify_record(
            &verifier,
            &encoded,
            [7; 32],
            None,
            &[],
            &mut Some(&mut io.history_io()),
        )?;
        let historical = work.receipt();
        assert_eq!(
            historical.bytes_hashed, live.bytes_hashed,
            "retained historical authentication rehashed its wire"
        );
        assert!(historical.items_examined >= live.items_examined + encoded.len() as u64);
    }
    // Equal-length corruption must miss the shared exact-wire owner. These
    // same-claims controls fail if cache identity uses length or a pointer.
    let mut forged = scope.clone();
    forged.workload[20] ^= 1;
    assert!(
        verifier
            .verify(&forged, &mut Some(&mut io.history_io()))
            .is_err()
    );
    let mut forged_record = encoded.clone();
    forged_record[20] ^= 1;
    assert!(
        verify_record(
            &verifier,
            &forged_record,
            [7; 32],
            None,
            &[],
            &mut Some(&mut io.history_io())
        )
        .is_err()
    );
    issuer.revoke_workload(&scope)?;
    assert!(
        verifier
            .verify(&scope, &mut Some(&mut io.history_io()))
            .is_err()
    );
    assert!(
        verify_record(
            &verifier,
            &encoded,
            [7; 32],
            None,
            &[],
            &mut Some(&mut io.history_io())
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn alternating_live_scopes_share_certificate_authentication_and_recheck_authority() -> Result<()> {
    let issuer = fixture();
    let grants = Capabilities::new(["event:append"]);
    let root = issuer.bootstrap_scope("root", &grants, None, &mut None)?;
    let first = issuer.attenuate(&root, "first", &grants)?;
    let second = issuer.attenuate(&root, "second", &grants)?;
    let verifier = issuer.verifier(&mut None)?;
    verifier.verify(&first, &mut None)?;
    let wire = acyclic_auth::authority::decode_capability(&first.workload).map_err(unauthorized)?;
    let scopes = [&second, &first, &second];
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            for scope in scopes {
                verifier.verify(scope, &mut None)?;
            }
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert_eq!(wire.blocks.len(), 2, "root and one attenuated child");
    assert_eq!(
        signatures.len(),
        scopes.len(),
        "only each changed child block"
    );
    // Restoring a fresh certificate owner for each scope performs the old
    // certificate and ancestor signatures in addition to each changed child.
    let (result, repeated) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            let trust = issuer.public_workload_trust()?;
            for scope in scopes {
                verify_trusted(&trust, issuer.audience(), scope, now(), &mut None)?;
            }
            Ok(())
        },
    );
    result?;
    let repeated: Vec<acyclic_auth::crypto::SignatureVerification> = repeated;
    assert!(repeated.len() > signatures.len());
    let mut forged = first.clone();
    *forged.workload.last_mut().unwrap() ^= 1;
    assert!(verifier.verify(&forged, &mut None).is_err());
    verifier
        .public
        .trust
        .write()
        .map_err(unauthorized)?
        .minimum_key_epoch = 2;
    for scope in [&first, &second] {
        assert!(verifier.verify(scope, &mut None).is_err());
    }
    verifier
        .public
        .trust
        .write()
        .map_err(unauthorized)?
        .minimum_key_epoch = 1;
    for scope in [&first, &second] {
        verifier.verify(scope, &mut None)?;
    }
    issuer.revoke_workload(&root)?;
    for scope in [&first, &second] {
        assert!(verifier.verify(scope, &mut None).is_err());
    }
    Ok(())
}

#[test]
fn retained_authentication_rechecks_exact_claims_current_trust_time_and_audience() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let timestamp = now();
    let first = verify(&verifier, issuer.audience(), &scope, timestamp, &mut None)?;
    let second = verify(&verifier, issuer.audience(), &scope, timestamp, &mut None)?;
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    // Negative control: the former path authenticates an already known chain again.
    let fresh = verify_trusted(
        &issuer.public_workload_trust()?,
        issuer.audience(),
        &scope,
        timestamp,
        &mut None,
    )?;
    assert!(!std::sync::Arc::ptr_eq(&first, &fresh));
    for field in 0..7 {
        let mut forged = scope.clone();
        match field {
            0 => forged.id.push_str("-changed"),
            1 => forged.capabilities = Capabilities::new(["effect:plan"]),
            2 => forged.agent = Some(AgentId::from_bytes([3; 16])),
            3 => forged.parent_proof = Some([4; 32]),
            4 => *forged.proof = [5; 32],
            5 => forged.issuer.push_str("-foreign"),
            _ => {
                let byte = forged.workload.get_mut(20).ok_or_else(|| {
                    Error::Invalid("fixture workload has no signature byte".into())
                })?;
                *byte ^= 1;
            }
        }
        assert!(verifier.verify(&forged, &mut None).is_err());
    }
    let other = Authority {
        kind: AggregateKind::Task,
        id: "other".into(),
    };
    assert!(verify(&verifier, &other, &scope, timestamp, &mut None).is_err());
    assert!(
        verify(
            &verifier,
            issuer.audience(),
            &scope,
            first.effective().expires_at_unix_seconds(),
            &mut None
        )
        .is_err()
    );
    verifier
        .public
        .trust
        .write()
        .map_err(unauthorized)?
        .minimum_key_epoch = 2;
    assert!(verifier.verify(&scope, &mut None).is_err());
    verifier
        .public
        .trust
        .write()
        .map_err(unauthorized)?
        .minimum_key_epoch = 1;
    verifier.verify(&scope, &mut None)?;
    issuer.revoke_workload(&scope)?;
    assert!(verifier.verify(&scope, &mut None).is_err());
    // A saved successful decision would incorrectly accept the same retained proof.
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    Ok(())
}

#[test]
fn authentication_owner_replaces_its_only_retained_chain() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let root = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let child = issuer.attenuate(&root, "child", root.capabilities())?;
    let first = verify(&verifier, issuer.audience(), &root, now(), &mut None)?;
    let old = std::sync::Arc::downgrade(&first);
    verify(&verifier, issuer.audience(), &child, now(), &mut None)?;
    let replaced = verify(&verifier, issuer.audience(), &root, now(), &mut None)?;
    assert!(!std::sync::Arc::ptr_eq(&first, &replaced));
    drop(first);
    assert!(old.upgrade().is_none());
    Ok(())
}

#[test]
fn immutable_scope_facts_are_shared_by_verification_and_recording() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let trust = issuer.public_workload_trust()?;
    let first = authenticate(&verifier.public, &trust, &scope, now(), &mut None)?;
    verifier.verify(&scope, &mut None)?;
    verifier
        .admission
        .record("owner", issuer.audience(), &scope, [7; 32], &mut None)?;
    let second = authenticate(&verifier.public, &trust, &scope, now(), &mut None)?;
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert!(std::ptr::eq(
        first.attributes.as_slice(),
        second.attributes.as_slice()
    ));
    // The old per-use derivation allocates and parses the same immutable facts again.
    let (rebuilt, _) = admission::scope_facts(&first.capability, false)?;
    assert_eq!(first.attributes, rebuilt);
    assert!(!std::ptr::eq(
        first.attributes.as_slice(),
        rebuilt.as_slice()
    ));
    Ok(())
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

#[test]
fn historical_owner_reuses_only_exact_proof_and_rechecks_each_binding() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let encoded =
        verifier
            .admission
            .record("owner", issuer.audience(), &scope, [7; 32], &mut None)?;
    let expected = RecordedScope::from_wire(
        scope.id.clone(),
        scope.capabilities.clone(),
        scope.issuer.clone(),
        scope.agent,
    );
    let first = verify_record(
        &verifier,
        &encoded,
        [7; 32],
        Some(&expected),
        &["event:append"],
        &mut None,
    )?;
    let second = verify_record(
        &verifier,
        &encoded,
        [7; 32],
        Some(&expected),
        &[],
        &mut None,
    )?;
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    let fresh = AuthorityVerifier::from_public(
        "owner".into(),
        issuer.audience().clone(),
        issuer.public_workload_trust()?,
        verifier.public_checkpoint_key(),
    )?;
    let rebuilt = verify_record(&fresh, &encoded, [7; 32], Some(&expected), &[], &mut None)?;
    // Former per-use authentication fails this single-proof ownership oracle.
    assert!(!std::sync::Arc::ptr_eq(&first, &rebuilt));
    assert!(verify_record(&verifier, &encoded, [8; 32], None, &[], &mut None).is_err());
    assert!(
        verify_record(
            &verifier,
            &encoded,
            [7; 32],
            None,
            &["effect:run"],
            &mut None
        )
        .is_err()
    );
    for field in 0..4 {
        let mut changed = expected.clone();
        match field {
            0 => changed.id.push_str("-forged"),
            1 => changed.capabilities = Capabilities::new(["effect:run"]),
            2 => changed.issuer.push_str("-foreign"),
            _ => changed.agent = Some(AgentId::from_bytes([9; 16])),
        }
        assert!(
            verify_record(&verifier, &encoded, [7; 32], Some(&changed), &[], &mut None).is_err()
        );
    }
    let mut other = verifier.clone();
    other.audience.id.push_str("-other");
    assert!(verify_record(&other, &encoded, [7; 32], None, &[], &mut None).is_err());
    let mut forged = encoded.clone();
    forged[20] ^= 1;
    assert!(verify_record(&verifier, &forged, [7; 32], None, &[], &mut None).is_err());
    issuer.revoke_workload(&scope)?;
    assert!(verify_record(&verifier, &encoded, [7; 32], None, &[], &mut None).is_err());
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    Ok(())
}

#[test]
fn historical_owner_replaces_its_only_retained_record() -> Result<()> {
    let issuer = fixture();
    let verifier = issuer.verifier(&mut None)?;
    let scope = issuer.bootstrap_scope(
        "root",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let first_bytes =
        verifier
            .admission
            .record("owner", issuer.audience(), &scope, [7; 32], &mut None)?;
    let next_bytes =
        verifier
            .admission
            .record("owner", issuer.audience(), &scope, [8; 32], &mut None)?;
    let first = verify_record(&verifier, &first_bytes, [7; 32], None, &[], &mut None)?;
    let old = std::sync::Arc::downgrade(&first);
    verify_record(&verifier, &next_bytes, [8; 32], None, &[], &mut None)?;
    let replaced = verify_record(&verifier, &first_bytes, [7; 32], None, &[], &mut None)?;
    assert!(!std::sync::Arc::ptr_eq(&first, &replaced));
    drop(first);
    assert!(old.upgrade().is_none());
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
        Some(&name),
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
