use super::*;

#[test]
fn contributed_resource_issuance_preserves_caller_and_both_original_dependencies() -> Result<()> {
    struct Clock;
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            1_800_000_000_000
        }
    }
    let timestamp = 1_800_000_000;
    for revoke_caller in [false, true] {
        let issuer = AuthorityRoot::with_clock(
            "owner",
            [29; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "original".into(),
            },
            std::sync::Arc::new(Clock),
        );
        let trust = issuer.public_workload_trust()?;
        let grants = Capabilities::new(["read"]);
        let caller = issuer
            .bootstrap_scope(
                "caller",
                &grants,
                Some(AgentId::from_bytes([1; 16])),
                &mut None,
            )?
            .with_workload_expiry(timestamp + 3, &trust, issuer.audience(), timestamp)?;
        let selected = issuer.bootstrap_scope(
            "selected",
            &grants,
            Some(AgentId::from_bytes([2; 16])),
            &mut None,
        )?;
        let unrelated = issuer.bootstrap_scope(
            "unrelated",
            &grants,
            Some(AgentId::from_bytes([3; 16])),
            &mut None,
        )?;
        let capability = verify(
            &issuer.verifier(&mut None)?,
            issuer.audience(),
            &caller,
            timestamp,
            &mut None,
        )?;
        let evidence = capability.trust_evidence().map_err(unauthorized)?;
        let principal = capability.effective().principal();
        assert!(!principal.lineage.is_empty());
        let resources: &[(&str, bool, &[&str])] =
            &[("streams:/selected", false, &["streams/follow"])];
        let joined = issuer.resource_credential(
            &selected,
            resources,
            Some((principal, &evidence)),
            timestamp,
        )?;
        assert!(joined.is_sealed());
        assert_eq!(joined.effective().principal(), principal);
        assert_eq!(joined.effective().expires_at_unix_seconds(), timestamp + 3);
        let reopened = verify::verify(&joined.encode(), &trust, timestamp).map_err(unauthorized)?;
        let negative = issuer.resource_credential(
            if revoke_caller { &selected } else { &unrelated },
            resources,
            (!revoke_caller).then_some((principal, &evidence)),
            timestamp,
        )?;
        let audience = issuer.audience_resource()?;
        let authorize = |credential: &VerifiedCapability, resource, action, now| {
            credential.authorize(
                &trust,
                &Request {
                    target: Target::Resource { action, resource },
                    audience: &audience,
                    holder: None,
                    now_unix_seconds: now,
                    keys: &[],
                    attributes: &RecordAttributes::default(),
                    usage: &usage(),
                    cpu_millis: 0,
                    memory_bytes: 0,
                },
                PolicyDecision::Allow,
                acyclic_auth::authority::Scope::Follow,
            )
        };
        authorize(&reopened, "streams:/selected", "streams/follow", timestamp)
            .map_err(unauthorized)?;
        assert!(authorize(&reopened, "streams:/other", "streams/follow", timestamp).is_err());
        assert!(authorize(&reopened, "streams:/selected", "streams/list", timestamp).is_err());
        assert!(
            authorize(
                &reopened,
                "streams:/selected",
                "streams/follow",
                timestamp + 3
            )
            .is_err()
        );
        issuer.revoke_workload(if revoke_caller { &caller } else { &selected })?;
        assert!(authorize(&reopened, "streams:/selected", "streams/follow", timestamp).is_err());
        // Removing exactly the lost dependency admits the same resource, so
        // selected-issuer validity alone cannot establish the joined guarantee.
        authorize(&negative, "streams:/selected", "streams/follow", timestamp)
            .map_err(unauthorized)?;
    }
    Ok(())
}

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
fn repeated_revocation_authenticates_exact_target_and_publishes_once() -> Result<()> {
    use futures::FutureExt;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    };

    struct Clock(AtomicU64);
    impl acyclic_stream::UnixMillisClock for Clock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::Acquire)
        }
    }
    let timestamp = 1_800_000_000;
    let clock = Arc::new(Clock(AtomicU64::new(timestamp * 1000)));
    let issuer = AuthorityRoot::with_clock(
        "owner",
        [29; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "root".into(),
        },
        clock.clone(),
    );
    let grants = Capabilities::new(["event:append"]);
    let root = issuer.bootstrap_scope("root", &grants, None, &mut None)?;
    let child = issuer.attenuate(&root, "child", &grants)?;
    let descendant = issuer.attenuate(&child, "descendant", &grants)?;
    let sibling = issuer.attenuate(&root, "sibling", &grants)?;
    let verifier = issuer.verifier(&mut None)?;
    let changes = issuer.trust_changes();
    let changed = changes.changed();
    let mut revisions = issuer.runtime.trust.state();
    let ready = Barrier::new(2);
    std::thread::scope(|threads| -> Result<()> {
        let first = threads.spawn(|| {
            ready.wait();
            issuer.revoke_workload(&child)
        });
        ready.wait();
        let second = issuer.revoke_workload(&child);
        let first = first.join().expect("revocation worker panicked");
        assert!(
            first.is_ok() && second.is_ok(),
            "repeated revocation rejected its exact authenticated target"
        );
        Ok(())
    })?;
    assert_eq!(
        changed.now_or_never(),
        Some(Ok(())),
        "revocation publication did not wake its original owner"
    );
    revisions.borrow_and_update();
    assert!(verifier.verify(&child, &mut None).is_err());
    assert!(verifier.verify(&descendant, &mut None).is_err());
    verifier.verify(&sibling, &mut None)?;

    let original = issuer.public_workload_trust()?;
    issuer.revoke_workload(&child)?;
    let (result, signatures) =
        acyclic_test_support::differential::observe(|| (), |()| issuer.revoke_workload(&child));
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "repeated target reauthenticated its signatures"
    );
    assert!(Arc::ptr_eq(&original, &issuer.public_workload_trust()?));
    assert!(!revisions.has_changed().map_err(unauthorized)?);

    // A matching revoked proof alone does not authenticate the submitted scope.
    for field in 0..7 {
        let mut forged = child.clone();
        match field {
            0 => forged.id.push_str("-changed"),
            1 => forged.capabilities = Capabilities::new(["effect:plan"]),
            2 => forged.agent = Some(AgentId::from_bytes([3; 16])),
            3 => forged.parent_proof = Some([4; 32]),
            4 => *forged.proof = *sibling.proof,
            5 => forged.issuer.push_str("-foreign"),
            _ => *forged.workload.last_mut().expect("signed scope is empty") ^= 1,
        }
        assert!(issuer.revoke_workload(&forged).is_err());
    }
    let other = issuer.with_audience(Authority {
        kind: AggregateKind::Task,
        id: "foreign".into(),
    });
    assert!(other.revoke_workload(&child).is_err());
    issuer.revoke_workload(&root)?;
    assert!(
        issuer.revoke_workload(&child).is_err(),
        "target exemption also exempted its revoked ancestor"
    );
    revisions.borrow_and_update();
    clock
        .0
        .store((timestamp + CERTIFICATE_SECONDS) * 1000, Ordering::Release);
    assert!(
        issuer.revoke_workload(&root).is_err(),
        "revocation exemption ignored original expiry"
    );
    assert!(!revisions.has_changed().map_err(unauthorized)?);
    clock.0.store(timestamp * 1000, Ordering::Release);
    issuer.raise_minimum_epoch(2)?;
    assert!(issuer.revoke_workload(&root).is_err());
    let closed = changes.changed();
    drop(other);
    drop(issuer);
    assert_eq!(
        closed.now_or_never(),
        Some(Err(acyclic_auth::trust::SourceClosed))
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
    // A published epoch floor retires every key certified below it; the
    // runtime re-certifies at the new epoch.
    issuer.raise_minimum_epoch(2)?;
    for scope in [&first, &second] {
        assert!(verifier.verify(scope, &mut None).is_err());
    }
    let fresh = issuer.bootstrap_scope("fresh", &Capabilities::new(["read"]), None, &mut None)?;
    verifier.verify(&fresh, &mut None)?;
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
    // A holder can sign an append, but cannot widen its parent's grants.
    let name = resource(&issuer.audience)?;
    let mut widened = child.clone();
    widened.capabilities = root.capabilities.clone();
    widened.parent_proof = Some(*child.proof);
    assert!(
        verified
            .attenuate(facts(&widened, &name, child.agent)?, &entropy())
            .is_err()
    );
    // A block without scope facts is not a delegation: a later block cannot
    // stand in for it, though each one alone is well formed.
    let skipped = verified
        .attenuate(vec![], &entropy())
        .map_err(unauthorized)?;
    let mut misplaced = child.clone();
    misplaced.parent_proof = Some(*skipped.block_ids().last().unwrap());
    let chain = skipped
        .attenuate(facts(&misplaced, &name, child.agent)?, &entropy())
        .map_err(unauthorized)?;
    retain(&mut misplaced, &chain, &mut None)?;
    assert!(verifier.verify(&misplaced, &mut None).is_err());
    // Control: the same block appended directly is admitted.
    let mut direct = child.clone();
    direct.parent_proof = Some(*child.proof);
    let chain = verified
        .attenuate(facts(&direct, &name, child.agent)?, &entropy())
        .map_err(unauthorized)?;
    retain(&mut direct, &chain, &mut None)?;
    verifier.verify(&direct, &mut None)?;
    Ok(())
}

#[test]
fn a_holder_delegates_to_another_agent_and_only_its_key_extends_the_chain() -> Result<()> {
    use acyclic_auth::contract::{CapabilityProto, SignedBlock, TipProof};
    use ed25519_dalek::Signer as _;
    use prost::Message as _;
    let issuer = fixture();
    let audience = issuer.audience.clone();
    let trust = issuer.public_workload_trust()?;
    let first = AgentId::from_bytes([1; 16]);
    let second = AgentId::from_bytes([2; 16]);
    let parent = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new(["tool:call:read", "tool:call:write"]),
        Some(first),
        &mut None,
    )?;
    let delegated = parent.attenuate_workload(
        "delegate",
        &Capabilities::new(["tool:call:read"]),
        Some(second),
        &trust,
        &audience,
        now(),
    )?;
    delegated.verify_workload(&trust, &audience, now(), &mut None)?;
    assert_eq!(delegated.agent, Some(second));
    let grandchild = delegated.attenuate_workload(
        "grandchild",
        &Capabilities::new(["tool:call:read"]),
        Some(second),
        &trust,
        &audience,
        now(),
    )?;
    grandchild.verify_workload(&trust, &audience, now(), &mut None)?;
    // A delegate never drops its acting agent, nor claims another unsigned.
    assert!(
        delegated
            .attenuate_workload(
                "anonymous",
                &Capabilities::new(["tool:call:read"]),
                None,
                &trust,
                &audience,
                now()
            )
            .is_err()
    );
    let mut claimed = delegated.clone();
    claimed.agent = Some(first);
    assert!(
        claimed
            .verify_workload(&trust, &audience, now(), &mut None)
            .is_err()
    );
    // A widened delegate is refused.
    assert!(
        delegated
            .attenuate_workload(
                "widened",
                &Capabilities::new(["tool:call:write"]),
                Some(second),
                &trust,
                &audience,
                now()
            )
            .is_err()
    );
    // The tip block re-signed by a key other than the previous holder's.
    let mut capability =
        acyclic_auth::authority::decode_capability(&delegated.workload).map_err(unauthorized)?;
    let index = capability.blocks.len() - 1;
    let previous = verify::verify(&parent.workload, &trust, now())
        .map_err(unauthorized)?
        .block_ids()
        .last()
        .copied()
        .ok_or_else(|| unauthorized("missing tip"))?;
    let SignedBlock { payload, .. } = capability.blocks[index].clone();
    let transcript = acyclic_auth::crypto::block_transcript(index as u64, &previous, &payload);
    capability.blocks[index].signature = SigningKey::from_bytes(&[42; 32])
        .sign(&transcript)
        .to_bytes()
        .to_vec();
    let mut resigned = delegated.clone();
    resigned.workload = CapabilityProto::from(capability.clone()).encode_to_vec();
    assert!(
        resigned
            .verify_workload(&trust, &audience, now(), &mut None)
            .is_err()
    );
    // A presenter without the final holder key.
    let mut capability =
        acyclic_auth::authority::decode_capability(&delegated.workload).map_err(unauthorized)?;
    assert!(matches!(capability.proof, TipProof::Secret(_)));
    capability.proof = TipProof::Secret(vec![43; 32]);
    let mut keyless = delegated.clone();
    keyless.workload = CapabilityProto::from(capability).encode_to_vec();
    assert!(
        keyless
            .verify_workload(&trust, &audience, now(), &mut None)
            .is_err()
    );
    // Revoking the middle block refuses its descendants and keeps its parent.
    issuer.revoke_workload(&delegated)?;
    assert!(
        grandchild
            .verify_workload(&trust, &audience, now(), &mut None)
            .is_err()
    );
    parent.verify_workload(&trust, &audience, now(), &mut None)?;
    Ok(())
}

#[test]
fn workload_holders_attenuate_with_public_trust_and_revocation_invalidates_descendants()
-> Result<()> {
    let issuer = fixture();
    let audience = issuer.audience.clone();
    let trust = issuer.public_workload_trust()?;
    let parent = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new(["tool:call:read", "tool:call:write"]),
        None,
        &mut None,
    )?;
    let child = parent.attenuate_workload(
        "reader",
        &Capabilities::new(["tool:call:read"]),
        None,
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
                None,
                &trust,
                &audience,
                now()
            )
            .is_err()
    );
    issuer.revoke_workload(&parent)?;
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
                None,
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
