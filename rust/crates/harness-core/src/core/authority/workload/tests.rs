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
    let mut revisions = issuer.trust_revisions();
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
    assert_eq!(*revisions.borrow_and_update(), 1);
    assert!(verifier.verify(&child, &mut None).is_err());
    assert!(verifier.verify(&descendant, &mut None).is_err());
    verifier.verify(&sibling, &mut None)?;

    let original = issuer.public_trust_revision()?.0;
    issuer.revoke_workload(&child)?;
    let (result, signatures) =
        acyclic_test_support::differential::observe(|| (), |()| issuer.revoke_workload(&child));
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "repeated target reauthenticated its signatures"
    );
    assert!(Arc::ptr_eq(&original, &issuer.public_trust_revision()?.0));
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
    {
        let mut trust = issuer.runtime.public.trust.write().map_err(unauthorized)?;
        Arc::make_mut(&mut trust).minimum_key_epoch = 2;
    }
    assert!(issuer.revoke_workload(&child).is_err());
    {
        let mut trust = issuer.runtime.public.trust.write().map_err(unauthorized)?;
        Arc::make_mut(&mut trust).minimum_key_epoch = 1;
    }
    issuer.revoke_workload(&root)?;
    assert!(
        issuer.revoke_workload(&child).is_err(),
        "target exemption also exempted its revoked ancestor"
    );
    assert_eq!(*revisions.borrow_and_update(), 2);
    clock
        .0
        .store((timestamp + CERTIFICATE_SECONDS) * 1000, Ordering::Release);
    assert!(
        issuer.revoke_workload(&root).is_err(),
        "revocation exemption ignored original expiry"
    );
    assert!(!revisions.has_changed().map_err(unauthorized)?);
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
    std::sync::Arc::make_mut(&mut *verifier.public.trust.write().map_err(unauthorized)?)
        .minimum_key_epoch = 2;
    for scope in [&first, &second] {
        assert!(verifier.verify(scope, &mut None).is_err());
    }
    std::sync::Arc::make_mut(&mut *verifier.public.trust.write().map_err(unauthorized)?)
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
    std::sync::Arc::make_mut(&mut *verifier.public.trust.write().map_err(unauthorized)?)
        .minimum_key_epoch = 2;
    assert!(verifier.verify(&scope, &mut None).is_err());
    std::sync::Arc::make_mut(&mut *verifier.public.trust.write().map_err(unauthorized)?)
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
