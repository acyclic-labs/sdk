use super::*;
fn fixture() -> AuthorityIssuer {
    AuthorityIssuer::new(
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
    let root = issuer.issue(
        "root",
        &Capabilities::new(["model:generate", "tool:call:read"]),
        None,
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
    let mut trust = issuer.public_workload_trust();
    let parent = issuer.issue(
        "owner",
        &Capabilities::new(["tool:call:read", "tool:call:write"]),
        None,
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
