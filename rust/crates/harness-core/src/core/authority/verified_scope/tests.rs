use super::*;
use crate::conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::resources::ProviderRef;

fn fixture() -> Result<(AuthorityRoot, AuthorityVerifier, Scope, VolumeRef)> {
    let issuer = AuthorityRoot::new(
        "verified-scope",
        [106; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "verified-scope".into(),
        },
    );
    let volume = VolumeRef::new(
        ProviderRef::new(
            "verified-scope".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let capabilities = Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]);
    let root = issuer.bootstrap_scope("root", &capabilities, None, &mut None)?;
    let scope = issuer.attenuate(&root, "child", &capabilities)?;
    let verifier = issuer.verifier(&mut None)?;
    Ok((issuer, verifier, scope, volume))
}

#[test]
fn content_grants_recheck_fresh_trust_without_reauthenticating_the_scope() -> Result<()> {
    let (_issuer, verifier, scope, volume) = fixture()?;
    let grant = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut None,
    )?;
    let cloned = grant.clone();
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            grant.require(&volume, VolumeOperation::Read, None)?;
            cloned.require(&volume, VolumeOperation::Read, None)?;
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "immutable scope was authenticated again"
    );
    let original = verifier
        .public
        .trust
        .read()
        .map_err(workload::unauthorized)?
        .clone();
    let capability = workload::verify(
        &verifier,
        &verifier.audience,
        &scope,
        workload::now(),
        &mut None,
    )?;
    let wire = acyclic_auth::authority::decode_capability(&scope.workload)
        .map_err(workload::unauthorized)?;
    let next = acyclic_auth::authority::decode_block(&wire.blocks[1].payload)
        .map_err(workload::unauthorized)?
        .next_public_key
        .as_slice()
        .try_into()
        .map_err(workload::unauthorized)?;
    for change in 0..9 {
        let mut trust = original.as_ref().clone();
        match change {
            0 => trust.environment_keys.clear(),
            1 => trust.issuer_keys.clear(),
            2 => trust.account = "unavailable".into(),
            3 => trust.minimum_key_epoch = u64::MAX,
            4 => {
                trust.revoked_key_ids.insert("runtime".into());
            }
            5 => {
                trust.revoked_block_ids.insert(capability.block_ids()[0]);
            }
            6 => {
                trust.revoked_block_ids.insert(capability.block_ids()[1]);
            }
            7 => trust.environment_keys.push(next),
            _ => trust.issuer_keys.push(next),
        }
        *verifier
            .public
            .trust
            .write()
            .map_err(workload::unauthorized)? = std::sync::Arc::new(trust);
        assert!(
            grant.require(&volume, VolumeOperation::Read, None).is_err(),
            "content grant ignored changed trust: {change}"
        );
        assert!(
            cloned
                .require(&volume, VolumeOperation::Read, None)
                .is_err()
        );
    }
    *verifier
        .public
        .trust
        .write()
        .map_err(workload::unauthorized)? = original;
    grant.require(&volume, VolumeOperation::Read, None)?;
    Ok(())
}

#[test]
fn authenticated_scope_keeps_the_half_open_live_validity_interval() -> Result<()> {
    let (issuer, verifier, scope, _volume) = fixture()?;
    let timestamp = workload::now();
    let bounded = scope.with_workload_expiry(
        timestamp + 60,
        &issuer.public_workload_trust()?,
        &issuer.audience,
        timestamp,
    )?;
    let authenticated = verifier.authenticate_scope(&bounded, &mut None)?;
    assert!(authenticated.check_at(timestamp, &mut None).is_ok());
    assert!(authenticated.check_at(timestamp + 59, &mut None).is_ok());
    assert!(authenticated.check_at(timestamp + 60, &mut None).is_err());
    assert!(authenticated.check_at(0, &mut None).is_err());
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn grant_construction_releases_failed_custody_and_accounts_for_success() -> Result<()> {
    let (issuer, verifier, scope, volume) = fixture()?;
    let denied =
        issuer.bootstrap_scope("denied", &Capabilities::new(["other"]), None, &mut None)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = crate::executor::PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )?;
    assert!(
        ContentGrant::verify(
            &verifier,
            &denied,
            &volume,
            VolumeOperation::Read,
            u64::MAX,
            &mut Some(&mut io.history_io())
        )
        .is_err()
    );
    // The shared authority owns authentication; this attempt owns no grant.
    assert_eq!(io.owned_bytes()?, 0);
    let grant = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut Some(&mut io.history_io()),
    )?;
    assert_eq!(
        io.owned_bytes()?,
        grant.logical_owned_bytes()?,
        "grant custody was not separated from the authority"
    );
    let custody = io.guard_owned(grant.logical_owned_bytes()?)?;
    drop((grant, custody));
    assert_eq!(io.owned_bytes()?, 0);
    assert!(io.remaining()?.peak_allocation_bytes < 1_000_000);
    drop((verifier, issuer));
    assert_eq!(
        io.remaining()?.peak_allocation_bytes,
        1_000_000,
        "retired authority leaked wire custody"
    );
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn compact_evidence_funds_every_allocation_before_conversion() -> Result<()> {
    let (_issuer, verifier, scope, _volume) = fixture()?;
    let capability = workload::verify(
        &verifier,
        &verifier.audience,
        &scope,
        workload::now(),
        &mut None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let io = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    let before = io.remaining()?.allocation_operations;
    let expected_allocations = capability
        .trust_evidence_storage()
        .map_err(workload::unauthorized)?
        .allocation_bytes()
        .count() as u64;
    {
        let mut observer = io.history_io();
        let mut work = Some(&mut observer as &mut dyn HistoryIo);
        let mut quota = CodecQuota::new(&mut work, 0)?;
        let evidence = compact_evidence(&capability, &mut quota)?;
        assert_eq!(
            before - io.remaining()?.allocation_operations,
            expected_allocations,
            "evidence allocation operations were undercounted"
        );
        assert_eq!(
            io.owned_bytes()?,
            evidence.owned_bytes().map_err(workload::unauthorized)?
        );
        drop(evidence);
    }
    assert_eq!(io.owned_bytes()?, 0);
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn authority_metadata_copies_release_every_partial_allocation() -> Result<()> {
    let (issuer, verifier, _scope, _volume) = fixture()?;
    let first = issuer.id.len() as u64;
    let total = first + issuer.audience.id.len() as u64;
    for entry in 0..3 {
        let mut successes = 0;
        for operations in 0..=2 {
            for bytes in [0, first - 1, first, total - 1, total, 1_000_000] {
                let cancellation = acyclic_fs::CancellationToken::new();
                let mut budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
                budget.allocation_operations = operations;
                budget.peak_allocation_bytes = bytes;
                let io = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
                let mut observer = io.history_io();
                let mut work = Some(&mut observer as &mut dyn HistoryIo);
                let result = match entry {
                    0 => issuer
                        .copy(&mut work)
                        .map(|copy| (copy.id.capacity() + copy.audience.id.capacity()) as u64),
                    1 => issuer
                        .verifier(&mut work)
                        .and_then(|copy| copy.logical_owned_bytes()),
                    _ => verifier
                        .copy(&mut work)
                        .and_then(|copy| copy.logical_owned_bytes()),
                };
                match result {
                    Ok(owned) => {
                        successes += 1;
                        assert_eq!(operations, 2);
                        assert!(bytes >= total);
                        assert_eq!(io.owned_bytes()?, owned);
                    }
                    Err(error) => {
                        drop(error);
                        assert_eq!(
                            io.owned_bytes()?,
                            0,
                            "partial authority metadata copy leaked retained custody: entry={entry} operations={operations} bytes={bytes}"
                        );
                    }
                }
            }
        }
        assert_eq!(
            successes, 2,
            "metadata copies never reached both complete budgets"
        );
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn grant_construction_releases_custody_at_every_allocation_failure() -> Result<()> {
    let (issuer, verifier, scope, volume) = fixture()?;
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = acyclic_fs::CancellationToken::new();
    let probe = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
    let grant = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut Some(&mut probe.history_io()),
    )?;
    assert_eq!(probe.owned_bytes()?, grant.logical_owned_bytes()?);
    let required = budget.allocation_operations - probe.remaining()?.allocation_operations;
    assert!(required > 3, "grant did not reach the metadata copies");
    let custody = probe.guard_owned(grant.logical_owned_bytes()?)?;
    drop((grant, custody, verifier, issuer));
    assert_eq!(
        probe.remaining()?.peak_allocation_bytes,
        budget.peak_allocation_bytes,
        "retired grant and authority leaked custody"
    );
    for operations in 0..required {
        // Each attempt starts cold, including the authority's wire allocation.
        let (issuer, verifier, scope, volume) = fixture()?;
        let mut limited = budget;
        limited.allocation_operations = operations;
        let io = crate::executor::PreparationIo::from_budget(limited, &cancellation)?;
        let result = ContentGrant::verify(
            &verifier,
            &scope,
            &volume,
            VolumeOperation::Read,
            u64::MAX,
            &mut Some(&mut io.history_io()),
        );
        assert!(
            result.is_err(),
            "allocation failure was not exercised: {operations}"
        );
        drop(result);
        drop((verifier, issuer));
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            budget.peak_allocation_bytes,
            "partial grant or authority allocation leaked retained custody: operations={operations}"
        );
    }
    Ok(())
}
