use super::*;
use crate::conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::resources::ProviderRef;

fn fixture() -> Result<(AuthorityIssuer, AuthorityVerifier, Scope, VolumeRef)> {
    let issuer = AuthorityIssuer::new(
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
    let root = issuer.issue("root", &capabilities, None, None, &mut None)?;
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
        let mut trust = original.clone();
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
            .map_err(workload::unauthorized)? = trust;
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
    let denied = issuer.issue(
        "denied",
        &Capabilities::new(["other"]),
        None,
        None,
        &mut None,
    )?;
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
    assert_eq!(io.owned_bytes()?, 0, "failed grant leaked retained custody");
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
        "grant omitted retained custody"
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
    {
        let mut observer = io.history_io();
        let mut work = Some(&mut observer as &mut dyn HistoryIo);
        let mut quota = CodecQuota::new(&mut work, 0)?;
        let evidence = compact_evidence(capability, &mut quota)?;
        assert_eq!(
            before - io.remaining()?.allocation_operations,
            3,
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
