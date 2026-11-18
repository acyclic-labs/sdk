use super::*;

#[test]
fn owner_bound_private_write_rejects_an_attached_agent_with_a_copied_capability() -> Result<()> {
    let owner = AgentId::from_bytes([1; 16]);
    let attached = AgentId::from_bytes([2; 16]);
    let private = file(owner, "scratch/note.txt")?.volume().clone();
    let write = private.capability(VolumeOperation::Write, &mut None)?;
    let issuer = crate::core::AuthorityRoot::new(
        "owner",
        [7; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: owner.to_string(),
        },
    );
    let read = private.capability(VolumeOperation::Read, &mut None)?;
    let scope = issuer.bootstrap_scope(
        "write",
        &crate::Capabilities::new([write.clone(), read.clone()]),
        Some(owner),
        &mut None,
    )?;
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &private,
        VolumeOperation::Write,
        &mut None,
    )?;
    let unsigned = issuer.bootstrap_scope(
        "copied-capability",
        &crate::Capabilities::new([write.clone()]),
        None,
        &mut None,
    )?;
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &unsigned,
            &private,
            VolumeOperation::Write,
            &mut None
        )
        .is_err()
    );
    let inherited =
        issuer.attenuate(&scope, "child", &crate::Capabilities::new([write.clone()]))?;
    assert_eq!(inherited.agent(), Some(owner));
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &inherited,
        &private,
        VolumeOperation::Write,
        &mut None,
    )?;
    let foreign_issuer = crate::core::AuthorityRoot::new(
        "attached",
        [8; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: attached.to_string(),
        },
    );
    let copied = foreign_issuer.bootstrap_scope(
        "copied",
        &crate::Capabilities::new([write]),
        Some(attached),
        &mut None,
    )?;
    assert!(
        ContentGrant::verify(
            &foreign_issuer.verifier(&mut None)?,
            &copied,
            &private,
            VolumeOperation::Write,
            &mut None
        )
        .is_err()
    );
    let exact_file = file(owner, "scratch/note.txt")?;
    let delegated =
        issuer.delegate_private_file_read(&scope, attached, "attached-read", &exact_file)?;
    assert_eq!(delegated.agent(), Some(attached));
    ContentGrant::verify_read(&issuer.verifier(&mut None)?, &delegated, &exact_file)?;
    let authored = file(owner, ".system/execution/state.json")?;
    ContentGrant::verify_read(&issuer.verifier(&mut None)?, &scope, &authored)?;
    let authored_exact = issuer.bootstrap_scope(
        "authored-exact",
        &crate::Capabilities::new([authored.read_capability(&mut None)?]),
        Some(owner),
        &mut None,
    )?;
    ContentGrant::verify_read(&issuer.verifier(&mut None)?, &authored_exact, &authored)?;
    let delegated = issuer.delegate_private_file_read(&scope, attached, "authored", &authored)?;
    ContentGrant::verify_read(&issuer.verifier(&mut None)?, &delegated, &authored)?;
    let directory = issuer.delegate_private_directory_read(
        &scope,
        attached,
        "attached-directory",
        &private,
        ".system",
    )?;
    let directory_grant = ContentGrant::verify_directory_read(
        &issuer.verifier(&mut None)?,
        &directory,
        &private,
        ".system",
        &mut None,
    )?;
    directory_grant.require_directory_path(&private, ".system/execution", None)?;
    directory_grant.require_file_read(&authored, None)?;
    assert!(
        directory_grant
            .require_directory_path(&private, ".systematic", None)
            .is_err()
    );
    assert!(
        directory_grant
            .require_directory_path(&private, "scratch", None)
            .is_err()
    );
    assert!(
        directory_grant
            .require(&private, VolumeOperation::Write, None)
            .is_err()
    );
    let foreign_owner_scope = foreign_issuer.bootstrap_scope(
        "copied-read",
        &crate::Capabilities::new([read]),
        Some(attached),
        &mut None,
    )?;
    assert!(
        foreign_issuer
            .delegate_private_file_read(&foreign_owner_scope, attached, "invalid", &exact_file)
            .is_err()
    );
    Ok(())
}

#[test]
fn retained_content_grants_reject_revoked_workload_ancestors() -> Result<()> {
    let owner = AgentId::from_bytes([1; 16]);
    let selected = file(owner, "scratch/note.txt")?;
    let volume = selected.volume();
    let issuer = crate::core::AuthorityRoot::new(
        "retained",
        [71; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: owner.to_string(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            selected.read_capability(&mut None)?,
            volume.directory_read_capability("scratch", &mut None)?,
        ]),
        Some(owner),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reads = [
        ContentGrant::verify_read(&verifier, &scope, &selected)?,
        ContentGrant::verify_directory_read(&verifier, &scope, volume, "scratch", &mut None)?,
    ];
    let write = ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Write, &mut None)?;
    for grant in &reads {
        grant.require_file_read(&selected, None)?;
    }
    reads[1].require_directory_path(volume, "scratch", None)?;
    write.require(volume, VolumeOperation::Write, None)?;
    issuer.revoke_workload(&scope)?;
    for grant in &reads {
        assert!(matches!(
            grant.require_file_read(&selected, None),
            Err(Error::Unauthorized(_))
        ));
    }
    assert!(matches!(
        reads[1].require_directory_path(volume, "scratch", None),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        write.require(volume, VolumeOperation::Write, None),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
