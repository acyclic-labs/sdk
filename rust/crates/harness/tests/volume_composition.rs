//! Separate skills forks and pinned destination-owned imports.
#![cfg(feature = "filesystem")]

use acyclic_fs::{Fs, JoinOutcome};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, Result,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemHost, workspace_ref},
    resources::ProviderRef,
};

#[test]
fn agent_owned_read_delegation_is_independent_of_volume_role() -> Result<()> {
    let owner = AgentId::from_bytes([3; 16]);
    let reader = AgentId::from_bytes([4; 16]);
    let issuer = AuthorityIssuer::new(
        "delegation",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "delegation".into(),
        },
    );
    for role in [
        VolumeClass::AgentPrivate,
        VolumeClass::SessionShared,
        VolumeClass::Project,
    ] {
        let volume = VolumeRef::new(
            ProviderRef::new("delegation".into(), "filesystem".into(), "2".into(), &mut None)?,
            "owned".into(),
            role,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        let capability = volume.capability(VolumeOperation::Read, &mut None)?;
        let scope = issuer.issue("owner", &Capabilities::new([capability.clone()]), Some(owner), None, &mut None).expect("scope proof");
        let delegated = issuer
            .delegate_private_directory_read(&scope, reader, "reader", &volume, "selected")?;
        assert!(
            ContentGrant::verify_directory_read(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &delegated,
                &volume,
                "selected",
            )
            .is_ok()
        );
        assert!(ContentGrant::verify_directory_read(
            &issuer.verifier(&mut None).expect("verifier copy"), &delegated, &volume, "outside",
        ).is_err());
        for unauthorized in [
            issuer.issue("foreign", &Capabilities::new([capability]), Some(reader), None, &mut None).expect("scope proof"),
            issuer.issue("no-read", &Capabilities::new(std::iter::empty::<String>()), Some(owner), None, &mut None).expect("scope proof"),
        ] {
            assert!(
                issuer
                    .delegate_private_directory_read(
                        &unauthorized,
                        reader,
                        "reader",
                        &volume,
                        "selected",
                    )
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded consumer workflow keeps grants, pinned import, and source immutability assertions together"
)]
async fn skills_are_explicitly_forked_and_imported_with_read_only_builtins() -> Result<()> {
    let provider = ProviderRef::new("skills".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
    let agent = AgentId::from_bytes([1; 16]);
    let foreign_agent = AgentId::from_bytes([2; 16]);
    let root = VolumeRef::new(
        provider.clone(),
        "root-skills".into(),
        VolumeClass::SessionShared,
        VolumeOwner::Session("root".into()),
        &mut None,
    )?;
    let skills = VolumeRef::new(
        provider.clone(),
        "editable-skills".into(),
        VolumeClass::SessionShared,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let builtins = VolumeRef::new(
        provider.clone(),
        "builtins".into(),
        VolumeClass::Project,
        VolumeOwner::Project("builtin-owner".into()),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "skills",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "session".into(),
        },
    );
    let setup = issuer.issue("setup", &Capabilities::new([
            root.capability(VolumeOperation::Write, &mut None)?,
            builtins.capability(VolumeOperation::Write, &mut None)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
    let scope = issuer.issue("session", &Capabilities::new([
            root.capability(VolumeOperation::Read, &mut None)?,
            skills.capability(VolumeOperation::Read, &mut None)?,
            skills.capability(VolumeOperation::Write, &mut None)?,
            builtins.capability(VolumeOperation::Read, &mut None)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
    host.create_volume(&root).await?;
    host.create_volume(&builtins).await?;
    let root_writer =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &setup, &root, VolumeOperation::Write, u64::MAX, &mut None)?;
    host.put_content(
        &root,
        &root_writer,
        "instructions.txt",
        b"root skill",
        "text/plain",
        "instructions.txt",
        1024,
        &IdempotencyKey::new("root-skill")?,
    )
    .await?;
    let builtin_writer = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &setup,
        &builtins,
        VolumeOperation::Write,
        u64::MAX, &mut None,
    )?;
    let builtin = host
        .put_content(
            &builtins,
            &builtin_writer,
            "help.txt",
            b"builtin",
            "text/plain",
            "help.txt",
            1024,
            &IdempotencyKey::new("builtin")?,
        )
        .await?;
    let root_workspace = workspace_ref(provider.clone(), &root.storage_name(&mut None)?)?;
    let pinned = host.resolve(&root_workspace).await?;
    let root_reader =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &root, VolumeOperation::Read, u64::MAX, &mut None)?;
    let skills_writer =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &skills, VolumeOperation::Write, u64::MAX, &mut None)?;
    host.fork_volume(
        &root,
        &root_reader,
        &pinned.generation,
        &skills,
        &skills_writer,
        None,
        &IdempotencyKey::new("skills-fork")?,
    )
    .await?;
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &builtins,
            VolumeOperation::Write
        , u64::MAX, &mut None)
        .is_err()
    );
    assert!(
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &root, VolumeOperation::Write, u64::MAX, &mut None).is_err()
    );
    let foreign = issuer.issue("foreign", &Capabilities::new([skills.capability(VolumeOperation::Write, &mut None)?]), Some(foreign_agent), None, &mut None).expect("scope proof");
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &foreign,
            &skills,
            VolumeOperation::Write
        , u64::MAX, &mut None)
        .is_err()
    );
    let builtin_reader =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &builtins, VolumeOperation::Read, u64::MAX, &mut None)?;
    assert_eq!(
        host.read_content(&builtin, &builtin_reader, 1024)
            .await?
            .as_ref(),
        b"builtin"
    );
    host.put_content(
        &skills,
        &skills_writer,
        "instructions.txt",
        b"session skill",
        "text/plain",
        "instructions.txt",
        1024,
        &IdempotencyKey::new("edit-skill")?,
    )
    .await?;
    assert_eq!(
        host.read(&root_workspace, None, "/instructions.txt", 1024)
            .await?
            .as_ref(),
        b"root skill"
    );
    let skills_workspace = workspace_ref(provider.clone(), &skills.storage_name(&mut None)?)?;
    let selected = host.resolve(&skills_workspace).await?;
    let skills_reader =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &skills, VolumeOperation::Read, u64::MAX, &mut None)?;
    let approved = issuer.issue("approved-writeback", &Capabilities::new([root.capability(VolumeOperation::Write, &mut None)?]), Some(agent), None, &mut None).expect("scope proof");
    let destination_writer =
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &approved, &root, VolumeOperation::Write, u64::MAX, &mut None)?;
    let plan = host
        .prepare_volume_import(
            &skills,
            &skills_reader,
            &selected.generation,
            &root,
            &destination_writer,
        )
        .await?;
    host.put_content(
        &skills,
        &skills_writer,
        "late.txt",
        b"late",
        "text/plain",
        "late.txt",
        1024,
        &IdempotencyKey::new("late-skill")?,
    )
    .await?;
    assert!(matches!(
        plan.apply(&IdempotencyKey::new("approved-import")?).await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(
        host.read(&root_workspace, None, "/instructions.txt", 1024)
            .await?
            .as_ref(),
        b"session skill"
    );
    assert!(
        host.read(&root_workspace, None, "/late.txt", 1024)
            .await
            .is_err()
    );
    assert_eq!(
        host.read(&skills_workspace, None, "/late.txt", 1024)
            .await?
            .as_ref(),
        b"late"
    );
    Ok(())
}
