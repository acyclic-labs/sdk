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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded consumer workflow keeps grants, pinned import, and source immutability assertions together"
)]
async fn skills_are_explicitly_forked_and_imported_with_read_only_builtins() -> Result<()> {
    let provider = ProviderRef::new("skills", "filesystem", "2")?;
    let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
    let agent = AgentId::from_bytes([1; 16]);
    let foreign_agent = AgentId::from_bytes([2; 16]);
    let root = VolumeRef::new(
        provider.clone(),
        "root-skills",
        VolumeClass::SessionShared,
        VolumeOwner::Session("root".into()),
    )?;
    let skills = VolumeRef::new(
        provider.clone(),
        "editable-skills",
        VolumeClass::SessionShared,
        VolumeOwner::Agent(agent),
    )?;
    let builtins = VolumeRef::new(
        provider.clone(),
        "builtins",
        VolumeClass::Project,
        VolumeOwner::Project("builtin-owner".into()),
    )?;
    let issuer = AuthorityIssuer::new(
        "skills",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "session".into(),
        },
    );
    let setup = issuer.root_for_agent(
        agent,
        "setup",
        Capabilities::new([
            root.capability(VolumeOperation::Write)?,
            builtins.capability(VolumeOperation::Write)?,
        ]),
    );
    let scope = issuer.root_for_agent(
        agent,
        "session",
        Capabilities::new([
            root.capability(VolumeOperation::Read)?,
            skills.capability(VolumeOperation::Read)?,
            skills.capability(VolumeOperation::Write)?,
            builtins.capability(VolumeOperation::Read)?,
        ]),
    );
    host.create_volume(&root).await?;
    host.create_volume(&builtins).await?;
    let root_writer =
        ContentGrant::verify(&issuer.verifier(), &setup, &root, VolumeOperation::Write)?;
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
        &issuer.verifier(),
        &setup,
        &builtins,
        VolumeOperation::Write,
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
    let root_workspace = workspace_ref(provider.clone(), &root.storage_name()?)?;
    let pinned = host.resolve(&root_workspace).await?;
    let root_reader =
        ContentGrant::verify(&issuer.verifier(), &scope, &root, VolumeOperation::Read)?;
    let skills_writer =
        ContentGrant::verify(&issuer.verifier(), &scope, &skills, VolumeOperation::Write)?;
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
            &issuer.verifier(),
            &scope,
            &builtins,
            VolumeOperation::Write
        )
        .is_err()
    );
    assert!(
        ContentGrant::verify(&issuer.verifier(), &scope, &root, VolumeOperation::Write).is_err()
    );
    let foreign = issuer.root_for_agent(
        foreign_agent,
        "foreign",
        Capabilities::new([skills.capability(VolumeOperation::Write)?]),
    );
    assert!(
        ContentGrant::verify(
            &issuer.verifier(),
            &foreign,
            &skills,
            VolumeOperation::Write
        )
        .is_err()
    );
    let builtin_reader =
        ContentGrant::verify(&issuer.verifier(), &scope, &builtins, VolumeOperation::Read)?;
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
    let skills_workspace = workspace_ref(provider.clone(), &skills.storage_name()?)?;
    let selected = host.resolve(&skills_workspace).await?;
    let skills_reader =
        ContentGrant::verify(&issuer.verifier(), &scope, &skills, VolumeOperation::Read)?;
    let approved = issuer.root_for_agent(
        agent,
        "approved-writeback",
        Capabilities::new([root.capability(VolumeOperation::Write)?]),
    );
    let destination_writer =
        ContentGrant::verify(&issuer.verifier(), &approved, &root, VolumeOperation::Write)?;
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
