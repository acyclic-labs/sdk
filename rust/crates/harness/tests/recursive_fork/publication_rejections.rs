use super::*;

pub(super) async fn publication_rejections(
    seed: &ForkSeed,
    child_agent: AgentId,
    previous_attachment: &FileRef,
    aggregate: &mut StreamAggregate<MemoryStream>,
    grant_scope: &Scope,
    stream: &Arc<MemoryStream>,
    authority: &Authority,
    issuer: &AuthorityIssuer,
    host: &Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    stream_provider: &ProviderRef,
    child_private: &VolumeRef,
    inherited: &FileRef,
    provider: &ProviderRef,
    shared: &VolumeRef,
    child_project: &VolumeRef,
) -> Result<()> {
    let mut missing_manifest_reader = seed.clone();
    missing_manifest_reader.reference_grants.retain(|grant| {
        !(grant.reader == child_agent
            && grant.file == seed.attachment_manifests[0]
            && grant.attachment_manifest.is_none())
    });
    assert!(missing_manifest_reader.validate(&mut None).is_err());
    let mut missing_member = seed.clone();
    missing_member.reference_grants.retain(|grant| {
        !(grant.reader == child_agent
            && &grant.file == previous_attachment
            && grant.attachment_manifest.is_some())
    });
    assert!(missing_member.validate(&mut None).is_ok());
    assert!(
        aggregate
            .execute(
                fork_command(217, seed.parent_revision, &grant_scope, missing_member,)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
    let mut over_budget = StreamAggregate::open(
        &stream,
        (
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(host.clone(), 64 * 1_024, Some(1), None)?,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?);
    assert!(
        over_budget
            .execute(
                fork_command(216, seed.parent_revision, &grant_scope, seed.clone(),)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(over_budget.reducer().revision(), seed.parent_revision);
    let mut incomplete = StreamAggregate::open(
        &stream,
        (
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_fork_verifier(CompositeForkVerifier::new(
        &[FilesystemForkVerifier::new(
            host.clone(),
            64 * 1_024,
            None,
            None,
        )?],
        &mut None,
    )?);
    assert!(
        incomplete
            .execute(
                fork_command(219, seed.parent_revision, &grant_scope, seed.clone(),)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(incomplete.reducer().revision(), seed.parent_revision);
    let mut foreign_history = seed.clone();
    let foreign = ProviderRef::new("foreign".into(), "stream".into(), "2".into(), &mut None)?;
    let reference = ResourceRevision::History(StreamRef::new(
        foreign,
        authority.stream_path(&mut None)?.into_bytes(),
        Some(seed.parent_revision.to_string()),
    )?);
    foreign_history.resources[1].source = reference.clone();
    foreign_history.resources[1].revision = reference;
    foreign_history.validate(&mut None)?;
    assert!(
        aggregate
            .execute(
                fork_command(218, seed.parent_revision, &grant_scope, foreign_history,)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
    let mut missing = seed.clone();
    missing.inherited_context[0] = FileRef::new(
        child_private.clone(),
        ".system/inherited-conversation/missing.txt".into(),
        missing.inherited_context[0].version().into(),
        missing.inherited_context[0].descriptor().clone(),
        "missing.txt".into(),
        &mut None,
    )?;
    for grant in &mut missing.reference_grants {
        if &grant.file == inherited {
            grant.file = missing.inherited_context[0].clone();
        }
    }
    assert!(
        aggregate
            .execute(
                fork_command(220, aggregate.reducer().revision(), &grant_scope, missing,)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
    let rogue_agent = AgentId::from_bytes([200; 16]);
    let rogue_private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(rogue_agent),
    )?;
    let rogue_head = host.create_volume(&rogue_private).await?;
    let rogue_authority = Authority {
        kind: AggregateKind::Conversation,
        id: "rogue-child".into(),
    };
    let rogue_issuer = AuthorityIssuer::new("qualification", [17; 32], rogue_authority.clone());
    let rogue_scope = scope(&rogue_issuer, &rogue_private, &shared, &child_project)?;
    let rogue_write = ContentGrant::verify(
        &rogue_issuer.verifier(&mut None).expect("verifier copy"),
        &rogue_scope,
        &rogue_private,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    host.put_content(
        &rogue_private,
        &rogue_write,
        "secret.txt",
        b"preexisting scratch",
        "text/plain",
        "secret.txt",
        1_024,
        &IdempotencyKey::new("rogue-scratch")?,
        None,
        None,
        None,
    )
    .await?;
    let mut rogue_seed = seed.clone();
    rogue_seed.child = rogue_authority;
    rogue_seed.child_agent = rogue_agent;
    rogue_seed.shared_grants[0].child_agent = rogue_agent;
    rogue_seed.shared_grants.truncate(1);
    rogue_seed.child_private_volume = rogue_private.clone();
    rogue_seed.child_private_generation = rogue_head.generation.clone();
    rogue_seed.resources[0].revision = ResourceRevision::PrivateVolume {
        volume: rogue_private,
        generation: rogue_head.generation,
        paths: Vec::new(),
    };
    rogue_seed.inherited_context.clear();
    rogue_seed.inherited_through_sequence = 0;
    rogue_seed.attached_agents.clear();
    rogue_seed
        .reference_grants
        .retain(|grant| grant.reader == child_agent);
    for grant in &mut rogue_seed.reference_grants {
        grant.reader = rogue_agent;
    }
    rogue_seed.validate(&mut None)?;
    assert!(
        aggregate
            .execute(
                fork_command(
                    221,
                    aggregate.reducer().revision(),
                    &grant_scope,
                    rogue_seed,
                )?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), seed.parent_revision);

    Ok(())
}
