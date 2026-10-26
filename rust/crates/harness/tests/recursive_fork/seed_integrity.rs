use super::*;

pub(super) async fn seed_integrity(
    level: u16,
    seed: &ForkSeed,
    child_private: &VolumeRef,
    inherited: &FileRef,
    aggregate: &StreamAggregate<MemoryStream>,
    grant_scope: &Scope,
    child_agent: AgentId,
    child_issuer: &AuthorityRoot,
    host: &Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    root_manifest: &FileRef,
    previous_file: &FileRef,
    root_attachment: &FileRef,
    shared: &VolumeRef,
    private: &VolumeRef,
    previous_project_reference: &Option<FileRef>,
    previous_shared_reference: &Option<FileRef>,
) -> Result<()> {
    let mut forged_prefix = seed.clone();
    forged_prefix.inherited_context[0] = FileRef::new(
        child_private.clone(),
        inherited.path().into(),
        inherited.version().into(),
        FileDescriptor::from_bytes(
            b"forged parent transcript",
            "application/vnd.acyclic.harness.inherited-conversation+json",
            &mut None,
        )?,
        inherited.display_name().into(),
        &mut None,
    )?;
    assert!(
        aggregate
            .reducer()
            .plan(&fork_command(
                level + 80,
                aggregate.reducer().revision(),
                &grant_scope,
                forged_prefix,
            )?)
            .is_err(),
        "fork must bind the exact authoritative parent prefix"
    );
    let mut renamed_prefix = seed.clone();
    renamed_prefix.inherited_context[0] = FileRef::new(
        child_private.clone(),
        inherited.path().into(),
        inherited.version().into(),
        inherited.descriptor().clone(),
        "misleading-name.json".into(),
        &mut None,
    )?;
    assert!(
        aggregate
            .reducer()
            .plan(&fork_command(
                level + 81,
                aggregate.reducer().revision(),
                &grant_scope,
                renamed_prefix,
            )?)
            .is_err(),
        "fork must bind inherited prefix display metadata"
    );
    let mut extra_reference = seed.clone();
    extra_reference
        .reference_grants
        .push(acyclic_harness::fork::ReferenceGrant {
            file: FileRef::new(
                previous_file.volume().clone(),
                previous_file.path().into(),
                previous_file.version().into(),
                previous_file.descriptor().clone(),
                "unpublished-display-name.txt".into(),
                &mut None,
            )?,
            reader: child_agent,
            attachment_manifest: None,
        });
    extra_reference.validate(&mut None)?;
    assert!(
        aggregate
            .reducer()
            .plan(&fork_command(
                level + 82,
                aggregate.reducer().revision(),
                &grant_scope,
                extra_reference
            )?)
            .is_err(),
        "a retained original grant cannot authorize extra unpublished FileRef metadata"
    );
    let child_reference_scope = child_issuer
        .bootstrap_scope(
            "child-reference-reader",
            &seed.reference_capabilities(child_agent)?,
            None,
            &mut None,
        )
        .expect("scope proof");
    let child_reference_resolver = FilesystemContentVerifier::new(
        host.clone(),
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &child_reference_scope,
        1_024,
        None,
    )?;
    child_reference_resolver
        .verify_manifest(&root_manifest, 1, &Limits::default(), None)?
        .await?;
    let inherited_reader = AgentId::from_bytes([201; 16]);
    let capabilities = seed.reference_capabilities(inherited_reader)?;
    assert!(capabilities.contains(&previous_file.read_capability(&mut None)?));
    assert!(capabilities.contains(&inherited.read_capability(&mut None)?));
    assert!(capabilities.contains(&root_attachment.read_capability(&mut None)?));
    assert!(
        seed.shared_capabilities(inherited_reader)?
            .contains(&shared.capability(VolumeOperation::Read, &mut None)?)
    );
    let reader_scope = child_issuer
        .bootstrap_scope(
            "attached-reader",
            &capabilities,
            Some(inherited_reader),
            &mut None,
        )
        .expect("scope proof");
    let exact = ContentGrant::verify_file_read(
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &reader_scope,
        &previous_file,
    )?;
    assert_eq!(
        host.read_content(&previous_file, &exact, 1_024, None)
            .await?
            .as_ref(),
        b"root"
    );
    let inherited_resolver = FilesystemContentVerifier::new(
        host.clone(),
        &child_issuer.verifier(&mut None).expect("verifier copy"),
        &reader_scope,
        1_024,
        None,
    )?;
    inherited_resolver
        .verify(&previous_file, 1_024, None)?
        .await?;
    inherited_resolver
        .verify(&root_attachment, 1_024, None)?
        .await?;
    inherited_resolver
        .verify_manifest(&root_manifest, 1, &Limits::default(), None)?
        .await?;
    inherited_resolver
        .verify(
            previous_project_reference
                .as_ref()
                .ok_or_else(|| acyclic_harness::Error::Invalid("project ref missing".into()))?,
            1_024,
            None,
        )?
        .await?;
    inherited_resolver
        .verify(
            previous_shared_reference
                .as_ref()
                .ok_or_else(|| acyclic_harness::Error::Invalid("shared ref missing".into()))?,
            1_024,
            None,
        )?
        .await?;
    assert!(exact.require_file_read(&inherited, None).is_err());
    assert!(
        exact
            .require(&private, VolumeOperation::Write, None)
            .is_err()
    );

    Ok(())
}
