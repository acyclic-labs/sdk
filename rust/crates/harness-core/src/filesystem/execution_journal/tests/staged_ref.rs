use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    /// A staged payload is recovered only by its original operation/key and a
    /// signed read grant; neither another identity nor a write grant suffices.
    #[test]
    fn staged_lookup_preserves_identity_and_requires_read(
        identity in any::<[u8; 16]>(),
        key in "[a-z]{1,12}",
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap()
            .block_on(check(identity, &key, &bytes)).unwrap();
    }
}

async fn check(identity: [u8; 16], key: &str, bytes: &[u8]) -> Result<()> {
    let provider = ProviderRef::new(
        "staged-lookup".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([4; 16]);
    let volume = VolumeRef::new(
        provider,
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "staged-lookup",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Task,
            id: crate::TaskId::from_bytes([5; 16]).to_string(),
        },
    );
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([read, write.clone()]),
        Some(agent),
        None,
        &mut None,
    )?;
    let mut journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 4096,
            io: None,
        },
    )?;
    let operation = OperationId::from_bytes(identity);
    let expected = journal
        .stage(operation, key, bytes, "application/octet-stream", None)?
        .await?;
    let recovered = journal.staged_ref_with_io(operation, key, None).await?;
    assert_eq!(recovered, expected);
    assert_eq!(journal.load(&recovered, None)?.await?.as_ref(), bytes);
    let other = OperationId::from_bytes(identity.map(|byte| byte ^ 1));
    for (operation, key) in [(other, key.to_owned()), (operation, format!("{key}-other"))] {
        assert!(matches!(
            journal.staged_ref_with_io(operation, &key, None).await,
            Err(Error::NotFound(_))
        ));
    }
    journal.scope = issuer.issue(
        "writer",
        &crate::Capabilities::new([write]),
        Some(agent),
        None,
        &mut None,
    )?;
    assert!(matches!(
        journal.staged_ref_with_io(operation, key, None).await,
        Err(Error::Unauthorized(_))
    ));
    // The original authenticated reader still sees the retained payload, so
    // the writer's denial above cannot pass because the payload is absent.
    let (identity, path) = journal.stage_identity_with_io(operation, key, None)?;
    assert_eq!(
        journal
            .host
            .content_for_operation_record_io(
                super::super::super::PublicationLookup {
                    volume: &volume,
                    key: &identity,
                    expected_path: Some(&path),
                    maximum_bytes: 4096,
                    internal_class: Some(InternalContentClass::Execution),
                    purpose: super::super::super::PublicationPurpose::Content,
                    scope: &scope,
                    ceiling: None,
                },
                None,
            )
            .await?
            .map(|(file, body)| {
                assert_eq!(body.expect("authorized payload").as_ref(), bytes);
                file
            }),
        Some(expected)
    );
    // Projection cannot return payload bytes through a metadata-only request,
    // and asking for payload independently requires Read in the same compiler.
    use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
    for read in [false, true] {
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: [Action::List, Action::ReadHistory]
                    .into_iter()
                    .chain(read.then_some(Action::Read))
                    .collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        };
        let scoped = issuer.issue(
            "projection",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                capability
                    .to_scope_label()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        for read_content in [false, true] {
            let result = journal
                .host
                .content_for_operation_record_io(
                    super::super::super::PublicationLookup {
                        volume: &volume,
                        key: &identity,
                        expected_path: Some(&path),
                        maximum_bytes: 4096,
                        internal_class: Some(InternalContentClass::Execution),
                        purpose: if read_content {
                            super::super::super::PublicationPurpose::Content
                        } else {
                            super::super::super::PublicationPurpose::Metadata
                        },
                        scope: &scoped,
                        ceiling: None,
                    },
                    None,
                )
                .await;
            if read_content && !read {
                assert_eq!(
                    result.unwrap_err().code(),
                    crate::wire::ErrorCode::Unauthorized
                );
            } else {
                let (_, payload) = result?.expect("original publication");
                assert_eq!(
                    payload.as_ref().map(AsRef::as_ref),
                    read_content.then_some(bytes)
                );
            }
        }
    }
    Ok(())
}
