use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    /// A staged payload is recovered only by its original operation/key and a
    /// signed read grant; neither another identity nor a write grant suffices.
    #[test]
    fn staged_lookup_preserves_identity_and_requires_read(
        identity in any::<[u8; 16]>(),
        key in prop_oneof!["[a-z]{1,12}", "summary:[0-9]:output", "backstop:[0-9]:marker", "summary:[0-9]:part:[0-9]:rewrite", "summary:[0-9]:part:[0-9]:output", "summary:[0-9]:output:[0-9a-f]{64}", "tool:[0-9]:[a-z]{1,12}:result", "tool:[0-9]:[a-z]{1,12}:result:[0-9a-f]{64}"],
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
        published in any::<bool>(),
        text in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap()
            .block_on(check(identity, &key, &bytes, published, text)).unwrap();
    }
}

async fn check(
    identity: [u8; 16],
    key: &str,
    bytes: &[u8],
    published: bool,
    text: bool,
) -> Result<()> {
    let media_type = if text {
        "text/plain"
    } else {
        "application/octet-stream"
    };
    let display_name = if text {
        "execution.txt"
    } else {
        "execution.json"
    };
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
    let issuer = crate::core::AuthorityRoot::host_owned(
        "staged-lookup",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Task,
            id: crate::TaskId::from_bytes([5; 16]).to_string(),
        },
    );
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    // The journal is host custody: its writer always holds the host role.
    let custody = crate::contract::capability::HARNESS_HOST.to_owned();
    let scope = issuer.bootstrap_scope(
        "owner",
        &crate::Capabilities::new([read, write.clone(), custody.clone()]),
        Some(agent),
        &mut None,
    )?;
    let mut journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            io: None,
        },
    )?;
    let operation = OperationId::from_bytes(identity);
    let original = if published {
        let (identity, _) = journal.stage_identity_with_io(operation, key, None)?;
        let path = format!(".system/execution/{operation}/retained.json");
        let grant = ContentGrant::verify(
            &journal.verifier,
            &scope,
            &volume,
            VolumeOperation::Write,
            &mut None,
        )?;
        Some(
            journal
                .host
                .put_internal_content(
                    &volume,
                    &grant,
                    &path,
                    bytes,
                    media_type,
                    display_name,
                    Some(4096),
                    &identity,
                    InternalContentClass::Execution,
                    None,
                )
                .await?,
        )
    } else {
        None
    };
    let expected = journal
        .stage(operation, key, bytes, media_type, None)?
        .await?;
    if let Some(original) = original {
        assert_eq!(
            expected, original,
            "retry must preserve the original receipt"
        );
    }
    assert_eq!(
        journal
            .stage(operation, key, bytes, media_type, None)?
            .await?,
        expected
    );
    let mut changed = bytes.to_vec();
    changed.push(0);
    for (bytes, media_type) in [
        (changed.as_slice(), media_type),
        (
            bytes,
            if text {
                "application/octet-stream"
            } else {
                "text/plain"
            },
        ),
    ] {
        assert!(matches!(
            journal
                .stage(operation, key, bytes, media_type, None)?
                .await,
            Err(Error::Conflict(_))
        ));
    }
    let recovered = (&journal as &dyn ExecutionJournal)
        .staged_ref(operation, key, None)?
        .await?
        .expect("original staged payload");
    assert_eq!(recovered, expected);
    assert_eq!(journal.load(&recovered, None)?.await?.as_ref(), bytes);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1 << 20, 1 << 20),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let funded = (&journal as &dyn ExecutionJournal)
        .staged_ref(operation, key, Some(&io))?
        .await?;
    assert_eq!(funded.as_ref(), Some(&expected));
    let occupied = work.claim(
        work.remaining()?.peak_allocation_bytes,
        1,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let refused = match (&journal as &dyn ExecutionJournal).staged_ref(operation, key, Some(&io)) {
        Ok(future) => future.await,
        Err(error) => Err(error),
    };
    assert!(
        matches!(refused, Err(Error::NativeWork { .. })),
        "staged lookup must retain original allocation refusal"
    );
    drop(occupied);
    cancellation.cancel();
    let refused = match (&journal as &dyn ExecutionJournal).staged_ref(operation, key, Some(&io)) {
        Ok(future) => future.await,
        Err(error) => Err(error),
    };
    assert!(matches!(refused, Err(error) if error.code() == crate::wire::ErrorCode::Cancelled));
    let other = OperationId::from_bytes(identity.map(|byte| byte ^ 1));
    for (operation, key) in [(other, key.to_owned()), (operation, format!("{key}-other"))] {
        assert!(matches!(
            (&journal as &dyn ExecutionJournal)
                .staged_ref(operation, &key, None)?
                .await,
            Ok(None)
        ));
    }
    journal.scope = issuer.bootstrap_scope(
        "writer",
        &crate::Capabilities::new([write, custody.clone()]),
        Some(agent),
        &mut None,
    )?;
    assert!(matches!(
        (&journal as &dyn ExecutionJournal)
            .staged_ref(operation, key, None)?
            .await,
        Err(Error::Unauthorized(_))
    ));
    // The original authenticated reader still sees the retained payload, so
    // the writer's denial above cannot pass because the payload is absent.
    let (identity, _) = journal.stage_identity_with_io(operation, key, None)?;
    let path = expected.path();
    assert_eq!(
        journal
            .host
            .content_for_operation_record_io(
                super::super::super::PublicationLookup {
                    volume: &volume,
                    key: &identity,
                    expected_path: Some(&path),
                    maximum_bytes: Some(4096),
                    internal: true,
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
            })
            .as_ref(),
        Some(&expected)
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
        let scoped = issuer.bootstrap_scope(
            "projection",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                custody.clone(),
                capability
                    .to_scope_label()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ]),
            Some(agent),
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
                        maximum_bytes: Some(4096),
                        internal: true,
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
