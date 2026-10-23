use super::*;

#[tokio::test]
async fn local_deep_same_path_recursive_forks_keep_parent_controls() -> Result<()> {
    const N: u8 = 1;
    const DEPTH: u8 = 10 * N;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let fs_options = LocalOptions::new(directory.path().join("filesystem"));
    let stream_root = directory.path().join("streams");
    let provider = ProviderRef::new(
        "local-deep-e2e".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let stream_provider = ProviderRef::new(
        "local-deep-e2e".into(),
        "stream".into(),
        "2".into(),
        &mut None,
    )?;
    let fs = Fs::local(fs_options)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = FilesystemHost::new(fs.clone(), &provider, None)?;
    let stream = Arc::new(
        LocalStream::open(&stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    );

    let mut authority = Authority {
        kind: AggregateKind::Conversation,
        id: "deep-root".into(),
    };
    let mut agent = AgentId::from_bytes([0; 16]);
    let mut issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], authority.clone());
    let mut private = volume(
        &provider,
        VolumeClass::AgentPrivate,
        "scratch".into(),
        VolumeOwner::Agent(agent),
    )?;
    let mut project = volume(
        &provider,
        VolumeClass::Project,
        "deep-project-0".into(),
        VolumeOwner::Project("deep-project".into()),
    )?;
    let mut grant_scope = scope(&issuer, agent, &private, &project)?;
    let mut project_head = host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let mut aggregate = open_aggregate(
        &stream,
        authority.clone(),
        &issuer,
        host.clone(),
        grant_scope.clone(),
        stream_provider.clone(),
    )
    .await?;
    aggregate
        .execute(
            command(1, 0, &grant_scope, Action::BindConversation { agent })?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let root_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &private,
        VolumeOperation::Write,
        65_536,
        &mut None,
    )?;
    let mut previous_file = host
        .put_content(
            &private,
            &root_write,
            "messages/current.txt",
            b"deep level 0",
            "text/plain",
            "current.txt",
            1_024,
            &IdempotencyKey::new("deep-message-0")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let mut previous_attachment = host
        .put_content(
            &private,
            &root_write,
            "attachments/evidence.bin",
            b"deep attachment 0",
            "application/octet-stream",
            "evidence.bin",
            1_024,
            &IdempotencyKey::new("deep-attachment-0")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    aggregate
        .execute(
            command(
                2,
                1,
                &grant_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([2; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: previous_file.clone(),
                        attachments: ReferencedAttachments::Inline {
                            items: vec![Attachment {
                                file: previous_attachment.clone(),
                                label: None,
                            }],
                        },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: Default::default(),
                    }),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;

    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes([level; 16]);
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("deep-child-{level}"),
        };
        let child_issuer = AuthorityIssuer::new("local-deep-e2e", [9; 32], child_authority.clone());
        let child_private = volume(
            &provider,
            VolumeClass::AgentPrivate,
            "scratch".into(),
            VolumeOwner::Agent(child_agent),
        )?;
        assert_ne!(
            private.storage_name(&mut None)?,
            child_private.storage_name(&mut None)?
        );
        let child_project = volume(
            &provider,
            VolumeClass::Project,
            format!("deep-project-{level}"),
            VolumeOwner::Project("deep-project".into()),
        )?;
        let child_scope = scope(&child_issuer, child_agent, &child_private, &child_project)?;
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            8 * 1_024,
            None,
        )?;
        let preparer = FilesystemForkPreparer::new(
            host.clone(),
            acyclic_harness::core::Reducer::restore(
                aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
                issuer.verifier(&mut None)?,
                SchemaRegistry::new(),
                &mut None,
            )?,
            issuer.verifier(&mut None).expect("verifier copy"),
            grant_scope.clone(),
            project.clone(),
            stream_provider.clone(),
            reader,
            None,
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )?;
        let request = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes(identity(20 + level)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_result_volume: None,
                summary: None,
                child_project_volume: child_project.clone(),
                child_private_volume: child_private.clone(),
                inherited_through_sequence: 1,
                maximum_inherited_messages: 2,
                maximum_inherited_bytes: 8 * 1_024,
                maximum_inherited_references: 4,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: (private.clone()).into(),
                        generation: (host
                            .resolve(
                                &workspace_ref(
                                    provider.clone(),
                                    &private.storage_name(&mut None)?,
                                )?,
                                None,
                            )
                            .await?
                            .generation)
                            .into(),
                        paths: Vec::new(),
                    },
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(
                        (StreamRef::new(
                            stream_provider.clone(),
                            authority.stream_path(&mut None)?.into_bytes(),
                            Some(aggregate.reducer().revision().to_string()),
                        )?)
                        .into(),
                    ),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: (project.clone()).into(),
                        generation: (project_head.generation.clone()).into(),
                    },
                },
            ],
            boundary: None,
        };
        let report = aggregate.prepare_fork(&preparer, request).await?;
        let reference_capability = report
            .reference_grants
            .iter()
            .find(|grant| grant.reader == child_agent && grant.file == previous_attachment)
            .map(|grant| grant.capability())
            .transpose()?
            .ok_or_else(|| Error::Invalid("deep child attachment grant is missing".into()))?;
        let inherited_file = report
            .inherited_context
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("deep inherited context is missing".into()))?;
        let mut child_aggregate = open_aggregate(
            &stream,
            child_authority.clone(),
            &child_issuer,
            host.clone(),
            child_scope.clone(),
            stream_provider.clone(),
        )
        .await?;
        child_aggregate
            .spawn_from_report(
                &mut aggregate,
                report,
                grant_scope.clone(),
                child_scope.clone(),
            )
            .await?;

        if level == N || level == DEPTH {
            let delegated = issuer.delegate_private_file_read(
                &grant_scope,
                child_agent,
                format!("deep-parent-attachment-{level}"),
                &previous_attachment,
            )?;
            let exact_parent_read = ContentGrant::verify_read(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &delegated,
                &previous_attachment,
            )?;
            assert_eq!(
                host.read_content(&previous_attachment, &exact_parent_read, 1_024, None)
                    .await?
                    .as_ref(),
                format!("deep attachment {}", level - 1).as_bytes(),
            );
            assert!(
                ContentGrant::verify_read(
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &delegated,
                    &previous_file
                )
                .is_err()
            );

            let child_read = ContentGrant::verify(
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &child_scope,
                &child_private,
                VolumeOperation::Read,
                65_536,
                &mut None,
            )?;
            assert!(
                !host
                    .read_content(&inherited_file, &child_read, 8 * 1_024, None)
                    .await?
                    .is_empty()
            );
            assert!(
                host.read_content(&previous_file, &child_read, 1_024, None)
                    .await
                    .is_err()
            );
            assert!(
                host.read_content(&previous_attachment, &child_read, 1_024, None)
                    .await
                    .is_err()
            );
            let reference_scope = child_issuer
                .issue(
                    &format!("deep-reference-reader-{level}"),
                    &Capabilities::new([
                        child_private.capability(VolumeOperation::Read, &mut None)?,
                        reference_capability,
                    ]),
                    Some(child_agent),
                    None,
                    &mut None,
                )
                .expect("scope proof");
            let child_reference_read = ContentGrant::verify_read(
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &reference_scope,
                &previous_attachment,
            )?;
            assert_eq!(
                host.read_content(&previous_attachment, &child_reference_read, 1_024, None)
                    .await?
                    .as_ref(),
                format!("deep attachment {}", level - 1).as_bytes(),
            );
            assert!(
                ContentGrant::verify_read(
                    &child_issuer.verifier(&mut None).expect("verifier copy"),
                    &reference_scope,
                    &previous_file
                )
                .is_err()
            );
        }

        let child_write = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
            65_536,
            &mut None,
        )?;
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                "messages/current.txt",
                format!("deep level {level}").as_bytes(),
                "text/plain",
                "current.txt",
                1_024,
                &IdempotencyKey::new(format!("deep-message-{level}"))?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?;
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                "attachments/evidence.bin",
                format!("deep attachment {level}").as_bytes(),
                "application/octet-stream",
                "evidence.bin",
                1_024,
                &IdempotencyKey::new(format!("deep-attachment-{level}"))?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?;
        child_aggregate
            .execute(
                command(
                    100 + level,
                    1,
                    &child_scope,
                    Action::AppendConversationMessage {
                        message: Box::new(ConversationMessage {
                            id: Uuid::from_bytes([100 + level; 16]),
                            sequence: 1,
                            kind: MessageKind::User,
                            content: child_file.clone(),
                            attachments: ReferencedAttachments::Inline {
                                items: vec![Attachment {
                                    file: child_attachment.clone(),
                                    label: None,
                                }],
                            },
                            reply_to: None,
                            tool_call_id: None,
                            extensions: Default::default(),
                        }),
                    },
                )?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        project_head = host
            .resolve(
                &workspace_ref(provider.clone(), &child_project.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        // Fork depth must not change what reading a workspace costs. Each
        // level's tree has its own hashed file identities, which order its
        // file table differently, and a fork's first generation records its
        // source, so a level is not compared with its parent. Instead two
        // successive untouched forks of the level, whose trees and head
        // records have the same shape, must cost exactly the same.
        let child_verifier = child_issuer.verifier(&mut None).expect("verifier copy");
        let mut source = (child_private.clone(), child_scope.clone());
        let mut work = Vec::new();
        for echo in ["echo", "echo-echo"] {
            let echo = volume(
                &provider,
                VolumeClass::AgentPrivate,
                echo.into(),
                VolumeOwner::Agent(child_agent),
            )?;
            let echo_scope = scope(&child_issuer, child_agent, &echo, &child_project)?;
            host.fork_volume(
                &source.0,
                &ContentGrant::verify(
                    &child_verifier,
                    &source.1,
                    &source.0,
                    VolumeOperation::Read,
                    65_536,
                    &mut None,
                )?,
                &host
                    .resolve(
                        &workspace_ref(provider.clone(), &source.0.storage_name(&mut None)?)?,
                        None,
                    )
                    .await?
                    .generation,
                &echo,
                &ContentGrant::verify(
                    &child_verifier,
                    &echo_scope,
                    &echo,
                    VolumeOperation::Write,
                    65_536,
                    &mut None,
                )?,
                None,
                &IdempotencyKey::new(format!("deep-echo-{level}-{}", work.len()))?,
            )
            .await?;
            work.push(read_work(&fs, &echo).await?);
            source = (echo, echo_scope);
        }
        assert_eq!(
            work[1], work[0],
            "one more fork level changed the read work at level {level}"
        );
        previous_file = child_file;
        previous_attachment = child_attachment;
        private = child_private;
        project = child_project;
        agent = child_agent;
        authority = child_authority;
        issuer = child_issuer;
        grant_scope = child_scope;
        aggregate = child_aggregate;
    }

    assert_eq!(aggregate.reducer().revision(), 2);
    assert_eq!(agent, AgentId::from_bytes([DEPTH; 16]));
    assert_eq!(project.id(), format!("deep-project-{DEPTH}"));
    Ok(())
}

/// Exact work to open a workspace and list its root.
async fn read_work<A, O>(fs: &Fs<A, O>, volume: &VolumeRef) -> Result<serde_json::Value>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let cancellation = acyclic_fs::CancellationToken::new();
    let opened = fs
        .open_workspace(
            volume.storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| Error::Storage(failure.error.to_string()))?;
    let listed = opened
        .value
        .list_directory(
            "/",
            None,
            3,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| Error::Storage(failure.error.to_string()))?;
    serde_json::to_value(
        opened
            .work
            .checked_add(listed.work)
            .map_err(|error| Error::Storage(error.to_string()))?,
    )
    .map_err(|error| Error::Storage(error.to_string()))
}
