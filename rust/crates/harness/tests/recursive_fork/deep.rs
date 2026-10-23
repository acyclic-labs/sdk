use super::*;

pub(super) async fn run_thousand_twenty_four_recursive_forks() -> Result<()> {
    let RecursiveState {
        host,
        provider,
        stream,
        stream_provider,
        shared,
        root_agent,
        mut private,
        mut project,
        mut project_head,
        parent_project_workspace,
        mut authority,
        mut issuer,
        mut grant_scope,
        mut aggregate,
        mut previous_file,
        root_attachment,
        root_manifest,
        mut previous_attachment,
        mut previous_project_reference,
        mut previous_shared_reference,
    } = setup().await?;
    let mut final_parent_project = project.clone();
    let mut final_parent_scope = grant_scope.clone();
    let mut final_parent_reducer = retained_reducer(aggregate.reducer(), &issuer)?;
    let mut prepublication_workspaces = None;
    for level in 1..=DEPTH {
        let child_agent = AgentId::from_bytes(identity(level));
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
            format!("project-{level}"),
            VolumeOwner::Project("project".into()),
        )?;
        let controller = ParentProjectController::new(
            &host,
            aggregate.reducer(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            &project,
            None,
        )?;
        let forked = controller
            .fork_project(
                &project_head.generation,
                &child_project,
                &IdempotencyKey::new(format!("project-fork-{level}"))?,
            )
            .await?;
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("recursive-{level}"),
        };
        let child_issuer = AuthorityIssuer::new("qualification", [17; 32], child_authority.clone());
        let child_scope = scope(&child_issuer, &child_private, &shared, &child_project)?;
        let shared_read = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &shared,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
        assert!(
            shared_read
                .require(&shared, VolumeOperation::Read, None)
                .is_ok()
        );
        assert!(
            ContentGrant::verify(
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &child_scope,
                &private,
                VolumeOperation::Read,
                u64::MAX,
                &mut None,
            )
            .is_err()
        );
        let child_write = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let source_private_generation = host
            .resolve(
                &workspace_ref(provider.clone(), &private.storage_name(&mut None)?)?,
                None,
            )
            .await?
            .generation;
        let parent_read = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            &private,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
        let private_fork = host
            .fork_volume(
                &private,
                &parent_read,
                &source_private_generation,
                &child_private,
                &child_write,
                Some(Vec::new()),
                &IdempotencyKey::new(format!("scratch-fork-{level}"))?,
            )
            .await?;
        let controller = ParentProjectController::new(
            &host,
            aggregate.reducer(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            &project,
            None,
        )?;
        let attached_agents = if level == 1 || level == DEEP_RETRY {
            vec![AgentId::from_bytes([201; 16]), root_agent]
        } else {
            Vec::new()
        };
        let parent_resolver = FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            64 * 1_024,
            None,
        )?;
        if level == 1 {
            let dirty_private = volume(
                &provider,
                VolumeClass::AgentPrivate,
                "dirty-scratch".into(),
                VolumeOwner::Agent(child_agent),
            )?;
            let dirty = host.create_volume(&dirty_private).await?;
            host.apply(
                &dirty.workspace,
                Some(&dirty.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/scratch.txt".into(),
                    bytes: b"unselected private state".to_vec().into(),
                }],
                &IdempotencyKey::new("dirty-private-write")?,
            )
            .await?;
            assert!(
                controller
                    .materialize_inherited_conversation(
                        aggregate.reducer(),
                        &dirty_private,
                        &dirty.generation,
                        child_agent,
                        &[],
                        2,
                        2,
                        64 * 1_024,
                        4_096,
                        parent_resolver.as_ref(),
                        &IdempotencyKey::new("dirty-private-prefix")?,
                        &[],
                        None,
                        None,
                        None
                    )
                    .await
                    .is_err(),
                "parent cannot adopt an already-used child private volume"
            );
        }
        let captured = controller
            .materialize_inherited_conversation(
                aggregate.reducer(),
                &child_private,
                &private_fork.generation,
                child_agent,
                &attached_agents,
                if level == 1 { 2 } else { 1 },
                2,
                64 * 1_024,
                4_096,
                parent_resolver.as_ref(),
                &IdempotencyKey::new(format!("inherited-{level}"))?,
                &[],
                None,
                None,
                None,
            )
            .await?;
        if level == 1 {
            assert!(
                controller
                    .materialize_inherited_conversation(
                        aggregate.reducer(),
                        &child_private,
                        &private_fork.generation,
                        child_agent,
                        &[],
                        2,
                        2,
                        64 * 1_024,
                        4_096,
                        parent_resolver.as_ref(),
                        &IdempotencyKey::new("inherited-1")?,
                        &[],
                        None,
                        None,
                        None
                    )
                    .await
                    .is_err(),
                "one retry identity cannot change attached readers"
            );
        }
        let inherited = captured.file.clone();
        let seed = ForkSeed {
            history_witness: None,
            summary: None,
            authorized_source: None,
            child_result_volume: None,
            child_result_generation: None,
            operation_id: OperationId::from_bytes(identity(level + 80)),
            parent: authority.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: child_authority.clone(),
            child_agent,
            resources: vec![
                CapturedResource {
                    source: ResourceRevision::PrivateVolume {
                        volume: (private.clone()).into(),
                        generation: (source_private_generation).into(),
                        paths: Vec::new(),
                    },
                    revision: ResourceRevision::PrivateVolume {
                        volume: (child_private.clone()).into(),
                        generation: (private_fork.generation).into(),
                        paths: Vec::new(),
                    },
                },
                CapturedResource {
                    source: ResourceRevision::History(
                        (StreamRef::new(
                            stream_provider.clone(),
                            authority.stream_path(&mut None)?.into_bytes(),
                            Some(aggregate.reducer().revision().to_string()),
                        )?)
                        .into(),
                    ),
                    revision: ResourceRevision::History(
                        (StreamRef::new(
                            stream_provider.clone(),
                            authority.stream_path(&mut None)?.into_bytes(),
                            Some(aggregate.reducer().revision().to_string()),
                        )?)
                        .into(),
                    ),
                },
                CapturedResource {
                    source: ResourceRevision::Project {
                        volume: (project.clone()).into(),
                        generation: (project_head.generation.clone()).into(),
                    },
                    revision: ResourceRevision::Project {
                        volume: (child_project.clone()).into(),
                        generation: (forked.generation.clone()).into(),
                    },
                },
                CapturedResource {
                    source: ResourceRevision::SharedVolume((shared.clone()).into()),
                    revision: ResourceRevision::SharedVolume((shared.clone()).into()),
                },
            ],
            omissions: Vec::new(),
            child_private_volume: child_private.clone(),
            child_private_generation: captured.generation.clone(),
            inherited_context: vec![inherited.clone()],
            shared_grants: vec![SharedGrant {
                volume: shared.clone(),
                child_agent,
                operations: BTreeSet::from([VolumeOperation::Read]),
            }]
            .into_iter()
            .chain((level == 1 || level == DEEP_RETRY).then_some(SharedGrant {
                volume: shared.clone(),
                child_agent: AgentId::from_bytes([201; 16]),
                operations: BTreeSet::from([VolumeOperation::Read]),
            }))
            .collect(),
            attached_agents,
            reference_grants: captured.reference_grants,
            attachment_manifests: captured.attachment_manifests,
            inherited_through_sequence: if level == 1 { 2 } else { 1 },
            boundary: None,
        };
        seed.validate(&mut None)?;
        if level == DEEP_RETRY {
            let attached = AgentId::from_bytes([201; 16]);
            let readable = seed.reference_capabilities(attached)?;
            assert_eq!(seed.attachment_manifests.len(), 1);
            assert!(readable.contains(&previous_file.read_capability(&mut None)?));
            assert!(readable.contains(&previous_attachment.read_capability(&mut None)?));
            assert!(readable.contains(&inherited.read_capability(&mut None)?));
            assert!(readable.contains(&seed.attachment_manifests[0].read_capability(&mut None)?));
            assert!(
                !readable.contains(&child_private.capability(VolumeOperation::Write, &mut None)?)
            );
            assert!(
                seed.shared_capabilities(attached)?
                    .contains(&shared.capability(VolumeOperation::Read, &mut None)?)
            );
            let reader_scope = child_issuer
                .issue(
                    "deep-attached-reader",
                    &readable,
                    Some(attached),
                    None,
                    &mut None,
                )
                .expect("scope proof");
            let reader = FilesystemContentVerifier::new(
                host.clone(),
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &reader_scope,
                1_024,
                None,
            )?;
            reader
                .verify_manifest(&seed.attachment_manifests[0], 1, &Limits::default(), None)?
                .await?;
            let environment = seed.attached_read_capabilities(attached)?;
            assert!(
                environment.contains(&child_project.capability(VolumeOperation::Read, &mut None)?)
            );
            assert!(environment.contains(&shared.capability(VolumeOperation::Read, &mut None)?));
            assert!(
                !environment
                    .contains(&child_project.capability(VolumeOperation::Write, &mut None)?)
            );
            let environment_scope = child_issuer
                .issue(
                    "deep-attached-environment-reader",
                    &environment,
                    Some(attached),
                    None,
                    &mut None,
                )
                .expect("scope proof");
            let project_read = ContentGrant::verify(
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &environment_scope,
                &child_project,
                VolumeOperation::Read,
                u64::MAX,
                &mut None,
            )?;
            assert_eq!(
                host.read(
                    &forked.workspace,
                    Some(&forked.generation),
                    "/marker.txt",
                    1_024,
                    None
                )
                .await?,
                acyclic_stream::OwnedBytes::from(format!("level {}", level - 1).into_bytes()),
            );
            assert!(
                project_read
                    .require(&child_project, VolumeOperation::Read, None)
                    .is_ok()
            );
        }
        if level == 1 {
            let unbound = issuer
                .issue(
                    "unbound-parent",
                    grant_scope.capabilities(),
                    None,
                    None,
                    &mut None,
                )
                .expect("scope proof");
            assert!(
                host.claim_fork_seed(
                    &seed,
                    aggregate.reducer(),
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    &unbound
                )
                .await
                .is_err()
            );
        }
        host.claim_fork_seed(
            &seed,
            aggregate.reducer(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
        )
        .await?;
        if level == 1 {
            seed_integrity(
                level,
                &seed,
                &child_private,
                &inherited,
                &aggregate,
                &grant_scope,
                child_agent,
                &child_issuer,
                &host,
                &root_manifest,
                &previous_file,
                &root_attachment,
                &shared,
                &private,
                &previous_project_reference,
                &previous_shared_reference,
            )
            .await?;
        }
        if level == 1 {
            publication_rejections(
                &seed,
                child_agent,
                &previous_attachment,
                &mut aggregate,
                &grant_scope,
                &stream,
                &authority,
                &issuer,
                &host,
                &stream_provider,
                &child_private,
                &inherited,
                &provider,
                &shared,
                &child_project,
            )
            .await?;
        }
        let report = ForkReport {
            history_witness: None,
            summary: None,
            child_result_volume: seed.child_result_volume.clone(),
            child_result_generation: seed.child_result_generation.clone(),
            request: ForkRequest {
                authorized_source: seed.authorized_source.clone(),
                operation_id: seed.operation_id,
                parent: seed.parent.clone(),
                parent_revision: seed.parent_revision,
                child: seed.child.clone(),
                child_agent: seed.child_agent,
                attached_agents: seed.attached_agents.clone(),
                preparation: ForkPreparation {
                    child_result_volume: seed.child_result_volume.clone(),
                    summary: None,
                    child_project_volume: match &seed.resources[2].revision {
                        ResourceRevision::Project { volume, .. } => volume.as_ref().clone(),
                        _ => unreachable!("fork seed includes the child project"),
                    },
                    child_private_volume: seed.child_private_volume.clone(),
                    inherited_through_sequence: seed.inherited_through_sequence,
                    maximum_inherited_messages: acyclic_harness::fork::MAX_FORK_INHERITED_MESSAGES,
                    maximum_inherited_bytes: 64 * 1024 * 1024,
                    maximum_inherited_references: 65_536,
                },
                selections: seed
                    .resources
                    .iter()
                    .map(|resource| ForkSelection {
                        revision: resource.source.clone(),
                        required: true,
                    })
                    .collect(),
                boundary: seed.boundary.clone(),
            },
            captures: seed
                .resources
                .iter()
                .cloned()
                .map(|resource| Capture::Captured(Box::new(resource)))
                .collect(),
            child_private_volume: seed.child_private_volume.clone(),
            child_private_generation: seed.child_private_generation.clone(),
            inherited_context: seed.inherited_context.clone(),
            inherited_through_sequence: seed.inherited_through_sequence,
            shared_grants: seed.shared_grants.clone(),
            reference_grants: seed.reference_grants.clone(),
            attachment_manifests: seed.attachment_manifests.clone(),
        };
        assert_eq!(report.clone().into_seed(&mut None)?, seed);
        let mut child_aggregate = StreamAggregate::open(
            &stream,
            (
                child_authority.clone(),
                child_issuer.verifier(&mut None).expect("verifier copy"),
                SchemaRegistry::new(),
            ),
            None,
            acyclic_harness::store::default_projection_read_limits(),
            acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?
        .with_content_verifier(FilesystemContentVerifier::new(
            host.clone(),
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            1_024,
            None,
        )?)
        .with_fork_verifier(CompositeForkVerifier::new(
            &[
                FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
                StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
            ],
            &mut None,
        )?);
        if level == 1 {
            let prebound_stream = Arc::new(MemoryStream::default());
            let mut prebound_child = StreamAggregate::open(
                &prebound_stream,
                (
                    child_authority.clone(),
                    child_issuer.verifier(&mut None).expect("verifier copy"),
                    SchemaRegistry::new(),
                ),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None,
            )
            .await?;
            prebound_child
                .execute(
                    command(
                        1,
                        0,
                        &child_scope,
                        Action::BindConversation { agent: child_agent },
                    )?,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
            assert!(matches!(
                prebound_child
                    .spawn_from_report(
                        &mut aggregate,
                        report.clone(),
                        grant_scope.clone(),
                        child_scope.clone(),
                    )
                    .await,
                Err(acyclic_harness::Error::Conflict(_))
            ));
            assert_eq!(aggregate.reducer().revision(), seed.parent_revision);
        }
        if level == DEPTH {
            prepublication_workspaces = Some(FilesystemProjectWorkspaces::new(
                &host,
                aggregate.reducer(),
                &issuer.verifier(&mut None).expect("verifier copy"),
                &grant_scope,
                project.clone(),
            )?);
        }
        assert_eq!(
            child_aggregate
                .spawn_from_report(
                    &mut aggregate,
                    report.clone(),
                    grant_scope.clone(),
                    child_scope.clone(),
                )
                .await?,
            seed
        );
        assert_eq!(aggregate.reducer().fork(&child_authority), Some(&seed));
        if level == 1 {
            inherited_context(
                &mut aggregate,
                &seed,
                child_agent,
                root_agent,
                &grant_scope,
                &previous_file,
                &child_issuer,
                &host,
            )
            .await?;
        }
        if level == DEEP_RETRY {
            aggregate = StreamAggregate::open(
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
            .with_content_verifier(FilesystemContentVerifier::new(
                host.clone(),
                &issuer.verifier(&mut None).expect("verifier copy"),
                &grant_scope,
                1_024,
                None,
            )?)
            .with_fork_verifier(CompositeForkVerifier::new(
                &[
                    FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
                    StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
                ],
                &mut None,
            )?);
            child_aggregate = StreamAggregate::open(
                &stream,
                (
                    child_authority.clone(),
                    child_issuer.verifier(&mut None).expect("verifier copy"),
                    SchemaRegistry::new(),
                ),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None,
            )
            .await?
            .with_content_verifier(FilesystemContentVerifier::new(
                host.clone(),
                &child_issuer.verifier(&mut None).expect("verifier copy"),
                &child_scope,
                1_024,
                None,
            )?)
            .with_fork_verifier(CompositeForkVerifier::new(
                &[
                    FilesystemForkVerifier::new(host.clone(), 64 * 1_024, None, None)?,
                    StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
                ],
                &mut None,
            )?);
            assert_eq!(aggregate.reducer().fork(&child_authority), Some(&seed));
        }
        if level == 1 || level == DEEP_RETRY {
            assert_eq!(
                child_aggregate
                    .spawn_from_report(
                        &mut aggregate,
                        report,
                        grant_scope.clone(),
                        child_scope.clone(),
                    )
                    .await?,
                seed,
                "lost publication or child-bind acknowledgement reconciles exactly"
            );
        }
        child_aggregate
            .bind_published_child(
                &aggregate,
                &seed,
                child_scope.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let binding = child_aggregate
            .reducer()
            .events_after(0, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                acyclic_harness::Error::Storage("child binding event is missing".into())
            })?;
        assert_eq!(
            binding.causal_parent,
            Some(acyclic_harness::core::EventReference {
                authority: authority.clone(),
                revision: seed.parent_revision + 1,
            })
        );
        let text = format!("level {level}");
        let child_file = host
            .put_content(
                &child_private,
                &child_write,
                "messages/current.txt",
                text.as_bytes(),
                "text/plain",
                "current.txt",
                1_024,
                &IdempotencyKey::new(format!("message-{level}"))?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?;
        let attachment_bytes = format!("evidence at depth {level}");
        let child_attachment = host
            .put_content(
                &child_private,
                &child_write,
                "attachments/evidence.bin",
                attachment_bytes.as_bytes(),
                "application/octet-stream",
                "evidence.bin",
                1_024,
                &IdempotencyKey::new(format!("attachment-{level}"))?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?;
        let child_read = ContentGrant::verify(
            &child_issuer.verifier(&mut None).expect("verifier copy"),
            &child_scope,
            &child_private,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
        assert_eq!(
            host.read_content(&child_attachment, &child_read, 1_024, None)
                .await?,
            acyclic_stream::OwnedBytes::from(attachment_bytes.as_bytes().to_vec())
        );
        assert!(
            host.read_content(&previous_file, &child_read, 1_024, None)
                .await
                .is_err()
        );
        let attachments = if level == DEEP_RETRY - 1 {
            let manifest_bytes = serde_json::to_vec(&vec![Attachment {
                file: child_attachment.clone(),
                label: Some(format!("depth {level}")),
            }])
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
            let manifest = host
                .put_content(
                    &child_private,
                    &child_write,
                    "attachments/manifest.json",
                    &manifest_bytes,
                    "application/vnd.acyclic.harness.attachments+json",
                    "manifest.json",
                    1_024,
                    &IdempotencyKey::new(format!("manifest-{level}"))?,
                    None,
                    acyclic_harness::conversation::ContentAccess::default(),
                )
                .await?;
            ReferencedAttachments::Manifest {
                manifest,
                item_count: 1,
            }
        } else {
            ReferencedAttachments::Inline {
                items: vec![Attachment {
                    file: child_attachment.clone(),
                    label: Some(format!("depth {level}")),
                }],
            }
        };
        child_aggregate
            .execute(
                command(
                    2,
                    1,
                    &child_scope,
                    Action::AppendConversationMessage {
                        message: Box::new(ConversationMessage {
                            id: Uuid::from_bytes(identity(level)),
                            sequence: 1,
                            kind: MessageKind::User,
                            content: child_file.clone(),
                            attachments,
                            reply_to: None,
                            tool_call_id: None,
                            extensions: BTreeMap::new(),
                        }),
                    },
                )?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let updated = host
            .apply(
                &forked.workspace,
                Some(&forked.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/marker.txt".into(),
                    bytes: text.into_bytes().into(),
                }],
                &IdempotencyKey::new(format!("project-write-{level}"))?,
            )
            .await?;
        final_parent_project = project;
        final_parent_scope = grant_scope.clone();
        final_parent_reducer = retained_reducer(aggregate.reducer(), &issuer)?;
        project = child_project;
        project_head = acyclic_harness::filesystem::WorkspaceObservation {
            workspace: forked.workspace,
            generation: updated,
        };
        private = child_private;
        previous_file = child_file;
        previous_attachment = child_attachment;
        previous_project_reference = None;
        previous_shared_reference = None;
        authority = child_authority;
        issuer = child_issuer;
        grant_scope = child_scope;
        aggregate = child_aggregate;
    }
    let last_write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        &private,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let image = host
        .put_content(
            &private,
            &last_write,
            "attachments/final.png",
            &[137, 80, 78, 71],
            "image/png",
            "final.png",
            1_024,
            &IdempotencyKey::new("deep-final-image")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let manifest = host
        .put_attachment_manifest(
            &private,
            &last_write,
            "manifests/final.json",
            &[Attachment {
                file: image.clone(),
                label: None,
            }],
            1_024,
            &IdempotencyKey::new("deep-final-manifest")?,
        )
        .await?;
    aggregate
        .execute(
            command(
                3,
                2,
                &grant_scope,
                Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: Uuid::from_bytes([0xF0; 16]),
                        sequence: 2,
                        kind: MessageKind::User,
                        content: previous_file.clone(),
                        attachments: manifest,
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let state = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| acyclic_harness::Error::Invalid("conversation not bound".into()))?;
    let resolver = FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &grant_scope,
        1_024,
        None,
    )?;
    let selected = select_model_context(
        state,
        (state).logical_revision(),
        ModelContextSelection {
            checkpoint: None,
            conversation_revision: 2,
            message_ids: vec![Uuid::from_bytes([0xF0; 16])],
        },
        resolver.as_ref(),
        acyclic_harness::conversation::Limits {
            context_messages: 256,
            attachments: 2,
            render_bytes: 128 * 1024,
            ..Default::default()
        },
        2,
        None,
    )
    .await?;
    assert!(
        matches!(selected.messages[0].content, ModelContent::Parts(ref parts)
        if matches!(parts.get(1), Some(ModelContentPart::File { file, policy: FileProjectionPolicy::Native(_) }) if file == &image))
    );
    assert_eq!(state.messages().len(), 2);
    assert!(matches!(
        aggregate.reducer().conversation().map(|state| &state.messages()[0].attachments),
        Some(ReferencedAttachments::Inline { items }) if items.len() == 1,
    ));
    assert!(
        ParentProjectController::new(
            &host,
            &final_parent_reducer,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &grant_scope,
            &final_parent_project,
            None,
        )
        .is_err(),
        "child scope must not authorize its parent's project"
    );
    let workspaces = prepublication_workspaces.ok_or_else(|| {
        acyclic_harness::Error::Invalid("missing prepublication project binding".into())
    })?;
    let plan = workspaces
        .prepare_project_import(
            &final_parent_scope,
            &final_parent_reducer,
            &authority,
            &project,
            &host
                .resolve(
                    &workspace_ref(provider.clone(), &(project).storage_name(&mut None)?)?,
                    None,
                )
                .await?
                .generation,
        )
        .await?;
    let merge_operation = OperationId::from_bytes([93; 16]);
    let notice_file = final_parent_reducer
        .conversation()
        .and_then(|conversation| conversation.messages().last())
        .map(|message| message.content.clone())
        .ok_or_else(|| {
            acyclic_harness::Error::Invalid("parent merge notice content is missing".into())
        })?;
    let notice = ConversationMessage {
        id: Uuid::from_bytes([94; 16]),
        sequence: 2,
        kind: MessageKind::Merge,
        content: notice_file,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: BTreeMap::new(),
    };
    let outcome = plan
        .apply(
            &final_parent_scope,
            merge_operation,
            &authority,
            &notice,
            &[],
        )
        .await?;
    let (ProjectJoinOutcome::Applied(receipt) | ProjectJoinOutcome::AlreadyApplied(receipt)) =
        outcome
    else {
        return Err(acyclic_harness::Error::Conflict(
            "deep project merge was not applied".into(),
        ));
    };
    let merge_verifier = FilesystemProjectMergeVerifier::new(host.clone(), None)?;
    merge_verifier.verify(&receipt).await?;
    assert!(
        host.read(
            &parent_project_workspace,
            None,
            "/messages/current.txt",
            1_024,
            None
        )
        .await
        .is_err()
    );
    let mut forged_source = receipt.clone();
    forged_source.source_generation = receipt.expected_target_generation.clone();
    assert!(merge_verifier.verify(&forged_source).await.is_err());
    let mut forged_operation = receipt.clone();
    forged_operation.provider_operation_id = vec![95; 16];
    assert!(merge_verifier.verify(&forged_operation).await.is_err());
    let mut forged_harness_operation = receipt.clone();
    forged_harness_operation.operation_id = OperationId::from_bytes([96; 16]);
    assert!(
        merge_verifier
            .verify(&forged_harness_operation)
            .await
            .is_err()
    );
    let mut forged_proof = receipt.clone();
    forged_proof.provider_proof.statement["source_generation"] =
        serde_json::to_value([0_u8; 32])
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert!(merge_verifier.verify(&forged_proof).await.is_err());
    Ok(())
}
