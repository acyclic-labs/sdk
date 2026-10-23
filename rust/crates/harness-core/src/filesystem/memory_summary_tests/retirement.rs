use super::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "actual retired parent, full fork publication and cold inherited paging"
)]
async fn full_fork_after_retirement_requires_exact_archive_witness() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 262_144).await?;
    let limits = Limits {
        context_messages: 8,
        ..Limits::default()
    };
    let model = Arc::new(summary_model(12));
    let bundle = storage
        .builder()
        .model(
            Model::new(
                "synthetic".into(),
                "retired-full-fork".into(),
                "1".into(),
                Value::Null,
            )?,
            model,
        )
        .grant("model:generate")
        .limits(limits)
        .build()?;
    for index in 0..6 {
        let operation = OperationId::new();
        let input = storage
            .stage(
                operation,
                &format!("full/{index}.txt"),
                b"small input",
                "text/plain",
                "input.txt",
            )
            .await?;
        storage
            .run_conversation(&bundle, operation, input, Vec::new(), 1)
            .await?;
    }
    let mut parent = storage.journal.conversation_projection(limits).await?;
    let conversation = parent
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("missing full-fork parent".into()))?;
    assert!(conversation.resident_after_sequence() > 0);
    assert_eq!(conversation.logical_revision(), 12);
    let history = HistoryReader::new(
        &storage.stream,
        &storage.conversation,
        &(storage.verifier()),
        &mut None,
    )?;
    assert!(
        history
            .fork_history(
                parent.reducer(),
                12,
                HistoryReadLimits {
                    maximum_events: 11,
                    maximum_bytes: 2_097_152
                },
                &mut None
            )
            .await
            .is_err()
    );
    let archived = history
        .fork_history(
            parent.reducer(),
            12,
            HistoryReadLimits {
                maximum_events: 12,
                maximum_bytes: 2_097_152,
            },
            &mut None,
        )
        .await?;
    let provider = storage.volume.provider().clone();
    let project = VolumeRef::new(
        provider.clone(),
        "full-parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("full-project".into()),
        &mut None,
    )?;
    let project_head = storage.host.create_volume(&project).await?;
    let scope = storage
        .issuer
        .issue(
            "full-parent",
            &Capabilities::new(
                storage
                    .scope
                    .capabilities()
                    .iter()
                    .map(str::to_owned)
                    .chain([
                        "fork:publish".to_owned(),
                        project.capability(VolumeOperation::Read, &mut None)?,
                    ]),
            ),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let stream_provider = ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None)?;
    let preparer = FilesystemForkPreparer::new(
        storage.host.clone(),
        parent.reducer().copy(&mut None)?,
        storage.verifier(),
        scope.clone(),
        project.clone(),
        stream_provider.clone(),
        storage.content_verifier.clone(),
        Some(archived),
        None,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?;
    let child_agent = AgentId::new();
    let request = ForkRequest {
        authorized_source: None,
        operation_id: OperationId::new(),
        parent: storage.conversation.clone(),
        parent_revision: parent.reducer().revision(),
        child: Authority {
            kind: AggregateKind::Conversation,
            id: "retired-full-child".into(),
        },
        child_agent,
        attached_agents: Vec::new(),
        preparation: ForkPreparation {
            child_result_volume: None,
            summary: None,
            child_project_volume: VolumeRef::new(
                provider.clone(),
                "full-child-project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("full-project".into()),
                &mut None,
            )?,
            child_private_volume: VolumeRef::new(
                provider,
                "full-child-private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
                &mut None,
            )?,
            inherited_through_sequence: 0,
            maximum_inherited_messages: 12,
            maximum_inherited_bytes: 262_144,
            maximum_inherited_references: 64,
        },
        selections: vec![
            ForkSelection {
                required: true,
                revision: ResourceRevision::PrivateVolume {
                    volume: (storage.volume.clone()).into(),
                    generation: (storage
                        .host
                        .resolve(
                            &crate::filesystem::workspace_ref(
                                storage.volume.provider().clone(),
                                &storage.volume.storage_name(&mut None)?,
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
                        storage.conversation.stream_path(&mut None)?.into_bytes(),
                        Some(parent.reducer().revision().to_string()),
                    )?)
                    .into(),
                ),
            },
            ForkSelection {
                required: true,
                revision: ResourceRevision::Project {
                    volume: (project).into(),
                    generation: (project_head.generation).into(),
                },
            },
        ],
        boundary: None,
    }
    .with_history_policy(parent.reducer(), ForkHistoryPolicy::Fork, &mut None)?;
    let report = parent.prepare_fork(&preparer, request.clone()).await?;
    assert_eq!(
        parent.reconcile_fork(&preparer, request).await?,
        Some(report.clone())
    );
    let seed = report.into_seed(&mut None)?;
    assert!(seed.summary.is_none());
    assert!(seed.history_witness.is_some());
    let revision = parent.reducer().revision();
    for mutation in 0..4 {
        let mut changed = seed.clone();
        match mutation {
            0 => changed.history_witness = None,
            1 => changed.history_witness = Some([0; 32]),
            2 => {
                changed.reference_grants.pop();
            }
            _ => changed.parent_revision -= 1,
        }
        assert!(matches!(
            parent
                .execute(
                    Command {
                        operation_id: changed.operation_id,
                        idempotency_key: IdempotencyKey::new(format!(
                            "full-witness-mutation-{mutation}"
                        ))?,
                        expected_revision: revision,
                        scope: scope.clone(),
                        causal_parent: None,
                        action: Action::PublishFork {
                            seed: Box::new(changed)
                        },
                    },
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None
                )
                .await,
            Err(Error::Invalid(_) | Error::Unauthorized(_))
        ));
        assert_eq!(parent.reducer().revision(), revision);
    }
    let fork_verifier = CompositeForkVerifier::new(
        &[
            FilesystemForkVerifier::new(storage.host.clone(), 262_144, None, None)?,
            StreamHistoryForkVerifier::new(&stream_provider, &mut None)?,
        ],
        &mut None,
    )?;
    *parent = crate::store::StreamAggregate::open(
        &storage.stream,
        (
            storage.conversation.clone(),
            storage.verifier(),
            crate::core::SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone())
    .with_fork_verifier(fork_verifier.clone());
    parent.set_limits(limits)?;
    let command = Command {
        operation_id: seed.operation_id,
        idempotency_key: IdempotencyKey::new("publish-retired-full-fork")?,
        expected_revision: revision,
        scope,
        causal_parent: None,
        action: Action::PublishFork {
            seed: Box::new(seed.clone()),
        },
    };
    parent.execute(command.clone(), None, &mut None).await?;
    drop(parent);
    let mut reopened = crate::store::StreamAggregate::open(
        &storage.stream,
        (
            storage.conversation.clone(),
            storage.verifier(),
            crate::core::SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(storage.content_verifier.clone())
    .with_fork_verifier(fork_verifier);
    reopened.set_limits(limits)?;
    assert!(matches!(
        reopened
            .execute(
                command,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await?,
        crate::core::ApplyResult::Replayed { .. }
    ));
    let mut count = 0;
    while count < 12 {
        let page = history
            .inherited_conversation_page(
                &seed,
                child_agent,
                count,
                HistoryReadLimits {
                    maximum_events: 3,
                    maximum_bytes: 262_144,
                },
            )
            .await?;
        assert!(!page.is_empty());
        for message in page {
            count += 1;
            assert_eq!(message.sequence, count);
        }
    }
    assert_eq!(count, 12);
    Ok(())
}
