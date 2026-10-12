use super::*;

use crate::filesystem::*;
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeOwner},
    executor::ModelPurpose,
    resources::ProviderRef,
};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one production journal roundtrip and isolated persisted-header adversaries"
)]
async fn observation_reopen_preserves_original_turn_and_rejects_header_substitution() -> Result<()>
{
    let provider = ProviderRef::new(
        "observation-identity".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "observation-identity".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "observation-identity",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    let scope = issuer
        .issue(
            "owner",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let open = |operation, turn| {
        FilesystemExecutionJournal::new(
            stream.clone(),
            host.clone(),
            &volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &crate::core::SchemaRegistry::new(),
            &scope,
            4096,
            None,
        )
        .map(|journal| journal.with_turn(operation, turn))
    };
    let turn = OperationId::new();
    let operation =
        crate::filesystem::task_runtime::execution_operation(crate::TaskId::new(), turn, None)?;
    assert_ne!(operation, turn);
    let event = ExecutionEvent::Started {
        request_digest: [9; 32],
    };
    open(operation, turn)?
        .append(operation, "start".into(), event.clone())
        .await?;
    let reopened = open(operation, turn)?;
    let records = reopened.replay(operation, 0, 1, None)?.await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records.first().map(|record| &record.event), Some(&event));
    reopened
        .append(operation, "start".into(), event.clone())
        .await?;
    assert_eq!(reopened.replay(operation, 0, 2, None)?.await?.len(), 1);
    assert!(matches!(
        open(operation, OperationId::new())?
            .replay(operation, 0, 1)
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        reopened.replay(turn, 0, 1, None)?.await,
        Err(Error::Unauthorized(_))
    ));
    let summary_operation = ModelPurpose::Summary.attempt_operation(operation, None)?;
    assert_ne!(summary_operation, operation);
    for mutation in [
        "wrong_execution",
        "summary_attempt_as_execution",
        "wrong_turn",
        "missing_turn",
        "missing_version",
        "old_version",
        "unknown_version",
        "wrong_version_type",
        "unknown_field",
    ] {
        let target = OperationId::new();
        let mut wire = serde_json::to_value(Observation {
            version: OBSERVATION_VERSION,
            operation_id: target,
            turn_operation: turn,
            retry_digest: blake3::hash(b"start").to_hex().to_string(),
            event: event.clone(),
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        match mutation {
            "wrong_execution" => wire["operation_id"] = serde_json::json!(operation),
            "summary_attempt_as_execution" => {
                wire["operation_id"] = serde_json::json!(summary_operation);
            }
            "wrong_turn" => wire["turn_operation"] = serde_json::json!(OperationId::new()),
            "missing_turn" => {
                wire.as_object_mut()
                    .ok_or_else(|| Error::Invalid("envelope".into()))?
                    .remove("turn_operation");
            }
            "missing_version" => {
                wire.as_object_mut()
                    .ok_or_else(|| Error::Invalid("envelope".into()))?
                    .remove("version");
            }
            "old_version" => wire["version"] = serde_json::json!(1),
            "unknown_version" => wire["version"] = serde_json::json!(3),
            "wrong_version_type" => wire["version"] = serde_json::json!("2"),
            "unknown_field" => wire["event_operation"] = serde_json::json!(operation),
            _ => unreachable!("enumerated envelope mutation"),
        }
        let journal = open(target, turn)?;
        journal
            .path(target)?
            .append_batch(
                vec![
                    Bytes::from(
                        serde_json::to_vec(&wire)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                    .into(),
                ],
                Some(0),
                None,
            )
            .await?;
        assert!(
            matches!(
                open(target, turn)?.replay(target, 0, 1).await,
                Err(Error::Storage(_) | Error::Conflict(_))
            ),
            "accepted {mutation}"
        );
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    reason = "one real four-record canonical source barrier timeline uses its fixed fixture identities"
)]
async fn cold_canonical_source_checks_complete_mixed_delta_without_reducer_restore() -> Result<()> {
    use crate::conversation::{
        ContentPublisher as _, ConversationMessage, MessageKind, ModelContextSelection,
    };
    use crate::core::{Action, AggregateKind, Authority, AuthorityIssuer, Command};
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<
        acyclic_stream::MemoryStream,
    >::default());
    let client = provider.clone();
    let fs_provider = ProviderRef::new(
        "cold-canonical-source".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &fs_provider, None)?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        fs_provider,
        "cold-source".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: crate::ConversationId::new().to_string(),
    };
    let issuer = AuthorityIssuer::new("cold-canonical-owner", [72; 32], authority.clone());
    let scope = issuer
        .issue(
            "owner",
            &crate::Capabilities::new([
                "conversation:bind".into(),
                "conversation:append".into(),
                "conversation:select_context".into(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let content = crate::filesystem::FilesystemContentVerifier::new(
        host.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4096,
        None,
    )?;
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4096,
        None,
    )?;
    let mut aggregate = StreamAggregate::open(
        &client,
        authority.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(content.clone());
    aggregate.set_resident_event_limit(1)?;
    aggregate
        .execute(
            Command {
                operation_id: OperationId::new(),
                idempotency_key: crate::IdempotencyKey::new("cold-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let mut ids = Vec::new();
    let mut references = Vec::new();
    for (position, kind) in [
        MessageKind::User,
        MessageKind::Interaction,
        MessageKind::Assistant,
        MessageKind::User,
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = position as u64 + 1;
        let file = publisher
            .stage(
                OperationId::new(),
                &format!("messages/{sequence}.txt"),
                format!("canonical-{sequence}").as_bytes(),
                "text/plain",
                "message.txt",
                None,
            )?
            .await?
            .0;
        let id = uuid::Uuid::new_v4();
        let reply_to = if kind == MessageKind::Assistant {
            ids.first().copied()
        } else {
            None
        };
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::new(),
                    idempotency_key: crate::IdempotencyKey::new(format!(
                        "cold-message-{sequence}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(ConversationMessage {
                            id,
                            sequence,
                            kind,
                            content: file.clone(),
                            attachments: crate::conversation::ReferencedAttachments::Inline {
                                items: Vec::new(),
                            },
                            reply_to,
                            tool_call_id: None,
                            extensions: Default::default(),
                        }),
                    },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        ids.push(id);
        references.push(file);
    }
    let omitted_operation = OperationId::new();
    let omitted = ModelContextSelection {
        conversation_revision: 4,
        message_ids: vec![ids[3]],
        checkpoint: None,
    };
    let complete_operation = OperationId::new();
    let complete = ModelContextSelection {
        conversation_revision: 4,
        message_ids: vec![ids[0], ids[2], ids[3]],
        checkpoint: None,
    };
    for (operation, selection) in [
        (omitted_operation, omitted.clone()),
        (complete_operation, complete.clone()),
    ] {
        aggregate
            .execute(
                Command {
                    operation_id: operation,
                    idempotency_key: crate::IdempotencyKey::new(format!(
                        "cold-selection:{operation}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::SelectModelContext { selection },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let limits = Limits {
        context_messages: 3,
        file_bytes: 4096,
        render_bytes: 4096,
        ..Limits::default()
    };
    async fn source(
        history: &crate::conversation::ConversationState,
        selection: ModelContextSelection,
        content: &dyn ContentResidencyVerifier,
        limits: Limits,
    ) -> Result<crate::context::Context> {
        let selected = select_model_context(
            history,
            selection.conversation_revision,
            selection,
            content,
            limits,
            limits.attachments,
            None,
        )
        .await?;
        let input = selected
            .messages
            .last()
            .ok_or_else(|| Error::NotFound("cold current input".into()))?
            .content
            .clone();
        crate::context::ContextPipeline::base_context(
            &crate::context::ContextInput {
                input,
                selected_context: Some(selected),
                step: 0,
                prior_messages: Vec::new(),
            },
            limits,
            None,
        )
    }
    let history = aggregate
        .reducer()
        .conversation()
        .ok_or_else(|| Error::NotFound("cold history".into()))?;
    let omitted_source = source(history, omitted.clone(), content.as_ref(), limits).await?;
    let complete_source = source(history, complete.clone(), content.as_ref(), limits).await?;
    let selected = select_model_context(
        history,
        complete.conversation_revision,
        complete.clone(),
        content.as_ref(),
        limits,
        limits.attachments,
        None,
    )
    .await?;
    let placeholder = references
        .last()
        .ok_or_else(|| Error::NotFound("cold placeholder".into()))?
        .clone();
    let envelope = |operation_id, selection| crate::context::CanonicalContextCheckpoint {
        operation_id,
        selection,
        source: placeholder.clone(),
        retained: placeholder.clone(),
        compaction: placeholder.clone(),
    };
    let new_journal = || {
        FilesystemExecutionJournal::new(
            client.clone(),
            host.clone(),
            &volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &SchemaRegistry::new(),
            &scope,
            4096,
            None,
        )
        .map(|journal| journal.with_input_verifier(content.clone()))
    };
    let journal = new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
        maximum_events: 4,
        maximum_bytes: 16_384,
    })?;
    let execution_operation = crate::filesystem::task_runtime::execution_operation(
        crate::TaskId::new(),
        complete_operation,
        None,
    )?;
    assert_ne!(execution_operation, complete_operation);
    let bound = new_journal()?
        .with_turn(execution_operation, complete_operation)
        .with_history_read_limits(crate::store::HistoryReadLimits {
            maximum_events: 4,
            maximum_bytes: 16_384,
        })?;
    bound
        .verify_selected_context(execution_operation, &selected, None)?
        .await?;
    bound
        .verify_canonical_source_with_io(
            &envelope(execution_operation, complete.clone()),
            &complete_source,
            limits,
            None,
        )
        .await?;
    assert!(matches!(
        new_journal()?
            .with_turn(execution_operation, omitted_operation)
            .verify_selected_context(execution_operation, &selected, None)?
            .await,
        Err(Error::Conflict(_))
    ));
    provider.forbid_writes.store(true, Ordering::SeqCst);
    assert!(matches!(
        journal
            .verify_canonical_source_with_io(
                &envelope(omitted_operation, omitted),
                &omitted_source,
                limits,
                None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    journal
        .verify_canonical_source_with_io(
            &envelope(complete_operation, complete.clone()),
            &complete_source,
            limits,
            None,
        )
        .await?;
    let mut altered = complete_source.clone();
    altered
        .messages
        .last_mut()
        .ok_or_else(|| Error::NotFound("cold source input".into()))?
        .content = crate::model::ModelContent::Text("altered source".into());
    assert!(matches!(
        journal
            .verify_canonical_source_with_io(
                &envelope(complete_operation, complete.clone()),
                &altered,
                limits,
                None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let bounded = new_journal()?;
    assert!(matches!(
        bounded
            .verify_canonical_source_with_io(
                &envelope(complete_operation, complete.clone()),
                &complete_source,
                limits,
                None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let byte_bounded =
        new_journal()?.with_history_read_limits(crate::store::HistoryReadLimits {
            maximum_events: 4,
            maximum_bytes: 1,
        })?;
    assert!(matches!(
        byte_bounded
            .verify_canonical_source_with_io(
                &envelope(complete_operation, complete),
                &complete_source,
                limits,
                None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}
