use super::*;
use futures::TryStreamExt as _;

#[tokio::test]
async fn conversation_head_pins_racing_append_and_rejects_corrupt_atomic_proof() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    let agent = crate::AgentId::from_bytes([8; 16]);
    let file = content()?;
    let scope = issuer()
        .issue(
            "head-writer",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                file.read_capability()?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let bind = Command {
        operation_id: OperationId::from_bytes([101; 16]),
        idempotency_key: IdempotencyKey::new("head-bind")?,
        expected_revision: 0,
        scope: scope.clone(),
        causal_parent: None,
        action: Action::BindConversation { agent },
    };
    writer
        .execute(
            bind.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let reader = writer.history_reader()?;
    assert!(reader.latest_conversation_message(65_536).await?.is_none());
    let message = crate::conversation::ConversationMessage {
        id: uuid::Uuid::from_u128(1),
        sequence: 1,
        kind: crate::conversation::MessageKind::User,
        content: file,
        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    let command = Command {
        operation_id: OperationId::from_bytes([102; 16]),
        idempotency_key: IdempotencyKey::new("head-racing-user")?,
        expected_revision: 1,
        scope,
        causal_parent: None,
        action: Action::AppendConversationMessage {
            message: Box::new(message.clone()),
        },
    };
    let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
        writer.reducer().plan(&command)?;
    let publication = operations::IndexedPublication::new(
        &authority(),
        &event,
        encode_event(&authority(), &event, &mut None)?,
        &mut None,
    )?;
    *provider
        .message_head_race
        .lock()
        .map_err(|_| Error::Storage("head race lock".into()))? = Some(
        publication
            .request(
                &client,
                StreamIdempotencyKey::new("head-racing-commit")?,
                &mut None,
            )
            .await?
            .0,
    );
    assert!(reader.latest_conversation_message(65_536).await?.is_none());
    assert_eq!(
        reader.latest_conversation_message(65_536).await?,
        Some(message)
    );
    drop(writer);
    let reopened = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(
        reopened
            .reducer()
            .conversation()
            .map(|state| state.messages().len()),
        Some(1)
    );
    for fault in [1, 2, 3, 4] {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(reader.latest_conversation_message(65_536).await.is_err());
    }
    let head = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        "harness/v2/conversation-heads/conversations/conversation-1",
    )?;
    let records = head.read(1, 1).await?.try_collect::<Vec<_>>().await?;
    let mut forged: Value = crate::executor::decode_json(
        &records
            .first()
            .ok_or_else(|| Error::Storage("head record missing".into()))?
            .value,
        None,
    )?
    .0;
    forged["sequence"] = json!(2);
    head.append(crate::contract::canonical_json_bytes(&forged)?)
        .await?;
    assert!(matches!(reader.latest_conversation_message(65_536).await,
            Err(Error::Storage(detail)) if detail.contains("atomic canonical event")));

    let incomplete = Arc::new(MemoryStream::default());
    let reducer = Reducer::new(
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
    );
    let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) = reducer.plan(&bind)?;
    acyclic_stream::Stream::new(
        std::sync::Arc::clone(&incomplete),
        authority().stream_path(&mut None)?,
    )?
    .append(encode_event(&authority(), &event, &mut None)?)
    .await?;
    let incomplete_reader = HistoryReader::new(
        &incomplete,
        &authority(),
        &(issuer().verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    assert!(matches!(
        incomplete_reader.latest_conversation_message(65_536).await,
        Err(Error::Storage(_))
    ));
    Ok(())
}

#[tokio::test]
async fn indexed_conversation_lookup_is_atomic_bounded_and_pinned_after_eviction() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    writer.set_resident_event_limit(1)?;
    let agent = crate::AgentId::from_bytes([8; 16]);
    let file = content()?;
    let scope = issuer()
        .issue(
            "message-writer",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                file.read_capability()?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    writer
        .execute(
            Command {
                operation_id: OperationId::from_bytes([101; 16]),
                idempotency_key: IdempotencyKey::new("message-index-bind")?,
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
    let reader = writer.history_reader()?;
    let empty = reader.pin(0, None).await?;
    assert!(reader.latest_conversation_message(65_536).await?.is_none());
    let message = |sequence| crate::conversation::ConversationMessage {
        id: uuid::Uuid::from_u128(u128::from(sequence)),
        sequence,
        kind: crate::conversation::MessageKind::User,
        content: file.clone(),
        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    let mut first_command = None;
    for sequence in 1..=10_000u64 {
        let next = Command {
            operation_id: OperationId::from_bytes(
                uuid::Uuid::from_u128(u128::from(sequence) + 100_000).into_bytes(),
            ),
            idempotency_key: IdempotencyKey::new(format!("message-index-{sequence}"))?,
            expected_revision: writer.reducer().revision(),
            scope: scope.clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(message(sequence)),
            },
        };
        if sequence == 1 {
            first_command = Some(next.clone());
        }
        writer
            .execute(
                next,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        if matches!(sequence, 1 | 1_000 | 10_000) {
            let cold = HistoryReader::new(
                &client,
                &authority(),
                &(issuer().verifier(&mut None).expect("verifier copy")),
                &mut None,
            )?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert_eq!(
                cold.latest_conversation_message(65_536).await?,
                Some(message(sequence))
            );
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
    }
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let restored = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(restored.reducer().revision(), writer.reducer().revision());
    let restored_history = restored
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("restored conversation missing".into()))?;
    let live_history = writer
        .reducer()
        .conversation()
        .ok_or_else(|| Error::Invalid("live conversation missing".into()))?;
    assert_eq!(restored_history.logical_revision(), 10_000);
    assert_eq!(
        restored_history.history_prefix_digest(10_000, &mut None)?,
        live_history.history_prefix_digest(10_000, &mut None)?
    );
    assert!(
        restored
            .reducer()
            .events_after(restored.reducer().archived_through_revision(), 64)?
            .len()
            <= 64
    );
    assert!(provider.observation_maximum.load(Ordering::SeqCst) <= 64);
    provider.observation_maximum.store(0, Ordering::SeqCst);
    provider.forbid_writes.store(false, Ordering::SeqCst);
    let archived = writer.reducer().archived_through_revision();
    assert_eq!(archived, writer.reducer().revision() - 1);
    assert_eq!(writer.reducer().events_after(archived, 2)?.len(), 1);
    assert!(matches!(
        writer.reducer().events_after(0, 2),
        Err(Error::Unsupported(_))
    ));
    let selection_operation = OperationId::from_bytes([98; 16]);
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: 10_000,
        message_ids: [1, 5_000, 10_000]
            .into_iter()
            .map(|sequence| message(sequence).id)
            .collect(),
        checkpoint: None,
    };
    writer
        .execute(
            Command {
                operation_id: selection_operation,
                idempotency_key: IdempotencyKey::new("indexed-context-selection")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::SelectModelContext {
                    selection: selection.clone(),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let pinned = reader.pin(0, None).await?;
    let mut future = message(10_001);
    future.kind = crate::conversation::MessageKind::Interaction;
    writer
        .execute(
            Command {
                operation_id: OperationId::from_bytes([99; 16]),
                idempotency_key: IdempotencyKey::new("message-after-pin")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(future.clone()),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(matches!(
        writer
            .execute(
                first_command.ok_or_else(|| Error::Invalid("missing first command".into()))?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await?,
        ApplyResult::Replayed { .. }
    ));
    let first_path = format!(
        "harness/v2/conversation-messages/conversations/conversation-1/{}",
        message(1).id
    );
    let records = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), &first_path)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut forged: Value = crate::executor::decode_json(
        &records
            .first()
            .ok_or_else(|| Error::Storage("message index missing".into()))?
            .value,
        None,
    )?
    .0;
    let forged_id = uuid::Uuid::from_u128(50_000);
    forged["message_id"] = json!(forged_id);
    acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        format!("harness/v2/conversation-messages/conversations/conversation-1/{forged_id}"),
    )?
    .append(crate::contract::canonical_json_bytes(&forged)?)
    .await?;
    let mut forged_sequence: Value = crate::executor::decode_json(
        &records
            .first()
            .ok_or_else(|| Error::Storage("message index missing".into()))?
            .value,
        None,
    )?
    .0;
    forged_sequence["sequence"] = json!(20_001);
    acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        "harness/v2/conversation-sequences/conversations/conversation-1/20001",
    )?
    .append(crate::contract::canonical_json_bytes(&forged_sequence)?)
    .await?;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert_eq!(
        reader.latest_conversation_message(65_536).await?,
        Some(future.clone())
    );
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader.latest_conversation_message(0).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        reader.latest_conversation_message(1).await,
        Err(Error::Invalid(_))
    ));
    let range_limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: 65_536,
    };
    provider.observation_reads.store(0, Ordering::SeqCst);
    let selected = reader
        .selected_conversation(&pinned, &selection, range_limits)
        .await?;
    assert_eq!(
        selected
            .messages()
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        selection.message_ids
    );
    assert!(selected.agent.is_none());
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .selected_conversation(
                &pinned,
                &selection,
                HistoryReadLimits {
                    maximum_events: 2,
                    ..range_limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert_eq!(
        reader
            .conversation_range(&pinned, 9_997, 10_000, range_limits, &mut None)
            .await?,
        [9_998, 9_999, 10_000]
            .into_iter()
            .map(message)
            .collect::<Vec<_>>()
    );
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .conversation_range(
                &pinned,
                9_997,
                10_000,
                HistoryReadLimits {
                    maximum_events: 2,
                    ..range_limits
                },
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        reader
            .conversation_range(&pinned, 10_000, 10_001, range_limits, &mut None)
            .await,
        Err(Error::Storage(_))
    ));
    let mut two_record_bytes = 0u64;
    for sequence in [9_998u64, 9_999] {
        let index = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&client),
            format!("harness/v2/conversation-sequences/conversations/conversation-1/{sequence}"),
        )?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
        let event = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&client),
            authority().stream_path(&mut None)?,
        )?
        .read(sequence, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
        two_record_bytes += index
            .first()
            .ok_or_else(|| Error::Storage("sequence index missing".into()))?
            .value
            .len() as u64
            + event
                .first()
                .ok_or_else(|| Error::Storage("message event missing".into()))?
                .value
                .len() as u64;
    }
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .conversation_range(
                &pinned,
                9_997,
                10_000,
                HistoryReadLimits {
                    maximum_bytes: two_record_bytes,
                    ..range_limits
                },
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let last_selection = crate::conversation::ModelContextSelection {
        conversation_revision: 10_000,
        message_ids: [9_998, 9_999, 10_000]
            .into_iter()
            .map(|sequence| message(sequence).id)
            .collect(),
        checkpoint: None,
    };
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .selected_conversation(
                &pinned,
                &last_selection,
                HistoryReadLimits {
                    maximum_bytes: two_record_bytes,
                    ..range_limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .conversation_range(&pinned, 19_999, 20_000, range_limits, &mut None)
            .await,
        Err(Error::Storage(_))
    ));
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader
            .conversation_range(&pinned, 20_000, 20_001, range_limits, &mut None)
            .await,
        Err(Error::Storage(_))
    ));
    let mixed_cut = reader.pin(0, None).await?;
    assert_eq!(
        reader
            .conversation_range(&mixed_cut, 10_000, 10_001, range_limits, &mut None)
            .await?,
        vec![future.clone()]
    );
    assert!(matches!(
        reader
            .conversation_range(
                &mixed_cut,
                10_000,
                10_001,
                HistoryReadLimits {
                    maximum_events: 0,
                    ..range_limits
                },
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    for sequence in [1, 5_000, 10_000] {
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader
                .conversation_message(&pinned, message(sequence).id, 65_536)
                .await?,
            Some(message(sequence))
        );
    }
    assert!(
        reader
            .conversation_message(&pinned, future.id, 65_536)
            .await?
            .is_none()
    );
    assert!(
        reader
            .conversation_message(&empty, message(1).id, 65_536)
            .await?
            .is_none()
    );
    let current = reader.pin(0, None).await?;
    assert_eq!(
        reader
            .conversation_message(&current, future.id, 65_536)
            .await?,
        Some(future)
    );
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(matches!(
        reader.conversation_message(&pinned, message(1).id, 1).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        reader
            .conversation_message(&pinned, forged_id, 65_536)
            .await,
        Err(Error::Storage(_))
    ));
    #[cfg(feature = "filesystem")]
    {
        use crate::executor::ExecutionJournal as _;
        let filesystem_provider = crate::resources::ProviderRef::new(
            "cold-selected-context".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = Arc::new(crate::filesystem::FilesystemHost::new(
            acyclic_fs::Fs::memory(),
            filesystem_provider.clone(),
        )?);
        let private = crate::conversation::VolumeRef::new(
            filesystem_provider,
            "journal".into(),
            crate::conversation::VolumeClass::AgentPrivate,
            crate::conversation::VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&private).await?;
        let journal_scope = issuer()
            .issue(
                "cold-selection-journal",
                &Capabilities::new([
                    private.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                    private.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        let journal = crate::filesystem::FilesystemExecutionJournal::new(
            client.clone(),
            host,
            &private,
            &issuer().verifier(&mut None).expect("verifier copy"),
            &schemas(),
            &journal_scope,
            65_536,
            None,
        )?
        .with_input_verifier(Arc::new(TestContent));
        let sparse = crate::conversation::ConversationState::from_wire(
            crate::conversation::ConversationStateWire {
                agent: None,
                messages: [1, 5_000, 10_000].into_iter().map(message).collect(),
                archived_prefix: None,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut None,
        )
        .map(|(state, _)| state)?;
        assert_eq!(sparse.messages().len(), 3);
        assert!(sparse.agent.is_none());
        let selected = crate::projection::select_model_context(
            &sparse,
            selection.conversation_revision,
            selection.clone(),
            &TestContent,
            crate::conversation::Limits {
                context_messages: 3,
                attachments: 8,
                render_bytes: 65_536,
                ..Default::default()
            },
            8,
            None,
        )
        .await?;
        provider.observation_reads.store(0, Ordering::SeqCst);
        journal
            .verify_selected_context(selection_operation, &selected)
            .await?;
        let mut altered = selected.clone();
        altered.messages.pop();
        assert!(matches!(
            journal
                .verify_selected_context(selection_operation, &altered)
                .await,
            Err(Error::Conflict(_))
        ));
        let mut wrong_selection = selected;
        wrong_selection.selection.message_ids.pop();
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            journal
                .verify_selected_context(selection_operation, &wrong_selection)
                .await,
            Err(Error::Conflict(_))
        ));
    }
    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
    Ok(())
}
