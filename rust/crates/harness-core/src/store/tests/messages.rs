use super::*;
use futures::TryStreamExt as _;
use std::sync::atomic::Ordering;

fn message(sequence: u64) -> Result<crate::conversation::ConversationMessage> {
    Ok(crate::conversation::ConversationMessage {
        id: uuid::Uuid::from_u128(u128::from(sequence)),
        sequence,
        kind: crate::conversation::MessageKind::User,
        content: content()?,
        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    })
}

fn conversation_command(revision: u64, action: Action) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes(
            uuid::Uuid::from_u128(u128::from(revision) + 100_000).into_bytes(),
        ),
        idempotency_key: IdempotencyKey::new(format!("conversation-{revision}"))?,
        expected_revision: revision,
        scope: issuer().issue(
            "message-writer",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                content()?.read_capability(&mut None)?,
            ]),
            Some(crate::AgentId::from_bytes([8; 16])),
            None,
            &mut None,
        )?,
        causal_parent: None,
        action,
    })
}

#[tokio::test]
async fn conversation_head_pins_racing_append_and_rejects_corrupt_atomic_proof() -> Result<()> {
    let client = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let mut writer = with_content(&client).await?;
    writer
        .execute(
            conversation_command(
                0,
                Action::BindConversation {
                    agent: crate::AgentId::from_bytes([8; 16]),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let reader = writer.history_reader()?;
    let command = conversation_command(
        1,
        Action::AppendConversationMessage {
            message: Box::new(message(1)?),
        },
    )?;
    let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
        writer.reducer().plan(&command)?;
    let publication = operations::IndexedPublication::new(
        &authority(),
        &event,
        encode_event(&authority(), &event, &mut None)?,
        None,
        &mut None,
    )?;
    *client
        .message_head_race
        .lock()
        .map_err(|error| Error::Storage(error.to_string()))? = Some(
        publication
            .request(
                &client,
                StreamIdempotencyKey::new("racing-message")?,
                &mut None,
            )
            .await?
            .0,
    );
    assert!(reader.latest_conversation_message(65_536).await?.is_none());
    assert_eq!(
        reader.latest_conversation_message(65_536).await?,
        Some(message(1)?)
    );
    let head = acyclic_stream::Stream::new(
        client.clone(),
        "harness/v2/conversation-heads/conversations/conversation-1",
    )?;
    let records = head.read(1, 1).await?.try_collect::<Vec<_>>().await?;
    let mut forged: Value = crate::executor::decode_json(&records[0].value, None)?.0;
    forged["sequence"] = json!(2);
    head.append(crate::contract::canonical_json_bytes(&forged)?)
        .await?;
    assert!(matches!(
        reader.latest_conversation_message(65_536).await,
        Err(Error::Storage(_))
    ));
    Ok(())
}

#[tokio::test]
async fn indexed_conversation_lookup_is_atomic_bounded_and_pinned_after_eviction() -> Result<()> {
    let client = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let mut writer = with_content(&client).await?;
    writer.set_resident_event_limit(1)?;
    writer
        .execute(
            conversation_command(
                0,
                Action::BindConversation {
                    agent: crate::AgentId::from_bytes([8; 16]),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let reader = writer.history_reader()?;
    let empty = reader.pin(0, None).await?;
    // Cross two persisted checkpoint boundaries and retain a nonempty suffix.
    // Read counts prove bounded work independently of history size.
    const MESSAGES: u64 = 2 * super::super::checkpoints::DEFAULT_PROJECTION_EVENTS as u64 + 2;
    const MIDDLE: u64 = MESSAGES / 2;
    let lookup_work = || {
        (
            client.observation_reads.load(Ordering::SeqCst),
            client.observation_maximum.load(Ordering::SeqCst),
        )
    };
    let reset_lookup_work = || {
        client.observation_reads.store(0, Ordering::SeqCst);
        client.observation_maximum.store(0, Ordering::SeqCst);
    };
    for sequence in 1..=MESSAGES {
        writer
            .execute(
                conversation_command(
                    sequence,
                    Action::AppendConversationMessage {
                        message: Box::new(message(sequence)?),
                    },
                )?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        if [1, MIDDLE, MESSAGES].contains(&sequence) {
            let cold = writer.history_reader()?;
            client.forbid_writes.store(true, Ordering::SeqCst);
            reset_lookup_work();
            assert_eq!(
                cold.latest_conversation_message(65_536).await?,
                Some(message(sequence)?)
            );
            assert_eq!(
                lookup_work(),
                (2, 1),
                "head lookup must read only its locator and canonical event"
            );
            client.forbid_writes.store(false, Ordering::SeqCst);
        }
    }
    assert_eq!(writer.reducer().archived_through_revision(), MESSAGES);
    client.forbid_writes.store(true, Ordering::SeqCst);
    // Negative control: restoring a canonical-history scan must fail the same
    // exact work bound even though the returned latest message is unchanged.
    reset_lookup_work();
    let scanned = acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
        .read(0, u32::try_from(MESSAGES + 1).expect("bounded fixture"))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(scanned.len() as u64, MESSAGES + 1);
    assert_eq!(
        reader.latest_conversation_message(65_536).await?,
        Some(message(MESSAGES)?)
    );
    assert_ne!(
        lookup_work(),
        (2, 1),
        "canonical scan escaped the work detector"
    );
    let restored = with_content(&client).await?;
    assert_eq!(
        restored
            .reducer()
            .conversation()
            .unwrap()
            .history_prefix_digest(MESSAGES, &mut None)?,
        writer
            .reducer()
            .conversation()
            .unwrap()
            .history_prefix_digest(MESSAGES, &mut None)?
    );
    client.forbid_writes.store(false, Ordering::SeqCst);
    let selection = crate::conversation::ModelContextSelection {
        conversation_revision: MESSAGES,
        message_ids: [1, MIDDLE, MESSAGES]
            .map(|sequence| uuid::Uuid::from_u128(u128::from(sequence)))
            .into(),
        checkpoint: None,
    };
    writer
        .execute(
            conversation_command(
                MESSAGES + 1,
                Action::SelectModelContext {
                    selection: selection.clone(),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let pinned = reader.pin(0, None).await?;
    let mut future = message(MESSAGES + 1)?;
    future.kind = crate::conversation::MessageKind::Interaction;
    writer
        .execute(
            conversation_command(
                MESSAGES + 2,
                Action::AppendConversationMessage {
                    message: Box::new(future.clone()),
                },
            )?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(matches!(
        writer
            .execute(
                conversation_command(
                    1,
                    Action::AppendConversationMessage {
                        message: Box::new(message(1)?)
                    }
                )?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?,
        ApplyResult::Replayed { .. }
    ));
    client.forbid_writes.store(true, Ordering::SeqCst);
    let limits = HistoryReadLimits {
        maximum_events: 3,
        maximum_bytes: 65_536,
    };
    let selected = reader
        .selected_conversation(&pinned, &selection, limits)
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
    assert!(matches!(
        reader
            .selected_conversation(
                &pinned,
                &selection,
                HistoryReadLimits {
                    maximum_events: 2,
                    ..limits
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        reader
            .conversation_range(&pinned, MESSAGES - 3, MESSAGES, limits, &mut None)
            .await?,
        [MESSAGES - 2, MESSAGES - 1, MESSAGES]
            .into_iter()
            .map(message)
            .collect::<Result<Vec<_>>>()?
    );
    assert!(matches!(
        reader
            .conversation_range(&pinned, MESSAGES, MESSAGES + 1, limits, &mut None)
            .await,
        Err(Error::Storage(_))
    ));
    let mut two_record_bytes = 0;
    for sequence in [MESSAGES - 2, MESSAGES - 1] {
        for (path, after) in [
            (
                format!(
                    "harness/v2/conversation-sequences/conversations/conversation-1/{sequence}"
                ),
                0,
            ),
            (authority().stream_path(&mut None)?, sequence),
        ] {
            let records = acyclic_stream::Stream::new(client.clone(), path)?
                .read(after, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            two_record_bytes += records[0].value.len() as u64;
        }
    }
    assert!(matches!(
        reader
            .conversation_range(
                &pinned,
                MESSAGES - 3,
                MESSAGES,
                HistoryReadLimits {
                    maximum_bytes: two_record_bytes,
                    ..limits
                },
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    for sequence in [1, MIDDLE, MESSAGES] {
        assert_eq!(
            reader
                .conversation_message(&pinned, message(sequence)?.id, 65_536)
                .await?,
            Some(message(sequence)?)
        );
    }
    assert!(
        reader
            .conversation_message(&empty, message(1)?.id, 65_536)
            .await?
            .is_none()
    );
    assert!(
        reader
            .conversation_message(&pinned, future.id, 65_536)
            .await?
            .is_none()
    );
    assert!(matches!(
        reader
            .conversation_message(&pinned, message(1)?.id, 1)
            .await,
        Err(Error::Invalid(_))
    ));
    let current = reader.pin(0, None).await?;
    assert_eq!(
        reader
            .conversation_range(&current, MESSAGES, MESSAGES + 1, limits, &mut None)
            .await?,
        vec![future.clone()]
    );
    assert_eq!(
        reader
            .conversation_message(&current, future.id, 65_536)
            .await?,
        Some(future)
    );
    Ok(())
}
