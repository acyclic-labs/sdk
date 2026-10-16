use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[path = "prefix_properties.rs"]
mod prefix_properties;

#[test]
fn checkpoint_suffix_preserves_logical_head_digest_and_next_turn() -> Result<()> {
    let agent = AgentId::new();
    let content = file(agent, "history.txt")?;
    let checkpoint = file(agent, "checkpoint.json")?;
    let mut state = ConversationState::default();
    state.bind(agent)?;
    for sequence in 1..=1_000_u64 {
        state.append(message(&content, sequence), &mut None)?;
    }
    let digest = state.history_prefix_digest(1_000, &mut None)?;
    let selection = ModelContextSelection {
        conversation_revision: 998,
        message_ids: vec![Uuid::from_u128(997), Uuid::from_u128(998)],
        checkpoint: Some(checkpoint.clone()),
    };
    let suffix = state
        .checkpointed_suffix(&selection, &mut None, None)
        .map(|(state, _)| state)?;
    assert_eq!(suffix.messages().len(), 2);
    assert_eq!(suffix.logical_revision(), 1_000);
    assert_eq!(suffix.resident_after_sequence(), 998);
    assert_eq!(suffix.history_prefix_digest(1_000, &mut None)?, digest);
    assert!(suffix.history_prefix_digest(997, &mut None).is_err());
    assert!(suffix.page(0, 1_000, 10).is_err());
    assert!(suffix.model_messages_between(0, 1_000, 10).is_err());
    assert_eq!(suffix.page(998, 1_000, 10)?.len(), 2);
    assert!(suffix.checkpoint_covers_archived_prefix(&checkpoint, 998));
    assert!(!suffix.checkpoint_covers_archived_prefix(&content, 998));
    let bytes = crate::contract::canonical_json_bytes(&suffix)?;
    let mut restored: ConversationState =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(restored, suffix);
    assert_eq!(restored.history_prefix_digest(1_000, &mut None)?, digest);
    let next = message(&content, 1_001);
    restored.append(next, &mut None)?;
    assert_eq!(restored.logical_revision(), 1_001);
    assert_eq!(restored.messages().len(), 3);
    assert_eq!(restored.unresolved_user(), Some(Uuid::from_u128(1_001)));
    assert_eq!(
        restored
            .checkpointed_suffix(&selection, &mut None, None)
            .map(|(state, _)| state)?,
        restored
    );
    Ok(())
}

#[test]
fn full_history_fork_fingerprint_is_incremental_and_rebuilds_after_decode() -> Result<()> {
    let agent = AgentId::new();
    let content = file(agent, "fingerprint.txt")?;
    let mut state = ConversationState::default();
    state.bind(agent)?;
    for sequence in 1..=10_000_u64 {
        state.append(
            ConversationMessage {
                kind: if sequence % 2 == 0 {
                    MessageKind::Interaction
                } else {
                    MessageKind::User
                },
                reply_to: None,
                ..message(&content, sequence)
            },
            &mut None,
        )?;
    }
    let authority = crate::core::Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "fingerprint-parent".into(),
    };
    let pinned = crate::fork::InheritedConversationPrefix::select(
        authority.clone(),
        10_001,
        10_000,
        &[],
        &state,
        &mut None,
    )?;
    for _ in 0..32 {
        assert_eq!(
            crate::fork::InheritedConversationPrefix::select(
                authority.clone(),
                10_001,
                10_000,
                &[],
                &state,
                &mut None
            )?,
            pinned
        );
    }
    assert_eq!(pinned.format_version, 3);
    let old = state.history_prefix_digest(2, &mut None)?;
    let encoded = crate::contract::canonical_json_bytes(&state)?;
    let mut reopened: ConversationState =
        serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        reopened.history_prefix_digest(10_000, &mut None)?,
        pinned.message_digest
    );
    let mut next = reopened
        .messages()
        .last()
        .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
        .clone();
    next.sequence += 1;
    next.id = Uuid::from_u128(10_001);
    reopened.append(next.clone(), &mut None)?;
    assert_ne!(
        reopened.history_prefix_digest(10_001, &mut None)?,
        pinned.message_digest
    );
    assert_eq!(
        reopened.history_prefix_digest(10_000, &mut None)?,
        pinned.message_digest
    );
    assert_eq!(reopened.history_prefix_digest(2, &mut None)?, old);
    let before = reopened.history_prefix_digest(10_001, &mut None)?;
    assert!(reopened.append(next, &mut None).is_err());
    assert_eq!(reopened.history_prefix_digest(10_001, &mut None)?, before);
    let sparse = ConversationState::from_wire(
        ConversationStateWire {
            agent: None,
            archived_prefix: None,
            messages: vec![
                reopened
                    .messages()
                    .last()
                    .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
                    .clone(),
            ],
        },
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &mut None,
    )
    .map(|(state, _)| state)?;
    assert!(sparse.history_prefix_digest(1, &mut None).is_err());
    assert!(state.history_prefix_digest(10_001, &mut None).is_err());
    Ok(())
}

#[test]
fn pending_turn_rebuilds_without_retaining_settled_turn_identities() -> Result<()> {
    fn append(
        state: &mut ConversationState,
        content: &FileRef,
        kind: MessageKind,
        reply_to: Option<Uuid>,
        terminal: bool,
    ) -> Result<Uuid> {
        let sequence = state.messages().len() as u64 + 1;
        let mut record = ConversationMessage {
            kind,
            reply_to,
            ..message(content, sequence)
        };
        if terminal {
            record
                .extensions
                .insert("acyclic.turn.outcome".into(), content.clone());
        }
        let id = record.id;
        state.append(record, &mut None)?;
        Ok(id)
    }

    fn reopen(state: &ConversationState) -> Result<ConversationState> {
        let bytes = crate::contract::canonical_json_bytes(state)?;
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
    }

    let agent = AgentId::new();
    let content = file(agent, "pending.txt")?;
    let mut state = ConversationState::default();
    state.bind(agent)?;
    let mut previous = None;
    for _ in 0..256 {
        let user = append(&mut state, &content, MessageKind::User, None, false)?;
        assert_eq!(state.unresolved_user(), Some(user));
        if let Some(previous) = previous {
            append(
                &mut state,
                &content,
                MessageKind::Assistant,
                Some(previous),
                false,
            )?;
            assert_eq!(state.unresolved_user(), Some(user));
        }
        append(
            &mut state,
            &content,
            MessageKind::Assistant,
            Some(user),
            false,
        )?;
        assert_eq!(state.unresolved_user(), None);
        previous = Some(user);
    }
    assert_eq!(reopen(&state)?, state);
    let user = append(&mut state, &content, MessageKind::User, None, false)?;
    append(&mut state, &content, MessageKind::System, Some(user), false)?;
    assert_eq!(state.unresolved_user(), Some(user));
    assert_eq!(reopen(&state)?, state);
    let outcome = append(&mut state, &content, MessageKind::System, Some(user), true)?;
    assert_eq!(state.unresolved_user(), None);
    assert_eq!(
        state.turn_outcome(user).map(|message| message.id),
        Some(outcome)
    );
    assert_eq!(reopen(&state)?, state);
    let next = append(&mut state, &content, MessageKind::User, None, false)?;
    assert_eq!(state.unresolved_user(), Some(next));
    assert_eq!(reopen(&state)?, state);
    Ok(())
}

#[test]
fn history_rebuilds_and_pinned_pages_exclude_later_appends() -> Result<()> {
    let agent = AgentId::new();
    let mut state = ConversationState::default();
    state.bind(agent)?;
    let content = file(agent, "history.txt")?;
    for sequence in 1..=65 {
        state.append(
            ConversationMessage {
                id: Uuid::new_v4(),
                kind: MessageKind::User,
                reply_to: None,
                ..message(&content, sequence)
            },
            &mut None,
        )?;
    }
    let pinned = 64;
    let bytes = serde_json::to_vec(&state).map_err(|error| Error::Invalid(error.to_string()))?;
    let reopened: ConversationState =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, state);
    let mut after = 0;
    let mut visited = Vec::new();
    loop {
        let page = reopened.page(after, pinned, 7)?;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 7);
        for message in page {
            assert_eq!(reopened.message(message.id), Some(message));
            visited.push(message.sequence);
        }
        after = page
            .last()
            .ok_or_else(|| Error::Invalid("empty page".into()))?
            .sequence;
    }
    assert_eq!(visited, (1..=pinned).collect::<Vec<_>>());
    assert!(reopened.page(0, 66, 7).is_err());
    assert!(reopened.page(0, pinned, 0).is_err());
    let mut corrupt =
        serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
    corrupt["messages"][1]["id"] = corrupt["messages"][0]["id"].clone();
    assert!(serde_json::from_value::<ConversationState>(corrupt).is_err());
    Ok(())
}

#[test]
fn failed_append_preserves_history_and_sparse_decode_cannot_append() -> Result<()> {
    let agent = AgentId::new();
    let mut state = ConversationState::default();
    state.bind(agent)?;
    let mut candidate = ConversationMessage {
        id: Uuid::new_v4(),
        reply_to: Some(Uuid::new_v4()),
        ..message(&file(agent, "history.txt")?, 1)
    };
    let before = state.copy(&mut None, None).map(|(state, _)| state)?;
    assert!(state.append(candidate.clone(), &mut None).is_err());
    assert_eq!(state, before);
    assert!(state.message(candidate.id).is_none());
    candidate.reply_to = None;
    state.append(candidate.clone(), &mut None)?;
    assert_eq!(state.message(candidate.id), Some(&candidate));
    let mut wire =
        serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
    wire["messages"][0]["sequence"] = 3.into();
    let mut sparse: ConversationState =
        serde_json::from_value(wire).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(sparse.page(0, 3, 1)?.len(), 1);
    candidate.id = Uuid::new_v4();
    candidate.sequence = 2;
    let (before, _) = sparse.copy(&mut None, None)?;
    assert!(sparse.append(candidate, &mut None).is_err());
    assert_eq!(sparse, before);
    Ok(())
}
