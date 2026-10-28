use super::*;

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
