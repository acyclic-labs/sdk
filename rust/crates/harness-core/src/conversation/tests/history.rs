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
