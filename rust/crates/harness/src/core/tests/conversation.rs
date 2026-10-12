use super::*;

#[test]
fn interactions_are_ref_only_versioned_and_responder_bound() -> Result<()> {
    use crate::interaction::{Interaction, InteractionKind};
    let mut reducer = reducer_fixture();
    let request = Interaction::approval("approve exact effect".into(), operation(12), [3; 32])?;
    let bytes = serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
    let ticket = InteractionTicket {
        id: uuid::Uuid::from_bytes([5; 16]), kind: InteractionKind::Approval,
        request: request_file(&bytes)?, deadline_unix_ms: Some(1_000),
        approval: Some(crate::interaction::ApprovalBinding { operation_id: operation(12), action_digest: [3; 32] }),
    };
    ticket.validate_request_bytes(&bytes, &mut None)?;
    reducer.apply(command(operation(19), 0, Action::BindConversation { agent: AgentId::from_bytes([1; 16]) }))?;
    assert!(matches!(reducer.apply(command(operation(20), 1, Action::OpenInteraction { ticket: ticket.clone() }))?,
        ApplyResult::Applied { .. }));
    let resolution = InteractionResolution {
        id: ticket.id, expected_version: 2, outcome: InteractionOutcome::Approved, detail: None,
    };
    let ungranted = command(operation(21), 2, Action::ResolveInteraction { resolution: resolution.clone() });
    assert!(matches!(reducer.plan(&ungranted), Err(Error::Unauthorized(_))));
    let resolve = |id, revision, outcome| {
        let mut command = command(operation(id), revision, Action::ResolveInteraction { resolution: outcome });
        command.scope = issuer().issue("responder",
            &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
            None, None, &mut None).expect("scope proof");
        command
    };
    let mut unknown = InteractionResolution { expected_version: 1,
        outcome: InteractionOutcome::Indeterminate { operation_id: operation(30) }, ..resolution.clone() };
    assert!(matches!(reducer.plan(&resolve(24, 2, unknown.clone())), Err(Error::Conflict(_))));
    unknown.outcome = InteractionOutcome::Indeterminate { operation_id: operation(24) };
    assert!(matches!(reducer.apply(resolve(24, 2, unknown))?, ApplyResult::Applied { .. }));
    let granted = resolve(21, 3, resolution.clone());
    let committed = crate::interaction::ResolutionReceipt::from_apply_result(reducer.apply(granted.clone())?)?;
    assert_eq!((committed.id, committed.version, committed.conversation_revision, committed.replayed),
        (ticket.id, 2, 4, false));
    assert_eq!(reducer.operation_revision(operation(21)), Some(4));
    let replayed = crate::interaction::ResolutionReceipt::from_apply_result(reducer.apply(granted)?)?;
    assert!(replayed.replayed);
    assert_eq!(replayed.conversation_revision, committed.conversation_revision);
    assert_eq!(reducer.interaction(&ticket.id).and_then(|(_, outcome)| outcome.as_ref()), Some(&resolution));
    assert!(matches!(reducer.plan(&resolve(22, 4, resolution)), Err(Error::Conflict(_))));
    let restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0,
        issuer().verifier(&mut None).expect("verifier copy"), schemas())?;
    assert!(restored.interaction(&ticket.id).and_then(|(_, outcome)| outcome.as_ref()).is_some());
    Ok(())
}

#[test]
fn conversation_binding_and_messages_replay_through_snapshot() -> Result<()> {
    use crate::conversation::{MessageKind, ReferencedAttachments, VolumeClass};
    use crate::resources::ProviderRef;
    let agent = AgentId::from_bytes([9; 16]);
    let volume = VolumeRef::new(ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
        "agent-9".into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(agent), &mut None)?;
    let message = ConversationMessage {
        id: uuid::Uuid::from_bytes([1; 16]), sequence: 1, kind: MessageKind::User,
        content: FileRef::new(volume, "messages/one.txt".into(), "generation-1".into(),
            FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?, "one.txt".into(), &mut None)?,
        attachments: ReferencedAttachments::Inline { items: Vec::new() }, reply_to: None,
        tool_call_id: None, extensions: BTreeMap::new(),
    };
    let verifier = issuer().verifier(&mut None).expect("verifier copy");
    let mut reducer = reducer_fixture();
    let append = |id, revision| command(operation(id), revision,
        Action::AppendConversationMessage { message: Box::new(message.clone()) });
    assert!(matches!(reducer.plan(&append(1, 0)), Err(Error::Conflict(_))));
    reducer.apply(command(operation(1), 0, Action::BindConversation { agent }))?;
    let ApplyResult::Applied { event } = reducer.apply(append(2, 1))? else { panic!("fresh append replayed") };
    assert!(!serde_json::to_string(&event).map_err(|error| Error::Invalid(error.to_string()))?.contains("hello"));
    assert_eq!(reducer.conversation().map(|state| state.messages.as_slice()), Some(&[message.clone()][..]));
    let selection = ModelContextSelection { checkpoint: None, conversation_revision: 1, message_ids: vec![message.id] };
    let select = |id, revision| command(operation(id), revision, Action::SelectModelContext { selection: selection.clone() });
    reducer.apply(select(3, 2))?;
    assert_eq!(reducer.resident_context_selections().collect::<Vec<_>>(), vec![&selection]);
    assert!(matches!(reducer.plan(&command(operation(4), 3, Action::SelectModelContext {
        selection: ModelContextSelection { conversation_revision: 0, ..selection.clone() },
    })), Err(Error::Conflict(_))));
    let mut canonical = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, verifier.clone(), schemas())?;
    assert_eq!(canonical.conversation(), reducer.conversation());
    assert_eq!(canonical.resident_context_selections().collect::<Vec<_>>(), reducer.resident_context_selections().collect::<Vec<_>>());
    reducer.set_resident_event_limit(1)?;
    for id in 5..=68 {
        let next = select(id, reducer.revision());
        assert!(matches!(reducer.plan(&next), Err(Error::Unsupported(_))));
        let ApplyResult::Applied { event } = canonical.apply(next)? else { panic!("fresh selection replayed") };
        reducer.apply_committed(event, &mut None)?;
        assert_eq!(reducer.resident_context_selections().count(), 1);
    }
    assert!(reducer.context_selection_for_operation(operation(3)).is_none());
    assert_eq!(reducer.context_selection_for_operation(operation(68)), Some(&selection));
    let restored = Reducer::restore(reducer.snapshot(usize::MAX, &mut None)?.0, verifier, schemas())?;
    assert_eq!(restored.resident_context_selections().count(), 1);
    assert_eq!(restored.context_selection_for_operation(operation(68)), Some(&selection));
    Ok(())
}
