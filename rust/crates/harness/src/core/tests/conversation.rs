use super::*;

#[test]
fn interactions_are_ref_only_versioned_and_responder_bound() -> Result<()> {
    use crate::interaction::{ApprovalBinding, Interaction, InteractionKind};
    let mut reducer = reducer_fixture();
    let request = Interaction::approval("approve exact effect".into(), operation(12), [3; 32])?;
    let bytes = serde_json::to_vec(&request).unwrap();
    let ticket = InteractionTicket {
        id: uuid::Uuid::from_bytes([5; 16]),
        kind: InteractionKind::Approval,
        request: request_file(&bytes)?,
        deadline_unix_ms: Some(1_000),
        approval: Some(ApprovalBinding {
            operation_id: operation(12),
            action_digest: [3; 32],
        }),
    };
    ticket.validate_request_bytes(&bytes, &mut None)?;
    let agent = AgentId::from_bytes([1; 16]);
    apply(&mut reducer, 19, Action::BindConversation { agent })?;
    let open = Action::OpenInteraction {
        ticket: ticket.clone(),
    };
    apply(&mut reducer, 20, open)?;
    let resolution = InteractionResolution {
        id: ticket.id,
        expected_version: 2,
        outcome: InteractionOutcome::Approved,
        detail: None,
    };
    let ungranted = Action::ResolveInteraction {
        resolution: resolution.clone(),
    };
    assert!(apply(&mut reducer, 21, ungranted).is_err());
    let resolve = |id, revision, resolution| -> Result<Command> {
        let mut command = command(
            operation(id),
            revision,
            Action::ResolveInteraction { resolution },
        );
        command.scope = issuer().issue(
            "responder",
            &Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
            None,
            None,
            &mut None,
        )?;
        Ok(command)
    };
    let mut unknown = InteractionResolution {
        expected_version: 1,
        outcome: InteractionOutcome::Indeterminate {
            operation_id: operation(30),
        },
        ..resolution.clone()
    };
    assert!(reducer.plan(&resolve(24, 2, unknown.clone())?).is_err());
    unknown.outcome = InteractionOutcome::Indeterminate {
        operation_id: operation(24),
    };
    reducer.apply(resolve(24, 2, unknown)?)?;
    reducer.apply(resolve(21, 3, resolution.clone())?)?;
    assert_eq!(
        reducer.interaction(&ticket.id).unwrap().1.as_ref(),
        Some(&resolution)
    );
    assert!(reducer.plan(&resolve(22, 4, resolution.clone())?).is_err());
    assert_eq!(restore(&reducer, &issuer())?, reducer);
    Ok(())
}

#[test]
fn conversation_binding_and_messages_replay_through_snapshot() -> Result<()> {
    let message: ConversationMessage = serde_json::from_slice(include_bytes!(
        "../../../fixtures/v2/conversation-message.json"
    ))
    .unwrap();
    let VolumeOwner::Agent(agent) = message.content.volume().owner() else {
        panic!("agent-owned fixture")
    };
    let mut reducer = reducer_fixture();
    let append = Action::AppendConversationMessage {
        message: Box::new(message.clone()),
    };
    assert!(apply(&mut reducer, 1, append.clone()).is_err());
    apply(&mut reducer, 1, Action::BindConversation { agent: *agent })?;
    apply(&mut reducer, 2, append)?;
    assert_eq!(
        reducer.conversation().unwrap().messages,
        vec![message.clone()]
    );
    let selection = ModelContextSelection {
        checkpoint: None,
        conversation_revision: 1,
        message_ids: vec![message.id],
    };
    let select = |id, revision| {
        command(
            operation(id),
            revision,
            Action::SelectModelContext {
                selection: selection.clone(),
            },
        )
    };
    reducer.apply(select(3, 2))?;
    let stale = ModelContextSelection {
        conversation_revision: 0,
        ..selection.clone()
    };
    assert!(
        apply(
            &mut reducer,
            4,
            Action::SelectModelContext { selection: stale }
        )
        .is_err()
    );
    let mut canonical = restore(&reducer, &issuer())?;
    assert_eq!(canonical.conversation(), reducer.conversation());
    reducer.set_resident_event_limit(1)?;
    for id in 5..=68 {
        let next = select(id, reducer.revision());
        assert!(reducer.plan(&next).is_err());
        let ApplyResult::Applied { event } = canonical.apply(next)? else {
            panic!("fresh selection replayed")
        };
        reducer.apply_committed(event, &mut None)?;
        assert_eq!(reducer.resident_context_selections().count(), 1);
    }
    assert_eq!(
        reducer.context_selection_for_operation(operation(68)),
        Some(&selection)
    );
    let restored = restore(&reducer, &issuer())?;
    assert_eq!(restored.resident_context_selections().count(), 1);
    assert_eq!(
        restored.context_selection_for_operation(operation(68)),
        Some(&selection)
    );
    Ok(())
}
