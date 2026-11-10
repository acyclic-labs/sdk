//! Primitive-backed ordering and frozen inbox cursor regressions; no model calls.
use super::*;

fn notice(index: usize, wake: bool) -> InboxRecord {
    InboxRecord::Notice {
        id: intent(&format!("event-{index}")),
        notice: if wake {
            NoticePayload::Waking(Box::new(
                super::corpus::notice_file().expect("valid Notice reference"),
            ))
        } else {
            NoticePayload::Quiet(acyclic_harness_core::conversation::wire::Notice {
                source: "events/source".into(),
                kind: "test.event".into(),
                body: serde_json::json!({"index": index}),
                wake,
                coalesce_key: None,
            })
        },
    }
}

#[tokio::test]
async fn steering_review_sends_skip_closed_recipients() -> Result {
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        let stream = MemoryStream::default();
        let sender = create(&stream, id(10)).await?;
        let recipient = create(&stream, id(11)).await?;
        let state = AgentState::adopted(sender.clone(), &adopted(sender.id())).expect("adopts");
        let message = Message {
            from: Sender::Agent { path: sender },
            id: intent("message"),
            body: Box::new(super::driver::file("message.md")?),
            attachments: None,
        };
        let record = match delivery {
            MessageDelivery::Steer => InboxRecord::Message(message),
            MessageDelivery::FollowUp => InboxRecord::FollowUp(message),
        };
        let mut step = Step {
            state,
            outs: vec![Out::Send {
                to: recipient.clone(),
                record,
            }],
            // Soft delivery accompanies the sender's own durable publication.
            status: Some(StatusRecord {
                child_wakes: Vec::new(),
                phase: Phase::Idle,
                cursor: 1,
                conversation: 0,
            }),
            alarm: Alarm::Clear,
        };
        step.state.status_tail += 1;
        let open = lower(&step, 1, intent_key("open", &[])?)?;
        assert!(matches!(
            stream.commit(open).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(inbox_tail(&stream, &recipient).await?, 2);

        // Close after preparation, before submission: the same effect must skip.
        step.state.status_tail += 1;
        let late = lower(&step, 1, intent_key("late", &[])?)?;
        submit(&stream, archive(&recipient, &user(), &intent("close"))?).await?;
        let closed_tail = inbox_tail(&stream, &recipient).await?;
        assert!(matches!(
            stream.commit(late).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(inbox_tail(&stream, &recipient).await?, closed_tail);

        // The actual same append lands if its original close condition is removed.
        step.state.status_tail += 1;
        let mut unguarded = lower(&step, 1, intent_key("unguarded", &[])?)?;
        for effect in unguarded.effects.iter_mut().filter(|effect| effect.soft) {
            effect
                .conditions
                .retain(|condition| !matches!(condition, CommitCondition::Absent { .. }));
        }
        assert!(matches!(
            stream.commit(unguarded).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(inbox_tail(&stream, &recipient).await?, closed_tail + 1);
    }
    Ok(())
}

#[tokio::test]
async fn steering_keeps_reserved_turns_and_stops_at_interrupts() -> Result {
    let stream = MemoryStream::default();
    let path = create(&stream, id(1)).await?;
    let state = AgentState::adopted(path.clone(), &adopted(path.id())).expect("adopts");
    let message = Message {
        from: user(),
        id: intent("followup"),
        body: Box::new(super::driver::file("message.md")?),
        attachments: None,
    };
    for record in [
        notice(1, false),
        notice(2, true),
        InboxRecord::FollowUp(message),
        notice(3, true),
        cancel("stop"),
        notice(4, true),
    ] {
        submit(&stream, send(&path, &user(), &record)?).await?;
    }
    let first = select_boundary(&stream, &state.path, 1, None).await?;
    assert_eq!((first.from, first.through, first.inputs.len()), (1, 5, 3));
    // An explicitly started follow-up can admit steering that arrived after it.
    let next = select_boundary(&stream, &state.path, 4, None).await?;
    assert_eq!((next.through, next.inputs.len()), (5, 1));
    assert_eq!(
        select_boundary(&stream, &state.path, 5, None)
            .await?
            .through,
        5
    );
    // The frozen range cannot absorb records that arrived after preparation.
    assert_eq!(
        select_boundary(&stream, &state.path, 1, Some(first.through)).await?,
        first
    );
    assert!(
        select_boundary(&stream, &state.path, 1, Some(6))
            .await
            .is_err()
    );
    // Independent turns retain their entries, but cannot block a later reply.
    let approval = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: path.child(&id(3))?,
        id: intent("approval"),
    });
    let other = create(&stream, id(2)).await?;
    let other_state = AgentState::adopted(other.clone(), &adopted(other.id())).expect("adopts");
    for record in [notice(5, false), approval, notice(6, true)] {
        submit(&stream, send(&other, &user(), &record)?).await?;
    }
    let selected = select_boundary(&stream, &other_state.path, 1, None).await?;
    assert_eq!((selected.through, selected.inputs.len()), (4, 2));
    assert_eq!(
        select_boundary(&stream, &other_state.path, 1, Some(4)).await?,
        selected
    );
    Ok(())
}
