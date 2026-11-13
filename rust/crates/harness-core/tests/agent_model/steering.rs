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
        #[cfg(not(feature = "test-support"))]
        let stream = MemoryStream::default();
        #[cfg(feature = "test-support")]
        let stream = super::hosting::Evidence::default();
        let sender = create(&stream, id(10)).await?;
        let recipient = create(&stream, id(11)).await?;
        let state = AgentState::adopted(sender.clone(), &adopted(sender.id())).expect("adopts");
        let expected = Message {
            from: Sender::Agent {
                path: sender.clone(),
            },
            id: intent("message"),
            re: Some(intent("original-message")),
            body: Box::new(super::driver::file("message.md")?),
            attachments: None,
        };
        #[cfg(feature = "test-support")]
        {
            let receipt = sender.sent_message(&expected.id)?;
            stream.missing(receipt.clone(), StreamError::AccessDenied);
            assert!(matches!(
                sent(&stream, &sender, &expected.id).await,
                Err(acyclic_harness_core::agent::Error::Stream(
                    StreamError::AccessDenied
                ))
            ));
            stream.missing(receipt, super::hosting::missing_refusal());
        }
        assert!(sent(&stream, &sender, &expected.id).await?.is_none());
        let outs = effect_outs(
            &state,
            vec![TurnEffect::Send {
                peer: None,
                to: recipient.clone(),
                delivery,
                id: expected.id.clone(),
                re: expected.re.clone(),
                body: expected.body.clone(),
                attachments: None,
            }],
        );
        let mut step = Step {
            state,
            outs,
            // Soft delivery accompanies the sender's own durable publication.
            status: Some(StatusRecord {
                cancelled_input: None,
                child_settlement: None,
                child_wakes: None,
                phase: Phase::Idle,
                cursor: 1,
                conversation: 0,
            }),
            alarm: Alarm::Clear,
        };
        step.state.status_tail += 1;
        let open = lower(&step, 1, intent_key("open", &[])?)?;
        assert!(matches!(
            stream.commit(open.clone()).await?,
            CommitOutcome::Committed(_)
        ));
        // A lost acknowledgement replays this same root-to-root publication.
        assert!(matches!(
            stream.commit(open).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(inbox_tail(&stream, &recipient).await?, 2);
        assert_eq!(
            sent(&stream, &sender, &expected.id).await?,
            Some(SentMessage {
                to: recipient.clone(),
                id: expected.id.clone(),
                delivery: SentDelivery::Complete(acyclic_actors::route::Routed::Delivered(
                    acyclic_actors::route::Delivered { inbox: 1 }
                )),
            })
        );
        let records = values(&stream, recipient.inbox()?).await?;
        let record = InboxRecord::decode(&records[1])?;
        match (&record, delivery) {
            (InboxRecord::Message(message), MessageDelivery::Steer)
            | (InboxRecord::FollowUp(message), MessageDelivery::FollowUp) => {
                assert_eq!(message, &expected);
            }
            _ => return Err("peer delivery changed its turn semantics".into()),
        }
        let boundary = select_boundary(&stream, &recipient, 1, 1, BoundaryEnd::Open).await?;
        assert_eq!(
            boundary.inputs.len(),
            usize::from(delivery == MessageDelivery::Steer)
        );

        // Close after preparation, before submission: the same effect must skip.
        for out in &mut step.outs {
            if let Out::Send {
                record: InboxRecord::Message(message) | InboxRecord::FollowUp(message),
                ..
            } = out
            {
                message.id = intent("late");
            }
        }
        step.state.status_tail += 1;
        let late = lower(&step, 1, intent_key("late", &[])?)?;
        submit(&stream, archive(&recipient, &user(), &intent("close"))?).await?;
        let closed_tail = inbox_tail(&stream, &recipient).await?;
        assert!(matches!(
            stream.commit(late).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(inbox_tail(&stream, &recipient).await?, closed_tail);
        assert_eq!(
            sent(&stream, &sender, &intent("late")).await?,
            Some(SentMessage {
                to: recipient.clone(),
                id: intent("late"),
                delivery: SentDelivery::Complete(acyclic_actors::route::Routed::Rejected(
                    acyclic_actors::route::Rejection::Closed
                )),
            })
        );

        // The actual same append lands if its original close condition is removed.
        for out in &mut step.outs {
            if let Out::Send {
                record: InboxRecord::Message(message) | InboxRecord::FollowUp(message),
                ..
            } = out
            {
                message.id = intent("unguarded");
            }
        }
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
async fn steering_withdraws_queued_messages_without_changing_frozen_admission() -> Result {
    let stream = MemoryStream::default();
    let path = create(&stream, id(7)).await?;
    let message = |name: &str| -> Result<InboxRecord> {
        Ok(InboxRecord::Message(Message {
            from: user(),
            id: intent(name),
            body: Box::new(super::driver::file("message.md")?),
            re: None,
            attachments: None,
        }))
    };
    for name in ["running", "withdrawn", "retained"] {
        submit(&stream, send(&path, &user(), &message(name)?)?).await?;
    }
    let cancellation = |name: &str, input| {
        InboxRecord::Cancel(Cancellation {
            by: user(),
            id: intent(name),
            input,
            conversation: 0,
        })
    };
    submit(
        &stream,
        send(&path, &user(), &cancellation("withdraw-queued", 2))?,
    )
    .await?;
    // B and its accepted cancellation are both committed before A's boundary.
    // B must never reach the model; the unrelated message still joins A.
    let selected = select_boundary(&stream, &path, 1, 2, BoundaryEnd::Open).await?;
    assert_eq!((selected.from, selected.through), (2, 5));
    assert!(
        matches!(selected.inputs.as_slice(), [(3, InboxRecord::Message(message))]
        if message.id == intent("retained"))
    );
    assert_eq!(
        select_boundary(&stream, &path, 1, 2, BoundaryEnd::Exact(5)).await?,
        selected
    );
    // Once the candidate's original range is frozen, a later cancellation
    // cannot rewrite it. The next boundary reports its ordered withdrawal.
    submit(
        &stream,
        send(&path, &user(), &cancellation("withdraw-admitted", 3))?,
    )
    .await?;
    assert_eq!(
        select_boundary(&stream, &path, 1, 2, BoundaryEnd::Exact(5)).await?,
        selected
    );
    let later = select_boundary(&stream, &path, 1, 5, BoundaryEnd::Open).await?;
    assert_eq!(later.through, 6);
    assert!(matches!(later.inputs.as_slice(), [(5, InboxRecord::Notice {
        notice: NoticePayload::Quiet(notice), ..
    })] if notice.kind == "harness.input_cancelled" && notice.body["input"] == 3));
    Ok(())
}

#[tokio::test]
async fn steering_keeps_reserved_turns_and_freezes_cancellation_notices() -> Result {
    let stream = MemoryStream::default();
    let path = create(&stream, id(1)).await?;
    let state = AgentState::adopted(path.clone(), &adopted(path.id())).expect("adopts");
    let message = Message {
        from: user(),
        id: intent("followup"),
        body: Box::new(super::driver::file("message.md")?),
        re: None,
        attachments: None,
    };
    for record in [
        notice(1, false),
        notice(2, true),
        InboxRecord::FollowUp(message.clone()),
        notice(3, true),
        cancel("stop"),
        InboxRecord::Message(Message {
            id: intent("after-cancel"),
            ..message
        }),
    ] {
        submit(&stream, send(&path, &user(), &record)?).await?;
    }
    let first = select_boundary(
        &stream,
        &state.path,
        3,
        1,
        acyclic_harness_core::agent::BoundaryEnd::Open,
    )
    .await?;
    assert_eq!((first.from, first.through, first.inputs.len()), (1, 7, 5));
    assert!(matches!(&first.inputs[3].1, InboxRecord::Notice {
        notice: NoticePayload::Quiet(notice), ..
    } if notice.kind == "harness.input_cancelled" && notice.body["input"] == 1));
    // The same accepted cancellation is a barrier for its own running input,
    // even before the asynchronous interrupt predicate has completed. The
    // later message remains available for a separate turn.
    let stopped = select_boundary(&stream, &path, 1, 1, BoundaryEnd::Open).await?;
    assert_eq!((stopped.through, stopped.inputs.len()), (5, 3));
    assert!(stopped.inputs.iter().all(|(sequence, _)| *sequence < 5));
    assert_eq!(
        select_boundary(&stream, &path, 1, 1, BoundaryEnd::Exact(5)).await?,
        stopped
    );
    assert!(
        select_boundary(&stream, &path, 1, 1, BoundaryEnd::Exact(7))
            .await
            .is_err()
    );
    // An explicitly started follow-up can admit steering that arrived after it.
    let next = select_boundary(
        &stream,
        &state.path,
        3,
        4,
        acyclic_harness_core::agent::BoundaryEnd::Open,
    )
    .await?;
    assert_eq!((next.through, next.inputs.len()), (7, 3));
    assert_eq!(
        select_boundary(
            &stream,
            &state.path,
            3,
            5,
            acyclic_harness_core::agent::BoundaryEnd::Open
        )
        .await?
        .through,
        7
    );
    // The frozen range cannot absorb records that arrived after preparation.
    assert_eq!(
        select_boundary(
            &stream,
            &state.path,
            3,
            1,
            acyclic_harness_core::agent::BoundaryEnd::Exact(first.through)
        )
        .await?,
        first
    );
    assert!(
        select_boundary(
            &stream,
            &state.path,
            3,
            1,
            acyclic_harness_core::agent::BoundaryEnd::Exact(8)
        )
        .await
        .is_err()
    );
    // Completion/cancellation races, including reserved follow-ups, are proved
    // by handler::tests::unrelated_turn_completion_preserves_a_queued_cancellation
    // using a real atomic settlement rather than an invented conversation row.
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
    let selected = select_boundary(
        &stream,
        &other_state.path,
        1,
        1,
        acyclic_harness_core::agent::BoundaryEnd::Open,
    )
    .await?;
    assert_eq!((selected.through, selected.inputs.len()), (4, 2));
    assert_eq!(
        select_boundary(
            &stream,
            &other_state.path,
            1,
            1,
            acyclic_harness_core::agent::BoundaryEnd::Exact(4)
        )
        .await?,
        selected
    );
    Ok(())
}
