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
async fn steering_keeps_followups_and_interrupts_behind_their_source_barriers() -> Result {
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
    let first = select_boundary(&stream, &state, 0, 1, 0, None).await?;
    assert_eq!((first.from, first.through, first.inputs.len()), (1, 3, 2));
    // An explicitly started follow-up can admit steering that arrived after it.
    let next = select_boundary(&stream, &state, 0, 4, 3, None).await?;
    assert_eq!((next.through, next.inputs.len()), (5, 1));
    assert_eq!(
        select_boundary(&stream, &state, 0, 5, 3, None)
            .await?
            .through,
        5
    );
    // The frozen range cannot absorb records that arrived after preparation.
    assert_eq!(
        select_boundary(&stream, &state, 0, 1, 0, Some(first.through)).await?,
        first
    );
    assert!(
        select_boundary(&stream, &state, 0, 1, 0, Some(4))
            .await
            .is_err()
    );
    // Every input that keeps its own turn is a barrier, including a child's
    // approval request. Later steering cannot overtake that conversation entry.
    let approval = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: path.child(&id(3))?,
        id: intent("approval"),
    });
    let other = create(&stream, id(2)).await?;
    let other_state = AgentState::adopted(other.clone(), &adopted(other.id())).expect("adopts");
    for record in [notice(5, false), approval, notice(6, true)] {
        submit(&stream, send(&other, &user(), &record)?).await?;
    }
    let selected = select_boundary(&stream, &other_state, 0, 1, 0, None).await?;
    assert_eq!((selected.through, selected.inputs.len()), (2, 1));
    assert!(
        select_boundary(&stream, &other_state, 0, 1, 0, Some(4))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn consumed_approval_and_hook_responses_do_not_block_later_steering() -> Result {
    let stream = MemoryStream::default();
    let path = create(&stream, id(4)).await?;
    let approval = InboxRecord::Approval(Approval {
        id: intent("resume"),
    });
    let hook = InboxRecord::Hook(Hook {
        host: "editor".into(),
        id: intent("hook"),
        event: Box::new(super::driver::file("hook")?),
    });
    for record in [
        notice(1, false),
        approval,
        notice(2, true),
        hook,
        notice(3, false),
    ] {
        submit(&stream, send(&path, &user(), &record)?).await?;
    }
    let mut state = AgentState::adopted(path, &adopted(id(4))).expect("adopts");
    state.queued.extend([
        Queued {
            inbox: 2,
            input: TurnInput::Approval {
                id: intent("resume"),
            },
        },
        Queued {
            inbox: 4,
            input: TurnInput::Hook {
                host: "editor".into(),
                id: intent("hook"),
                event: Box::new(super::driver::file("hook")?),
            },
        },
    ]);
    let before = select_boundary(&stream, &state, 6, 1, 0, None).await?;
    assert_eq!(before.through, 2, "a queued response retains its own turn");
    state.queued.pop_front();
    let resumed = select_boundary(&stream, &state, 6, 1, 0, None).await?;
    assert_eq!((resumed.through, resumed.inputs.len()), (4, 2));
    state.queued.clear();
    // The existing overflow exceptions distinguish an out-of-order response
    // already taken from one still unread behind the retained window.
    state.overflow = Some(2);
    state.taken.extend([2, 4]);
    let selected = select_boundary(&stream, &state, 6, 1, 0, None).await?;
    assert_eq!((selected.through, selected.inputs.len()), (6, 3));
    submit(&stream, send(&state.path, &user(), &notice(4, true))?).await?;
    assert_eq!(
        select_boundary(&stream, &state, 6, 1, 0, Some(selected.through)).await?,
        selected,
        "replay cannot absorb a later source suffix"
    );
    state.taken.remove(&2);
    assert_eq!(
        select_boundary(&stream, &state, 6, 1, 0, None)
            .await?
            .through,
        2
    );
    Ok(())
}
