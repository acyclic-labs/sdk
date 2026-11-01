//! The one committed-reply correlation: an accepted message's settled entry,
//! found by binary search over the agent's own committed conversation range.

use super::{Result, create, driver::file, id, intent, user};
use acyclic_harness_core::agent::*;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, StreamProvider,
};

fn message(name: &str) -> Result<TurnInput> {
    Ok(TurnInput::Message {
        from: user(),
        id: intent(name),
        body: Box::new(file(name)?),
        attachments: None,
    })
}

fn entry(inbox: u64, input: TurnInput) -> Result<Vec<u8>> {
    Ok(ConversationEntry {
        inbox,
        input,
        output: file("output")?,
        outcome: Default::default(),
        goal: None,
        targets: None,
        tool_results: None,
        steering: None,
        notices: None,
    }
    .encode()?)
}

async fn append(provider: &MemoryStream, path: &AgentPath, records: Vec<Vec<u8>>) -> Result {
    let conversation = path.conversation()?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: conversation.clone(),
            expected: 0,
        }],
        mutations: vec![CommitMutation::Append {
            path: conversation,
            records: records.into_iter().map(Into::into).collect(),
        }],
        idempotency_key: intent_key(path.as_str(), &[])?,
    };
    assert!(matches!(
        provider.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn a_reply_is_answered_pending_dropped_or_a_conflict() -> Result {
    let provider = MemoryStream::default();
    let root = create(&provider, id(1)).await?;
    let approval = TurnInput::ApprovalRequested {
        child: root.child(&id(7))?,
        id: intent("q"),
    };
    // Settled m1 and m3 and an approval request at 4; m6's entry is staged
    // past the committed tail, so it is not yet a reply.
    append(
        &provider,
        &root,
        vec![
            entry(1, message("m1")?)?,
            entry(3, message("m3")?)?,
            entry(4, approval)?,
            entry(6, message("m6")?)?,
        ],
    )
    .await?;
    let mut state = AgentState::adopted(root.clone(), &super::adopted(id(1))).expect("adopts");
    state.conversation_tail = 3;
    state.queued.push_back(Queued {
        inbox: 5,
        input: message("m5")?,
    });
    let provider = &provider;
    let ask = move |state: AgentState, name: &'static str, inbox: u64| async move {
        reply(provider, &state, 7, &intent(name), inbox).await
    };
    for (name, inbox, sequence) in [("m1", 1, 0), ("m3", 3, 1)] {
        let Reply::Answered {
            sequence: found,
            entry,
            ..
        } = ask(state.clone(), name, inbox).await?
        else {
            return Err(format!("{name} is not answered").into());
        };
        assert_eq!((found, entry.inbox), (sequence, inbox));
    }
    // Another request's entry is never this request's answer.
    for (name, inbox) in [("m1", 3), ("m4", 4)] {
        assert!(matches!(
            ask(state.clone(), name, inbox).await,
            Err(Error::Conflict(_))
        ));
    }
    assert_eq!(ask(state.clone(), "m5", 5).await?, Reply::Pending);
    assert_eq!(state.settled_before(7), 5);
    assert_eq!(ask(state.clone(), "m8", 8).await?, Reply::Pending);
    // m2 was consumed without an entry: a cancel cleared it.
    assert_eq!(ask(state.clone(), "m2", 2).await?, Reply::Dropped);
    assert_eq!(ask(state.clone(), "m6", 6).await?, Reply::Dropped);
    // Another prompt's running turn leaves a dropped prompt dropped.
    state.turn = Some(Progress {
        inbox: 7,
        checkpoint: Vec::new(),
    });
    assert_eq!(ask(state.clone(), "m6", 6).await?, Reply::Dropped);
    state.turn = Some(Progress {
        inbox: 6,
        checkpoint: Vec::new(),
    });
    assert_eq!(ask(state.clone(), "m6", 6).await?, Reply::Pending);
    state.queued.clear();
    assert_eq!(
        state.settled_before(7),
        6,
        "a consumed paused turn is still pending"
    );
    state.turn = None;
    state.overflow = Some(4);
    assert_eq!(
        state.settled_before(7),
        4,
        "unloaded inputs retain the original frontier"
    );
    state.overflow = None;
    assert_eq!(state.settled_before(7), 7);

    // A forked agent searches only its own entries above the inherited base.
    let forked = create(provider, id(2)).await?;
    append(
        provider,
        &forked,
        vec![
            entry(1, message("m1")?)?,
            entry(3, message("m3")?)?,
            entry(5, message("c5")?)?,
        ],
    )
    .await?;
    let mut adoption = super::adopted(id(2));
    adoption.conversation_tail = 2;
    let mut state = AgentState::adopted(forked, &adoption).expect("adopts");
    state.conversation_tail = 3;
    assert_eq!(ask(state.clone(), "m1", 1).await?, Reply::Dropped);
    assert!(matches!(
        ask(state, "c5", 5).await?,
        Reply::Answered { sequence: 2, .. }
    ));
    Ok(())
}

#[tokio::test]
async fn admitted_steering_joins_its_settling_turn_and_original_identity() -> Result {
    let provider = MemoryStream::default();
    let path = create(&provider, id(11)).await?;
    for name in ["initial", "steered", "after"] {
        let record = InboxRecord::Message(Message {
            from: user(),
            id: intent(name),
            body: Box::new(file(name)?),
            attachments: None,
        });
        submit(&provider, send(&path, &user(), &record)?).await?;
    }
    let mut state = AgentState::adopted(path.clone(), &super::adopted(id(11))).expect("adopts");
    let selected = select_boundary(&provider, &state, 0, 2, 1, Some(3)).await?;
    assert_eq!(selected.inputs.len(), 1);
    let mut settled = ConversationEntry::decode(&entry(1, message("initial")?)?)?;
    settled.steering = Some(SteeringTranscript {
        through: selected.through,
        file: file("transcript")?,
    });
    append(
        &provider,
        &path,
        vec![settled.encode()?, entry(3, message("after")?)?],
    )
    .await?;
    state.queued.push_back(Queued {
        inbox: 2,
        input: message("steered")?,
    });
    // Staging an entry is not settling it: only the committed tail admits it.
    assert_eq!(
        reply(&provider, &state, 4, &intent("steered"), 2).await?,
        Reply::Pending
    );
    state.conversation_tail = 1;
    state.queued.clear();
    state.steered_through = selected.through;
    assert!(matches!(
        reply(&provider, &state, 4, &intent("steered"), 2).await?,
        Reply::Answered { sequence: 0, entry, origin: None } if entry == settled
    ));
    assert!(matches!(
        reply(&provider, &state, 4, &intent("wrong"), 2).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        reply(&provider, &state, 4, &intent("after"), 3).await?,
        Reply::Dropped,
        "the exclusive admitted cursor cannot answer a later input"
    );
    state.conversation_tail = 2;
    assert!(matches!(
        reply(&provider, &state, 4, &intent("steered"), 2).await?,
        Reply::Answered { sequence: 0, .. }
    ));
    assert!(matches!(
        reply(&provider, &state, 4, &intent("after"), 3).await?,
        Reply::Answered { sequence: 1, .. }
    ));
    // Fork history cannot answer a request accepted by the child itself.
    state.conversation_base = 1;
    assert_eq!(
        reply(&provider, &state, 4, &intent("steered"), 2).await?,
        Reply::Dropped
    );
    Ok(())
}
