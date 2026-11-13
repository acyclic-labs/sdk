//! Headless goal execution is a client composition of ordinary file inputs,
//! the existing durable goal continuation and the shared supervision feed.
use super::*;
use crate::{
    client_files::{ClientFileChange, FileRoot},
    goal::{GoalState, GoalStatus},
    supervision::{Action, Kind, Lifecycle, Policy, RunAdmission},
};
use serde::Serialize;

/// Terminal observation of one unattended goal run. Policy enforcement belongs
/// to the shared host; this client never invents a second agent loop.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOutcome {
    /// The original goal continuation ended with this file state.
    Goal {
        /// Exact goal contents observed after the committed terminal reply.
        goal: GoalState,
    },
    /// The execution owner published a durable pause or stop.
    Terminal {
        /// Pause or stop; alerts continue the run.
        action: Action,
        /// Original signal, or null for an explicit file control.
        signal: Option<Kind>,
    },
}

impl Conversation {
    /// Admit file inputs once and run until goal completion or the host's
    /// terminal supervision event. Works over the same local/remote session.
    /// Reusing `operation` refuses changed input and preserves an already
    /// modified GOAL.md; recovery never overwrites the agent's progress.
    pub async fn run_until_goal(
        &self,
        operation: crate::OperationId,
        goal: &str,
        policy: &Policy,
    ) -> Result<RunOutcome> {
        let parsed = GoalState::from_bytes(goal.as_bytes(), u64::MAX)?;
        if parsed.status != GoalStatus::Active {
            return Err(Error::Invalid("a new run requires an active goal".into()));
        }
        policy
            .validate()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let policy = serde_json::to_string(policy).map_err(failed)?;
        let admission = RunAdmission::new(operation, goal.as_bytes(), policy.as_bytes());
        let marker = format!("runs/{operation}.json");
        let paths = vec![
            marker.clone(),
            "GOAL.md".into(),
            "control/supervision.json".into(),
        ];
        let mut original = self
            .files_at_publication(FileRoot::Home {}, paths.clone(), operation)
            .await?;
        if original.files.values().all(Option::is_none) {
            let files = self.files(FileRoot::Home {}, paths.clone(), None).await?;
            // The same original conditional batch owns goal, policy and
            // admission. A conflict or lost observer cannot partially
            // admit a policy or reset a previously admitted goal.
            self.publish_files(
                FileRoot::Home {},
                operation,
                [
                    ("GOAL.md".to_owned(), goal.to_owned()),
                    ("control/supervision.json".to_owned(), policy.clone()),
                    (
                        marker.clone(),
                        serde_json::to_string(&admission).map_err(failed)?,
                    ),
                ]
                .into_iter()
                .map(|(path, text)| ClientFileChange {
                    expected: files.files[&path]
                        .as_ref()
                        .and_then(|file| file.original().cloned()),
                    path,
                    text: Some(text),
                })
                .collect(),
            )
            .await?;
            original = self
                .files_at_publication(FileRoot::Home {}, paths, operation)
                .await?;
        }
        let select = |path: &str| {
            original.files[path]
                .as_ref()
                .ok_or_else(|| Error::Conflict("original run admission omits an input".into()))
        };
        let declaration: RunAdmission =
            serde_json::from_str(&select(&marker)?.text).map_err(failed)?;
        declaration.verify(operation, goal.as_bytes(), policy.as_bytes())?;
        declaration.verify(
            operation,
            select("GOAL.md")?.text.as_bytes(),
            select("control/supervision.json")?.text.as_bytes(),
        )?;
        let mut feed = self.connection.supervision_own().await?;
        let owner = feed.owner_control().await?;
        if let Some((_, event)) = &owner {
            match event.event {
                Lifecycle::Paused { signal } => {
                    return Ok(RunOutcome::Terminal {
                        action: Action::Pause,
                        signal,
                    });
                }
                Lifecycle::Stopped { signal } => {
                    return Ok(RunOutcome::Terminal {
                        action: Action::Stop,
                        signal,
                    });
                }
                _ => {}
            }
        }
        let owner_through = owner.map(|(sequence, _)| sequence);
        let mut events = self.observe(None).await?;
        let id = operation.to_string();
        let prompt = format!(
            "Complete the objective in GOAL.md. Update its status to done when complete.\n\n{goal}"
        );
        let inbox = self.send(&id, &prompt).await?;
        let completion = async {
            loop {
                match self.settle(&id, inbox, &mut events).await? {
                    Settlement::Approval { .. } => {
                        // External approvers use the same durable approval
                        // files; wait on their event, never retry readiness.
                        events.next().await?;
                    }
                    Settlement::Reply(Reply::Answered { entry, .. }) => {
                        // A failed or cancelled turn cannot satisfy a goal.
                        // Preserve its original committed cause rather than
                        // replacing it with an active-goal protocol error.
                        match &entry.outcome {
                            crate::agent::TurnOutcome::Completed => {}
                            crate::agent::TurnOutcome::Failed { message } => {
                                return Err(Error::Refused(message.clone()));
                            }
                            crate::agent::TurnOutcome::Cancelled => {
                                return Err(Error::Refused("goal run was cancelled".into()));
                            }
                            crate::agent::TurnOutcome::RestoreConflict => {
                                return Err(Error::Refused(self.output_text(&entry).await?));
                            }
                        }
                        let files = self
                            .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
                            .await?;
                        let file = files.files["GOAL.md"]
                            .as_ref()
                            .ok_or_else(|| Error::Conflict("run ended without GOAL.md".into()))?;
                        let goal = GoalState::from_bytes(file.text.as_bytes(), u64::MAX)?;
                        if goal.status == GoalStatus::Active {
                            return Err(Error::Conflict(
                                "goal continuation ended with an active goal".into(),
                            ));
                        }
                        return Ok(RunOutcome::Goal { goal });
                    }
                    Settlement::Reply(Reply::Dropped) => {
                        return Err(Error::Refused(self.dropped().await?));
                    }
                    Settlement::Reply(_) => return Err(unexpected()),
                    Settlement::Ended(reason) => return Err(Error::Refused(reason)),
                }
            }
        };
        tokio::pin!(completion);
        let mut run = None;
        let mut completed = None;
        loop {
            if let Some((_, cut)) = &completed
                && feed.reached(cut)?
            {
                return completed.take().ok_or_else(unexpected)?.0;
            }
            tokio::select! {
                // Drain the captured source cut before returning a reply.
                // Bias alone cannot order independently delivered follows.
                biased;
                event = feed.next_record() => {
                    let (position, event) = event?;
                    if event.agent != *self.agent() { continue; }
                    if owner_through.is_some_and(|sequence| position.sequence <= sequence)
                        && (event.run.is_none() || matches!(event.event, Lifecycle::Paused { .. })) { continue; }
                    if matches!(event.event, Lifecycle::Started { input } if input == inbox) {
                        run = event.run;
                    }
                    if event.run.is_some() && run != event.run { continue; }
                    match event.event {
                        Lifecycle::Paused { signal } => return Ok(RunOutcome::Terminal { action: Action::Pause, signal }),
                        Lifecycle::Stopped { signal } => return Ok(RunOutcome::Terminal { action: Action::Stop, signal }),
                        _ => {}
                    }
                }
                result = &mut completion, if completed.is_none() => {
                    completed = Some((result, feed.root_cut().await?));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LocalHarness,
        local::tests::{volume, volume_config},
        test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    };
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn file_pause_preserves_in_flight_model_and_resume_reuses_its_original_reply()
    -> Result<()> {
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let gate = Mutex::new(Some((entered, released)));
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("completed".into())])
                .with_dispatch_observer(move |_| {
                    let next = gate.lock().unwrap().take();
                    Box::pin(async move {
                        if let Some((entered, released)) = next {
                            let _ = entered.send(());
                            released.await.map_err(failed)?;
                        }
                        Ok(())
                    })
                }),
        );
        let directory = tempfile::tempdir().map_err(failed)?;
        let project = volume(directory.path()).await?;
        let harness = Arc::new(
            LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?,
        );
        let conversation = Conversation::open(harness.connect().await?);
        let operation = crate::OperationId::new();
        let policy = Policy::default();
        let run = conversation.run_until_goal(operation, "Finish the objective.", &policy);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("run ended before model admission: {result:?}"),
            result = entering => result.map_err(failed)?,
        }
        conversation
            .publish_files(
                FileRoot::Private {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: "control/pause".into(),
                    expected: None,
                    text: Some(String::new()),
                }],
            )
            .await?;
        assert!(matches!(
            run.await?,
            RunOutcome::Terminal {
                action: Action::Pause,
                signal: None
            }
        ));
        let goal = conversation
            .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
            .await?;
        conversation
            .publish_files(
                FileRoot::Home {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: "GOAL.md".into(),
                    expected: Some(
                        goal.files["GOAL.md"]
                            .as_ref()
                            .ok_or_else(unexpected)?
                            .original()
                            .unwrap()
                            .clone(),
                    ),
                    text: Some("---\nstatus: done\n---\nFinish the objective.\n".into()),
                }],
            )
            .await?;
        release
            .send(())
            .map_err(|_| Error::Invalid("pause cancelled the original model delivery".into()))?;
        let pause = conversation
            .files(FileRoot::Private {}, vec!["control/pause".into()], None)
            .await?;
        let mut feed = conversation.connection.supervision_own().await?;
        conversation
            .publish_files(
                FileRoot::Private {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: "control/pause".into(),
                    expected: Some(
                        pause.files["control/pause"]
                            .as_ref()
                            .ok_or_else(unexpected)?
                            .original()
                            .unwrap()
                            .clone(),
                    ),
                    text: None,
                }],
            )
            .await?;
        loop {
            if matches!(feed.next().await?.event, Lifecycle::Resumed {}) {
                break;
            }
        }
        assert!(
            matches!(conversation.run_until_goal(operation, "Finish the objective.", &policy).await?,
            RunOutcome::Goal { goal } if goal.status == GoalStatus::Done)
        );
        assert!(
            provider.request(1).is_none(),
            "resume re-dispatched an original model call"
        );
        Ok(())
    }

    #[tokio::test]
    async fn unattended_failure_preserves_original_committed_cause() -> Result<()> {
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("unused".into())])
                .with_dispatch_observer(|_| {
                    Box::pin(async { Err(Error::Invalid("declined by mock model".into())) })
                }),
        );
        let directory = tempfile::tempdir().map_err(failed)?;
        let project = volume(directory.path()).await?;
        let harness = Arc::new(
            LocalHarness::open(volume_config(directory.path(), project, provider)?).await?,
        );
        let conversation = Conversation::open(harness.connect().await?);
        assert!(
            matches!(conversation.run_until_goal(crate::OperationId::new(), "Finish the objective.", &Policy::default()).await,
            Err(Error::Refused(message)) if message.contains("declined by mock model"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn unattended_capacity_decisions_are_enforced_by_shared_actor() -> Result<()> {
        for action in [Action::Pause, Action::Stop] {
            for priced in [false, true] {
                for capacity in [0, 1] {
                    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(
                        vec![
                            ScriptedEvent::Event(crate::model::ModelEvent::Content {
                                delta: "observed output".into(),
                            }),
                            ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                                metadata: serde_json::json!({"usage":{"input_tokens":1,"output_tokens":1}}),
                            }),
                        ],
                    )]));
                    let directory = tempfile::tempdir().map_err(failed)?;
                    let project = volume(directory.path()).await?;
                    let harness = Arc::new(
                        LocalHarness::open(volume_config(
                            directory.path(),
                            project,
                            provider.clone(),
                        )?)
                        .await?,
                    );
                    let conversation = Conversation::open(harness.connect().await?);
                    let policy = Policy {
                        reservation: crate::supervision::Reservation {
                            tokens: (!priced).then_some(capacity),
                            usd_micros: priced.then_some(capacity),
                        },
                        prices: if priced {
                            [(
                                "scripted".into(),
                                [(
                                    "test".into(),
                                    crate::executor::ModelPriceRecord {
                                        version: 1,
                                        revision: "owner-price".into(),
                                        provider: "scripted".into(),
                                        model: "test".into(),
                                        effective_at: 0,
                                        currency: "USD".into(),
                                        input_nano_units_per_million: Some(500_000_000),
                                        output_nano_units_per_million: Some(500_000_000),
                                        cached: crate::executor::TokenPrice::Included,
                                        cache_write: crate::executor::TokenPrice::Included,
                                        reasoning: crate::executor::TokenPrice::Included,
                                    },
                                )]
                                .into(),
                            )]
                            .into()
                        } else {
                            Default::default()
                        },
                        actions: [(Kind::Spend, action)].into(),
                        ..Default::default()
                    };
                    assert!(
                        matches!(conversation.run_until_goal(crate::OperationId::new(), "Continue until completed.", &policy).await?,
                RunOutcome::Terminal { action: observed, signal: Some(Kind::Spend) } if observed == action)
                    );
                    assert!(
                        provider.request(1).is_none(),
                        "another model request passed the original capacity decision"
                    );
                    assert_eq!(
                        provider.request(0).is_some(),
                        capacity > 0,
                        "zero capacity dispatched a provider request"
                    );
                    let (agents, root) = harness.agents().await?;
                    assert_eq!(
                        agents.state(root).await.map_err(failed)?.closed,
                        action == Action::Stop
                    );
                    let mut feed = conversation.connection.supervision_own().await?;
                    let started = feed.next().await?;
                    assert!(matches!(started.event, Lifecycle::Started { .. }));
                    let signal = feed.next().await?;
                    assert!(
                        matches!(signal.event, Lifecycle::Signal { signal, action: observed } if signal.kind == Kind::Spend && observed == action)
                    );
                    let terminal = feed.next().await?;
                    assert_eq!(terminal.run, started.run);
                    assert_eq!(terminal.source, signal.source);
                    assert!(matches!(
                        (action, terminal.event),
                        (
                            Action::Pause,
                            Lifecycle::Paused {
                                signal: Some(Kind::Spend)
                            }
                        ) | (
                            Action::Stop,
                            Lifecycle::Stopped {
                                signal: Some(Kind::Spend)
                            }
                        )
                    ));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn unattended_output_loop_pauses_before_another_dispatch() -> Result<()> {
        for tool_calls in [false, true] {
            let responses = ["first-transport-id", "second-transport-id"].map(|call_id| {
                if tool_calls {
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(crate::model::ModelEvent::ToolCall(
                            crate::model::ToolCall::new(
                                call_id.into(),
                                "unavailable_for_this_test".into(),
                                serde_json::json!({"value":1}),
                            ),
                        )),
                        ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                            metadata: serde_json::Value::Null,
                        }),
                    ])
                } else {
                    ScriptedTurn::Text("same response".into())
                }
            });
            let provider = Arc::new(ScriptedModelProvider::new(responses));
            let directory = tempfile::tempdir().map_err(failed)?;
            let project = volume(directory.path()).await?;
            let harness = Arc::new(
                LocalHarness::open(volume_config(directory.path(), project, provider.clone())?)
                    .await?,
            );
            let conversation = Conversation::open(harness.connect().await?);
            assert!(matches!(
                conversation
                    .run_until_goal(
                        crate::OperationId::new(),
                        "Finish the objective.",
                        &Policy::default()
                    )
                    .await?,
                RunOutcome::Terminal {
                    action: Action::Pause,
                    signal: Some(Kind::LoopOutput)
                }
            ));
            assert!(provider.request(1).is_some());
            assert!(
                provider.request(2).is_none(),
                "repeated output admitted another model request"
            );
            let mut feed = conversation.connection.supervision_own().await?;
            let started = feed.next().await?;
            assert!(matches!(started.event, Lifecycle::Started { .. }));
            let mut observed = None;
            loop {
                let event = feed.next().await?;
                assert_eq!(event.run, started.run);
                match event.event {
                    Lifecycle::Signal { signal, action } => {
                        assert_eq!(
                            (signal.kind, signal.count, action),
                            (Kind::LoopOutput, 2, Action::Pause)
                        );
                        observed = Some(event.source);
                    }
                    Lifecycle::Paused {
                        signal: Some(Kind::LoopOutput),
                    } => {
                        assert_eq!(observed, Some(event.source));
                        break;
                    }
                    Lifecycle::TurnDone { .. } => {}
                    other => panic!("unexpected supervision event {other:?}"),
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejected_model_calls_count_original_invocations_and_keep_distinct_arguments_healthy()
    -> Result<()> {
        for repeated in [false, true] {
            let (entered, entering) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let gate = Mutex::new(Some((entered, released)));
            let count = std::sync::atomic::AtomicUsize::new(0);
            let mut responses = (0..3)
                .map(|index| {
                    ScriptedTurn::Events(vec![
                        ScriptedEvent::Event(crate::model::ModelEvent::ToolCall(
                            crate::model::ToolCall::new(
                                format!("call-{index}"),
                                "unavailable_for_this_test".into(),
                                serde_json::json!({"value":if repeated {1} else {index}}),
                            ),
                        )),
                        ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                            metadata: serde_json::Value::Null,
                        }),
                    ])
                })
                .collect::<Vec<_>>();
            responses.push(ScriptedTurn::Text("done".into()));
            let provider = Arc::new(
                ScriptedModelProvider::new(responses).with_dispatch_observer(move |_| {
                    if count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) != 3 {
                        return Box::pin(async { Ok(()) });
                    }
                    if repeated {
                        return Box::pin(async {
                            Err(Error::Invalid(
                                "another provider dispatch passed repeated rejected calls".into(),
                            ))
                        });
                    }
                    let (entered, released) = gate.lock().unwrap().take().unwrap();
                    Box::pin(async move {
                        let _ = entered.send(());
                        released.await.map_err(failed)?;
                        Ok(())
                    })
                }),
            );
            let directory = tempfile::tempdir().map_err(failed)?;
            let project = volume(directory.path()).await?;
            let harness = Arc::new(
                LocalHarness::open(volume_config(directory.path(), project, provider.clone())?)
                    .await?,
            );
            let conversation = Conversation::open(harness.connect().await?);
            let policy = Policy {
                thresholds: crate::supervision::Thresholds {
                    loop_output: None,
                    ..Default::default()
                },
                ..Default::default()
            };
            let run = conversation.run_until_goal(
                crate::OperationId::new(),
                "Finish the objective.",
                &policy,
            );
            tokio::pin!(run);
            if !repeated {
                tokio::select! {
                    result = &mut run => panic!("distinct arguments stopped before goal completion: {result:?}"),
                    result = entering => result.map_err(failed)?,
                }
                let files = conversation
                    .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
                    .await?;
                conversation
                    .publish_files(
                        FileRoot::Home {},
                        crate::OperationId::new(),
                        vec![ClientFileChange {
                            path: "GOAL.md".into(),
                            expected: Some(
                                files.files["GOAL.md"]
                                    .as_ref()
                                    .unwrap()
                                    .original()
                                    .unwrap()
                                    .clone(),
                            ),
                            text: Some("---\nstatus: done\n---\nFinish the objective.\n".into()),
                        }],
                    )
                    .await?;
                let _ = release.send(());
            }
            let outcome = run.await?;
            let mut feed = conversation.connection.supervision_own().await?;
            let cut = feed.root_cut().await?;
            if repeated {
                assert!(matches!(
                    outcome,
                    RunOutcome::Terminal {
                        action: Action::Pause,
                        signal: Some(Kind::RepeatFailure)
                    }
                ));
                assert!(provider.request(2).is_some());
                assert!(
                    provider.request(3).is_none(),
                    "another request passed repeated rejected invocations"
                );
                assert!(matches!(
                    feed.next().await?.event,
                    Lifecycle::Started { .. }
                ));
                for (count, action) in [(2, Action::Alert), (3, Action::Pause)] {
                    assert!(
                        matches!(feed.next().await?.event, Lifecycle::Signal { signal, action: observed }
                        if signal.kind == Kind::RepeatFailure && signal.count == count && observed == action)
                    );
                }
                assert!(matches!(
                    feed.next().await?.event,
                    Lifecycle::Paused {
                        signal: Some(Kind::RepeatFailure)
                    }
                ));
                assert!(feed.reached(&cut)?);
            } else {
                assert!(
                    matches!(outcome, RunOutcome::Goal { goal } if goal.status == GoalStatus::Done)
                );
                assert!(provider.request(3).is_some());
                while !feed.reached(&cut)? {
                    assert!(
                        !matches!(
                            feed.next().await?.event,
                            Lifecycle::Signal { .. }
                                | Lifecycle::Paused { .. }
                                | Lifecycle::Stopped { .. }
                        ),
                        "different original arguments produced a repeat-failure signal"
                    );
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn run_admission_replay_preserves_goal_progress_and_refuses_changed_input() -> Result<()>
    {
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (continued, continuing) = oneshot::channel();
        let (finish, finishing) = oneshot::channel();
        let (intervened, intervening) = oneshot::channel();
        let (release_input, input_released) = oneshot::channel();
        let gate = Mutex::new(std::collections::VecDeque::from([
            (entered, released),
            (intervened, input_released),
            (continued, finishing),
        ]));
        let provider = Arc::new(
            ScriptedModelProvider::new(["first", "independent input", "done"].map(|text| {
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(crate::model::ModelEvent::Content { delta: text.into() }),
                    ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                        metadata: serde_json::json!({"usage":{"input_tokens":1,"output_tokens":1}}),
                    }),
                ])
            }))
            .with_dispatch_observer(move |_| {
                let selected = gate.lock().unwrap().pop_front();
                Box::pin(async move {
                    if let Some((entered, released)) = selected {
                        let _ = entered.send(());
                        released.await.map_err(failed)?;
                    }
                    Ok(())
                })
            }),
        );
        let directory = tempfile::tempdir().map_err(failed)?;
        let project = volume(directory.path()).await?;
        let harness = Arc::new(
            LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?,
        );
        let conversation = Conversation::open(harness.connect().await?);
        let operation = crate::OperationId::new();
        let policy = Policy {
            no_progress_turns: Some(7),
            reservation: crate::supervision::Reservation {
                tokens: Some(1000),
                ..Default::default()
            },
            ..Default::default()
        };
        let run = conversation.run_until_goal(operation, "Finish the objective.", &policy);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("run ended before the model admission: {result:?}"),
            result = entering => result.map_err(failed)?,
        }
        let paths = vec![
            "GOAL.md".into(),
            "control/supervision.json".into(),
            format!("runs/{operation}.json"),
        ];
        let files = conversation.files(FileRoot::Home {}, paths, None).await?;
        let original = files.files["GOAL.md"].as_ref().unwrap();
        assert_eq!(original.text, "Finish the objective.");
        let admitted: Policy = serde_json::from_str(
            &files.files["control/supervision.json"]
                .as_ref()
                .unwrap()
                .text,
        )
        .map_err(failed)?;
        assert_eq!(admitted, policy);
        let hosted = harness.agents.get().unwrap();
        let bound = hosted.server().bound();
        let pinned = bound
            .run_admission(conversation.agent(), operation)
            .await
            .map_err(failed)?
            .unwrap();
        assert_eq!(pinned.value, policy);
        assert_eq!(
            &pinned.policy,
            files.files["control/supervision.json"]
                .as_ref()
                .unwrap()
                .original()
                .unwrap()
        );
        let done = "---\nstatus: done\n---\nFinish the objective.\n";
        let mut interleaved_policy = policy.clone();
        interleaved_policy.no_progress_turns = Some(11);
        interleaved_policy.reservation.tokens = Some(2000);
        conversation
            .publish_files(
                FileRoot::Home {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: "control/supervision.json".into(),
                    expected: Some(pinned.policy.clone()),
                    text: Some(serde_json::to_string(&interleaved_policy).map_err(failed)?),
                }],
            )
            .await?;
        let recovered = bound
            .run_admission(conversation.agent(), operation)
            .await
            .map_err(failed)?
            .unwrap();
        assert_eq!(
            recovered.value, policy,
            "writable edits changed the original run policy"
        );
        assert_eq!(recovered.policy, pinned.policy);
        assert_eq!(
            recovered.goal, pinned.goal,
            "goal progress replaced the original admission"
        );
        let forged = crate::OperationId::new();
        let mut forged_policy = Policy::default();
        forged_policy.reservation.tokens = Some(0);
        let declaration = RunAdmission::new(
            forged,
            b"Finish the objective.",
            &serde_json::to_vec(&forged_policy).map_err(failed)?,
        );
        conversation
            .publish_files(
                FileRoot::Home {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: format!("runs/{forged}.json"),
                    expected: None,
                    text: Some(serde_json::to_string(&declaration).map_err(failed)?),
                }],
            )
            .await?;
        assert!(
            bound
                .run_admission(conversation.agent(), forged)
                .await
                .map_err(failed)?
                .is_none(),
            "a writable lookalike declaration bypassed the original publication"
        );
        // Ordinary messages during a live turn are steering. Reserve a real
        // follow-up so B owns an independent turn between A and its continuation.
        let by = crate::agent::Sender::User {
            principal: "interleaved-user".into(),
        };
        let id = crate::agent::IntentId::try_from("interleaved-user").map_err(failed)?;
        let body = bound
            .stage_text(
                conversation.agent(),
                "message",
                &by,
                &id,
                ("Independent user input", "Independent user input"),
            )
            .await
            .map_err(failed)?;
        let input = crate::agent::InboxRecord::FollowUp(crate::agent::Message {
            from: by.clone(),
            id,
            re: None,
            body: Box::new(body),
            attachments: None,
        });
        let crate::agent::Delivered::Committed { inbox: interleaved } = bound
            .agents()
            .send(conversation.agent(), &by, &input)
            .await
            .map_err(failed)?
        else {
            return Err(Error::Conflict("independent follow-up was closed".into()));
        };
        release.send(()).unwrap();
        tokio::select! {
            result = &mut run => panic!("run ended before the independent input: {result:?}"),
            result = intervening => result.map_err(failed)?,
        }
        // B is an independent user turn, not a second unattended goal loop.
        // Complete the shared goal before B settles; A already queued its continuation.
        let goal = conversation
            .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
            .await?;
        conversation
            .publish_files(
                FileRoot::Home {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    path: "GOAL.md".into(),
                    expected: Some(
                        goal.files["GOAL.md"]
                            .as_ref()
                            .unwrap()
                            .original()
                            .unwrap()
                            .clone(),
                    ),
                    text: Some(done.into()),
                }],
            )
            .await?;
        release_input.send(()).unwrap();
        tokio::select! {
            result = &mut run => panic!("run ended before the continued model admission: {result:?}"),
            result = continuing => result.map_err(failed)?,
        }
        // While A's continuation is held, B's completed actor transition is
        // the committed state. In-flight driver state is not observable here.
        assert!(
            bound
                .agents()
                .state(conversation.agent())
                .await
                .map_err(failed)?
                .run_admission
                .is_none(),
            "independent input inherited another run's admission",
        );
        finish.send(()).unwrap();
        let outcome = (&mut run).await;
        assert!(matches!(outcome?, RunOutcome::Goal { goal } if goal.status == GoalStatus::Done));
        // B has no continuation. A's completed transition is now the durable
        // state, so this checks its restored original admission.
        assert_eq!(
            bound
                .agents()
                .state(conversation.agent())
                .await
                .map_err(failed)?
                .run_admission,
            Some(operation),
            "continuation lost its original owner admission",
        );
        let mut interleaved_events = conversation.observe(None).await?;
        assert!(matches!(
            conversation
                .settle("interleaved-user", interleaved, &mut interleaved_events)
                .await?,
            Settlement::Reply(Reply::Answered { .. }),
        ));
        let accepted = provider.request_count();
        let first = provider.request(0).unwrap();
        let second = provider.request(2).unwrap();
        assert!(second.messages.len() >= first.messages.len());
        assert_eq!(
            &second.messages[..first.messages.len()],
            first.messages.as_slice(),
            "run policy binding or continuation replaced the prompt prefix"
        );
        assert!(
            conversation
                .run_until_goal(operation, "A changed objective.", &policy)
                .await
                .is_err()
        );
        let marker = format!("runs/{operation}.json");
        let current = conversation
            .files(FileRoot::Home {}, vec![marker.clone()], None)
            .await?;
        conversation
            .publish_files(
                FileRoot::Home {},
                crate::OperationId::new(),
                vec![ClientFileChange {
                    expected: current.files[&marker]
                        .as_ref()
                        .and_then(|file| file.original().cloned()),
                    path: marker,
                    text: Some("edited writable declaration".into()),
                }],
            )
            .await?;
        assert!(
            matches!(conversation.run_until_goal(operation, "Finish the objective.", &policy).await?, RunOutcome::Goal { goal } if goal.status == GoalStatus::Done)
        );
        let files = conversation
            .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
            .await?;
        assert_eq!(
            files.files["GOAL.md"].as_ref().unwrap().text,
            done,
            "recovery overwrote goal progress"
        );
        assert_eq!(
            provider.request_count(),
            accepted,
            "replayed admission dispatched another model"
        );
        assert!(matches!(
            conversation
                .run_until_goal(forged, "Finish the objective.", &forged_policy)
                .await?,
            RunOutcome::Terminal {
                signal: Some(Kind::Spend),
                ..
            }
        ));
        assert_eq!(
            bound
                .run_admission(conversation.agent(), forged)
                .await
                .map_err(failed)?
                .unwrap()
                .value,
            forged_policy
        );
        assert_eq!(
            provider.request_count(),
            accepted,
            "a forged marker bypassed its original reservation"
        );
        Ok(())
    }
}
