//! Successive waits through the production executor, inbox and filesystem journal.
use super::*;
use acyclic_harness_core::{
    conversation::wire::Notice,
    filesystem::{Grants, StockExecutors},
    model::{ModelEvent, ToolCall},
    test_support::ScriptedEvent,
};

fn wait(id: &str, timeout: Option<u64>) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            "wait".into(),
            serde_json::json!({"input":true,"timeout_ms":timeout}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        }),
    ])
}

#[tokio::test]
async fn successive_input_waits_require_a_new_reply_or_their_original_alarm() -> Result {
    for (answer, reserved) in [(true, 0), (false, 0), (true, MAX_QUEUED + 2)] {
        let path = AgentPath::root(&id(226));
        let stream = Arc::new(MemoryStream::default());
        let (filesystem, binding) = private_in(&stream, &path).await?;
        let mut turns = vec![
            ScriptedTurn::Text("reply body".into()),
            ScriptedTurn::Text("none".into()),
            wait("first-input", (reserved == 0).then_some(10)),
            wait("second-input", Some(20)),
            ScriptedTurn::Text("both waits finished".into()),
            ScriptedTurn::Text("none".into()),
        ];
        if reserved > 0 {
            turns.extend((0..reserved + 1).flat_map(|_| {
                [
                    ScriptedTurn::Text("reserved turn finished".into()),
                    ScriptedTurn::Text("none".into()),
                ]
            }));
        }
        let model = Arc::new(ScriptedModelProvider::new(turns));
        let executors = StockExecutors::new(
            (
                Model::new("test".into(), "text".into(), "1".into())?,
                model.clone(),
            ),
            ToolRegistry::default(),
            (Default::default(), CompactionPolicy::Disabled),
            Arc::new(Grants(vec!["tool:call:wait".into()])),
        )
        .with_agent_tools(&["wait".into()]);
        let turn_host = FilesystemTurnHost::new(
            stream.clone(),
            within(),
            filesystem.clone(),
            AuthorityRoot::new(
                "agents",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "agents".into(),
                },
            ),
            TurnAdmission::LocalRoot,
            Arc::new(executors),
            LIMITS,
            acyclic_harness_core::approvals::ApprovalPolicy::accept_all("owner"),
        );
        let (clock, mut control) = acyclic_actors::local::ControlledClock::new(100);
        let host = AgentHost::with_spawn_volumes(
            stream.clone(),
            ExecutorDriver::new(turn_host),
            within(),
            acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(clock),
            Arc::new(acyclic_harness_core::filesystem::FilesystemSpawnVolumes::new(filesystem)),
        )?;
        let mut adoption = adopted(path.id());
        adoption.volumes = vec![binding];
        host.create(&intent("input-waits"), adoption).await?;
        let initial = deliver(&host, &path, requested("make-body", &path)).await?;
        assert_eq!(
            initial.phase,
            Phase::Idle,
            "initial model turn: {:?}",
            initial.phase
        );
        assert_eq!(
            initial.conversation_tail, 1,
            "initial model turn made no answer"
        );
        let body = answer_entry(&*stream, &path).await?;
        host.send(
            &path,
            &super::super::user(),
            &InboxRecord::Notice {
                id: intent("quiet-before-wait"),
                notice: NoticePayload::Quiet(Notice {
                    source: "filesystem".into(),
                    kind: "files.changed".into(),
                    body: serde_json::json!({"paths":["before"]}),
                    wake: false,
                    coalesce_key: None,
                }),
            },
        )
        .await?;
        let first = deliver(&host, &path, requested("start-input-waits", &path)).await?;
        assert_eq!(model.requests().len(), 3);
        assert_eq!(
            first.phase,
            Phase::Waiting {
                deadline_unix_ms: (reserved == 0).then_some(110),
                input: true
            }
        );
        assert!(
            first.pending_notices.is_some(),
            "parking consumed frozen notices"
        );
        host.send(
            &path,
            &super::super::user(),
            &InboxRecord::Notice {
                id: intent("quiet-while-parked"),
                notice: NoticePayload::Quiet(Notice {
                    source: "filesystem".into(),
                    kind: "files.changed".into(),
                    body: serde_json::json!({"paths":["after"]}),
                    wake: false,
                    coalesce_key: None,
                }),
            },
        )
        .await?;
        let message = |name| {
            InboxRecord::Message(Message {
                from: super::super::user(),
                id: intent(name),
                body: Box::new(body.clone()),
                re: None,
                attachments: None,
            })
        };
        if reserved > 0 {
            for index in 0..reserved {
                host.send(
                    &path,
                    &super::super::user(),
                    &InboxRecord::Approval(Approval {
                        id: intent(&format!("independent-before-reply-{index}")),
                    }),
                )
                .await?;
            }
            host.send(
                &path,
                &super::super::user(),
                &InboxRecord::FollowUp(Message {
                    from: super::super::user(),
                    id: intent("reserved-follow-up"),
                    body: Box::new(body.clone()),
                    re: None,
                    attachments: None,
                }),
            )
            .await?;
            host.drain(&path).await?;
            assert_eq!(
                model.requests().len(),
                3,
                "reserved inputs woke the parked model"
            );
        }
        let Delivered::Committed { inbox } = host
            .send(&path, &super::super::user(), &message("first-reply"))
            .await?
        else {
            return Err("reply was not committed".into());
        };
        host.drain(&path).await?;
        let mut second = host.state(&path).await?;
        assert_eq!(
            second.phase,
            Phase::Waiting {
                deadline_unix_ms: Some(120),
                input: true
            }
        );
        assert_eq!(
            model.requests().len(),
            4,
            "the old reply released the second wait"
        );
        assert_eq!(second.steered_through, inbox + 1);
        assert!(matches!(
            reply(
                &*stream,
                &mut second,
                inbox + 1,
                &intent("first-reply"),
                inbox
            )
            .await?,
            Reply::Pending
        ));
        assert!(
            second.queued.len() == reserved.min(MAX_QUEUED),
            "already admitted reply remained wakeable"
        );
        assert_eq!(
            second.turn.as_ref().map(|turn| turn.inbox),
            first.turn.as_ref().map(|turn| turn.inbox)
        );
        host.drain(&path).await?;
        assert_eq!(
            model.requests().len(),
            4,
            "draining without new input released an input wait"
        );
        control.advance_to(119)?;
        host.drain(&path).await?;
        assert_eq!(host.state(&path).await?.phase, second.phase);
        assert_eq!(model.requests().len(), 4);
        let second_inbox = if answer {
            let Delivered::Committed { inbox } = host
                .send(&path, &super::super::user(), &message("second-reply"))
                .await?
            else {
                return Err("second reply was not committed".into());
            };
            Some(inbox)
        } else {
            control.advance_to(120)?;
            None
        };
        host.drain(&path).await?;
        let mut settled = host.state(&path).await?;
        assert!(matches!(
            reply(
                &*stream,
                &mut settled,
                inbox + 1,
                &intent("first-reply"),
                inbox
            )
            .await?,
            Reply::Answered { sequence: 1, .. }
        ));
        assert_eq!(settled.phase, Phase::Idle);
        assert!(
            settled.pending_notices.is_none(),
            "admitted notices survived settlement"
        );
        assert_eq!(
            settled.conversation_tail,
            if reserved > 0 { 3 + reserved as u64 } else { 2 },
            "resuming a wait invented another turn"
        );
        assert_eq!(
            model.requests().len(),
            if reserved > 0 { 8 + 2 * reserved } else { 6 }
        );
        let requests = model.requests();
        for (request, call) in [(3, "first-input"), (4, "second-input")] {
            let value = requests[request]
                .messages
                .iter()
                .flat_map(|message| message.content.parts())
                .find_map(|part| match part {
                    ModelContentPart::ToolResult {
                        call_id,
                        content: acyclic_harness_core::model::ToolResultContent::Json { value },
                        ..
                    } if call_id == call => Some(value),
                    _ => None,
                })
                .expect("released wait result");
            assert_eq!(
                value["reason"]["kind"],
                if call == "first-input" || answer {
                    "input"
                } else {
                    "deadline"
                }
            );
            if call == "first-input" || answer {
                assert_eq!(
                    value["reason"]["inbox"],
                    if call == "first-input" {
                        inbox
                    } else {
                        second_inbox.expect("second reply")
                    }
                );
            }
        }
    }
    Ok(())
}

async fn answer_entry(stream: &MemoryStream, path: &AgentPath) -> Result<FileRef> {
    Ok(super::answer(stream, path, 0).await?.output)
}
