//! Successive waits through the production executor, inbox and filesystem journal.
use super::*;
use acyclic_harness_core::{
    conversation::wire::Notice,
    filesystem::{Grants, StockExecutors},
    model::{ModelEvent, ToolCall},
    test_support::ScriptedEvent,
};

fn wait(id: &str, timeout: u64) -> ScriptedTurn {
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
    for (answer, reserved) in [(true, false), (false, false), (true, true)] {
        let path = AgentPath::root(&id(226));
        let stream = Arc::new(MemoryStream::default());
        let (filesystem, binding) = private_in(&stream, &path).await?;
        let model = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Text("reply body".into()),
            wait("first-input", 10),
            wait("second-input", 20),
            ScriptedTurn::Text("both waits finished".into()),
            ScriptedTurn::Text("reserved turn finished".into()),
            ScriptedTurn::Text("follow-up finished".into()),
        ]));
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
        deliver(&host, &path, requested("make-body", &path)).await?;
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
        assert_eq!(model.requests().len(), 2);
        assert_eq!(
            first.phase,
            Phase::Waiting {
                deadline_unix_ms: Some(110),
                input: true
            }
        );
        let reply = |name| {
            InboxRecord::Message(Message {
                from: super::super::user(),
                id: intent(name),
                body: Box::new(body.clone()),
                attachments: None,
            })
        };
        if reserved {
            host.send(
                &path,
                &super::super::user(),
                &requested("independent-before-reply", &path),
            )
            .await?;
            host.send(
                &path,
                &super::super::user(),
                &InboxRecord::FollowUp(Message {
                    from: super::super::user(),
                    id: intent("reserved-follow-up"),
                    body: Box::new(body.clone()),
                    attachments: None,
                }),
            )
            .await?;
            host.drain(&path).await?;
            assert_eq!(
                model.requests().len(),
                2,
                "reserved inputs woke the parked model"
            );
        }
        let Delivered::Committed { inbox } = host
            .send(&path, &super::super::user(), &reply("first-reply"))
            .await?
        else {
            return Err("reply was not committed".into());
        };
        host.drain(&path).await?;
        let second = host.state(&path).await?;
        assert_eq!(
            second.phase,
            Phase::Waiting {
                deadline_unix_ms: Some(120),
                input: true
            }
        );
        assert_eq!(
            model.requests().len(),
            3,
            "the old reply released the second wait"
        );
        assert_eq!(second.steered_through, inbox + 1);
        assert!(matches!(
            reply(&*stream, &second, inbox + 1, &intent("first-reply"), inbox).await?,
            Reply::Pending
        ));
        assert!(
            second.queued.len() == if reserved { 2 } else { 0 },
            "already admitted reply remained wakeable"
        );
        assert_eq!(
            second.turn.as_ref().map(|turn| turn.inbox),
            first.turn.as_ref().map(|turn| turn.inbox)
        );
        host.drain(&path).await?;
        assert_eq!(
            model.requests().len(),
            3,
            "draining without new input released an input wait"
        );
        control.advance_to(119)?;
        host.drain(&path).await?;
        assert_eq!(host.state(&path).await?.phase, second.phase);
        assert_eq!(model.requests().len(), 3);
        if answer {
            host.send(&path, &super::super::user(), &reply("second-reply"))
                .await?;
        } else {
            control.advance_to(120)?;
        }
        host.drain(&path).await?;
        let settled = host.state(&path).await?;
        assert!(matches!(
            reply(&*stream, &settled, inbox + 1, &intent("first-reply"), inbox).await?,
            Reply::Answered { sequence: 1, .. }
        ));
        assert_eq!(settled.phase, Phase::Idle);
        assert_eq!(
            settled.conversation_tail,
            if reserved { 4 } else { 2 },
            "resuming a wait invented another turn"
        );
        assert_eq!(model.requests().len(), if reserved { 6 } else { 4 });
    }
    Ok(())
}

async fn answer_entry(stream: &MemoryStream, path: &AgentPath) -> Result<FileRef> {
    Ok(super::answer(stream, path, 0).await?.output)
}
