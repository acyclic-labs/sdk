//! Real parent file-query reads of a child's published terminal failure.
use super::*;
use acyclic_harness_core::{
    executor::Executor,
    filesystem::{StockExecutors, TurnAuthority},
    model::{ModelEvent, ToolCall, ToolResultContent},
    test_support::ScriptedEvent,
    tool::{ToolProjection as _, files::FileProjection, schema::ProjectionMode},
};

struct Inspect {
    original: Arc<dyn TurnExecutors>,
    bindings: Arc<acyclic_harness_core::runtime::AgentHarness>,
    diagnostic: Diagnostic,
    expected: String,
}

impl TurnExecutors for Inspect {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let task = Arc::new(
                self.bindings
                    .agent_context(&authority.grants, authority.content.clone())?
                    .for_agent(agent.path.clone(), agent.volumes.clone())
                    .with_agent_children(authority.children.clone()),
            );
            let context =
                acyclic_harness_core::runtime::ToolContext::new(task, turn, "diagnostic")?;
            let tool = acyclic_harness_core::tool::files::read()?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .expect("test work");
            let mut complete = String::new();
            while complete.len() < self.expected.len() {
                let from = complete.len() as u64;
                let invocation = acyclic_harness_core::tool::ToolInvocation {
                    operation_id: turn,
                    call_id: "diagnostic".into(),
                    name: "read".into(),
                    arguments: serde_json::json!({
                        "path": self.diagnostic.query_path(),
                        "range": {"start": from, "end": from + 65536}
                    }),
                };
                let (result, custody) = tool
                    .executor
                    .execute_with_native_context(&context, &invocation, &work)?
                    .await?;
                for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
                    let projector = FileProjection::Read(mode);
                    let (projected, _hold) =
                        projector.project(&invocation, &result, Some(&work))?;
                    acyclic_harness_core::executor::validate_json_schema_value(
                        &projector.schema()?,
                        &projected,
                        "diagnostic projection",
                        &mut None,
                    )?;
                    assert_eq!(projected["value"], result.value);
                }
                let text = result.value["text"].as_str().expect("diagnostic text");
                assert!(!text.is_empty());
                let encoded = serde_json::to_vec(&result.value)
                    .expect("read result JSON")
                    .len() as u64;
                assert!(encoded <= acyclic_harness_core::tool::VIEW_BYTES);
                assert!(
                    context
                        .effective_scope()
                        .limits()
                        .render_bytes
                        .is_none_or(|maximum| encoded <= maximum)
                );
                complete.push_str(text);
                assert_eq!(result.value["page"]["end"], complete.len() as u64);
                assert_eq!(result.value["page"]["total"], self.expected.len() as u64);
                drop(result);
                drop(custody);
            }
            assert_eq!(complete, self.expected);
            let invocation = acyclic_harness_core::tool::ToolInvocation {
                operation_id: turn,
                call_id: "diagnostic".into(),
                name: "read".into(),
                arguments: serde_json::json!({"path": self.diagnostic.query_path(),
                    "range": {"start": u64::MAX - 1, "end": u64::MAX}}),
            };
            let (result, _custody) = tool
                .executor
                .execute_with_native_context(&context, &invocation, &work)?
                .await?;
            assert_eq!(result.value["text"], "");
            assert_eq!(result.value["page"]["start"], self.expected.len() as u64);
            assert_eq!(result.value["page"]["end"], self.expected.len() as u64);
            assert_eq!(result.value["page"]["eof"], true);
            assert!(result.value["page"].get("next").is_none());
            for agent in [
                self.diagnostic.agent.clone(),
                AgentPath::root(&AgentId::from_bytes([254; 16])),
            ] {
                let mut wrong = self.diagnostic.clone();
                wrong.agent = agent;
                wrong.digest[0] ^= 1;
                let invocation = acyclic_harness_core::tool::ToolInvocation {
                    operation_id: turn,
                    call_id: "diagnostic".into(),
                    name: "read".into(),
                    arguments: serde_json::json!({"path": wrong.query_path()}),
                };
                assert!(
                    tool.executor
                        .execute_with_native_context(&context, &invocation, &work)?
                        .await
                        .is_err()
                );
            }
            self.original.executor(agent, turn, input, authority).await
        })
    }
}

pub(crate) async fn check_diagnostic_read(
    stream: &Arc<MemoryStream>,
    parent: &AgentState,
    diagnostic: &Diagnostic,
    expected: &str,
) -> Result {
    let (filesystem, private) = private_in(stream, &parent.path).await?;
    let mut parent = parent.clone();
    parent.volumes = vec![private];
    let model = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "diagnostic".into(),
                "read".into(),
                serde_json::json!({"path": diagnostic.query_path(),
                    "range": {"start": 0, "end": 128}}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            }),
        ]),
        ScriptedTurn::Text("inspected".into()),
    ]));
    let mut tools = ToolRegistry::default();
    tools.register(
        (acyclic_harness_core::tool::files::read()?, None),
        &mut None,
        None,
    )?;
    let bindings = acyclic_harness_core::runtime::Bindings {
        scope: acyclic_harness_core::runtime::RuntimeScope::new(
            Capabilities::new([
                acyclic_harness_core::contract::capability::MODEL_GENERATE,
                "tool:call:read",
            ]),
            Default::default(),
        )?,
        ..Default::default()
    }
    .build()?;
    let inspect = Inspect {
        original: Arc::new(StockExecutors::new(
            (
                Model::new("test".into(), "text".into(), "1".into())?,
                model.clone(),
            ),
            tools,
            (Default::default(), CompactionPolicy::Disabled),
            Arc::new(Tasks(bindings.clone())),
        )),
        bindings,
        diagnostic: diagnostic.clone(),
        expected: expected.into(),
    };
    let turns = FilesystemTurnHost::new(
        stream.clone(),
        within(),
        filesystem,
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        Arc::new(inspect),
        LIMITS,
        acyclic_harness_core::approvals::ApprovalPolicy::accept_all("owner"),
    );
    let turn = OperationId::from_bytes([246; 16]);
    let input = ModelContent::Text("inspect".into());
    let opened = turns
        .open(
            &parent,
            acyclic_harness_core::agent::TurnIdentity::new(turn, 0, 0),
            &input,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
    let output = opened
        .executor
        .execute(
            acyclic_harness_core::executor::TurnInput {
                max_steps: None,
                operation_id: turn,
                input: opened.input.clone().unwrap_or(input),
                selected_context: opened.selected.clone(),
            },
            opened.journal.as_ref(),
        )
        .await?;
    assert_eq!(output.text, "inspected");
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    let value = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Json { value },
                ..
            } if name == "read" => Some(value),
            _ => None,
        })
        .ok_or("parent model did not receive its diagnostic read")?;
    let text = value["text"].as_str().ok_or("diagnostic text absent")?;
    assert!(!text.is_empty());
    assert_eq!(text, &expected[..text.len()]);
    assert_eq!(value["diagnostic"], serde_json::to_value(diagnostic)?);
    assert_eq!(value["page"]["end"], text.len() as u64);
    Ok(())
}
