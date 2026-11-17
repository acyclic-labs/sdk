//! Receipt admission through the public native workspace owner.
#[path = "support/native_receipt.rs"]
mod contract;

use acyclic_harness::OperationId;
use acyclic_harness::{
    agent::AgentPath,
    local::{
        LocalHarness, LocalHarnessConfig,
        host_owner::{HostAction, HostWorkspaceOwner, SessionAction},
    },
};
use std::sync::Arc;

/// Capture real replies from the shipped native owner, including siblings,
/// a nested child, and a fresh owner. No native model process is executed.
#[tokio::test]
async fn native_receipt_contract_uses_original_owner_replies()
-> Result<(), Box<dyn std::error::Error>> {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let open = || {
        LocalHarnessConfig::native_host(checkout.path()).state_directory(state.path().join("owner"))
    };
    let owner = HostWorkspaceOwner::new(Arc::new(LocalHarness::open(open()).await?))?;
    let root = owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: "codex".into(),
                session: "receipt-contract".into(),
            },
        )
        .await?;
    let parent: AgentPath = serde_json::from_value(root["task"].clone())?;
    let spawn = |parent, reference: &str| HostAction::Session {
        parent,
        reference: reference.into(),
        action: SessionAction::Spawn {
            prompt: "receipt contract".into(),
        },
        next: false,
    };
    let first = owner
        .dispatch(OperationId::new(), spawn(parent.clone(), "first"))
        .await?;
    let sibling = owner
        .dispatch(OperationId::new(), spawn(parent.clone(), "sibling"))
        .await?;
    let child: AgentPath = serde_json::from_value(first["task"].clone())?;
    let nested = owner
        .dispatch(OperationId::new(), spawn(child, "nested"))
        .await?;
    owner.retire().await?;
    let owner = HostWorkspaceOwner::new(Arc::new(LocalHarness::open(open()).await?))?;
    let reopened = owner
        .dispatch(
            OperationId::new(),
            HostAction::Session {
                parent,
                reference: "first".into(),
                action: SessionAction::Observe {},
                next: false,
            },
        )
        .await?;
    owner.retire().await?;
    let replies = vec![first, root, sibling, nested, reopened];
    let original = serde_json::to_vec(&replies)?;
    contract::verify(&original);
    println!("NATIVE_RECEIPT_CAPTURE={}", String::from_utf8(original)?);
    Ok(())
}

/// A tool call interrupted by its turn's own stop is the model's failed result:
/// the turn settles cancelled, the owner stays open, and the next model request
/// carries the interruption diagnostic. The call is not retried.
#[tokio::test]
async fn a_cancelled_tool_call_settles_as_an_error_and_the_owner_continues()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::{
        context::ModelContextCapacity,
        model::{Model, ModelContentPart, ModelEvent, ToolCall, ToolResultContent},
        runtime::ToolContext,
        test_support::{
            PromptOutcome, ScriptedEvent, ScriptedModelProvider, ScriptedTurn,
            local_harness_config, prompt_settled,
        },
        tool::{
            JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
            json_projection_schema,
        },
    };
    use acyclic_stream::BoxProviderFuture;
    use serde_json::json;
    use std::sync::Mutex;

    struct Pending {
        entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }
    impl ToolExecutor for Pending {
        fn cancellable_on_drop(&self) -> bool {
            true
        }
        fn execute<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<ToolContext>,
        ) -> BoxProviderFuture<'a, acyclic_harness::Result<ToolResult>> {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            Box::pin(std::future::pending())
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<ToolContext>,
        ) -> BoxProviderFuture<'a, acyclic_harness::Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }

    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let tool = Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "pending".into(),
            revision: "1".into(),
            description: "waits for its turn to stop".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"object"}),
            projection_schema: json_projection_schema(json!({"type":"object"})),
        },
        executor: Arc::new(Pending {
            entered: Mutex::new(Some(entered)),
        }),
        projection: Arc::new(JsonToolProjection),
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "call-1".into(),
                    "pending".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]),
            ScriptedTurn::Text("AFTER".into()),
        ])
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
            output_shares_context: false,
        }),
    );
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(&workspace)?;
    let config = local_harness_config(
        &workspace,
        (
            Model::new("scripted".into(), "test".into(), "1".into())?,
            provider.clone(),
        ),
    )?
    .without_default_tools()
    .tool(tool)
    .feedback_plugin(false);
    let harness = Arc::new(LocalHarness::open(config).await?);
    let conversation =
        acyclic_harness::local::agent_conversation::Conversation::open(harness.connect().await?);
    let cancelled = conversation.clone();
    let first = prompt_settled(&harness, "held", "CANCEL_THIS");
    tokio::pin!(first);
    tokio::select! {
        outcome = &mut first => panic!("the turn settled before its tool started: {outcome:?}"),
        entered = entered_rx => entered.expect("the tool never started"),
    }
    assert!(cancelled.cancel().await?.is_some());
    let first = first.await?;
    let interruption = "tool was interrupted before its outcome was recorded";
    match first {
        PromptOutcome::Settled(entry, output) => {
            assert_eq!(
                entry.outcome,
                acyclic_harness::agent::TurnOutcome::Cancelled
            );
            let carried = output.transcript.iter().any(|message| {
                message.content.parts().iter().any(|part| {
                    matches!(part, ModelContentPart::ToolResult {
                        content: ToolResultContent::Error { message }, ..
                    } if message.contains(interruption))
                })
            });
            assert!(
                carried,
                "the cancelled turn lost the interrupted tool settlement"
            );
        }
        PromptOutcome::Failed(message) => {
            panic!("the owner failed on the interrupted tool: {message}");
        }
    }
    match prompt_settled(&harness, "after", "AFTER_PROMPT").await? {
        PromptOutcome::Settled(entry, output) => {
            assert_eq!(
                entry.outcome,
                acyclic_harness::agent::TurnOutcome::Completed
            );
            assert_eq!(output.text, "AFTER");
        }
        PromptOutcome::Failed(message) => panic!("the owner did not continue: {message}"),
    }
    let request = provider
        .request(1)
        .ok_or("the next model request is absent")?;
    let carried = request.messages.iter().any(|message| {
        message.content.parts().iter().any(|part| {
            matches!(part, ModelContentPart::ToolResult {
                content: ToolResultContent::Error { message }, ..
            } if message.contains(interruption))
        })
    });
    assert!(
        carried,
        "the next model request lost the interruption diagnostic"
    );
    Ok(())
}
