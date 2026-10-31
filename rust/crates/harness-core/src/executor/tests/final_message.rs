use super::*;
use proptest::prelude::*;

#[test]
fn committed_completion_does_not_wait_for_transport_close() -> Result<()> {
    use futures::FutureExt as _;

    for (completed, closed) in [(true, false), (false, true), (false, false)] {
        let mut events = vec![ScriptedEvent::Event(ModelEvent::MessageCompleted {
            text: "done".into(),
        })];
        if completed {
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }));
        }
        if !closed {
            events.push(ScriptedEvent::Pending {});
        }
        let provider = Arc::new(ScriptedModelProvider::new([
            tool_turn([echo_call("call", json!({}))]),
            ScriptedTurn::Events(events),
        ]));
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "completion".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            echo_tools(
                Arc::new(FakeTool(AtomicUsize::new(0))),
                json!({"type":"object"}),
                json!({}),
                crate::tool::json_projection_schema(json!({})),
            )?,
        )
        .with_tool_authority(
            RuntimeScope::new(
                Capabilities::new(["tool:call:example.echo"]),
                Limits::default(),
            )?,
            allowed()?,
        )?;
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 2,
        };
        let result = executor.execute(input.clone(), &journal).now_or_never();
        if completed {
            let output = result.expect("committed completion waited for transport closure")?;
            assert_eq!(output.text, "done");
            assert_eq!(output.steps, 2);
            let replay = executor
                .execute(input, &journal)
                .now_or_never()
                .expect("settled replay waited")?;
            assert_eq!(replay, output);
        } else if closed {
            assert!(
                matches!(result, Some(Err(Error::Indeterminate(id))) if id == input.operation_id)
            );
        } else {
            assert!(
                result.is_none(),
                "an open response without completion cannot settle"
            );
        }
        assert_eq!(provider.requests().len(), 2);
    }
    Ok(())
}

#[test]
fn authoritative_messages_do_not_bypass_stream_admission() -> Result<()> {
    let limits = Limits {
        file_bytes: 4,
        ..Limits::default()
    };
    let mut admission = ModelEventAdmission::default();
    admission.observe(&ModelEvent::Content { delta: "é".into() }, limits, None)?;
    admission.observe(&ModelEvent::Content { delta: "ok".into() }, limits, None)?;
    assert_eq!(admission.text_bytes, 4);
    assert!(
        admission
            .observe(&ModelEvent::Content { delta: "x".into() }, limits, None)
            .is_err()
    );
    assert_eq!((admission.count, admission.text_bytes), (2, 4));
    admission.observe(
        &ModelEvent::MessageCompleted { text: "".into() },
        limits,
        None,
    )?;
    assert_eq!((admission.count, admission.text_bytes), (3, 4));
    assert!(
        admission
            .observe(
                &ModelEvent::MessageCompleted {
                    text: "ééx".into()
                },
                limits,
                None
            )
            .is_err()
    );
    assert_eq!((admission.count, admission.text_bytes), (3, 4));
    Ok(())
}

#[test]
fn completed_message_is_bounded_before_admission() -> Result<()> {
    let limits = Limits {
        file_bytes: 4,
        ..Limits::default()
    };
    for text in ["", "λ λ"] {
        let mut admission = ModelEventAdmission::default();
        let event = ModelEvent::MessageCompleted { text: text.into() };
        let accepted = text.len() <= 4;
        assert_eq!(admission.observe(&event, limits, None).is_ok(), accepted);
        assert_eq!(admission.count, usize::from(accepted));
    }
    Ok(())
}

#[tokio::test]
async fn sampling_returns_the_completed_message_instead_of_streamed_prefixes() -> Result<()> {
    use crate::mcp::callbacks::{McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks};
    for answer in ["", "final λ\n"] {
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let context = Arc::new(admitted_context(scope.clone())?);
        let parent = OperationId::new();
        let tool = crate::runtime::ToolContext::new(context, parent, "remote")?;
        let model = Arc::new(ScriptedModelProvider::new([tool_turn([
            ModelEvent::Content {
                delta: "older answer".into(),
            },
            ModelEvent::Content {
                delta: answer.into(),
            },
            ModelEvent::MessageCompleted {
                text: answer.into(),
            },
        ])]));
        let callbacks = McpRuntimeCallbacks::new(
            uncompacted_executor(
                Model::new("fixture".into(), "sampling".into(), "1".into())?,
                model,
                ContextPipeline::default(),
                ToolRegistry::default(),
            )
            .with_tool_authority(scope, allowed()?)?,
            Arc::new(Journal::default()),
            "fixture".into(),
            ModelContent::Text("continue".into()),
            None,
            None,
        )?;
        let request = McpCallbackRequest {
            parent,
            policy: McpCallbackPolicy {
                server: "sample".into(),
                revision: "1".into(),
                sampling: true,
                maximum_requests: 1,
            },
            message: json!({"jsonrpc":"2.0","id":"sample","method":"sampling/createMessage",
                "params":{"messages":[{"role":"user","content":{"type":"text","text":"sample"}}],"maxTokens":128}}),
        };
        let (_, reply) = callbacks.admit(tool, &request).await?;
        assert_eq!(reply.pointer("/result/content/text"), Some(&json!(answer)));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Any earlier step text and any UTF-8 delta partition leave the final step
    // unchanged, including when that step has no text. Replay must agree.
    #[test]
    fn final_message_is_independent_of_prior_steps(
        earlier in prop::collection::vec(".{0,16}", 1..4),
        final_chunks in prop::collection::vec(".{0,16}", 0..5),
        completed in prop::option::of(".{0,32}"),
    ) {
        let mut turns = Vec::new();
        for (index, text) in earlier.iter().enumerate() {
            turns.push(tool_turn([
                ModelEvent::Content { delta: text.clone() },
                echo_call(&format!("call-{index}"), json!({})),
            ]));
        }
        let mut final_events: Vec<_> = final_chunks.iter().map(|delta| ModelEvent::Content { delta: delta.clone() }).collect();
        if let Some(text) = &completed {
            final_events.push(ModelEvent::MessageCompleted { text: text.clone() });
        }
        turns.push(tool_turn(final_events));
        let provider = Arc::new(ScriptedModelProvider::new(turns).with_token_counter(fixture_token_count));
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "final".into(), "1".into())?,
            provider.clone(), ContextPipeline::default(),
            echo_tools(Arc::new(FakeTool(AtomicUsize::new(0))), json!({"type":"object"}),
                json!({}), crate::tool::json_projection_schema(json!({})))?,
        ).with_tool_authority(RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]), Limits::default())?, allowed()?)?;
        let journal = Journal::default();
        let input = TurnInput { operation_id: OperationId::new(), input: ModelContent::Text("x".into()),
            selected_context: None, max_steps: 4 };
        let actual = futures::executor::block_on(executor.execute(input.clone(), &journal))?;
        let expected = completed.unwrap_or_else(|| final_chunks.concat());
        prop_assert_eq!(&actual.text, &expected);
        prop_assert_eq!(actual.steps as usize, earlier.len() + 1);
        prop_assert_eq!(actual, futures::executor::block_on(executor.execute(input, &journal))?);
        prop_assert_eq!(provider.requests().len(), earlier.len() + 1);
    }
}
