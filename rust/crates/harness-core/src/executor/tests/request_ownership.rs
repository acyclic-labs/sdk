use super::*;
use proptest::prelude::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn funded_request_and_projection_share_one_owned_history() -> Result<()> {
    let executor = uncompacted_executor(
        Model::new("test".into(), "transfer".into(), "1".into())?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let scope = crate::filesystem::PreparationScope::new(Some(&io))?;
        let execution = StockExecution {
            io: scope.io(),
            ..StockExecution::ordinary(&executor)
        };
        let input = ContextInput {
            input: ModelContent::Text("input".into()),
            selected_context: None,
            step: 0,
            prior_messages: vec![],
        };
        let messages: Vec<_> = (0..32)
            .map(|index| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(format!("{index}:{}", "payload".repeat(16))),
            })
            .collect();
        let original = messages.as_ptr();
        let expected = crate::model::PreparedModelRequest::prepare(
            execution.request_from_messages(messages.clone())?,
            executor.limits,
            None,
        )?;
        let context = crate::context::Context {
            messages,
            current_input_index: Some(0),
            ..Default::default()
        };
        let (projection, prepared) = execution
            .prepare_transformed_request(&input, context)
            .await?;
        assert_eq!(
            prepared.request().messages.as_ptr(),
            original,
            "native={native}: preparation copied history"
        );
        assert!(
            projection.messages.is_empty(),
            "projection retained a second transcript"
        );
        assert_eq!(prepared.bytes(), expected.bytes());
        crate::context::validate_projected_messages(
            &projection,
            &prepared.request().messages,
            executor.limits,
            execution.io,
        )?;
        // The former funded path makes another vector and nested text buffers.
        let copied = crate::model::clone_messages(&prepared.request().messages, execution.io)?;
        assert_ne!(
            copied.as_ptr(),
            original,
            "negative control reused the request vector"
        );
        drop(copied);
        drop(prepared);
        drop(projection);
        drop(execution);
        drop(scope);
        assert_eq!(io.owned_bytes()?, 0);
    }
    Ok(())
}

fn buffers(messages: &[ModelMessage]) -> (usize, usize) {
    let ModelContent::Part(ModelContentPart::ToolResult {
        content: crate::model::ToolResultContent::Json { value },
        ..
    }) = &messages.last().unwrap().content
    else {
        panic!("result")
    };
    (
        messages.as_ptr() as usize,
        value["nested"][0].as_str().unwrap().as_ptr() as usize,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Admission and canonical bytes must agree with the copying path, while
    // the production transform and final request retain the original buffers.
    #[test]
    fn request_stages_transfer_history_without_copying(
        history in prop::collection::vec(".{0,128}", 0..16),
        payload in prop::collection::vec(".{0,128}", 0..16),
    ) {
        let tools = echo_tools(Arc::new(FakeTool(AtomicUsize::new(0))),
            json!(true), json!(true), json!(true)).unwrap();
        let executor = uncompacted_executor(
            Model::new("test".into(), "transfer".into(), "1".into()).unwrap(),
            Arc::new(ScriptedModelProvider::new([])), ContextPipeline::default(), tools,
        ).with_tool_authority(
            RuntimeScope::new(Capabilities::new(["model:generate", "tool:call:example.echo"]), Limits::default()).unwrap(),
            allowed().unwrap(),
        ).unwrap();
        let session = StockExecution::ordinary(&executor);
        let input = ContextInput {
            input: ModelContent::Text("input".into()), selected_context: None,
            step: 0, prior_messages: vec![],
        };
        let mut messages = vec![ModelMessage { role: ModelRole::User, content: input.input.clone() }];
        messages.extend(history.into_iter().map(|text| ModelMessage {
            role: ModelRole::User, content: ModelContent::Text(text),
        }));
        messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(crate::model::ToolCall::new(
                "call".into(), "example.echo".into(), json!({"arguments": payload}),
            ))),
        });
        messages.push(ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: "call".into(), name: "example.echo".into(),
                content: crate::model::ToolResultContent::Json { value: json!({"nested": ["retained", payload]}) },
            }),
        });
        let context = crate::context::Context { messages, current_input_index: Some(0), ..Default::default() };
        let original = buffers(&context.messages);
        // Isolate copy work from the process-wide schema cache: other tests may
        // evict validators concurrently. Plain history needs no schema lookup.
        let mut plain_executor = executor.clone();
        plain_executor.tools = ToolRegistry::default();
        let plain = StockExecution::ordinary(&plain_executor);
        let plain_context = crate::context::Context {
            messages: context.messages[..context.messages.len() - 2].to_vec(),
            current_input_index: Some(0), ..Default::default()
        };
        let mut copied = None;
        let copying = acyclic_test_support::measure(|| {
            copied = Some(crate::model::clone_messages(&plain_context.messages, None).unwrap());
        });
        drop(copied);
        let expected = crate::model::PreparedModelRequest::prepare(
            session.request_from_context(&context).unwrap(), executor.limits, None,
        ).unwrap();
        // The former selector really clones both the vector and nested JSON.
        prop_assert_ne!(buffers(&expected.request().messages).0, original.0);
        prop_assert_ne!(buffers(&expected.request().messages).1, original.1);
        let legacy_base = plain_context.clone();
        futures::executor::block_on(async {});
        // Execute the former transform, including the same validation/encoding.
        let legacy = acyclic_test_support::measure(|| {
            futures::executor::block_on(async {
                let context = plain.context_pipeline().transform(
                    &input, legacy_base, executor.limits, None,
                ).await.unwrap();
                let _prepared = crate::model::PreparedModelRequest::prepare(
                    plain.request_from_context(&context).unwrap(), executor.limits, None,
                ).unwrap();
                crate::context::validate_projected_context(&context, executor.limits, None).unwrap();
            });
        });
        let mut projected = None;
        let transferred = acyclic_test_support::measure(|| {
            projected = Some(futures::executor::block_on(plain.transform_projection(&input, plain_context)).unwrap());
        });
        let no_copy = |observed: acyclic_test_support::AllocationInfo| {
            observed.count_total + copying.count_total == legacy.count_total
                && observed.bytes_total + copying.bytes_total == legacy.bytes_total
        };
        prop_assert!(no_copy(transferred), "transient history copies: {transferred:?}, {legacy:?}, {copying:?}");
        prop_assert!(!no_copy(legacy), "restored copying transform escaped detection");
        drop(projected);
        let mut projected = futures::executor::block_on(session.transform_projection(&input, context)).unwrap();
        prop_assert_eq!(buffers(&projected.messages), original);
        let final_request = crate::model::PreparedModelRequest::prepare(
            session.request_taking_context(&mut projected).unwrap(), executor.limits, None,
        ).unwrap();
        prop_assert_eq!(buffers(&final_request.request().messages), original);
        prop_assert_eq!(final_request.bytes(), expected.bytes());
        prop_assert_eq!(final_request.request_digest(), expected.request_digest());
        prop_assert_eq!(final_request.request(), expected.request());
    }
}
