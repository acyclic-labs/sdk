use super::*;
use proptest::prelude::*;

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn response_collection_retires_consumed_and_ignored_deltas() -> Result<()> {
    let mut stock = uncompacted_executor(
        Model::new("generated".into(), "collection".into(), "1".into())?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    stock.limits.file_bytes = 11;
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text(String::new()),
        selected_context: None,
    };
    for native in [false, true] {
        for (authoritative, reject_tool, late_rejection, oversized) in [
            (false, false, false, false),
            (true, false, false, false),
            (false, true, false, false),
            (false, true, true, false),
            (true, true, true, false),
            (false, false, false, true),
        ] {
            let observed = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
                let execution = StockExecution {
                    io: Some(io),
                    single_request: reject_tool,
                    ..StockExecution::ordinary(&stock)
                };
                let sibling = stock_string_copy("retained sibling", Some(io))?;
                let sibling_bytes = sibling.capacity() as u64;
                let history = stock_string_copy("existing history", Some(io))?;
                let history_pointer = history.as_ptr();
                let mut prior = Vec::new();
                stock_vec_push(
                    &mut prior,
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text(history),
                    },
                    Some(io),
                )?;
                let baseline = io.owned_bytes()?;
                let mut events = Vec::new();
                // Generated operands use the original production ledger.
                let denied_call = || -> Result<ModelEvent> {
                    Ok(ModelEvent::ToolCall(crate::model::ToolCall::new(
                        stock_string_copy("denied call", Some(io))?,
                        stock_string_copy("denied tool", Some(io))?,
                        Value::Null,
                    )))
                };
                if reject_tool && !late_rejection {
                    stock_vec_push(&mut events, denied_call()?, Some(io))?;
                }
                for delta in ["first", "second"] {
                    stock_vec_push(
                        &mut events,
                        ModelEvent::Content {
                            delta: stock_string_copy(delta, Some(io))?,
                        },
                        Some(io),
                    )?;
                    stock_vec_push(
                        &mut events,
                        ModelEvent::Reasoning {
                            delta: stock_string_copy("ignored reasoning", Some(io))?,
                        },
                        Some(io),
                    )?;
                }
                let mut completed_pointer = None;
                if authoritative {
                    // Replacing a completion must retire its previous text.
                    for value in ["previous", "final"] {
                        let text = stock_string_copy(value, Some(io))?;
                        completed_pointer = Some(text.as_ptr());
                        stock_vec_push(
                            &mut events,
                            ModelEvent::MessageCompleted { text },
                            Some(io),
                        )?;
                    }
                }
                if reject_tool && late_rejection {
                    stock_vec_push(&mut events, denied_call()?, Some(io))?;
                }
                if oversized {
                    stock_vec_push(
                        &mut events,
                        ModelEvent::Content {
                            delta: stock_string_copy("oversized delta", Some(io))?,
                        },
                        Some(io),
                    )?;
                }
                // The iterator still owns this suffix when collection fails.
                stock_vec_push(
                    &mut events,
                    ModelEvent::Reasoning {
                        delta: stock_string_copy("unconsumed suffix", Some(io))?,
                    },
                    Some(io),
                )?;
                let result = execution.collect_step_events(&input, 0, events, &mut prior);
                if reject_tool || oversized {
                    assert_eq!(
                        result.err().map(|error| error.code()),
                        Some(if reject_tool {
                            crate::wire::ErrorCode::Unauthorized
                        } else {
                            crate::wire::ErrorCode::Invalid
                        })
                    );
                } else {
                    let result = result?;
                    assert_eq!(
                        result.text,
                        if authoritative {
                            "final"
                        } else {
                            "firstsecond"
                        }
                    );
                    assert!(result.calls.is_empty());
                    assert_eq!(result.metadata, Value::Null);
                    if let Some(pointer) = completed_pointer {
                        assert_eq!(result.text.as_ptr(), pointer);
                    }
                    assert_eq!(io.owned_bytes()?, baseline + result.text.capacity() as u64);
                    stock_retire_text(result.text, Some(io))?;
                }
                assert_eq!(prior.len(), 1);
                let ModelContent::Text(history) = &prior[0].content else {
                    unreachable!()
                };
                assert_eq!(history, "existing history");
                assert_eq!(history.as_ptr(), history_pointer);
                assert_eq!(io.owned_bytes()?, baseline);
                for message in stock_into_iter(prior, Some(io))? {
                    let ModelContent::Text(history) = message.content else {
                        unreachable!()
                    };
                    stock_retire_text(history, Some(io))?;
                }
                assert_eq!(io.owned_bytes()?, sibling_bytes);
                stock_retire_text(sibling, Some(io))?;
                Ok::<_, Error>(())
            })
            .map_err(|(cause, _)| Error::Storage(cause))?;
            observed.0?;
            assert_eq!(observed.2, 0);
            if native {
                assert_eq!(
                    observed.5,
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                );
            }
        }
    }
    Ok(())
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn assistant_part_admission_keeps_existing_history_and_retires_rejected_payload() -> Result<()> {
    let mut stock = uncompacted_executor(
        Model::new("generated".into(), "part-admission".into(), "1".into())?,
        Arc::new(ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    stock.limits.render_bytes = 4;
    for native in [false, true] {
        for text in ["ok", "too long"] {
            let observed = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
                let execution = StockExecution {
                    io: Some(io),
                    ..StockExecution::ordinary(&stock)
                };
                let history = stock_string_copy("existing history", Some(io))?;
                let original = history.as_ptr();
                let mut messages = Vec::new();
                stock_vec_push(
                    &mut messages,
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text(history),
                    },
                    Some(io),
                )?;
                let baseline = io.owned_bytes()?;
                let old_capacity = messages.capacity();
                let incoming = stock_string_copy(text, Some(io))?;
                let incoming_bytes = incoming.capacity() as u64;
                let incoming_pointer = incoming.as_ptr();
                let result = execution.append_assistant_part(
                    &mut messages,
                    ModelContentPart::Text {
                        text: incoming,
                        native: None,
                    },
                );
                if text.len() <= 4 {
                    result?;
                    assert_eq!(messages.len(), 2);
                    let ModelContent::Part(ModelContentPart::Text { text, .. }) =
                        &messages[1].content
                    else {
                        unreachable!()
                    };
                    assert_eq!(text.as_ptr(), incoming_pointer);
                    assert_eq!(
                        io.owned_bytes()?,
                        baseline
                            + incoming_bytes
                            + ((messages.capacity() - old_capacity)
                                * std::mem::size_of::<ModelMessage>())
                                as u64
                    );
                } else {
                    assert_eq!(
                        result.err().map(|error| error.code()),
                        Some(crate::wire::ErrorCode::Invalid)
                    );
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages.capacity(), old_capacity);
                    assert_eq!(io.owned_bytes()?, baseline);
                }
                let ModelContent::Text(history) = &messages[0].content else {
                    unreachable!()
                };
                assert_eq!(history, "existing history");
                assert_eq!(history.as_ptr(), original);
                for message in stock_into_iter(messages, Some(io))? {
                    let text = match message.content {
                        ModelContent::Text(text)
                        | ModelContent::Part(ModelContentPart::Text { text, .. }) => text,
                        _ => unreachable!(),
                    };
                    stock_retire_text(text, Some(io))?;
                }
                assert_eq!(io.owned_bytes()?, 0);
                Ok::<_, Error>(())
            })
            .map_err(|(cause, _)| Error::Storage(cause))?;
            observed.0?;
            assert_eq!(observed.2, 0);
            if native {
                assert_eq!(
                    observed.5,
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn committed_completion_does_not_wait_for_transport_close() -> Result<()> {
    use futures::FutureExt as _;

    for (completed, closed, delta) in [
        (true, false, false),
        (true, false, true),
        (false, true, true),
        (false, false, true),
    ] {
        let mut events = Vec::new();
        // Generated harness events control first-delta and missing-producer cases.
        if delta {
            events.push(ScriptedEvent::Event(ModelEvent::Content {
                delta: "".into(),
            }));
            events.push(ScriptedEvent::Event(ModelEvent::Reasoning {
                delta: "partial".into(),
            }));
        }
        events.push(ScriptedEvent::Event(ModelEvent::MessageCompleted {
            text: "done".into(),
        }));
        if completed {
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({"cache": ModelCacheObservation {
                    version: 1,
                    credential_scope: Some([1; 32]),
                    profile_prefix_digest: None,
                    ttl: ModelCacheTtl::ProviderManaged,
                    breakpoint: None,
                }}),
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
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
        };
        let result = executor.execute(input.clone(), &journal).now_or_never();
        if completed {
            let output = result.expect("committed completion waited for transport closure")?;
            assert_eq!(output.text, "done");
            assert_eq!(output.steps, 2);
            #[cfg(feature = "research")]
            assert_eq!(
                output.journal_cut, None,
                "journal declines original receipts"
            );
            let replay = executor
                .execute(input, &journal)
                .now_or_never()
                .expect("settled replay waited")?;
            #[cfg(feature = "research")]
            assert_eq!(
                replay.journal_cut, None,
                "replay cannot upgrade unknown coverage"
            );
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
        let usage: Vec<_> = journal
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|record| match &record.event {
                ExecutionEvent::Model {
                    usage: Some(file), ..
                }
                | ExecutionEvent::ModelInline {
                    usage: Some(file), ..
                } => Some(file.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            usage.len(),
            if completed { 2 } else { 1 },
            "a partial/closed stream is not a completed usage record; replay adds none"
        );
        for (index, file) in usage.into_iter().enumerate() {
            let bytes = futures::executor::block_on(journal.load(&file, None)?)?;
            let record: ModelUsageRecord = crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(record.provider, "fixture");
            assert_eq!(record.model, "completion");
            assert_eq!(record.tokens, ModelTokenUsage::default());
            assert_eq!(
                record.cache.is_some(),
                cfg!(feature = "research") && index == 1
            );
            #[cfg(all(feature = "research", not(target_arch = "wasm32")))]
            {
                assert_eq!(record.ttft_ns.is_some(), delta && index == 1);
                assert!(
                    record
                        .ttft_ns
                        .zip(record.latency_ns)
                        .is_none_or(|(first, total)| first <= total)
                );
            }
            #[cfg(not(all(feature = "research", not(target_arch = "wasm32"))))]
            {
                let _ = index;
                assert_eq!(record.ttft_ns, None);
            }
            #[cfg(not(target_arch = "wasm32"))]
            assert!(record.latency_ns.is_some());
            #[cfg(target_arch = "wasm32")]
            assert!(record.latency_ns.is_none());
        }
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
        let input = TurnInput { max_steps: None, operation_id: OperationId::new(), input: ModelContent::Text("x".into()),
            selected_context: None };
        let actual = futures::executor::block_on(executor.execute(input.clone(), &journal))?;
        let expected = completed.unwrap_or_else(|| final_chunks.concat());
        prop_assert_eq!(&actual.text, &expected);
        prop_assert_eq!(actual.steps as usize, earlier.len() + 1);
        prop_assert_eq!(actual, futures::executor::block_on(executor.execute(input, &journal))?);
        prop_assert_eq!(provider.requests().len(), earlier.len() + 1);
    }
}

#[test]
fn text_parts_keep_interim_text_and_the_final_native_parts() -> Result<()> {
    use crate::model::{NativePart, ReasoningBlock, ReasoningKind, ReasoningOrigin};
    use futures::FutureExt as _;

    let origin = ReasoningOrigin {
        provider: "gemini".into(),
        model: "fixture".into(),
        credential_scope: Some([7; 32]),
    };
    let native = |bytes: &str| NativePart {
        origin: origin.clone(),
        continuity: bytes.as_bytes().to_vec(),
    };
    let thought = ReasoningBlock {
        kind: ReasoningKind::Signature,
        text: Some("plan".into()),
        origin: origin.clone(),
        continuity: br#"{"text":"plan","thought":true}"#.to_vec(),
        outcome: crate::model::ReasoningOutcome::PreservedReadable,
    };
    let answer = Some(native(r#"{"text":"done","thoughtSignature":"s="}"#));
    let provider = Arc::new(ScriptedModelProvider::new([
        tool_turn([
            ModelEvent::Content {
                delta: "looking".into(),
            },
            ModelEvent::TextPart {
                text: "looking".into(),
                native: None,
            },
            echo_call("call", json!({})),
        ]),
        tool_turn([
            ModelEvent::ReasoningBlock {
                block: thought.clone(),
            },
            ModelEvent::Content {
                delta: "done".into(),
            },
            ModelEvent::TextPart {
                text: "done".into(),
                native: answer.clone(),
            },
        ]),
    ]));
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "parts".into(), "1".into())?,
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
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("hello".into()),
        selected_context: None,
    };
    let output = executor
        .execute(input, &Journal::default())
        .now_or_never()
        .expect("scripted turn settles")?;
    assert_eq!(output.text, "done");
    // The output keeps the whole transcript after the input: the last request's
    // messages after the input, then the final step's parts, so the next turn
    // extends that request byte for byte.
    let last = &provider.requests()[1].messages;
    let input_at = last
        .iter()
        .position(|message| message.content == ModelContent::Text("hello".into()))
        .expect("the input is in the request");
    let reply = |part| ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(part),
    };
    let mut transcript = last[input_at + 1..].to_vec();
    transcript.push(reply(ModelContentPart::Reasoning { block: thought }));
    transcript.push(reply(ModelContentPart::Text {
        text: "done".into(),
        native: answer,
    }));
    assert_eq!(output.transcript, transcript);
    // The tool step's interim text reaches the next request ahead of its call.
    let followup = &provider.requests()[1].messages;
    let interim = followup
        .iter()
        .position(|message| {
            message.content == ModelContent::Part(ModelContentPart::text("looking".into()))
        })
        .expect("interim text is kept");
    assert!(matches!(
        followup[interim + 1].content,
        ModelContent::Part(ModelContentPart::ToolCall(_))
    ));
    Ok(())
}
