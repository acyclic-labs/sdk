use super::*;
use crate::conversation::{FileDescriptor, VolumeOwner, VolumeRef};
use crate::executor::content::validate_model_content_scope;
use crate::resources::ProviderRef;
use proptest::prelude::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn summary_source_encoding_owns_its_bound_without_a_count_pass(
        texts in prop::collection::vec(".{0,64}", 1..4),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let source = crate::context::Context {
            messages: texts.into_iter().map(|text| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text),
            }).collect(),
            ..Default::default()
        };
        let expected = crate::contract::canonical_json_bytes(&source).unwrap();
        let mut stock = StockExecutor::new(
            Model::new("test".into(), "summary-source".into(), "1".into()).unwrap(),
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        ).with_compaction_policy(crate::context::CompactionPolicy::Disabled)
            .with_max_output_tokens(128).unwrap();
        stock.limits.file_bytes = Some(expected.len() as u64);
        let check = |execution: &StockExecution<'_>| {
            let (bytes, digest, messages) = execution.summary_source_identity(&source).unwrap();
            assert_eq!(bytes.as_ref(), expected);
            assert_eq!(digest, *blake3::hash(&expected).as_bytes());
            assert_eq!(messages as usize, source.messages.len());
        };
        check(&StockExecution::ordinary(&stock));
        for native in [false, true] {
            let storage = runtime.block_on(crate::filesystem::MemoryHarnessStorage::new(
                crate::AgentId::new(), 65_536,
            )).unwrap();
            let journal = storage.journal();
            let observed = crate::test_support::accounted::accounted(native, 262_144, false, |io| {
                let execution = StockExecution {
                    io: Some(io),
                    ..StockExecution::ordinary(&stock)
                };
                check(&execution);
                let material = (source.clone(), ModelContent::Text("Summarize".into()));
                let (result, trace) = acyclic_test_support::differential::observe::<_, _, super::super::json::SerializedType>(
                    || (),
                    |()| runtime.block_on(execution.summarize(
                        journal.as_ref(), None, OperationId::new(), 0, material,
                        &mut DispatchPhase::new(),
                    )),
                );
                // The original entry point encodes before model admission.
                // This provider has no native producer: require its typed
                // refusal, without inventing an unaccounted model fallback.
                match result {
                    Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "model provider has no original native-ledger producer" }) if native => (),
                    Err(Error::Unsupported(reason)) if !native =>
                        assert_eq!(reason, "stock model capacity requires original native work"),
                    other => panic!("unexpected original summary admission: {other:?}"),
                }
                assert_eq!(
                    trace.iter().filter(|event| event.0 == std::any::type_name::<crate::context::Context>()).count(),
                    1, "summarize must encode its source without a separate count pass",
                );
            });
            prop_assert!(observed.is_ok());
        }
        stock.limits.file_bytes = Some(expected.len() as u64 - 1);
        prop_assert!(matches!(StockExecution::ordinary(&stock).summary_source_identity(&source), Err(Error::Invalid(_))));
        for native in [false, true] {
            let observed = crate::test_support::accounted::accounted(native, 262_144, false, |io| {
                let execution = StockExecution {
                    io: Some(io),
                    ..StockExecution::ordinary(&stock)
                };
                assert!(matches!(execution.summary_source_identity(&source),
                    Err(Error::Invalid(_) | Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
            });
            prop_assert!(observed.is_ok());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // A readable summary never requires exposing any host-journal payload.
    #[test]
    fn summary_read_authority_is_independent_of_the_host_journal(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let volume = |name: &str| VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            name.into(), VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])), &mut None,
        ).unwrap();
        let private = volume("private");
        let journal = volume("journal");
        let scope = RuntimeScope::new(crate::Capabilities::new([
            private.capability(crate::conversation::VolumeOperation::Read, &mut None).unwrap(),
            journal.directory_read_capability("inputs", &mut None).unwrap(),
        ]), Limits::default()).unwrap();
        let content = |volume| ModelContent::Part(crate::model::ModelContentPart::File {
            file: FileRef::new(volume, "summaries/turn/0.txt".into(), "1".into(),
                FileDescriptor::from_bytes(&bytes, "text/plain", &mut None).unwrap(),
                "summary.txt".into(), &mut None).unwrap(),
            policy: crate::model::FileProjectionPolicy::BoundedFull,
        });
        let private = content(private);
        let journal = content(journal);
        prop_assert!(validate_model_content_scope(&private, Limits::default(), Some(&scope), None).is_ok());
        prop_assert!(matches!(validate_model_content_scope(&journal, Limits::default(), Some(&scope), None),
            Err(Error::Unauthorized(_))));
        // Broken control: omitting the task scope admits that same journal file.
        prop_assert!(validate_model_content_scope(&journal, Limits::default(), None, None).is_ok());
    }
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn completed_summary_moves_original_text_and_retires_consumed_backing() -> Result<()> {
    fn generated_event(source: ModelEvent, io: &PreparationIo) -> Result<ModelEvent> {
        // Generated operands use the production decoder and original ledger.
        let bytes = crate::contract::canonical_json_bytes(&source)?;
        let (event, mut custody) = io.decode(
            std::marker::PhantomData::<ModelEvent>,
            &bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        // The destination vector owns the slot; only the nested payload moves.
        custody
            .substitute(event.retained_owned_bytes()? - std::mem::size_of::<ModelEvent>() as u64)?;
        custody.into_retained()?;
        Ok(event)
    }

    let origin = crate::model::ReasoningOrigin {
        provider: "generated".into(),
        model: "summary-collection".into(),
        credential_scope: None,
    };
    let ignored = [
        ModelEvent::TextPart {
            text: "part".into(),
            native: Some(crate::model::NativePart {
                origin: origin.clone(),
                continuity: vec![1, 2, 3],
            }),
        },
        ModelEvent::Reasoning {
            delta: "reasoning delta".into(),
        },
        ModelEvent::ReasoningBlock {
            block: crate::model::ReasoningBlock {
                kind: crate::model::ReasoningKind::Readable,
                text: Some("readable".into()),
                origin,
                continuity: vec![4, 5, 6],
                outcome: crate::model::ReasoningOutcome::PreservedReadable,
            },
        },
        ModelEvent::Completed {
            metadata: serde_json::json!({"sparse": [{"nested": "metadata"}]}),
        },
    ];
    let mut stock = StockExecutor::new(
        Model::new("test".into(), "summary-collection".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    stock.limits.render_bytes = Some(8);
    for native in [false, true] {
        let observed = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
            let execution = StockExecution {
                io: Some(io),
                ..StockExecution::ordinary(&stock)
            };
            // These are generated collection operands, not provider fixtures.
            let mut events = Vec::new();
            for source in &ignored {
                stock_vec_push(&mut events, generated_event(source.clone(), io)?, Some(io))?;
            }
            stock_vec_push(
                &mut events,
                ModelEvent::Content {
                    delta: stock_string_copy("ignored", Some(io))?,
                },
                Some(io),
            )?;
            stock_vec_push(
                &mut events,
                ModelEvent::MessageCompleted {
                    text: stock_string_copy("earlier", Some(io))?,
                },
                Some(io),
            )?;
            let final_text = stock_string_copy("summary!", Some(io))?;
            let original = final_text.as_ptr();
            let capacity = final_text.capacity() as u64;
            stock_vec_push(
                &mut events,
                ModelEvent::MessageCompleted { text: final_text },
                Some(io),
            )?;
            let copied = io.receipt().bytes_copied;
            let text = execution.collect_summary_text(events)?;
            assert_eq!(text, "summary!");
            assert_eq!(text.as_ptr(), original);
            assert_eq!(
                io.receipt().bytes_copied,
                copied,
                "completed summary collection copied its already owned text"
            );
            assert_eq!(io.owned_bytes()?, capacity);
            stock_retire_text(text, Some(io))?;
            assert_eq!(io.owned_bytes()?, 0);
            let mut deltas = Vec::new();
            for delta in ["sum", "mary"] {
                stock_vec_push(
                    &mut deltas,
                    ModelEvent::Content {
                        delta: stock_string_copy(delta, Some(io))?,
                    },
                    Some(io),
                )?;
            }
            let text = execution.collect_summary_text(deltas)?;
            assert_eq!(text, "summary");
            assert_eq!(io.owned_bytes()?, text.capacity() as u64);
            stock_retire_text(text, Some(io))?;
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
    for native in [false, true] {
        for failure in 0..4 {
            let observed = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
                let sibling = stock_string_copy("unrelated live storage", Some(io))?;
                let sibling_bytes = sibling.capacity() as u64;
                let execution = StockExecution {
                    io: Some(io),
                    ..StockExecution::ordinary(&stock)
                };
                let mut events = Vec::new();
                let source = match failure {
                    0 => vec![
                        ModelEvent::MessageCompleted {
                            text: "summary".into(),
                        },
                        ModelEvent::ToolCall(crate::model::ToolCall::new(
                            "call".into(),
                            "shell".into(),
                            serde_json::json!({"arg": "value"}),
                        )),
                    ],
                    1 => vec![
                        ModelEvent::MessageCompleted {
                            text: "summary".into(),
                        },
                        ModelEvent::MessageCompleted {
                            text: "too large".into(),
                        },
                    ],
                    2 => vec![
                        ModelEvent::Content {
                            delta: "sum".into(),
                        },
                        ModelEvent::Content {
                            delta: "too large".into(),
                        },
                    ],
                    _ => vec![ModelEvent::MessageCompleted {
                        text: "summary".into(),
                    }],
                };
                for source in source.into_iter().chain(ignored.iter().cloned()) {
                    stock_vec_push(&mut events, generated_event(source, io)?, Some(io))?;
                }
                if failure == 3 {
                    io.cancellation().cancel();
                }
                assert!(execution.collect_summary_text(events).is_err());
                assert_eq!(
                    io.owned_bytes()?,
                    sibling_bytes,
                    "failed collection retained its input or retired another owner"
                );
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
    let execution = StockExecution::ordinary(&stock);
    assert_eq!(
        execution.collect_summary_text(vec![
            ModelEvent::Content {
                delta: "sum".into()
            },
            ModelEvent::Content {
                delta: "mary".into()
            },
        ])?,
        "summary"
    );
    for events in [
        vec![ModelEvent::MessageCompleted {
            text: "too large".into(),
        }],
        vec![ModelEvent::Content {
            delta: "too large".into(),
        }],
        vec![ModelEvent::MessageCompleted {
            text: String::new(),
        }],
        vec![ModelEvent::ToolCall(crate::model::ToolCall::new(
            "call".into(),
            "shell".into(),
            serde_json::Value::Null,
        ))],
    ] {
        assert!(matches!(
            execution.collect_summary_text(events),
            Err(Error::Invalid(_))
        ));
    }
    Ok(())
}
