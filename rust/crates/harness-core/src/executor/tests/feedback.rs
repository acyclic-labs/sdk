use super::*;

struct PublishedReplies {
    volume: VolumeRef,
    text: Mutex<Vec<u8>>,
}

impl crate::conversation::ContentPublisher for PublishedReplies {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage_once<'a>(
        &'a self,
        _operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            self.text.lock().unwrap().extend_from_slice(bytes);
            Ok((
                FileRef::new(
                    self.volume.clone(),
                    path.into(),
                    blake3::hash(bytes).to_hex().to_string(),
                    FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
                    display_name.into(),
                    &mut None,
                )?,
                None,
            ))
        })
    }
}

const NOTICE: &str =
    "What slowed you down or blocked you? Append feedback records; reply none if nothing.";

#[tokio::test]
async fn headless_feedback_retrospective_runs_once_per_settlement_and_preserves_the_prefix()
-> Result<()> {
    let seeded = crate::plugins::seed().await?;
    assert!(seeded.names.iter().any(|name| name == "feedback"));
    struct Seed;
    impl crate::config::Tree for Seed {
        fn read<'a>(
            &'a self,
            path: &'a str,
        ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
            Box::pin(async move {
                Ok(crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static))
            })
        }
    }
    for retrospective in [true, false] {
        let override_source = format!(
            "export default {{ feedback: {{ enabled: true, config: {{ retrospective: {retrospective} }} }} }};"
        );
        let disabled;
        let plugins = if retrospective {
            seeded.as_ref()
        } else {
            disabled = crate::plugins::load(
                &Seed,
                &[crate::layers::Layer {
                    name: "home",
                    generation: "test",
                    bytes: Some(override_source.as_bytes()),
                }],
                "headless",
            )
            .await?;
            &disabled
        };
        let per_settlement = if retrospective { 2 } else { 1 };
        let provider = Arc::new(
            ScriptedModelProvider::new((0..2 * per_settlement).map(|step| {
                ScriptedTurn::Text(
                    if retrospective && step % per_settlement != 0 {
                        "none"
                    } else {
                        "task complete"
                    }
                    .into(),
                )
            }))
            .with_token_counter(fixture_token_count),
        );
        let published = Arc::new(PublishedReplies {
            volume: crate::runtime::test_context::private_volume(1)?,
            text: Mutex::new(Vec::new()),
        });
        let executor = uncompacted_executor(
            Model::new("test".into(), "feedback".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_plugins(plugins)?;
        let mut journal = Journal::default();
        journal.11 = Some(LiveReply::new(published.clone(), "tools/turn".into()));
        let first = TurnInput {
            max_steps: None,
            operation_id: OperationId::new(),
            input: ModelContent::Text("headless task".into()),
            selected_context: None,
        };
        assert_eq!(
            executor.execute(first.clone(), &journal).await?.text,
            "task complete"
        );
        assert_eq!(
            executor.execute(first, &journal).await?.text,
            "task complete"
        );
        assert_eq!(
            provider.request_count(),
            per_settlement,
            "replay cannot ask again"
        );
        executor
            .execute(
                TurnInput {
                    max_steps: None,
                    operation_id: OperationId::new(),
                    input: ModelContent::Text("another task".into()),
                    selected_context: None,
                },
                &journal,
            )
            .await?;
        assert_eq!(
            &*published.text.lock().unwrap(),
            b"task completetask complete"
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 2 * per_settlement);
        assert!(
            requests[0]
                .messages
                .iter()
                .any(|message| matches!(&message.content,
            ModelContent::Text(text) if text.contains("Feedback is cheap and wanted")))
        );
        for settlement in 0..2 {
            let initial = &requests[settlement * per_settlement];
            let final_request = &requests[(settlement + 1) * per_settlement - 1];
            let notice_count = final_request.messages.iter().filter(|message|
                matches!(&message.content, ModelContent::Text(text) if text == NOTICE)).count();
            assert_eq!(notice_count, usize::from(retrospective));
            assert_eq!(
                crate::contract::canonical_json_bytes(&initial.messages)?,
                crate::contract::canonical_json_bytes(
                    &final_request.messages[..initial.messages.len()]
                )?,
                "the notice must be an appended delta, byte-stable over the original prefix"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn settlement_fallback_only_absorbs_definitive_provider_failures() -> Result<()> {
    #[derive(Clone, Copy)]
    enum Failure {
        Provider,
        Cancelled,
        Storage,
        Uncertain,
    }
    for (maximum, failure) in [
        (Some(1), Failure::Provider),
        (None, Failure::Provider),
        (None, Failure::Cancelled),
        (None, Failure::Storage),
        (None, Failure::Uncertain),
    ] {
        let operation = OperationId::new();
        let journal = Journal::default();
        let cancellation = journal.10.clone();
        let calls = AtomicUsize::new(0);
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("task complete".into()),
                ScriptedTurn::Events(vec![match failure {
                    Failure::Provider => ScriptedEvent::Interrupted {
                        error: "response unavailable".into(),
                    },
                    Failure::Storage => ScriptedEvent::Error {
                        error: "storage unavailable".into(),
                    },
                    Failure::Cancelled | Failure::Uncertain => ScriptedEvent::Pending {},
                }]),
            ])
            .with_token_counter(fixture_token_count)
            .with_reconciliations([crate::test_support::ScriptedReconciliation {
                orphan: false,
                observed: Vec::new(),
                continuation: None,
            }])
            .with_dispatch_observer(move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    match failure {
                        Failure::Cancelled => cancellation.cancel(),
                        Failure::Uncertain => {
                            return Box::pin(async move { Err(Error::Indeterminate(operation)) });
                        }
                        Failure::Provider | Failure::Storage => {}
                    }
                }
                Box::pin(async { Ok(()) })
            }),
        );
        let executor = uncompacted_executor(
            Model::new("test".into(), "feedback".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_plugins(crate::plugins::seed().await?.as_ref())?;
        let input = TurnInput {
            max_steps: maximum,
            operation_id: operation,
            input: ModelContent::Text("finish the task".into()),
            selected_context: None,
        };
        let output = executor.execute(input.clone(), &journal).await;
        let expected = match failure {
            Failure::Cancelled => Some(crate::wire::ErrorCode::InteractionCancelled),
            Failure::Storage => Some(crate::wire::ErrorCode::Storage),
            Failure::Uncertain => Some(crate::wire::ErrorCode::Indeterminate),
            Failure::Provider => None,
        };
        if let Some(expected) = expected {
            assert_eq!(
                output
                    .expect_err("control and storage failures must propagate")
                    .code(),
                expected
            );
            assert_eq!(provider.request_count(), 2);
            continue;
        }
        let output = output?;
        assert_eq!(output.text, "task complete");
        assert_eq!(
            output
                .transcript
                .iter()
                .filter(
                    |message| matches!(&message.content, ModelContent::Text(text) if text == NOTICE)
                )
                .count(),
            1,
            "the appended notice survives even when its response cannot finish"
        );
        let requests = provider.request_count();
        assert_eq!(requests, if maximum.is_some() { 1 } else { 2 });
        assert_eq!(executor.execute(input, &journal).await?.text, output.text);
        assert_eq!(provider.request_count(), requests);
    }
    Ok(())
}
