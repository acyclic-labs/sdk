use super::*;

#[tokio::test]
    async fn durable_source_reopen_applies_admitted_payload_bounds_before_body_reads() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("bounded/source-admission")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "source",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
        let context = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("x".repeat(400)),
            }],
            ..Context::default()
        };
        provider
            .append(0, context.clone(), None, Bytes::from_static(b"original"))
            .await?;
        let reopened = DurableContextProvider::new(
            stream,
            path,
            "source",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?;
        let input = ContextInput {
            input: ModelContent::Text("current".into()),
            selected_context: None,
            step: 0,
            prior_messages: Vec::new(),
        };
        for limits in [
            Limits {
                file_bytes: 128,
                ..payload_limits()
            },
            Limits {
                render_bytes: 128,
                ..payload_limits()
            },
        ] {
            payloads.reads.store(0, Ordering::SeqCst);
            assert!(reopened.load(&input, limits, None)?.await.is_err());
            assert_eq!(payloads.reads.load(Ordering::SeqCst), 0);
        }
        assert_eq!(
            reopened.load(&input, payload_limits(), None)?.await?,
            context.messages
        );
        assert_eq!(payloads.reads.load(Ordering::SeqCst), 1);
        Ok(())
    }
#[tokio::test]
    async fn context_pages_reject_corrupt_identity_and_unproven_compaction() -> Result<()> {
        let context = Context {
            messages: vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text("pinned".into()),
            }],
            metadata: BTreeMap::new(),
            current_input_index: None,
        };
        let context_bytes = crate::contract::canonical_json_bytes(&context)?;
        let payloads = Arc::new(TestPayloadStore::new()?);
        let (payload, _payload_hold) = payloads
            .stage(
                crate::OperationId::new(),
                "context/projections/pinned.json",
                &context_bytes,
                "application/json",
                "context.json",
                None,
            )?
            .await?;
        let record = serde_json::json!({
            "format_version": 5,
            "revision": 1,
            "source": "instructions",
            "source_revision": "1",
            "context": payload,
            "compaction": null,
        });
        let mut records = Vec::new();
        for (field, value) in [
            ("format_version", serde_json::json!(0)),
            ("revision", serde_json::json!(2)),
            ("source", serde_json::json!("another-source")),
            ("source_revision", serde_json::json!("another-revision")),
            ("unknown", serde_json::json!(true)),
        ] {
            let mut corrupt = record.clone();
            corrupt[field] = value;
            records.push(crate::contract::canonical_json_bytes(&corrupt)?);
        }
        records.push(
            serde_json::to_vec_pretty(&record)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        );
        records.push(b"not-json".to_vec());
        let mut missing_source = record;
        missing_source["compaction"] = serde_json::to_value(CompactionReference {
            source_digest: crate::contract::canonical_json_digest(&context)?,
            maximum_messages: 2,
            retention: CompactionRetention::default(),
            summary: None,
        }).map_err(|error| crate::Error::Invalid(error.to_string()))?;
        records.push(crate::contract::canonical_json_bytes(&missing_source)?);
        for bytes in records {
            let stream = Arc::new(MemoryStream::default());
            let path = StreamPath::new("corrupt/context")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            stream
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![bytes.into()],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let provider = DurableContextProvider::new(
                stream,
                path,
                "instructions",
                "1",
                1,
                payloads.clone(),
                payload_limits(),
            )?;
            assert!(provider.revisions(0, 1).await.is_err());
        }
        Ok(())
    }
#[tokio::test]
    #[allow(
        clippy::indexing_slicing,
        reason = "each index is preceded by an assert_eq! on the corresponding Vec's len(), proving it in-bounds"
    )]
    async fn durable_sources_and_compaction_reopen_exactly() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("runtime/context/memory")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
        assert_eq!(provider.latest().await?, Context::default());
        assert!(
            provider
                .append(
                    0,
                    Context {
                        messages: vec![ModelMessage {
                            role: ModelRole::User,
                            content: ModelContent::Text("secret".into())
                        }],
                        metadata: BTreeMap::new(),
                        current_input_index: Some(1),
                    },
                    None,
                    Bytes::from_static(b"invalid-marker-rejected")
                )
                .await
                .is_err()
        );
        let original = Context {
            messages: vec![
                message(ModelRole::User, "one")?,
                message(ModelRole::Assistant, "two")?,
                message(ModelRole::User, "three")?,
            ],
            metadata: BTreeMap::new(),
            current_input_index: None,
        };
        provider
            .append(0, original.clone(), None, Bytes::from_static(b"context-1"))
            .await?;
        let (compacted, reference) = DurableContextProvider::compact(&original, 2, Some(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: 3,
                source_digest: crate::contract::canonical_json_digest(&original)?,
                output: message(ModelRole::System, "summary")?
                    .content
                    .file_refs()
                    .into_iter()
                    .next()
                    .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
                    .clone(),
            }), CompactionRetention::default(), None)?;
        let mut forged = reference.clone();
        forged.source_digest[0] ^= 1;
        assert!(
            provider
                .append(
                    1,
                    compacted.clone(),
                    Some(forged),
                    Bytes::from_static(b"forged-context"),
                )
                .await
                .is_err()
        );
        provider
            .append(
                1,
                compacted.clone(),
                Some(reference.clone()),
                Bytes::from_static(b"context-2"),
            )
            .await?;
        provider
            .append(
                1,
                compacted.clone(),
                Some(reference.clone()),
                Bytes::from_static(b"context-2"),
            )
            .await?;
        provider
            .append(2, compacted.clone(), None, Bytes::from_static(b"context-3"))
            .await?;

        let reopened = Arc::new(DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            2,
            payloads.clone(),
            payload_limits(),
        )?);
        let revisions = reopened.revisions(0, 2).await?;
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[1].compaction, Some(reference));
        assert_eq!(reopened.revisions(1, 2).await?, revisions[1..]);
        assert_eq!(reopened.latest().await?, compacted);

        let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
            "memory",
            "1",
            reopened,
            ContextPlacement::Prepend,
        )) as Arc<dyn ContextStage>]);
        let assembled = pipeline
            .run(&ContextInput {
                input: ModelContent::Text("current input".into()),
                selected_context: None,
                step: 0,
                prior_messages: vec![message(ModelRole::User, "current")?],
            }, Limits::default(), None)
            .await?;
        assert_eq!(assembled.messages.len(), 4);
        assert_eq!(assembled.messages[3], message(ModelRole::User, "current")?);
        let undersized = DurableContextProvider::new(
            stream,
            path,
            "memory",
            "1",
            1,
            payloads,
            payload_limits(),
        )?;
        assert_eq!(undersized.latest().await?, compacted);
        assert_eq!(undersized.revisions(0, 2).await?.len(), 1);
        let second = undersized.revisions(1, 2).await?;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].context, compacted);
        Ok(())
    }
#[tokio::test]
    async fn continuing_compaction_publishes_and_retries_one_atomic_pair() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(TestPayloadStore::new()?);
        let path = StreamPath::new("runtime/context/continuing")
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let provider = DurableContextProvider::new(
            stream.clone(),
            path.clone(),
            "memory",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads.clone())?;
        let source = Context {
            messages: vec![
                message(ModelRole::User, "one")?,
                message(ModelRole::Assistant, "two")?,
                message(ModelRole::User, "three")?,
            ],
            current_input_index: Some(2),
            ..Context::default()
        };
        let mut covered = source.clone();
        covered.messages.truncate(2);
        covered.current_input_index = None;
        let output = message(ModelRole::System, "summary")?
            .content
            .file_refs()
            .into_iter()
            .next()
            .ok_or_else(|| crate::Error::Invalid("summary file is missing".into()))?
            .clone();
        let (compacted, reference) = DurableContextProvider::compact(&source, 2, Some(ContextSummary {
                operation_id: crate::OperationId::new(),
                step: 0,
                source_messages: 2,
                source_digest: crate::contract::canonical_json_digest(&covered)?,
                output,
            }), CompactionRetention::default(), None)?;
        let key = Bytes::from_static(b"atomic-compaction");
        // Discard the first successful acknowledgement, as a caller whose receipt was lost would.
        provider
            .append_compaction(
                0,
                source.clone(),
                compacted.clone(),
                reference.clone(),
                key.clone(),
            )
            .await?;
        let reopened = DurableContextProvider::new(
            stream,
            path,
            "memory",
            "1",
            1,
            payloads.clone(),
            payload_limits(),
        )?
        .with_publisher(payloads)?;
        let pair = reopened
            .append_compaction(0, source.clone(), compacted.clone(), reference.clone(), key)
            .await?;
        assert_eq!(reopened.tail_revision(None).await?, 2);
        assert_eq!(pair.first().map(|record| &record.context), Some(&source));
        assert_eq!(pair.last().map(|record| &record.context), Some(&compacted));
        assert_eq!(reopened.latest().await?, compacted);
        let pin = reopened
            .latest_revision()
            .await?
            .ok_or_else(|| crate::Error::Storage("published context revision is missing".into()))?;
        assert_eq!(pin.revision, 2);
        assert_eq!(reopened.revision(pin.revision).await?, pin);
        assert!(reopened.revision(0).await.is_err());
        assert_eq!(reopened.revisions(0, 1).await?, pair[..1]);
        assert_eq!(reopened.revisions(1, 2).await?, pair[1..]);
        assert!(matches!(
            reopened
                .append_compaction(
                    0,
                    source.clone(),
                    compacted.clone(),
                    reference.clone(),
                    Bytes::from_static(b"stale-compaction")
                )
                .await,
            Err(crate::Error::Conflict(_))
        ));
        let mut forged = reference.clone();
        forged.source_digest[0] ^= 1;
        assert!(
            reopened
                .append_compaction(
                    2,
                    source.clone(),
                    compacted.clone(),
                    forged,
                    Bytes::from_static(b"forged-compaction")
                )
                .await
                .is_err()
        );
        assert!(
            reopened
                .append_compaction(
                    u64::MAX - 1,
                    source,
                    compacted,
                    reference,
                    Bytes::from_static(b"overflow-compaction")
                )
                .await
                .is_err()
        );
        assert_eq!(reopened.tail_revision(None).await?, 2);
        reopened
            .append(
                2,
                Context::default(),
                None,
                Bytes::from_static(b"newer-context"),
            )
            .await?;
        assert_eq!(reopened.latest().await?, Context::default());
        assert_eq!(reopened.revision(pin.revision).await?, pin);
        Ok(())
    }
#[tokio::test]
    async fn context_payload_missing_corrupt_and_oversized_fail_without_fallback() -> Result<()> {
        for failure in [0, 1, 2] {
            let stream = Arc::new(MemoryStream::default());
            let payloads = Arc::new(TestPayloadStore::new()?);
            let path = StreamPath::new("context/payload-negatives")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            let provider = DurableContextProvider::new(
                stream.clone(),
                path.clone(),
                "memory",
                "1",
                1,
                payloads.clone(),
                payload_limits(),
            )?
            .with_publisher(payloads.clone())?;
            let context = Context {
                messages: vec![ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("pinned payload".into()),
                }],
                ..Context::default()
            };
            let published = provider
                .append(0, context, None, Bytes::from_static(b"payload"))
                .await?;
            if failure < 2 {
                {
                    let mut files = payloads.files.lock().unwrap();
                    if failure == 0 {
                        files.remove(published.content.path());
                    } else {
                        let (_, bytes) =
                            files.get_mut(published.content.path()).ok_or_else(|| {
                                crate::Error::Storage("payload fixture is missing".into())
                            })?;
                        if let Some(first) = bytes.first_mut() {
                            *first ^= 1;
                        }
                    }
                }
                assert!(provider.latest().await.is_err());
            } else {
                payloads.reads.store(0, std::sync::atomic::Ordering::SeqCst);
                let reader = DurableContextProvider::new(
                    stream,
                    path,
                    "memory",
                    "1",
                    1,
                    payloads.clone(),
                    Limits {
                        render_bytes: 1,
                        ..payload_limits()
                    },
                )?;
                assert!(reader.latest().await.is_err());
                assert_eq!(payloads.reads.load(std::sync::atomic::Ordering::SeqCst), 0);
            }
        }
        Ok(())
    }
#[tokio::test]
    async fn context_payload_bounds_and_missing_writer_do_not_publish() -> Result<()> {
        for (file_bytes, render_bytes, writer) in [
            (128, 1_024, true),
            (1_024, 128, true),
            (1_024, 1_024, false),
        ] {
            let payloads = Arc::new(TestPayloadStore::new()?);
            let provider = DurableContextProvider::new(
                Arc::new(MemoryStream::default()),
                StreamPath::new("context/bounds")
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                "memory",
                "1",
                1,
                payloads.clone(),
                Limits {
                    file_bytes,
                    render_bytes,
                    ..payload_limits()
                },
            )?;
            let provider = if writer {
                provider.with_publisher(payloads.clone())?
            } else {
                provider
            };
            let context = Context {
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("x".repeat(512)),
                }],
                current_input_index: Some(0),
                ..Context::default()
            };
            assert!(
                provider
                    .append(0, context, None, Bytes::from_static(b"oversized"))
                    .await
                    .is_err()
            );
            assert_eq!(provider.tail_revision(None).await?, 0);
            assert!(payloads.files.lock().unwrap().is_empty());
        }
        Ok(())
    }
