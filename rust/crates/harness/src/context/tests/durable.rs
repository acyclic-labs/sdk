use super::*;

#[tokio::test]
async fn durable_reopen_retry_pages_and_pins() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(TestPayloadStore::new()?);
    let provider = durable(stream.clone(), payloads.clone(), 1, payload_limits())?
        .with_publisher(payloads.clone())?;
    assert_eq!(provider.latest().await?, Context::default());
    let source = context(
        vec![
            text(ModelRole::User, "old"),
            text(ModelRole::Assistant, "answer"),
            text(ModelRole::User, "current"),
        ],
        Some(2),
    );
    let (retained, reference) = compact(&source, 2, 2)?;
    let key = Bytes::from_static(b"pair");
    let pair = provider
        .append_compaction(
            0,
            source.clone(),
            retained.clone(),
            reference.clone(),
            key.clone(),
        )
        .await?;
    let reader =
        durable(stream, payloads.clone(), 1, payload_limits())?.with_publisher(payloads)?;
    assert_eq!(
        reader
            .append_compaction(0, source.clone(), retained.clone(), reference.clone(), key)
            .await?,
        pair
    );
    assert_eq!(reader.tail_revision(None).await?, 2);
    assert_eq!(reader.revisions(0, 2).await?, pair[..1]);
    assert_eq!(reader.revisions(1, 2).await?, pair[1..]);
    assert_eq!(reader.latest().await?, retained);
    assert!(reader.revision(0).await.is_err());
    assert!(reader.revisions(2, 1).await.is_err());
    assert!(reader.revisions(0, 3).await.is_err());
    assert!(matches!(
        reader
            .append_compaction(
                0,
                source.clone(),
                retained.clone(),
                reference.clone(),
                Bytes::from_static(b"stale")
            )
            .await,
        Err(crate::Error::Conflict(_))
    ));
    let mut forged = reference.clone();
    forged.source_digest[0] ^= 1;
    assert!(
        reader
            .append_compaction(
                2,
                source.clone(),
                retained.clone(),
                forged,
                Bytes::from_static(b"forged")
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .append_compaction(
                u64::MAX - 1,
                source,
                retained,
                reference,
                Bytes::from_static(b"overflow")
            )
            .await
            .is_err()
    );
    reader
        .append(2, Context::default(), None, Bytes::from_static(b"next"))
        .await?;
    assert_eq!(reader.latest().await?, Context::default());
    assert_eq!(reader.revision(2).await?, pair[1]);
    Ok(())
}

#[tokio::test]
async fn payload_bounds_corruption_and_missing_writer_fail_closed() -> Result<()> {
    use std::sync::atomic::Ordering;
    let stream = Arc::new(MemoryStream::default());
    let payloads = Arc::new(TestPayloadStore::new()?);
    let writer = durable(stream.clone(), payloads.clone(), 1, payload_limits())?
        .with_publisher(payloads.clone())?;
    let source = context(vec![text(ModelRole::User, &"x".repeat(512))], Some(0));
    let pin = writer
        .append(0, source.clone(), None, Bytes::from_static(b"payload"))
        .await?;
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
        let reader = durable(stream.clone(), payloads.clone(), 1, limits)?;
        payloads.reads.store(0, Ordering::SeqCst);
        assert!(
            reader
                .load(&input(), payload_limits(), None)?
                .await
                .is_err()
        );
        assert_eq!(payloads.reads.load(Ordering::SeqCst), 0);
        let reader = durable(stream.clone(), payloads.clone(), 1, payload_limits())?;
        assert!(reader.load(&input(), limits, None)?.await.is_err());
        let empty = Arc::new(MemoryStream::default());
        let writer =
            durable(empty, payloads.clone(), 1, limits)?.with_publisher(payloads.clone())?;
        assert!(
            writer
                .append(0, source.clone(), None, Bytes::from_static(b"oversized"))
                .await
                .is_err()
        );
        assert_eq!(writer.tail_revision(None).await?, 0);
    }
    let reader = durable(stream, payloads.clone(), 1, payload_limits())?;
    assert_eq!(
        reader.load(&input(), payload_limits(), None)?.await?,
        source.messages
    );
    assert!(
        reader
            .append(1, source, None, Bytes::from_static(b"no-writer"))
            .await
            .is_err()
    );
    let original = payloads
        .files
        .lock()
        .unwrap()
        .remove(pin.content.path())
        .unwrap();
    assert!(reader.latest().await.is_err());
    let mut corrupted = original;
    corrupted.1[0] ^= 1;
    payloads
        .files
        .lock()
        .unwrap()
        .insert(pin.content.path().into(), corrupted);
    assert!(reader.latest().await.is_err());
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
        source_digest: crate::executor::json::canonical_json_digest(&context, u64::MAX, &mut None)?,
        maximum_messages: 2,
        retention: CompactionRetention::default(),
        summary: None,
    })
    .map_err(|error| crate::Error::Invalid(error.to_string()))?;
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
