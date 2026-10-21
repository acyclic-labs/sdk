use super::*;
use proptest::prelude::*;

proptest! {
    // Every sequence/key round-trips; every other locator width is rejected.
    #[test]
    fn origin_locator_has_one_exact_width(sequence in any::<u64>(), key in any::<[u8;32]>(), bytes in prop::collection::vec(any::<u8>(), 0..80)) {
        let encoded = origin_locator::encode(sequence, &key);
        prop_assert_eq!(origin_locator::decode(&encoded), Some((sequence, &key)));
        prop_assert_eq!(origin_locator::decode(&bytes).is_some(), bytes.len() == 40);
    }
}

#[tokio::test]
async fn a_separately_written_origin_locator_cannot_authenticate_a_source() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let operation = OperationId::from_bytes([132; 16]);
    let event = SchedulerEvent::Declared {
        spec: Box::new(spec(operation, 0)?),
    };
    let canonical = crate::contract::canonical_json_bytes(&event)?;
    let encoded = encode(
        1,
        operation,
        "seed",
        *blake3::hash(&canonical).as_bytes(),
        canonical,
        1,
        &mut None,
    )?;
    let (retry, _) = stream_key(6, format_args!("source"), &mut None)?;
    let source_path = acyclic_stream::StreamPath::new(COORDINATOR_PATH)?;
    client
        .commit(acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Absent {
                path: source_path.clone(),
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: source_path,
                records: vec![encoded.into()],
            }],
            idempotency_key: retry.clone(),
        })
        .await?;
    let path = origins::event_path(&event, &mut None)?.unwrap();
    let locator = origin_locator::encode(0, retry.as_bytes().try_into().unwrap()).to_vec();
    Stream::new(Arc::clone(&client), path.as_str())?
        .append_batch(vec![locator.into()], Some(0), None)
        .await?;
    let error = origins::read(&client, operation, path, None, &mut None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), wire::ErrorCode::Storage);
    assert!(error.to_string().contains("not atomic"));
    Ok(())
}
