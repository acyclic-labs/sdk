use acyclic_stream::{OwnedBytes as Bytes, *};

#[tokio::test]
async fn fenced_latest_preserves_indexed_absence_and_rejects_a_changed_source()
-> Result<(), StreamError> {
    for keyed in [false, true] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("observed")?;
        let key = keyed.then(|| Bytes::from_static(b"selected"));
        let (empty, absent) = provider
            .latest_fenced(path.clone(), key.clone(), Default::default())
            .await?;
        assert!(empty.is_none());
        assert_eq!(absent, CommitCondition::Absent { path: path.clone() });
        let append = |value: &[u8], key: &[u8]| AppendRequest {
            path: path.clone(),
            records: vec![AppendRecord::latest(value.to_vec(), key.to_vec(), false).unwrap()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        };
        assert!(matches!(
            provider
                .append(append(b"original", b"selected" as &[u8]))
                .await?,
            AppendOutcome::Committed(_)
        ));
        assert!(matches!(
            provider
                .append(append(b"unrelated", b"other" as &[u8]))
                .await?,
            AppendOutcome::Committed(_)
        ));
        let (record, fence) = provider
            .latest_fenced(path.clone(), key.clone(), Default::default())
            .await?;
        let record = record.ok_or(StreamError::InvalidResponse)?;
        assert_eq!(record.sequence, if keyed { 0 } else { 1 });
        assert_eq!(
            fence,
            CommitCondition::Tail {
                path: path.clone(),
                expected: 2
            }
        );
        let (missing, absence) = provider
            .latest_fenced(
                path.clone(),
                Some(Bytes::from_static(b"missing")),
                Default::default(),
            )
            .await?;
        assert!(missing.is_none());
        assert_eq!(
            absence, fence,
            "indexed absence must fence the whole captured head"
        );
        let publish = |condition, marker: &str| -> Result<CommitRequest, StreamError> {
            Ok(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::unique(),
                effects: vec![CommitEffect::hard(
                    vec![condition],
                    vec![CommitMutation::Append {
                        path: StreamPath::new(marker)?,
                        records: vec![b"decision".to_vec().into()],
                    }],
                )],
            })
        };
        let accepted = publish(fence.clone(), "decision")?;
        let receipt = provider.commit(accepted.clone()).await?;
        assert!(matches!(receipt, CommitOutcome::Committed(_)));
        assert!(matches!(
            provider
                .append(append(b"newer", b"selected" as &[u8]))
                .await?,
            AppendOutcome::Committed(_)
        ));
        for condition in [absent, fence, absence] {
            assert!(matches!(
                provider.commit(publish(condition, "stale")?).await?,
                CommitOutcome::Conflict(_)
            ));
        }
        assert!(matches!(
            provider.tail(StreamPath::new("stale")?).await,
            Err(error) if error.category() == StreamCategory::NotFound
        ));
        assert_eq!(
            provider.commit(accepted).await?,
            receipt,
            "exact replay lost the original decision"
        );
        let (record, fresh) = provider
            .latest_fenced(path, key, Default::default())
            .await?;
        assert_eq!(record.ok_or(StreamError::InvalidResponse)?.sequence, 2);
        assert!(matches!(
            provider.commit(publish(fresh, "fresh")?).await?,
            CommitOutcome::Committed(_)
        ));
    }
    Ok(())
}
