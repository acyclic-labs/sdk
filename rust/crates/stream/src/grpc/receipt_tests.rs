use super::tests::{TestResult, client};
use super::*;

async fn append(provider: &impl StreamProvider, path: &StreamPath) -> TestResult {
    let outcome = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"approval").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn remote_memory_receipts_preserve_the_retained_prefix() -> TestResult {
    let memory = crate::MemoryStream::default();
    let path = StreamPath::new("approval")?;
    append(&memory, &path).await?;
    let remote = client(memory.clone())?;
    // Acceptance alone makes no persistence claim, on either side.
    assert_eq!(remote.watermarks(path.clone()).await?.durable, 0);
    let receipt = remote
        .durable_receipt(path.clone(), 1)
        .await?
        .ok_or("missing receipt")?;
    assert_eq!((receipt.path(), receipt.through()), (&path, 1));
    assert!(remote.durable_receipt(path.clone(), 2).await?.is_none());
    assert_eq!(
        remote.durable_receipt(path.clone(), 1).await?,
        memory.durable_receipt(path, 1).await?
    );
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread")]
async fn remote_local_receipts_flush_only_at_the_explicit_barrier() -> TestResult {
    let directory = tempfile::tempdir()?;
    let local =
        crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default()).await?;
    let path = StreamPath::new("approval")?;
    let remote = client(local.clone())?;
    crate::deferring_durability(async {
        append(&local, &path).await?;
        // Negative control: the transport cannot turn a deferred acknowledgement
        // or an ordinary accepted observation into durability.
        let marks = remote.watermarks(path.clone()).await?;
        assert_eq!((marks.accepted, marks.durable), (1, 0));
        assert!(crate::DurableReceipt::covering(path.clone(), &marks, 1).is_none());
        assert!(remote.durable_receipt(path.clone(), 1).await?.is_some());
        assert_eq!(remote.watermarks(path.clone()).await?.durable, 1);
        Ok(()) as TestResult
    })
    .await?;
    drop(remote);
    drop(local);
    Ok(())
}

async fn accepted_projection(provider: &impl StreamProvider) -> TestResult<CommitRequest> {
    use crate::{AcceptedCommitOutcome, CommitCondition, CommitMutation, WriteOutcome};
    let path = StreamPath::new("accepted/input")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"original accepted body").into()],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    let accepted = provider.commit_accepted(request.clone()).await?;
    let AcceptedCommitOutcome::Committed { receipt, envelope } = &accepted else {
        panic!("accepted original commit");
    };
    assert!(receipt.matches_envelope(envelope));
    assert_eq!(provider.read_commit(receipt.commit_id).await?, *envelope);
    let mut compact_request = request.clone();
    compact_request.deadline_unix_millis = Some(0);
    assert_eq!(
        provider.commit(compact_request.clone()).await?,
        CommitOutcome::Committed(receipt.clone())
    );
    // The result selection is excluded from the exact keyed command. An unread
    // first response or elapsed caller deadline never changes the retained fact.
    assert_eq!(provider.commit_accepted(compact_request).await?, accepted);
    let original = crate::wire_codec::commit_write_outcome_to_wire(WriteOutcome::CommitAccepted(
        accepted.clone(),
    ))?;
    assert_eq!(
        crate::wire_codec::commit_write_outcome_from_wire(original.clone(), true)?,
        WriteOutcome::CommitAccepted(accepted)
    );
    for defect in 0..4 {
        let mut corrupt = original.clone();
        match defect {
            0 => corrupt.envelope = None,
            1 => corrupt.envelope.as_mut().unwrap().commit_id = bytes::Bytes::from_static(&[7; 32]),
            2 => corrupt.envelope.as_mut().unwrap().mutations.clear(),
            3 => {
                let mutation = corrupt
                    .envelope
                    .as_mut()
                    .unwrap()
                    .mutations
                    .first_mut()
                    .unwrap();
                let wire::committed_mutation::Mutation::Append(append) =
                    mutation.mutation.as_mut().unwrap()
                else {
                    panic!("append")
                };
                append.records[0].sequence += 1;
            }
            _ => unreachable!(),
        }
        assert!(crate::wire_codec::commit_write_outcome_from_wire(corrupt, true).is_err());
    }
    assert!(crate::wire_codec::commit_write_outcome_from_wire(original, false).is_err());
    let mut changed = request.clone();
    let CommitMutation::Append { records, .. } = &mut changed.mutations[0] else {
        panic!("append")
    };
    records[0].value = crate::OwnedBytes::from_static(b"substituted caller body");
    assert_eq!(
        provider.commit_accepted(changed).await,
        Err(StreamError::IdempotencyMismatch)
    );
    let mut conflict = request.clone();
    conflict.idempotency_key = IdempotencyKey::unique();
    assert!(matches!(
        provider.commit_accepted(conflict).await?,
        AcceptedCommitOutcome::Conflict(_)
    ));
    assert_eq!(provider.tail(path).await?, 1);
    Ok(request)
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_commit_replays_original_fact_across_projection_and_restore() -> TestResult {
    accepted_projection(&crate::MemoryStream::default()).await?;
    accepted_projection(&client(crate::MemoryStream::default())?).await?;
    #[cfg(feature = "local")]
    {
        let directory = tempfile::tempdir()?;
        let local = crate::LocalStream::open(directory.path(), Default::default()).await?;
        let request = accepted_projection(&local).await?;
        drop(local);
        let reopened = crate::LocalStream::open(directory.path(), Default::default()).await?;
        let path = StreamPath::new("accepted/input")?;
        let mut records = reopened
            .read(ReadRequest {
                path,
                from: 0,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let record = records.next().await.ok_or("restored record missing")??;
        let retained = reopened.read_commit(record.commit_id).await?;
        let crate::AcceptedCommitOutcome::Committed { envelope, .. } =
            reopened.commit_accepted(request).await?
        else {
            panic!("original accepted fact after reopen");
        };
        assert_eq!(envelope, retained);
        assert_eq!(retained.mutations.len(), 1);
        assert_eq!(record.value.as_ref(), b"original accepted body");
    }
    Ok(())
}
