//! A real poisoned notification registry must not separate visibility and retry identity.
use super::*;
use crate::{WriteOutcome, WriteRequest, test_meter::Meter};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn notification_failure_keeps_whole_write_and_exact_retry_for_every_family()
-> Result<(), Box<dyn std::error::Error>> {
    for (family, native) in [
        (0, false),
        (1, false),
        (2, false),
        (3, false),
        (0, true),
        (2, true),
        (3, true),
    ] {
        let provider = MemoryStream::default();
        let source = StreamPath::new("source")?;
        let destination = StreamPath::new("destination")?;
        let sibling = StreamPath::new("sibling")?;
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![
                    Bytes::from_static(b"first").into(),
                    Bytes::from_static(b"second").into(),
                ],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let before = provider.revisions.current();
        let retry_key = IdempotencyKey::unique();
        let commit = || CommitRequest {
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![
                    CommitMutation::Append {
                        path: destination.clone(),
                        records: vec![
                            Bytes::from_static(b"first").into(),
                            Bytes::from_static(b"second").into(),
                        ],
                    },
                    CommitMutation::Append {
                        path: sibling.clone(),
                        records: vec![Bytes::from_static(b"together").into()],
                    },
                ],
            )],
            deadline_unix_millis: None,
            idempotency_key: retry_key.clone(),
        };
        let request = match family {
            0 => WriteRequest::Append(AppendRequest {
                path: destination.clone(),
                records: vec![
                    Bytes::from_static(b"first").into(),
                    Bytes::from_static(b"second").into(),
                ],
                if_tail: None,
                idempotency_key: retry_key.clone(),
            }),
            1 => WriteRequest::Fork(ForkRequest {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: None,
                exclude: Vec::new(),
                idempotency_key: retry_key.clone(),
            }),
            2 => WriteRequest::Commit(commit()),
            _ => WriteRequest::CommitAccepted(commit()),
        };
        // Panic inside the actual registry's create callback while its mutex is
        // held. No mocked error or product-only test refusal path is involved.
        let signals = provider.state.read().await.signals.clone();
        let poison_path = StreamPath::new("poison")?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ =
                    signals.subscribe_with(poison_path, || panic!("notification producer failed"));
            }))
            .is_err()
        );
        let meter = Arc::new(Meter::default());
        let failed = if native {
            provider
                .native_write(request.clone(), meter.original())
                .await?
                .map(|output| output.value().clone())
        } else {
            provider.write(request.clone()).await
        };
        assert_eq!(failed, Err(StreamError::Unavailable));
        if native {
            assert_eq!(meter.write_operations.load(Ordering::SeqCst), 1);
            assert_eq!(
                meter.write_bytes.load(Ordering::SeqCst),
                if family == 0 { 11 } else { 19 },
                "notification failure erased accounting for the installed bytes"
            );
        }
        let observation = provider.inspect_idempotency(retry_key.clone()).await?;
        assert!(
            observation.is_some(),
            "notification failure lost the terminal retry observation: family {family}"
        );
        assert_eq!(
            provider.revisions.current().index,
            before.index + 1,
            "notification failure hid the installed revision"
        );
        assert_eq!(provider.tail(destination.clone()).await?, 2);
        if family >= 2 {
            assert_eq!(
                provider.tail(sibling).await?,
                1,
                "coordinated write exposed only one mutation"
            );
        }
        let replay = provider.write(request.clone()).await?;
        let commit_id = match &replay {
            WriteOutcome::Append(AppendOutcome::Committed(receipt)) => receipt.commit_id,
            WriteOutcome::Fork(receipt) => receipt.commit_id,
            WriteOutcome::Commit(CommitOutcome::Committed(receipt)) => receipt.commit_id,
            WriteOutcome::CommitAccepted(crate::AcceptedCommitOutcome::Committed {
                receipt,
                ..
            }) => receipt.commit_id,
            other => return Err(format!("retry changed family or conflicted: {other:?}").into()),
        };
        assert_eq!(
            provider.write(request).await?,
            replay,
            "retry changed the receipt"
        );
        assert_eq!(provider.inspect_idempotency(retry_key).await?, observation);
        assert_eq!(
            provider.revisions.current().index,
            before.index + 1,
            "retry applied twice"
        );
        assert_eq!(provider.tail(destination).await?, 2, "retry appended twice");
        let committed = provider.read_commit(commit_id).await?;
        assert_eq!(committed.commit_id, commit_id);
    }
    Ok(())
}
