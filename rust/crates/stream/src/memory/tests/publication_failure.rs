//! A real poisoned notification registry must not separate visibility and retry identity.
use super::*;
use crate::{WriteOutcome, WriteRequest};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[tokio::test]
async fn notification_failure_keeps_whole_write_and_exact_retry_for_every_family()
-> Result<(), Box<dyn std::error::Error>> {
    for family in 0..4 {
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
        assert_eq!(
            provider.write(request.clone()).await,
            Err(StreamError::Unavailable)
        );
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
