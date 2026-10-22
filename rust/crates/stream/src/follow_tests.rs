use crate::*;
use futures::{FutureExt as _, StreamExt as _};

async fn seed(
    provider: &MemoryStream,
    path: &StreamPath,
    body: &'static [u8],
) -> Result<CommitId, StreamError> {
    let result = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(body)).into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let AppendOutcome::Committed(receipt) = result else {
        return Err(StreamError::Unavailable);
    };
    Ok(receipt.commit_id)
}

#[tokio::test]
async fn checked_registration_never_waits_for_a_missing_or_reused_anchor()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("events")?;
    let id = seed(&provider, &path, b"one").await?;
    for (path, from, expected, error) in [
        (path.clone(), 2, id, StreamError::OutOfRange),
        (
            path.clone(),
            1,
            CommitId::from_bytes([42; 32]),
            StreamError::PrefixNotRetained,
        ),
        (StreamPath::new("missing")?, 1, id, StreamError::NotFound),
    ] {
        let result = provider
            .follow_checked(path, from, expected)
            .now_or_never()
            .ok_or("registration waited for an absent anchor")?;
        assert!(matches!(result, Err(actual) if actual == error));
    }
    let mut follow = provider.follow_checked(path.clone(), 1, id).await?;
    assert!(follow.next().now_or_never().is_none());
    provider
        .append(AppendRequest {
            path,
            records: vec![(OwnedBytes::from_static(b"two")).into()],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    assert_eq!(
        follow
            .next()
            .now_or_never()
            .ok_or("push was delayed")?
            .ok_or("follow ended")??
            .value
            .as_slice(),
        b"two"
    );
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn checked_delivery_rejects_history_loss_after_registration_or_while_waiting()
-> Result<(), Box<dyn std::error::Error>> {
    for loss in ["missing", "truncated", "reused", "unconsumed"] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("events")?;
        let first = seed(&provider, &path, b"one").await?;
        let prefix = provider.encode_state().await;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(OwnedBytes::from_static(b"two")).into()],
                if_tail: Some(1),
                idempotency_key: None,
            })
            .await?;
        let mut rows = provider
            .read(ReadRequest {
                path: path.clone(),
                from: 1,
                limit: 1,
            })
            .await?;
        let second = rows.next().await.ok_or("missing second")??.commit_id;
        let from = if loss == "unconsumed" { 1 } else { 2 };
        let mut follow = provider
            .follow_checked(path.clone(), from, if from == 1 { first } else { second })
            .await?;
        if loss != "unconsumed" {
            assert!(follow.next().now_or_never().is_none());
        }
        let replacement = MemoryStream::default();
        match loss {
            "missing" => {}
            "truncated" | "unconsumed" => replacement.install_state(&prefix).await?,
            "reused" => {
                seed(&replacement, &path, b"replacement").await?;
                replacement
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![(OwnedBytes::from_static(b"different")).into()],
                        if_tail: Some(1),
                        idempotency_key: None,
                    })
                    .await?;
            }
            _ => unreachable!(),
        }
        provider
            .install_state(&replacement.encode_state().await)
            .await?;
        let item = follow
            .next()
            .now_or_never()
            .ok_or("replacement did not wake the follower")?
            .ok_or("replacement ended silently")?;
        assert!(
            matches!(
                item,
                Err(StreamError::NotFound
                    | StreamError::OutOfRange
                    | StreamError::PrefixNotRetained)
            ),
            "{loss}: {item:?}"
        );
    }
    Ok(())
}
