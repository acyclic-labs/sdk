use super::*;

/// Registration precedes publication, but the consumer does not pull until
/// after creation and a second append. A read-then-subscribe implementation
/// loses this wake or skips the first record.
pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    for progress in [false, true] {
        let path = path(if progress {
            "absence/progress"
        } else {
            "absence/records"
        })?;
        if refusal(
            provider
                .follow(crate::FollowRequest::new(path.clone(), 0))
                .await,
        )
        .await
            != Some(StreamError::NotFound)
        {
            return Err(StreamError::Unavailable);
        }
        let mut request = crate::FollowRequest::new(path.clone(), 0);
        request.when_absent = crate::WhenAbsent::Wait;
        request.progress = progress;
        let mut follow = provider.follow(request).await?;
        for value in [b"created".as_slice(), b"appended"] {
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![value.into()],
                    if_tail: None,
                    idempotency_key: IdempotencyKey::new(format!(
                        "absence/{progress}/{}",
                        value.len()
                    ))?,
                })
                .await?;
        }
        for sequence in 0..2 {
            let Some(Ok(crate::FollowUpdate::Record(record))) = follow.next().await else {
                return Err(StreamError::Unavailable);
            };
            if record.sequence != sequence {
                return Err(StreamError::Unavailable);
            }
        }
    }
    let missing = path("absence/missing-anchor")?;
    let mut checked = crate::FollowRequest::new(missing, 1);
    checked.preceding = Some(crate::CommitId::from_bytes([1; 32]));
    checked.when_absent = crate::WhenAbsent::Wait;
    if refusal(provider.follow(checked).await).await != Some(StreamError::PrefixNotRetained) {
        return Err(StreamError::Unavailable);
    }
    let source = path("absence/fork-source/status")?;
    provider
        .append(AppendRequest {
            path: source,
            records: vec!["inherited".into()],
            if_tail: None,
            idempotency_key: key(b"absence/fork-seed")?,
        })
        .await?;
    let mut request = crate::FollowRequest::new(path("absence/fork-destination/status")?, 0);
    request.when_absent = crate::WhenAbsent::Wait;
    let mut follow = provider.follow(request).await?;
    provider
        .fork(ForkRequest {
            source: path("absence/fork-source")?,
            destination: path("absence/fork-destination")?,
            at_tail: Some(0),
            exclude: Vec::new(),
            idempotency_key: key(b"absence/fork")?,
        })
        .await?;
    match follow.next().await {
        Some(Ok(crate::FollowUpdate::Record(record))) if record.value.as_ref() == b"inherited" => {
            Ok(())
        }
        _ => Err(StreamError::Unavailable),
    }
}

async fn refusal(result: Result<crate::FollowStream, StreamError>) -> Option<StreamError> {
    match result {
        Err(error) => Some(error),
        Ok(mut follow) => follow.next().await.and_then(Result::err),
    }
}
