use super::*;

/// A soft append lands only on an existing, unclosed stream. Otherwise the
/// commit skips it, reports it and still publishes its other mutations; a
/// skipped path is never created. Replays and recovery report the same skips.
pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let open = path("soft/open")?;
    let closing = path("soft/closing")?;
    let marker = path("soft/closing-marker")?;
    let absent = path("soft/absent")?;
    let carrier = path("soft/carrier")?;
    for (stream, value) in [(&open, b"open"), (&closing, b"shut"), (&marker, b"mark")] {
        provider
            .append(AppendRequest {
                path: stream.clone(),
                records: vec![Bytes::from_static(value).into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(format!("soft/seed/{stream}"))?,
            })
            .await?;
    }
    let soft = |path: &StreamPath, closed: Option<&StreamPath>| CommitMutation::SoftAppend {
        path: path.clone(),
        records: vec![Bytes::from_static(b"offer").into()],
        closed: closed.cloned(),
    };
    let unmarked = path("soft/open-marker")?;
    let commit_key = key(b"soft-commit")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent {
            path: carrier.clone(),
        }],
        mutations: vec![
            CommitMutation::Append {
                path: carrier.clone(),
                records: vec![Bytes::from_static(b"carried").into()],
            },
            soft(&open, Some(&unmarked)),
            soft(&closing, Some(&marker)),
            soft(&absent, None),
        ],
        idempotency_key: commit_key.clone(),
    };
    let outcome = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        return Err(StreamError::Unavailable);
    };
    let mut expected = vec![absent.clone(), closing.clone()];
    expected.sort();
    if receipt.skipped != expected
        || receipt.appends.len() != 2
        || provider.tail(open.clone()).await? != 2
        || provider.tail(closing.clone()).await? != 1
        || provider.tail(carrier).await? != 1
        || provider.tail(absent.clone()).await != Err(StreamError::NotFound)
        || provider.commit(request.clone()).await? != outcome
    {
        return Err(StreamError::Unavailable);
    }
    receipt.validate_request(&{
        let mut canonical = request.clone();
        crate::request::validate_commit(&mut canonical)?;
        canonical
    })?;
    let facts = provider.read_commit(receipt.commit_id).await?;
    let observation = provider
        .inspect_idempotency(commit_key)
        .await?
        .ok_or(StreamError::Unavailable)?;
    let mut recovered = request.clone();
    let binding =
        crate::request::commit_binding_with(&mut recovered, |_| Ok::<(), StreamError>(()))??;
    if !receipt.matches_envelope(&facts)
        || observation.outcome != IdempotencyOutcome::Commit(facts.clone())
        || observation.request_digest != binding.digest()
        || !binding.matches_with(&facts, |_| Ok::<(), StreamError>(()))??
    {
        return Err(StreamError::Unavailable);
    }
    // Refusals: a commit of only soft appends publishes nothing to ride on, and
    // a condition on a soft append's path would decide it twice.
    for request in [
        CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![soft(&open, None)],
            idempotency_key: key(b"soft-only")?,
        },
        CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Present { path: open.clone() }],
            mutations: vec![
                soft(&open, None),
                CommitMutation::Append {
                    path: marker.clone(),
                    records: vec![Bytes::from_static(b"x").into()],
                },
            ],
            idempotency_key: key(b"soft-conditioned")?,
        },
    ] {
        if provider.commit(request).await != Err(StreamError::InvalidArgument) {
            return Err(StreamError::Unavailable);
        }
    }
    if provider.tail(open).await? != 2 || provider.tail(marker).await? != 1 {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}
