use super::*;

/// A write between open and delivery must not change the opening observation.
pub(crate) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), Failure> {
    let path = path("selected/history")?;
    let AppendOutcome::Committed(seed) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![b"original".as_slice().into(); 2],
            if_tail: Some(0),
            idempotency_key: key(b"selected/seed")?,
        })
        .await?
    else {
        return Err(violated!());
    };
    let request = ReadRequest {
        key: None,
        path: path.clone(),
        from: 0,
        limit: 8,
        at_least: crate::RevisionCut::of(seed.revision.clone()),
    };
    let mut records = provider.read(request.clone()).await?;
    let mut empty = provider.read(ReadRequest { from: 2, ..request }).await?;
    let mut finite_request = crate::FollowRequest::new(path.clone(), 0);
    finite_request.until = Some(u64::MAX);
    let mut finite = provider.follow(finite_request).await?;
    let mut latest_request = crate::FollowRequest::new(path.clone(), 0);
    latest_request.bootstrap_head = true;
    let latest = provider.follow(latest_request).await?;
    let absent = super::path("selected/absent")?;
    let mut absent_request = crate::FollowRequest::new(absent.clone(), 0);
    absent_request.when_absent = crate::WhenAbsent::Wait;
    let mut waiting = provider.follow(absent_request).await?;
    if [
        &records.snapshot,
        &empty.snapshot,
        &finite.snapshot,
        &latest.snapshot,
        &waiting.snapshot,
    ]
    .into_iter()
    .any(|revision| revision != &seed.revision)
        || records.selection.end != Some(2)
        || empty.selection.end != Some(2)
        || finite.selection.end != Some(2)
        || latest.selection.from != 2
        || latest.selection.preceding != Some(seed.commit_id)
        || latest
            .selection
            .bootstrap
            .as_ref()
            .is_none_or(|record| record.sequence != 1 || record.commit_id != seed.commit_id)
    {
        return Err(violated!());
    }
    let AppendOutcome::Committed(later) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![b"later".as_slice().into()],
            if_tail: Some(2),
            idempotency_key: key(b"selected/later")?,
        })
        .await?
    else {
        return Err(violated!());
    };
    // Negative control for the retired post-open sample: it now observes later,
    // while every original cursor must still report and retain the seed cut.
    let sampled = provider
        .wait_applied(&crate::RevisionCut::of(later.revision.clone()))
        .await?;
    if sampled == records.snapshot
        || records.snapshot != seed.revision
        || empty.next().await.is_some()
    {
        return Err(violated!());
    }
    for sequence in 0..2 {
        if records
            .next()
            .await
            .transpose()?
            .is_none_or(|record| record.commit_id != seed.commit_id || record.sequence != sequence)
            || !matches!(finite.next().await.transpose()?, Some(crate::FollowUpdate::Record(record)) if record.commit_id == seed.commit_id && record.sequence == sequence)
        {
            return Err(violated!());
        }
    }
    if records.next().await.is_some() || finite.next().await.is_some() {
        return Err(violated!());
    }
    provider
        .append(AppendRequest {
            path: absent,
            records: vec![b"created".as_slice().into()],
            if_tail: Some(0),
            idempotency_key: key(b"selected/creation")?,
        })
        .await?;
    if !matches!(waiting.next().await.transpose()?, Some(crate::FollowUpdate::Record(record)) if record.sequence == 0)
    {
        return Err(violated!());
    }
    Ok(())
}

#[cfg(all(test, feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn selected_open_survives_later_publication() -> Result<(), Box<dyn std::error::Error>> {
    verify(&crate::MemoryStream::default()).await?;
    let directory = tempfile::tempdir()?;
    let provider =
        crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default()).await?;
    verify(&provider).await?;
    Ok(())
}
