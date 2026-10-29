//! Read-your-writes revisions: parking, foreign refusal and durable incarnations.
use super::*;
use futures::StreamExt as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn path() -> StreamPath {
    StreamPath::new("revisions").unwrap()
}

async fn append(provider: &LocalStream, key: crate::IdempotencyKey) -> Result<crate::Revision> {
    match provider
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: key,
        })
        .await?
    {
        crate::AppendOutcome::Committed(receipt) => Ok(receipt.revision),
        crate::AppendOutcome::TailConflict { .. } => Err("unconditional append conflicted".into()),
    }
}

async fn read(provider: &LocalStream, at_least: crate::RevisionCut) -> Result<usize> {
    let records = provider
        .read(ReadRequest {
            key: None,
            path: path(),
            from: 0,
            limit: 16,
            at_least,
        })
        .await?;
    Ok(records.collect::<Vec<_>>().await.len())
}

#[tokio::test]
async fn a_lagging_read_parks_until_its_minimum_applies() -> Result {
    let directory = tempfile::tempdir()?;
    let provider =
        Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?);
    let first = append(&provider, crate::IdempotencyKey::unique()).await?;
    // Negative control: no minimum serves the local cut at once.
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    let next = crate::Revision {
        index: first.index + 1,
        ..first.clone()
    };
    let parked = tokio::spawn({
        let provider = Arc::clone(&provider);
        async move {
            read(&provider, crate::RevisionCut::of(next))
                .await
                .map_err(|e| e.to_string())
        }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        !parked.is_finished(),
        "a read above the applied cut must park"
    );
    append(&provider, crate::IdempotencyKey::unique()).await?;
    assert_eq!(parked.await??, 2);
    Ok(())
}

#[tokio::test]
async fn a_foreign_token_is_refused_and_an_own_token_survives_reopen() -> Result {
    let outer = tempfile::tempdir()?;
    let directory = outer.path().join("store");
    // Another spelling of the same directory.
    let alias = outer.path().join("side").join("..").join("store");
    std::fs::create_dir(outer.path().join("side"))?;
    let key = crate::IdempotencyKey::unique();
    let provider = LocalStream::open(&directory, LocalStreamLimits::default()).await?;
    let issued = append(&provider, key.clone()).await?;
    let foreign = crate::AppliedRevisions::in_process().at(0);
    assert_eq!(
        provider
            .wait_applied(&crate::RevisionCut::of(foreign))
            .await,
        Err(StreamError::RevisionForeign)
    );
    drop(provider);

    // A reopen, even through another path spelling, keeps the scope and the
    // incarnation, and a replay carries the original revision.
    let provider = LocalStream::open(&alias, LocalStreamLimits::default()).await?;
    let served = provider
        .wait_applied(&crate::RevisionCut::of(issued.clone()))
        .await?;
    assert!(served.same_authority(&issued) && served.index >= issued.index);
    assert_eq!(append(&provider, key).await?, issued);
    drop(provider);

    // Negative control: wiping the same store issues a new incarnation, which
    // refuses the earlier token instead of matching its reused index.
    for entry in std::fs::read_dir(&directory)? {
        std::fs::remove_file(entry?.path())?;
    }
    let provider = LocalStream::open(&directory, LocalStreamLimits::default()).await?;
    append(&provider, crate::IdempotencyKey::unique()).await?;
    assert_eq!(
        provider.wait_applied(&crate::RevisionCut::of(issued)).await,
        Err(StreamError::RevisionForeign)
    );
    Ok(())
}

#[tokio::test]
async fn a_run_that_issued_revisions_ahead_of_a_flush_rotates_its_incarnation() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    // Negative control: a device-flushed write keeps the record in place.
    let flushed = append(&provider, crate::IdempotencyKey::unique()).await?;
    assert!(directory.path().join(INCARNATION_FILE).exists());
    // A deferred write's revision can be issued before its frame is durable;
    // a crash could lose it and recovery would reissue its index.
    let deferred =
        crate::deferring_durability(append(&provider, crate::IdempotencyKey::unique())).await?;
    assert!(!directory.path().join(INCARNATION_FILE).exists());
    drop(provider);

    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for issued in [flushed, deferred] {
        assert_eq!(
            provider.wait_applied(&crate::RevisionCut::of(issued)).await,
            Err(StreamError::RevisionForeign)
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_pre_revision_snapshot_has_an_actionable_format_refusal() -> Result {
    use prost::Message as _;

    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let memory = MemoryStream::new(limits.memory);
    let key = crate::IdempotencyKey::unique();
    memory
        .append(AppendRequest {
            path: path(),
            records: vec![Bytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: key.clone(),
        })
        .await?;
    // Source: the current provider's actual snapshot writer. Remove only the
    // nested receipt revision to reproduce the earlier retained observation.
    let state = memory.encode_state().await;
    let mut observation = memory
        .inspect_idempotency(key)
        .await?
        .ok_or("missing replay")?;
    observation.revision = None;
    let mut wire = crate::wire_codec::observation_wire(observation);
    let original = wire.encode_to_vec();
    let mut framed = (original.len() as u64).to_le_bytes().to_vec();
    framed.extend_from_slice(&original);
    let positions: Vec<_> = state
        .windows(framed.len())
        .enumerate()
        .filter_map(|(offset, bytes)| (bytes == framed).then_some(offset))
        .collect();
    let [offset] = positions.as_slice() else {
        return Err("snapshot must contain exactly one retained observation".into());
    };
    let Some(crate::wire::idempotency_observation::Outcome::Append(response)) = &mut wire.outcome
    else {
        return Err("expected append outcome".into());
    };
    let Some(crate::wire::append_response::Outcome::Committed(receipt)) = &mut response.outcome
    else {
        return Err("expected committed receipt".into());
    };
    receipt.revision = None;
    let obsolete = wire.encode_to_vec();
    let mut replacement = (obsolete.len() as u64).to_le_bytes().to_vec();
    replacement.extend_from_slice(&obsolete);
    let mut obsolete_state = state.clone();
    obsolete_state.splice(*offset..*offset + framed.len(), replacement);

    let snapshot = directory.path().join(SNAPSHOT_FILE);
    let current = encode_snapshot(limits, 0, &state)?;
    std::fs::write(&snapshot, &current)?;
    // The same writer output with its revision intact opens and serves data.
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    drop(provider);

    let obsolete = encode_snapshot(limits, 0, &obsolete_state)?;
    std::fs::write(&snapshot, &obsolete)?;
    let error = match LocalStream::open(directory.path(), limits).await {
        Err(error) => error,
        Ok(_) => return Err("pre-revision snapshot was accepted".into()),
    };
    assert!(matches!(
        error,
        LocalStreamError::UnsupportedPreRevisionStore
    ));
    assert!(error.to_string().contains("re-create the store"));
    assert_eq!(std::fs::read(&snapshot)?, obsolete);
    // Refusal releases ownership and leaves the original current snapshot usable.
    std::fs::write(&snapshot, current)?;
    let provider = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(read(&provider, crate::RevisionCut::default()).await?, 1);
    Ok(())
}
