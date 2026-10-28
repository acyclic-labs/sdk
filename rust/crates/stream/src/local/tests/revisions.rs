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
    let directory = tempfile::tempdir()?;
    let key = crate::IdempotencyKey::unique();
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let issued = append(&provider, key.clone()).await?;
    let foreign = crate::AppliedRevisions::in_process().at(0);
    assert_eq!(
        provider
            .wait_applied(&crate::RevisionCut::of(foreign))
            .await,
        Err(StreamError::RevisionForeign)
    );
    drop(provider);

    // A reopen keeps the incarnation, and a replay carries the original revision.
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let served = provider
        .wait_applied(&crate::RevisionCut::of(issued.clone()))
        .await?;
    assert!(served.same_authority(&issued) && served.index >= issued.index);
    assert_eq!(append(&provider, key).await?, issued);
    drop(provider);

    // Negative control: wiping the same store issues a new incarnation, which
    // refuses the earlier token instead of matching its reused index.
    for entry in std::fs::read_dir(directory.path())? {
        std::fs::remove_file(entry?.path())?;
    }
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    append(&provider, crate::IdempotencyKey::unique()).await?;
    assert_eq!(
        provider.wait_applied(&crate::RevisionCut::of(issued)).await,
        Err(StreamError::RevisionForeign)
    );
    Ok(())
}
