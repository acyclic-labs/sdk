use super::*;

#[tokio::test]
async fn a_half_full_journal_is_compacted_into_a_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let mut outcomes = Vec::new();
    for index in 0..21 {
        outcomes.push(append_keyed(&provider, index).await?);
    }
    let journal = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?
        .operations;
    assert!(journal < 4, "the journal restarted: {journal} frames");
    drop(provider);
    assert!(directory.path().join(SNAPSHOT_FILE).exists());

    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 21);
    let history = reopened
        .read(ReadRequest {
            path: StreamPath::new("compacted")?,
            from: 0,
            limit: 21,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(history.len(), 21);
    for (index, record) in (0_u64..21).zip(&history) {
        assert_eq!(record.sequence, index);
        assert_eq!(record.value.as_ref(), index.to_le_bytes());
    }
    reopened
        .fork(ForkRequest {
            source: StreamPath::new("compacted")?,
            destination: StreamPath::new("compacted-fork")?,
            at_tail: Some(21),
            idempotency_key: None,
        })
        .await?;
    let fork_history = reopened
        .read(ReadRequest {
            path: StreamPath::new("compacted-fork")?,
            from: 0,
            limit: 21,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(fork_history, history);
    for (index, outcome) in (0..).zip(&outcomes) {
        assert_eq!(&append_keyed(&reopened, index).await?, outcome, "{index}");
    }
    conformance::verify(&reopened)
        .await
        .map_err(std::io::Error::other)?;
    Ok(())
}

#[tokio::test]
async fn a_journal_the_snapshot_already_holds_is_discarded()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let journal = directory.path().join(JOURNAL_FILE);
    let provider = LocalStream::open(directory.path(), limits).await?;
    for index in 0..3 {
        append_keyed(&provider, index).await?;
    }
    let before = std::fs::read(&journal)?;
    append_keyed(&provider, 3).await?;
    drop(provider);
    assert!(std::fs::metadata(&journal)?.len() < u64::try_from(before.len())?);
    std::fs::write(&journal, &before)?;
    std::fs::write(directory.path().join(SNAPSHOT_TEMPORARY), b"torn")?;

    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 4);
    drop(reopened);
    assert_eq!(
        std::fs::read(directory.path().join(SNAPSHOT_TEMPORARY))?,
        b"torn"
    );
    assert_eq!(
        std::fs::metadata(&journal)?.len(),
        u64::try_from(HEADER_BYTES)?
    );
    Ok(())
}

#[tokio::test]
async fn a_damaged_snapshot_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = compacting_limits();
    let provider = LocalStream::open(directory.path(), limits).await?;
    for index in 0..4 {
        append_keyed(&provider, index).await?;
    }
    drop(provider);
    let snapshot = directory.path().join(SNAPSHOT_FILE);
    let mut damaged = std::fs::read(&snapshot)?;
    let middle = damaged.len() / 2;
    *damaged.get_mut(middle).ok_or("snapshot is empty")? ^= 1;
    std::fs::write(&snapshot, damaged)?;
    assert!(matches!(
        LocalStream::open(directory.path(), limits).await,
        Err(LocalStreamError::Corrupt)
    ));
    Ok(())
}
