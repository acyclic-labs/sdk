use super::{
    AppendOutcome, AppendRecord, AppendRequest, Attribute, CommitId, FollowFilter, FollowRequest,
    FollowStream, FollowUpdate, IdempotencyKey, Record, RecordHeaders, StreamCategory, StreamError,
    StreamPath, StreamProvider, StreamProviderExt as _,
};
use acyclic_stream_wire::wire;
use futures::StreamExt as _;

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub(crate) async fn qualify_progress(provider: &impl StreamProvider) -> TestResult {
    let path = StreamPath::new("filtered")?;
    let first = append(provider, record("z", "alice")?).await?;
    let mut updates = provider
        .follow(FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: path.clone(),
            from: 1,
            preceding: Some(first),
            filter: filter()?,
            progress: true,
            at_least: Default::default(),
        })
        .await?;
    assert!(matches!(
        updates.next().await.ok_or("missing opening cut")??,
        FollowUpdate::Opening { tail: 1 }
    ));
    assert!(futures::poll!(updates.next()).is_pending());
    let skipped = append(provider, record("z", "alice")?).await?;
    let update = updates.next().await.ok_or("missing idle progress")??;
    assert!(
        matches!(update, FollowUpdate::Progress { next: 2, preceding } if preceding == skipped)
    );
    assert!(futures::poll!(updates.next()).is_pending());
    let matched = append(provider, record("b", "alice")?).await?;
    assert!(matches!(updates.next().await.ok_or("missing match")??,
        FollowUpdate::Record(Record { sequence: 2, commit_id, .. }) if commit_id == matched));
    let request = AppendRequest {
        path: path.clone(),
        records: vec![
            record("z", "alice")?,
            record("b", "alice")?,
            record("b", "bob")?,
            record("a", "alice")?,
        ],
        if_tail: None,
        idempotency_key: IdempotencyKey::unique(),
    };
    provider.append(request).await?;
    for expected in [4, 6] {
        let update = updates.next().await.ok_or("missing skipped prefix")??;
        let FollowUpdate::Progress { next, preceding } = update else {
            return Err("disclosed a filtered value".into());
        };
        assert_eq!(next, expected);
        qualify_resume(provider, &path, next, preceding, &mut updates).await?;
    }
    assert!(futures::poll!(updates.next()).is_pending());
    Ok(())
}

async fn qualify_resume(
    provider: &impl StreamProvider,
    path: &StreamPath,
    next: u64,
    preceding: CommitId,
    updates: &mut FollowStream,
) -> TestResult {
    // A checked resume at a skipped cut must lead to exactly the same match.
    let mut resumed = provider
        .follow(FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: path.clone(),
            from: next,
            preceding: Some(preceding),
            filter: filter()?,
            progress: true,
            at_least: Default::default(),
        })
        .await?;
    assert!(matches!(
        resumed
            .next()
            .await
            .ok_or("missing resumed opening cut")??,
        FollowUpdate::Opening { .. }
    ));
    for source in [updates, &mut resumed] {
        assert!(
            matches!(source.next().await.ok_or("missing resumed match")??,
                FollowUpdate::Record(Record { sequence, .. }) if sequence == next)
        );
    }
    assert!(matches!(
        provider
            .follow(FollowRequest {
                bootstrap_head: false,
                until: None,
                when_absent: Default::default(),
                path: path.clone(),
                from: next,
                preceding: Some(CommitId::from_bytes([0x55; 32])),
                filter: filter()?,
                progress: true,
                at_least: Default::default(),
            })
            .await,
        Err(error) if error.category() == StreamCategory::PrefixNotRetained
    ));
    Ok(())
}

pub(crate) fn record(key: &'static str, owner: &'static str) -> Result<AppendRecord, StreamError> {
    Ok(AppendRecord {
        value: "unchanged opaque body".into(),
        headers: RecordHeaders::new(
            vec![key.into()],
            vec![Attribute {
                name: "owner".into(),
                value: owner.into(),
            }],
        )?,
    })
}
pub(crate) fn filter() -> Result<FollowFilter, StreamError> {
    FollowFilter::new(wire::SessionFilter {
        all_of: vec![],
        keys: vec![wire::KeyRange {
            start: b"a".as_slice().into(),
            end: b"c".as_slice().into(),
        }],
        attributes: vec![wire::AttributeMatch {
            name: "owner".into(),
            any_of: vec![b"alice".as_slice().into()],
        }],
    })
}
pub(crate) async fn append(
    provider: &impl StreamProvider,
    body: AppendRecord,
) -> Result<CommitId, StreamError> {
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: StreamPath::new("filtered")?,
            records: vec![body],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?
    else {
        return Err(StreamError::Unavailable);
    };
    Ok(receipt.commit_id)
}
