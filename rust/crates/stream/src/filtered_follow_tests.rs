use crate::*;
use futures::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub(crate) async fn qualify_progress(provider: &impl StreamProvider) -> TestResult {
    let path = StreamPath::new("filtered")?;
    let first = append(provider, record("z", "alice")?).await?;
    let mut updates = provider
        .follow_updates(path.clone(), 1, Some(first), filter()?)
        .await?;
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
        idempotency_key: None,
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

async fn qualify_resume(provider: &impl StreamProvider, path: &StreamPath, next: u64, preceding: CommitId, updates: &mut FollowStream) -> TestResult {
        // A checked resume at a skipped cut must lead to exactly the same match.
        let mut resumed = provider
            .follow_updates(path.clone(), next, Some(preceding), filter()?)
            .await?;
        for source in [updates, &mut resumed] {
            assert!(
                matches!(source.next().await.ok_or("missing resumed match")??,
                FollowUpdate::Record(Record { sequence, .. }) if sequence == expected)
            );
        }
        assert!(matches!(
            provider
                .follow_updates(
                    path.clone(),
                    next,
                    Some(CommitId::from_bytes([0x55; 32])),
                    filter()?
                )
                .await,
            Err(StreamError::PrefixNotRetained)
        ));
    Ok(())
}

#[tokio::test]
async fn memory_filtered_progress_orders_skips_matches_and_checked_resume() -> TestResult {
    qualify_progress(&MemoryStream::default()).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_filtered_progress_orders_skips_matches_and_checked_resume() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    qualify_progress(&provider).await
}
#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn record(key: &'static str, owner: &'static str) -> Result<AppendRecord, StreamError> {
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
fn filter() -> Result<FollowFilter, StreamError> {
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
async fn append(
    provider: &impl StreamProvider,
    body: AppendRecord,
) -> Result<CommitId, StreamError> {
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: StreamPath::new("filtered")?,
            records: vec![body],
            if_tail: None,
            idempotency_key: None,
        })
        .await?
    else {
        return Err(StreamError::Unavailable);
    };
    Ok(receipt.commit_id)
}

async fn qualify(provider: &impl StreamProvider, omit: bool) -> TestResult {
    let path = StreamPath::new("filtered")?;
    let first = append(provider, record("z", "alice")?).await?;
    let mut follow = provider
        .follow_checked(
            path.clone(),
            1,
            first,
            if omit {
                FollowFilter::default()
            } else {
                filter()?
            },
        )
        .await?;
    let probe = Arc::new(Wakes::default());
    let waker = Waker::from(probe.clone());
    assert!(
        follow
            .as_mut()
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    append(provider, record("z", "alice")?).await?;
    assert_eq!(
        probe.0.load(Ordering::SeqCst),
        0,
        "unrelated record woke a filtered follow"
    );
    let preceding = append(provider, record("b", "bob")?).await?;
    assert_eq!(
        probe.0.load(Ordering::SeqCst),
        0,
        "wrong attribute woke a filtered follow"
    );
    let selected = record("b", "alice")?;
    append(provider, selected.clone()).await?;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    let matched = follow.next().await.ok_or("follow ended")??;
    assert_eq!(matched.sequence, 3);
    assert_eq!(matched.headers, selected.headers);
    let mut resumed = provider
        .follow_checked(path.clone(), 3, preceding, filter()?)
        .await?;
    assert_eq!(resumed.next().await.ok_or("resume ended")??, matched);
    let mut read = provider
        .read(ReadRequest {
            path,
            from: 3,
            limit: 1,
        })
        .await?;
    assert_eq!(read.next().await.ok_or("read ended")??, matched);
    Ok(())
}
#[tokio::test]
async fn memory_filters_before_waking_and_resumes_across_skipped_records() -> TestResult {
    qualify(&MemoryStream::default(), false).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_filters_before_waking_and_resumes_across_skipped_records() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    qualify(&provider, false).await
}
#[tokio::test]
#[should_panic(expected = "unrelated record woke a filtered follow")]
async fn omitted_pushdown_negative_control() {
    qualify(&MemoryStream::default(), true).await.unwrap();
}

async fn retry_headers(provider: &impl StreamProvider) -> TestResult {
    let path = StreamPath::new("retry-headers")?;
    let request = AppendRequest {
        path: path.clone(),
        records: vec![record("a", "alice")?],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new("append")?),
    };
    let outcome = provider.append(request.clone()).await?;
    assert_eq!(provider.append(request.clone()).await?, outcome);
    for body in [record("b", "alice")?, record("a", "bob")?] {
        let mut changed = request.clone();
        changed.records = vec![body];
        assert_eq!(
            provider.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
    }
    for fork in [false, true] {
        let request = CommitRequest {
            conditions: vec![if fork {
                CommitCondition::Absent {
                    path: StreamPath::new("header-fork")?,
                }
            } else {
                CommitCondition::Tail {
                    path: path.clone(),
                    expected: 1,
                }
            }],
            mutations: vec![if fork {
                CommitMutation::Fork {
                    source: path.clone(),
                    destination: StreamPath::new("header-fork")?,
                    at_tail: 1,
                    records: vec![record("a", "alice")?],
                }
            } else {
                CommitMutation::Append {
                    path: path.clone(),
                    records: vec![record("a", "alice")?],
                }
            }],
            idempotency_key: IdempotencyKey::new(if fork { "fork" } else { "commit" })?,
        };
        let outcome = provider.commit(request.clone()).await?;
        assert_eq!(provider.commit(request.clone()).await?, outcome);
        for body in [record("b", "alice")?, record("a", "bob")?] {
            let mut changed = request.clone();
            match changed.mutations.first_mut().ok_or("missing commit mutation")? {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                    *records = vec![body];
                }
            }
            assert_eq!(
                provider.commit(changed).await,
                Err(StreamError::IdempotencyMismatch)
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn memory_retry_identity_authenticates_headers_in_every_write_family() -> TestResult {
    retry_headers(&MemoryStream::default()).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_retry_identity_authenticates_headers_in_every_write_family() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    retry_headers(&provider).await
}
