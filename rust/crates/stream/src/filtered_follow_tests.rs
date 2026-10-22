use crate::*;
use futures::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;
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
fn record(key: &'static str, owner: &'static str) -> AppendRecord {
    AppendRecord {
        value: "unchanged opaque body".into(),
        headers: RecordHeaders::new(
            vec![key.into()],
            vec![Attribute {
                name: "owner".into(),
                value: owner.into(),
            }],
        )
        .unwrap(),
    }
}
fn filter() -> FollowFilter {
    FollowFilter::new(wire::SessionFilter {
        keys: vec![wire::KeyRange {
            start: b"a".as_slice().into(),
            end: b"c".as_slice().into(),
        }],
        attributes: vec![wire::AttributeMatch {
            name: "owner".into(),
            any_of: vec![b"alice".as_slice().into()],
        }],
    })
    .unwrap()
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
    let first = append(provider, record("z", "alice")).await?;
    let mut follow = provider
        .follow_checked(
            path.clone(),
            1,
            first,
            if omit {
                FollowFilter::default()
            } else {
                filter()
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
    append(provider, record("z", "alice")).await?;
    assert_eq!(
        probe.0.load(Ordering::SeqCst),
        0,
        "unrelated record woke a filtered follow"
    );
    let preceding = append(provider, record("b", "bob")).await?;
    assert_eq!(
        probe.0.load(Ordering::SeqCst),
        0,
        "wrong attribute woke a filtered follow"
    );
    let selected = record("b", "alice");
    append(provider, selected.clone()).await?;
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    let matched = follow.next().await.ok_or("follow ended")??;
    assert_eq!(matched.sequence, 3);
    assert_eq!(matched.headers, selected.headers);
    let mut resumed = provider
        .follow_checked(path.clone(), 3, preceding, filter())
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
        records: vec![record("a", "alice")],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new("append")?),
    };
    let outcome = provider.append(request.clone()).await?;
    assert_eq!(provider.append(request.clone()).await?, outcome);
    for body in [record("b", "alice"), record("a", "bob")] {
        let mut changed = request.clone();
        changed.records = vec![body];
        assert_eq!(
            provider.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
    }
    for fork in [false, true] {
        let request = CommitRequest {
            conditions: vec![],
            mutations: vec![if fork {
                CommitMutation::Fork {
                    source: path.clone(),
                    destination: StreamPath::new("header-fork")?,
                    at_tail: 1,
                    records: vec![record("a", "alice")],
                }
            } else {
                CommitMutation::Append {
                    path: path.clone(),
                    records: vec![record("a", "alice")],
                }
            }],
            idempotency_key: IdempotencyKey::new(if fork { "fork" } else { "commit" })?,
        };
        let outcome = provider.commit(request.clone()).await?;
        assert_eq!(provider.commit(request.clone()).await?, outcome);
        for body in [record("b", "alice"), record("a", "bob")] {
            let mut changed = request.clone();
            match &mut changed.mutations[0] {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                    *records = vec![body]
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
