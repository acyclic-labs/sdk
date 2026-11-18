use super::filtered_follow_fixture::{TestResult, append, filter, qualify_progress, record};
use acyclic_stream::StreamCategory;
use acyclic_stream::*;
use futures::Stream as _;
use futures::StreamExt;
use futures::TryFutureExt as _;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
};

#[tokio::test]
async fn memory_filtered_progress_orders_skips_matches_and_checked_resume() -> TestResult {
    qualify_progress(&MemoryStream::default()).await
}
#[cfg(not(target_arch = "wasm32"))]
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
async fn qualify(provider: &impl StreamProvider, omit: bool) -> TestResult {
    let path = StreamPath::new("filtered")?;
    let first = append(provider, record("z", "alice")?).await?;
    let mut follow = provider
        .follow(acyclic_stream::FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: path.clone(),
            from: 1,
            preceding: Some(first),
            filter: if omit {
                FollowFilter::default()
            } else {
                filter()?
            },
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(acyclic_stream::records_only)
        .await?;
    let probe = Arc::new(Wakes::default());
    let waker = Waker::from(probe.clone());
    assert!(
        std::pin::Pin::new(&mut follow)
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
        .follow(acyclic_stream::FollowRequest {
            bootstrap_head: false,
            until: None,
            when_absent: Default::default(),
            path: path.clone(),
            from: 3,
            preceding: Some(preceding),
            filter: filter()?,
            progress: false,
            at_least: Default::default(),
        })
        .map_ok(acyclic_stream::records_only)
        .await?;
    assert_eq!(resumed.next().await.ok_or("resume ended")??, matched);
    let mut read = provider
        .read(ReadRequest {
            key: None,
            path,
            from: 3,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    assert_eq!(read.next().await.ok_or("read ended")??, matched);
    Ok(())
}
#[tokio::test]
async fn memory_filters_before_waking_and_resumes_across_skipped_records() -> TestResult {
    qualify(&MemoryStream::default(), false).await
}
#[cfg(not(target_arch = "wasm32"))]
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
        idempotency_key: IdempotencyKey::new("append")?,
    };
    let outcome = provider.append(request.clone()).await?;
    assert_eq!(provider.append(request.clone()).await?, outcome);
    for body in [record("b", "alice")?, record("a", "bob")?] {
        let mut changed = request.clone();
        changed.records = vec![body];
        assert_eq!(
            provider
                .append(changed)
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::IdempotencyMismatch)
        );
    }
    for fork in [false, true] {
        let request = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![if fork {
                    CommitCondition::Absent {
                        path: StreamPath::new("header-fork")?,
                    }
                } else {
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: 1,
                    }
                }],
                vec![if fork {
                    CommitMutation::Fork {
                        exclude: Vec::new(),
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
            )],
            idempotency_key: IdempotencyKey::new(if fork { "fork" } else { "commit" })?,
        };
        let outcome = provider.commit(request.clone()).await?;
        assert_eq!(provider.commit(request.clone()).await?, outcome);
        for body in [record("b", "alice")?, record("a", "bob")?] {
            let mut changed = request.clone();
            match changed
                .effects
                .first_mut()
                .ok_or("missing effect")?
                .mutations
                .first_mut()
                .ok_or("missing commit mutation")?
            {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                    *records = vec![body];
                }
            }
            assert_eq!(
                provider
                    .commit(changed)
                    .await
                    .map_err(|error| error.category()),
                Err(StreamCategory::IdempotencyMismatch)
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn memory_retry_identity_authenticates_headers_in_every_write_family() -> TestResult {
    retry_headers(&MemoryStream::default()).await
}
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn local_retry_identity_authenticates_headers_in_every_write_family() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    retry_headers(&provider).await
}
