#![allow(
    clippy::indexing_slicing,
    reason = "fixture offsets and fields are assertion inputs"
)]
use super::*;
use crate::{
    conformance,
    test_meter::{Meter, NativeDenial},
};
use futures::TryStreamExt as _;
use std::sync::atomic::AtomicU64;
mod compaction;
mod native;
mod observation;
mod ownership;
mod recovery;

#[derive(Debug)]
pub(super) enum NativeGuardFailure {
    Provider(StreamError),
    Io(std::io::Error),
    Owner(u64),
}

impl From<StreamError> for NativeGuardFailure {
    fn from(error: StreamError) -> Self {
        Self::Provider(error)
    }
}

impl std::fmt::Display for NativeGuardFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(error) => std::fmt::Display::fmt(error, f),
            Self::Io(error) => std::fmt::Display::fmt(error, f),
            Self::Owner(identity) => write!(f, "original guard {identity}"),
        }
    }
}

impl std::error::Error for NativeGuardFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Owner(_) => None,
        }
    }
}

pub(super) struct GuardLeaseWitness(pub(super) Arc<AtomicU64>);

impl Drop for GuardLeaseWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn assert_original_denial(error: &std::io::Error, expected: &'static str) {
    let cause = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<NativeDenial>());
    assert_eq!(cause.map(|cause| cause.0), Some(expected));
}

pub(super) fn keyed_append(path: &str, tail: u64) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![OwnedBytes::from_static(b"native-append-record")],
        if_tail: Some(tail),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(format!(
            "{path}@{tail}"
        )))?),
    })
}

pub(super) fn request_control_capacity(request: &CommitRequest) -> usize {
    request.conditions.capacity() * std::mem::size_of::<crate::CommitCondition>()
        + request.mutations.capacity() * std::mem::size_of::<crate::CommitMutation>()
        + request
            .mutations
            .iter()
            .map(|mutation| {
                let records = match mutation {
                    crate::CommitMutation::Append { records, .. }
                    | crate::CommitMutation::Fork { records, .. } => records,
                };
                records.capacity() * std::mem::size_of::<OwnedBytes>()
            })
            .sum::<usize>()
}

pub(super) async fn prepared_fixture(provider: &LocalStream) -> Result<CommitRequest, StreamError> {
    for path in ["prepared/owner", "prepared/target"] {
        provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![OwnedBytes::from_static(b"seed")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    Ok(CommitRequest {
        conditions: vec![
            crate::CommitCondition::Tail {
                path: StreamPath::new("prepared/owner")?,
                expected: 1,
            },
            crate::CommitCondition::Tail {
                path: StreamPath::new("prepared/target")?,
                expected: 1,
            },
        ],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("prepared/target")?,
            records: vec![OwnedBytes::new()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-prepared-key"))?,
    })
}

pub(super) static PERSIST_BLOCKER_TESTS: Mutex<()> = Mutex::new(());

pub(super) fn keyed_commit(path: &str, tail: u64) -> Result<CommitRequest, StreamError> {
    let path = StreamPath::new(path)?;
    Ok(CommitRequest {
        conditions: vec![if tail == 0 {
            crate::CommitCondition::Absent { path: path.clone() }
        } else {
            crate::CommitCondition::Tail {
                path: path.clone(),
                expected: tail,
            }
        }],
        mutations: vec![crate::CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from(format!("{path}-{tail}"))],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from(format!("{path}@{tail}")))?,
    })
}

#[tokio::test(flavor = "multi_thread")]
pub(super) async fn a_poisoned_store_fails_waits_for_frames_already_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider.commit(keyed_commit("durable", 0)?).await?;
    let required = provider.inner.required()?;
    assert_eq!(required, 1);
    provider
        .await_durable(required, None, None)
        .await
        .map_err(ordinary_failure)?;
    provider.inner.poison();
    assert_eq!(
        provider
            .await_durable(required, None, None)
            .await
            .map_err(ordinary_failure),
        Err(StreamError::Unavailable)
    );
    assert_eq!(
        provider.tail(StreamPath::new("durable")?).await,
        Err(StreamError::Unavailable)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
pub(super) async fn concurrent_commits_on_distinct_paths_replay_after_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 8;
    const COMMITS: u64 = 25;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let provider = provider.clone();
        writers.push(tokio::spawn(async move {
            let path = format!("agents/{writer}/memory");
            let mut outcomes = Vec::new();
            for tail in 0..COMMITS {
                let outcome = provider.commit(keyed_commit(&path, tail)?).await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
                outcomes.push(outcome);
            }
            Ok::<_, StreamError>(outcomes)
        }));
    }
    // Cross-path atomic commits race the writers on paths of their own.
    let joint = {
        let provider = provider.clone();
        tokio::spawn(async move {
            let (left, right) = (
                StreamPath::new("joint/left")?,
                StreamPath::new("joint/right")?,
            );
            for tail in 0..COMMITS {
                let condition = |path: &StreamPath| {
                    if tail == 0 {
                        crate::CommitCondition::Absent { path: path.clone() }
                    } else {
                        crate::CommitCondition::Tail {
                            path: path.clone(),
                            expected: tail,
                        }
                    }
                };
                let outcome = provider
                    .commit(CommitRequest {
                        conditions: vec![condition(&left), condition(&right)],
                        mutations: [&left, &right]
                            .into_iter()
                            .map(|path| crate::CommitMutation::Append {
                                path: path.clone(),
                                records: vec![OwnedBytes::from(tail.to_le_bytes().to_vec())],
                            })
                            .collect(),
                        idempotency_key: IdempotencyKey::new(OwnedBytes::from(format!(
                            "joint@{tail}"
                        )))?,
                    })
                    .await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
            }
            Ok::<_, StreamError>(())
        })
    };
    let mut outcomes = Vec::new();
    for writer in writers {
        outcomes.push(writer.await??);
    }
    joint.await??;
    let frames = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?
        .appended;
    assert_eq!(frames, (WRITERS as u64 + 1) * COMMITS);
    // Nothing else flushes or closes the provider before it reopens.
    drop(provider);

    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for (writer, outcomes) in outcomes.iter().enumerate() {
        let path = format!("agents/{writer}/memory");
        assert_eq!(reopened.tail(StreamPath::new(&path)?).await?, COMMITS);
        let records = reopened
            .read(ReadRequest {
                path: StreamPath::new(&path)?,
                from: 0,
                limit: 100,
            })
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        for (tail, record) in (0..).zip(&records) {
            assert_eq!(record.value, OwnedBytes::from(format!("{path}-{tail}")));
        }
        for (tail, outcome) in (0..).zip(outcomes) {
            assert_eq!(&reopened.commit(keyed_commit(&path, tail)?).await?, outcome);
        }
    }
    for path in ["joint/left", "joint/right"] {
        assert_eq!(reopened.tail(StreamPath::new(path)?).await?, COMMITS);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub(super) async fn deferred_mutations_wait_for_the_next_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let unflushed = |provider: &LocalStream| -> Result<bool, Box<dyn std::error::Error>> {
        Ok(provider
            .inner
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .unflushed())
    };
    let append = |provider: LocalStream, key: &'static [u8]| async move {
        provider
            .append(AppendRequest {
                path: StreamPath::new("deferred")?,
                records: vec![OwnedBytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(key))?),
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };

    deferring_durability(append(provider.clone(), b"first")).await?;
    assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 1);
    assert!(unflushed(&provider)?);
    provider.flush()?;
    assert!(!unflushed(&provider)?);

    deferring_durability(append(provider.clone(), b"second")).await?;
    assert!(unflushed(&provider)?);
    append(provider.clone(), b"third").await?;
    assert!(
        !unflushed(&provider)?,
        "a flushed mutation flushes those before it"
    );
    assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 3);
    Ok(())
}

pub(super) async fn append_one(
    provider: &LocalStream,
    path: &str,
    journal: &Path,
) -> Result<u64, Box<dyn std::error::Error>> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![OwnedBytes::from_static(b"body")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    Ok(std::fs::metadata(journal)?.len())
}

pub(super) async fn tail_of(
    directory: &Path,
    path: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let provider = LocalStream::open(directory, LocalStreamLimits::default()).await?;
    Ok(provider.tail(StreamPath::new(path)?).await.ok())
}

pub(super) fn compacting_limits() -> LocalStreamLimits {
    LocalStreamLimits {
        journal_operations: 8,
        ..LocalStreamLimits::default()
    }
}

pub(super) async fn append_keyed(
    provider: &LocalStream,
    index: u64,
) -> Result<AppendOutcome, Box<dyn std::error::Error>> {
    Ok(provider
        .append(AppendRequest {
            path: StreamPath::new("compacted")?,
            records: vec![OwnedBytes::copy_from_slice(&index.to_le_bytes())],
            if_tail: Some(index),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::copy_from_slice(
                &index.to_le_bytes(),
            ))?),
        })
        .await?)
}
