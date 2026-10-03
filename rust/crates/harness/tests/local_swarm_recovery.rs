#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

//! Crash-window tests for the durable local swarm composition.
//!
//! These tests deliberately open the same on-disk swarm from two independent
//! handles.  The provider is only a gate/fault adapter; the registry,
//! conversation journal, `LocalFs`, and `LocalStream` are the production
//! implementations.  The child publication API is exercised by the recursive
//! fork fixture; these tests cover the admission and terminal fences that are
//! shared by root and child turns without manufacturing private registry
//! records in the integration test.

use acyclic_fs::{LocalFs, LocalOptions};
use acyclic_harness::{
    Error, OperationId, Result,
    conversation::Limits,
    filesystem::{LocalSessionPhase, LocalSwarmSession, PersistentLocalSwarm},
    model::{Model, ModelAttempt, ModelEvent, ModelProvider, ModelRequest},
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    StreamExt as _,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tempfile::tempdir;

/// A provider-side fault adapter.  `blocked` pauses the first event after the
/// model request has been admitted, which leaves the durable local registry
/// available to a second swarm handle.  `fail_first` models a provider whose
/// reply is lost after the executor has claimed the operation.
struct RecoveryProvider {
    calls: AtomicUsize,
    admissions: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    blocked: AtomicBool,
    release: Arc<AtomicBool>,
    fail_first: AtomicBool,
    reconcile_none: AtomicBool,
}

impl RecoveryProvider {
    fn normal() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            admissions: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            blocked: AtomicBool::new(false),
            release: Arc::new(AtomicBool::new(false)),
            fail_first: AtomicBool::new(false),
            reconcile_none: AtomicBool::new(true),
        })
    }

    fn blocked() -> Arc<Self> {
        let provider = Self::normal();
        provider.blocked.store(true, Ordering::SeqCst);
        provider
    }

    fn fail_once() -> Arc<Self> {
        let provider = Self::normal();
        provider.fail_first.store(true, Ordering::SeqCst);
        provider
    }

    async fn wait_for_calls(&self, expected: usize) {
        for _ in 0..50_000 {
            if self.calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "provider did not receive {expected} calls (received {})",
            self.calls.load(Ordering::SeqCst)
        );
    }

    fn release(&self) {
        self.release.store(true, Ordering::SeqCst);
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

impl ModelProvider for RecoveryProvider {
    fn admit(&self, _request: &ModelRequest) -> Result<()> {
        self.admissions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().expect("request lock").push(request);
        if self.fail_first.load(Ordering::SeqCst) && call == 0 {
            return Box::pin(stream::once(async {
                Err(Error::Storage("simulated lost provider reply".into()))
            }));
        }

        let blocked = self.blocked.load(Ordering::SeqCst);
        let release = self.release.clone();
        let first = stream::once(async move {
            if blocked {
                while !release.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            }
            Ok(ModelEvent::Content {
                delta: "durable local result".into(),
            })
        });
        Box::pin(first.chain(stream::iter([Ok(ModelEvent::Completed {
            metadata: json!({"finish": "stop"}),
        })])))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        let unavailable = self.reconcile_none.load(Ordering::SeqCst);
        Box::pin(async move {
            if unavailable {
                Ok(None)
            } else {
                Ok(Some(vec![
                    ModelEvent::Content {
                        delta: "reconciled local result".into(),
                    },
                    ModelEvent::Completed {
                        metadata: Value::Null,
                    },
                ]))
            }
        })
    }
}

/// Prime the real local providers before the swarm opens its task-scoped
/// filesystems and registry.  Opening the registry here makes the test fail if
/// a platform cannot create the durable local roots; the swarm then reopens
/// that same path as a second process would after a crash.
async fn prepare_local_root(root: &Path) -> Result<()> {
    LocalFs::local(LocalOptions::new(root.join("filesystem")))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    LocalStream::open(root.join("swarm"), LocalStreamLimits::default())
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

fn model() -> Result<Model> {
    Model::new("mock", "local-swarm-recovery", "1", json!({}))
}

fn assert_completed(session: &LocalSwarmSession) {
    assert_eq!(session.phase, LocalSessionPhase::Completed);
}

#[tokio::test]
async fn local_cancellation_survives_restart_and_blocks_dispatch() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::normal();
    let swarm = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let task = swarm.root_task().await?;
    swarm.cancel(task).await?;
    drop(swarm);

    let reopened = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    assert_eq!(
        reopened.session(task).await?.phase,
        LocalSessionPhase::Cancelled
    );
    assert!(reopened.run_root(OperationId::new(), "must not dispatch").await.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn local_completed_result_replays_after_restart_without_second_dispatch() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::normal();
    let operation = OperationId::new();
    let swarm = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let first = swarm.run_root(operation, "persist this result").await?;
    assert_eq!(first.text, "durable local result");
    assert_completed(&swarm.session(swarm.root_task().await?).await?);
    drop(swarm);

    let reopened = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let replay = reopened.run_root(operation, "persist this result").await?;
    assert_eq!(replay, first);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn local_lost_provider_reply_is_indeterminate_until_owner_reconciles() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::fail_once();
    let operation = OperationId::new();
    let swarm = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    assert!(swarm.run_root(operation, "reply may be lost").await.is_err());
    drop(swarm);

    let reopened = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let error = reopened
        .run_root(operation, "reply may be lost")
        .await
        .expect_err("unknown provider outcome must not silently retry");
    assert!(error.to_string().contains("indeterminate"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn local_retry_with_changed_input_cannot_reuse_claimed_operation() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::fail_once();
    let operation = OperationId::new();
    let swarm = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    assert!(swarm.run_root(operation, "original input").await.is_err());
    drop(swarm);

    let reopened = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let error = reopened
        .run_root(operation, "tampered retry input")
        .await
        .expect_err("operation identity must bind the original request");
    assert!(error.to_string().contains("changed") || error.to_string().contains("identity"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn local_handles_do_not_dispatch_the_same_operation_twice() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::blocked();
    let first = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let second = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let operation = OperationId::new();
    let left = first.run_root(operation, "one durable operation");
    let right = second.run_root(operation, "one durable operation");
    provider.wait_for_calls(1).await;
    // Give a second independent handle a chance to cross the admission
    // window.  A corrected implementation remains at one call; the current
    // cross-process gap reaches two calls before the gate is released.
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
    provider.release();
    let (left, right) = tokio::join!(left, right);
    let left = left?;
    let right = right?;
    assert_eq!(left, right);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn local_cancellation_cannot_be_overwritten_by_inflight_completion() -> Result<()> {
    let root = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    prepare_local_root(root.path()).await?;
    let provider = RecoveryProvider::blocked();
    let first = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let second = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider.clone(),
        Limits::default(),
    )
    .await?;
    let task = first.root_task().await?;
    let operation = OperationId::new();
    let running = first.run_root(operation, "cancel while running");
    provider.wait_for_calls(1).await;
    second.cancel(task).await?;
    provider.release();
    let _ = running.await;
    drop(first);
    drop(second);

    let reopened = PersistentLocalSwarm::open_with_model(
        root.path(),
        model()?,
        provider,
        Limits::default(),
    )
    .await?;
    assert_eq!(
        reopened.session(task).await?.phase,
        LocalSessionPhase::Cancelled,
        "a completion that was already in flight must not replace the durable cancellation"
    );
    Ok(())
}
