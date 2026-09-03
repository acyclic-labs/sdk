//! Bounded runtime-independent execution for synchronous native storage backends.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::notification::{
    AsyncNotificationStore, NotificationError, NotificationPoll, NotificationResult,
    NotificationStore,
};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityResult, AuthorityStore, AuthorityStoreError,
    CreateAuthorityOutcome, ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReceipt,
    ObjectResult, ObjectStore, ObjectStoreError, ReplayLimit,
};
use crate::{AsyncAuthorityStore, AsyncObjectStore};
use bytes::Bytes;
use std::future::Future;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use thiserror::Error;
use tokio::sync::Semaphore;

type Job = Box<dyn FnOnce() + Send + 'static>;

const MAXIMUM_WORKER_THREADS: usize = 1_024;
const MAXIMUM_IN_FLIGHT_OPERATIONS: usize = 65_536;

/// Bounded worker-pool configuration for native synchronous backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeExecutorConfig {
    /// Exact number of blocking worker threads.
    pub worker_threads: usize,
    /// Maximum queued plus executing operations.
    pub maximum_in_flight_operations: usize,
}

impl Default for NativeExecutorConfig {
    fn default() -> Self {
        let worker_threads = thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .min(8);
        Self {
            worker_threads,
            maximum_in_flight_operations: worker_threads.saturating_mul(64).max(worker_threads),
        }
    }
}

/// Invalid native worker-pool configuration.
#[derive(Debug, Error)]
pub enum NativeExecutorConfigError {
    /// No worker can make progress.
    #[error("native executor requires at least one worker thread")]
    NoWorkers,
    /// Queue admission cannot retain every worker.
    #[error("native executor in-flight bound must be at least the worker count")]
    InFlightBelowWorkers,
    /// The requested worker count could exhaust process resources during construction.
    #[error("native executor worker count exceeds the supported maximum")]
    TooManyWorkers,
    /// The Tokio admission semaphore cannot represent the requested bound.
    #[error("native executor in-flight bound exceeds the supported maximum")]
    InFlightTooLarge,
    /// A native worker thread could not be created.
    #[error("native executor worker creation failed: {0}")]
    WorkerSpawn(#[source] std::io::Error),
}

/// Stable executor-level failures before a backend receipt is available.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum NativeExecutionError {
    /// Cancellation won before the operation entered the bounded queue.
    #[error("native storage operation was cancelled before enqueue")]
    Cancelled,
    /// The bounded worker pool became unavailable before execution.
    #[error("native storage executor is unavailable")]
    Unavailable,
    /// Backend code panicked after execution began.
    #[error("native storage worker panicked")]
    WorkerPanicked,
}

struct NativeExecutorInner {
    sender: SyncSender<Job>,
    admission: Arc<Semaphore>,
}

/// Cloneable bounded native worker pool independent of the caller's async runtime.
#[derive(Clone)]
pub struct NativeExecutor {
    inner: Arc<NativeExecutorInner>,
}

impl NativeExecutor {
    /// Creates a fixed-size worker pool and bounded in-flight admission gate.
    ///
    /// # Errors
    ///
    /// Rejects zero/contradictory bounds or an operating-system thread-spawn failure.
    pub fn new(config: NativeExecutorConfig) -> Result<Self, NativeExecutorConfigError> {
        if config.worker_threads == 0 {
            return Err(NativeExecutorConfigError::NoWorkers);
        }
        if config.worker_threads > MAXIMUM_WORKER_THREADS {
            return Err(NativeExecutorConfigError::TooManyWorkers);
        }
        if config.maximum_in_flight_operations < config.worker_threads {
            return Err(NativeExecutorConfigError::InFlightBelowWorkers);
        }
        if config.maximum_in_flight_operations > MAXIMUM_IN_FLIGHT_OPERATIONS {
            return Err(NativeExecutorConfigError::InFlightTooLarge);
        }
        let (sender, receiver) = sync_channel(config.maximum_in_flight_operations);
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..config.worker_threads {
            let worker_receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("acyclic-fs-storage-{index}"))
                .spawn(move || worker_loop(&worker_receiver))
                .map_err(NativeExecutorConfigError::WorkerSpawn)?;
        }
        Ok(Self {
            inner: Arc::new(NativeExecutorInner {
                sender,
                admission: Arc::new(Semaphore::new(config.maximum_in_flight_operations)),
            }),
        })
    }

    pub(crate) async fn execute<R: Send + 'static>(
        &self,
        cancellation: &CancellationToken,
        operation: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, NativeExecutionError> {
        cancellation
            .check()
            .map_err(|_| NativeExecutionError::Cancelled)?;
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(NativeExecutionError::Cancelled),
            permit = Arc::clone(&self.inner.admission).acquire_owned() => {
                permit.map_err(|_| NativeExecutionError::Unavailable)?
            }
        };
        cancellation
            .check()
            .map_err(|_| NativeExecutionError::Cancelled)?;
        let (completion, call) = BlockingCall::new();
        let job = Box::new(move || {
            let result = catch_unwind(AssertUnwindSafe(operation))
                .map_err(|_| NativeExecutionError::WorkerPanicked);
            drop(permit);
            completion.complete(result);
        });
        match self.inner.sender.try_send(job) {
            Ok(()) => call.await,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                Err(NativeExecutionError::Unavailable)
            }
        }
    }
}

fn worker_loop(receiver: &Mutex<Receiver<Job>>) {
    loop {
        let job = match receiver.lock() {
            Ok(receiver) => receiver.recv(),
            Err(poisoned) => poisoned.into_inner().recv(),
        };
        match job {
            Ok(job) => {
                let _ = catch_unwind(AssertUnwindSafe(job));
            }
            Err(_) => return,
        }
    }
}

struct CompletionState<R> {
    result: Option<Result<R, NativeExecutionError>>,
    waker: Option<Waker>,
}

struct Completion<R> {
    state: Arc<Mutex<CompletionState<R>>>,
}

impl<R> Completion<R> {
    fn complete(self, result: Result<R, NativeExecutionError>) {
        let waker = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
    }
}

struct BlockingCall<R> {
    state: Arc<Mutex<CompletionState<R>>>,
}

impl<R> BlockingCall<R> {
    fn new() -> (Completion<R>, Self) {
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            waker: None,
        }));
        (
            Completion {
                state: Arc::clone(&state),
            },
            Self { state },
        )
    }
}

impl<R> Future for BlockingCall<R> {
    type Output = Result<R, NativeExecutionError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(mut state) = self.state.lock() else {
            return Poll::Ready(Err(NativeExecutionError::Unavailable));
        };
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            if !state
                .waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
            {
                state.waker = Some(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// Synchronous backend bound to a shared bounded native worker pool.
pub struct NativeStore<T> {
    inner: Arc<T>,
    executor: NativeExecutor,
}

impl<T> Clone for NativeStore<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            executor: self.executor.clone(),
        }
    }
}

impl<T> NativeStore<T> {
    /// Binds a synchronous backend to one shared native executor.
    #[must_use]
    pub fn new(inner: T, executor: NativeExecutor) -> Self {
        Self {
            inner: Arc::new(inner),
            executor,
        }
    }

    /// Returns the synchronous backend for explicit synchronous maintenance APIs.
    #[must_use]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub(crate) async fn execute_backend<R: Send + 'static>(
        &self,
        cancellation: &CancellationToken,
        operation: impl FnOnce(Arc<T>) -> R + Send + 'static,
    ) -> Result<R, NativeExecutionError>
    where
        T: Send + Sync + 'static,
    {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || operation(store))
            .await
    }
}

fn authority_executor_failure(
    error: NativeExecutionError,
    operation: &'static str,
    may_have_mutated: bool,
) -> AuthorityFailure {
    let error = match error {
        NativeExecutionError::Cancelled => AuthorityStoreError::Cancelled,
        NativeExecutionError::WorkerPanicked if may_have_mutated => {
            AuthorityStoreError::Indeterminate {
                operation,
                source: std::io::Error::other("native storage worker panicked"),
            }
        }
        NativeExecutionError::Unavailable | NativeExecutionError::WorkerPanicked => {
            AuthorityStoreError::Rejected(error.to_string())
        }
    };
    OperationFailure::before_work(error)
}

fn object_executor_failure(error: NativeExecutionError) -> ObjectFailure {
    let error = match error {
        NativeExecutionError::Cancelled => ObjectStoreError::Cancelled,
        NativeExecutionError::Unavailable | NativeExecutionError::WorkerPanicked => {
            ObjectStoreError::Rejected(error.to_string())
        }
    };
    OperationFailure::before_work(error)
}

impl<T: AuthorityStore + Send + Sync + 'static> AsyncAuthorityStore for NativeStore<T> {
    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.create_authority(authority_id, genesis_epoch, budget)
            })
            .await
            .map_err(|error| authority_executor_failure(error, "create", true))?
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || store.head(authority_id, budget))
            .await
            .map_err(|error| authority_executor_failure(error, "head", false))?
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.compare_and_append(authority_id, epoch, expected, commit, budget)
            })
            .await
            .map_err(|error| authority_executor_failure(error, "compare_and_append", true))?
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.replay(authority_id, after, limit, budget)
            })
            .await
            .map_err(|error| authority_executor_failure(error, "replay", false))?
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<crate::storage::FenceOutcome> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.fence(authority_id, expected, budget)
            })
            .await
            .map_err(|error| authority_executor_failure(error, "fence", true))?
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.find_operation(authority_id, operation_id, budget)
            })
            .await
            .map_err(|error| authority_executor_failure(error, "find_operation", false))?
    }
}

impl<T: ObjectStore + Send + Sync + 'static> AsyncObjectStore for NativeStore<T> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || store.put(object_id, bytes, budget))
            .await
            .map_err(object_executor_failure)?
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.read(object_id, maximum_bytes, budget)
            })
            .await
            .map_err(object_executor_failure)?
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let request_bytes = u64::try_from(requests.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(size_of::<ObjectReadRequest>()).unwrap_or(u64::MAX));
        let transport_work = WorkCounters {
            bytes_copied: request_bytes,
            allocation_operations: u64::from(!requests.is_empty()),
            peak_allocation_bytes: request_bytes,
            ..WorkCounters::default()
        };
        transport_work
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let mut requests_owned = Vec::new();
        requests_owned
            .try_reserve_exact(requests.len())
            .map_err(|_| {
                ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "native object batch transport allocation failed".to_owned(),
                ))
            })?;
        requests_owned.extend_from_slice(requests);
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                let mut remaining = transport_work
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), transport_work))?;
                remaining.peak_allocation_bytes = remaining
                    .peak_allocation_bytes
                    .checked_sub(request_bytes)
                    .ok_or_else(|| {
                        ObjectFailure::new(
                            ObjectStoreError::Work(crate::performance::WorkError::Overflow),
                            transport_work,
                        )
                    })?;
                match store.read_many(&requests_owned, remaining) {
                    Ok(receipt) => {
                        let work = combine_native_batch_work(
                            transport_work,
                            receipt.work,
                            request_bytes,
                            remaining,
                            budget,
                        )?;
                        Ok(ObjectReceipt {
                            value: receipt.value,
                            work,
                        })
                    }
                    Err(failure) => {
                        let nested = *failure.work;
                        match combine_native_batch_work(
                            transport_work,
                            nested,
                            request_bytes,
                            remaining,
                            budget,
                        ) {
                            Ok(work) => Err(ObjectFailure::new(failure.error, work)),
                            Err(work_failure) => Err(work_failure),
                        }
                    }
                }
            })
            .await
            .map_err(|error| {
                let failure = object_executor_failure(error);
                ObjectFailure::new(failure.error, transport_work)
            })?
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || store.contains(object_id, budget))
            .await
            .map_err(object_executor_failure)?
    }
}

fn combine_native_batch_work(
    transport: WorkCounters,
    nested: WorkCounters,
    request_bytes: u64,
    nested_budget: WorkBudget,
    operation_budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let mut combined = transport
        .checked_add(nested)
        .map_err(|error| ObjectFailure::new(error.into(), transport))?;
    combined.peak_allocation_bytes = request_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| {
            ObjectFailure::new(crate::performance::WorkError::Overflow.into(), transport)
        })?;
    nested
        .verify(nested_budget)
        .map_err(|error| ObjectFailure::new(error.into(), combined))?;
    combined
        .verify(operation_budget)
        .map_err(|error| ObjectFailure::new(error.into(), combined))?;
    Ok(combined)
}

impl<T: NotificationStore + Send + Sync + 'static> AsyncNotificationStore for NativeStore<T> {
    async fn publish_hint(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> NotificationResult<()> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.publish(authority_id, head, budget)
            })
            .await
            .map_err(notification_executor_failure)?
    }

    async fn poll_hint_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> NotificationResult<NotificationPoll> {
        let store = Arc::clone(&self.inner);
        self.executor
            .execute(cancellation, move || {
                store.poll_after(authority_id, after, budget)
            })
            .await
            .map_err(notification_executor_failure)?
    }
}

fn notification_executor_failure(
    error: NativeExecutionError,
) -> OperationFailure<NotificationError> {
    let error = match error {
        NativeExecutionError::Cancelled => NotificationError::Cancelled,
        NativeExecutionError::Unavailable | NativeExecutionError::WorkerPanicked => {
            NotificationError::Unavailable
        }
    };
    OperationFailure::before_work(error)
}

#[cfg(test)]
#[path = "tests/native_executor.rs"]
mod tests;
