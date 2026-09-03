use super::*;
use crate::foundation::Digest;
use crate::local_authority::{LocalAuthorityConfig, LocalAuthorityStore};
use crate::memory::MemoryObjectStore;
use crate::performance::WorkError;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::task::{Wake, Waker};
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn executor_configuration_rejects_every_nonprogressing_shape() {
    assert!(matches!(
        NativeExecutor::new(NativeExecutorConfig {
            worker_threads: 0,
            maximum_in_flight_operations: 1,
        }),
        Err(NativeExecutorConfigError::NoWorkers)
    ));
    assert!(matches!(
        NativeExecutor::new(NativeExecutorConfig {
            worker_threads: 2,
            maximum_in_flight_operations: 1,
        }),
        Err(NativeExecutorConfigError::InFlightBelowWorkers)
    ));
    assert!(matches!(
        NativeExecutor::new(NativeExecutorConfig {
            worker_threads: MAXIMUM_WORKER_THREADS + 1,
            maximum_in_flight_operations: MAXIMUM_WORKER_THREADS + 1,
        }),
        Err(NativeExecutorConfigError::TooManyWorkers)
    ));
    assert!(matches!(
        NativeExecutor::new(NativeExecutorConfig {
            worker_threads: 1,
            maximum_in_flight_operations: MAXIMUM_IN_FLIGHT_OPERATIONS + 1,
        }),
        Err(NativeExecutorConfigError::InFlightTooLarge)
    ));
}

struct PanicWake;

#[allow(clippy::panic)]
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any("completion wake panic");
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
#[allow(clippy::panic)]
fn completion_releases_its_lock_and_contains_a_panicking_waker() {
    let (completion, mut call) = BlockingCall::new();
    let panic_waker = Waker::from(Arc::new(PanicWake));
    let mut panic_context = Context::from_waker(&panic_waker);
    assert_eq!(Pin::new(&mut call).poll(&mut panic_context), Poll::Pending);
    completion.complete(Ok(41_u8));

    let regular_waker = Waker::from(Arc::new(NoopWake));
    let mut regular_context = Context::from_waker(&regular_waker);
    assert_eq!(
        Pin::new(&mut call).poll(&mut regular_context),
        Poll::Ready(Ok(41))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_work_never_runs_on_the_callers_async_thread()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = NativeExecutor::new(NativeExecutorConfig {
        worker_threads: 1,
        maximum_in_flight_operations: 1,
    })?;
    let caller = thread::current().id();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let task_executor = executor.clone();
    let task = tokio::spawn(async move {
        task_executor
            .execute(&CancellationToken::new(), move || {
                let _ = started_sender.send(());
                let _ = release_receiver.recv();
                thread::current().id()
            })
            .await
    });
    tokio::task::yield_now().await;
    started_receiver.recv_timeout(Duration::from_secs(1))?;
    tokio::task::yield_now().await;
    release_sender.send(())?;
    let worker = task.await??;
    assert_ne!(caller, worker);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::panic)]
async fn queued_cancellation_is_inert_and_worker_panics_are_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = NativeExecutor::new(NativeExecutorConfig {
        worker_threads: 1,
        maximum_in_flight_operations: 1,
    })?;
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let first_executor = executor.clone();
    let first = tokio::spawn(async move {
        first_executor
            .execute(&CancellationToken::new(), move || {
                let _ = started_sender.send(());
                let _ = release_receiver.recv();
            })
            .await
    });
    tokio::task::yield_now().await;
    started_receiver.recv_timeout(Duration::from_secs(1))?;

    let ran = Arc::new(AtomicBool::new(false));
    let queued_ran = Arc::clone(&ran);
    let cancellation = CancellationToken::new();
    let queued_cancellation = cancellation.clone();
    let queued_executor = executor.clone();
    let queued = tokio::spawn(async move {
        queued_executor
            .execute(&queued_cancellation, move || {
                queued_ran.store(true, Ordering::Release);
            })
            .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    assert!(matches!(
        queued.await?,
        Err(NativeExecutionError::Cancelled)
    ));
    assert!(!ran.load(Ordering::Acquire));
    release_sender.send(())?;
    first.await??;

    let panic = executor
        .execute(&CancellationToken::new(), || -> () {
            std::panic::panic_any("native executor test panic");
        })
        .await;
    assert!(matches!(panic, Err(NativeExecutionError::WorkerPanicked)));
    assert_eq!(
        executor
            .execute(&CancellationToken::new(), || 43_u8)
            .await?,
        43
    );
    Ok(())
}

#[derive(Clone)]
struct PeakProbeStore {
    observed_budget: Arc<Mutex<Option<WorkBudget>>>,
    returned_peak: u64,
    fail: bool,
}

impl ObjectStore for PeakProbeStore {
    fn put(&self, _object_id: ObjectId, _bytes: Bytes, _budget: WorkBudget) -> ObjectResult<()> {
        Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
            "unused probe operation".to_owned(),
        )))
    }

    fn read(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
            "unused probe operation".to_owned(),
        )))
    }

    fn read_many(
        &self,
        _requests: &[ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>> {
        *self
            .observed_budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(budget);
        let work = WorkCounters {
            peak_allocation_bytes: self.returned_peak,
            ..WorkCounters::default()
        };
        if self.fail {
            Err(ObjectFailure::new(
                ObjectStoreError::Rejected("backend probe failure".to_owned()),
                work,
            ))
        } else {
            Ok(ObjectReceipt {
                value: Vec::new(),
                work,
            })
        }
    }

    fn contains(&self, _object_id: ObjectId, _budget: WorkBudget) -> ObjectResult<bool> {
        Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
            "unused probe operation".to_owned(),
        )))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_batch_reserves_live_transport_memory_from_backend_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let request = ObjectReadRequest {
        object_id: ObjectId {
            kind: crate::storage::ObjectKind::Blob,
            digest: Digest::from_bytes([72; 32]),
        },
        maximum_bytes: 1,
    };
    let request_bytes = u64::try_from(size_of::<ObjectReadRequest>())?;
    for fail in [false, true] {
        let observed_budget = Arc::new(Mutex::new(None));
        let store = NativeStore::new(
            PeakProbeStore {
                observed_budget: Arc::clone(&observed_budget),
                returned_peak: 8,
                fail,
            },
            NativeExecutor::new(NativeExecutorConfig {
                worker_threads: 1,
                maximum_in_flight_operations: 1,
            })?,
        );
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = request_bytes + 7;
        let failure = AsyncObjectStore::read_many(
            &store,
            std::slice::from_ref(&request),
            budget,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized nested peak unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                ..
            })
        ));
        assert_eq!(
            observed_budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .ok_or("backend did not observe its budget")?
                .peak_allocation_bytes,
            7
        );
        assert_eq!(failure.work.peak_allocation_bytes, request_bytes + 8);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn native_batch_transport_rejects_before_copy_or_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = NativeExecutor::new(NativeExecutorConfig {
        worker_threads: 1,
        maximum_in_flight_operations: 1,
    })?;
    let store = NativeStore::new(MemoryObjectStore::default(), executor);
    let requests = vec![
        ObjectReadRequest {
            object_id: ObjectId {
                kind: crate::storage::ObjectKind::Blob,
                digest: Digest::from_bytes([71; 32]),
            },
            maximum_bytes: 1,
        };
        1_024
    ];

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = AsyncObjectStore::read_many(&store, &requests, WorkBudget::UNBOUNDED, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut zero_copy = WorkBudget::UNBOUNDED;
    zero_copy.bytes_copied = 0;
    let failure =
        AsyncObjectStore::read_many(&store, &requests, zero_copy, &CancellationToken::new())
            .await
            .err()
            .ok_or("unadmitted batch transport unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_durable_call_is_idempotently_recoverable() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let authority = Arc::new(LocalAuthorityStore::open(
        directory.path(),
        LocalAuthorityConfig::default(),
    )?);
    let authority_id = AuthorityId::from_bytes([81; 16]);
    let created = AuthorityStore::create_authority(
        &*authority,
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    let CreateAuthorityOutcome::Created(genesis) = created.value else {
        return Err("authority unexpectedly existed".into());
    };
    let operation_id = OperationId::from_bytes([82; 16]);
    let commit = ProposedCommit {
        operation_id,
        fingerprint: Digest::from_bytes([83; 32]),
        payload: Bytes::from_static(b"recoverable-after-drop"),
    };
    let executor = NativeExecutor::new(NativeExecutorConfig {
        worker_threads: 1,
        maximum_in_flight_operations: 1,
    })?;
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let (finished_sender, finished_receiver) = mpsc::channel();
    let worker_authority = Arc::clone(&authority);
    let worker_commit = commit.clone();
    let worker_executor = executor.clone();
    let caller = tokio::spawn(async move {
        worker_executor
            .execute(&CancellationToken::new(), move || {
                let _ = started_sender.send(());
                let _ = release_receiver.recv();
                let result = AuthorityStore::compare_and_append(
                    &*worker_authority,
                    authority_id,
                    Epoch::GENESIS,
                    genesis,
                    worker_commit,
                    WorkBudget::UNBOUNDED,
                );
                let _ = finished_sender.send(());
                result
            })
            .await
    });
    tokio::task::yield_now().await;
    started_receiver.recv_timeout(Duration::from_secs(1))?;
    caller.abort();
    let cancellation = caller
        .await
        .err()
        .ok_or("aborted durable caller unexpectedly returned")?;
    assert!(cancellation.is_cancelled());
    release_sender.send(())?;
    finished_receiver.recv_timeout(Duration::from_secs(1))?;

    drop(authority);
    let authority = LocalAuthorityStore::open(directory.path(), LocalAuthorityConfig::default())?;

    let durable = AuthorityStore::find_operation(
        &authority,
        authority_id,
        operation_id,
        WorkBudget::UNBOUNDED,
    )?;
    assert!(durable.value.is_some());
    let retry = AuthorityStore::compare_and_append(
        &authority,
        authority_id,
        Epoch::GENESIS,
        genesis,
        commit,
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(retry.value, AppendOutcome::AlreadyCommitted(_)));
    let distinct = AuthorityStore::compare_and_append(
        &authority,
        authority_id,
        Epoch::GENESIS,
        genesis,
        ProposedCommit {
            operation_id: OperationId::from_bytes([84; 16]),
            fingerprint: Digest::from_bytes([85; 32]),
            payload: Bytes::from_static(b"must-not-duplicate"),
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(distinct.value, AppendOutcome::Conflict { .. }));
    Ok(())
}
