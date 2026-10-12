//! Live-only bounded execution for arbitrary Rust futures.

use crate::{Admission, NativeWork, OperationId, Outcome};
use acyclic_stream::BoxProviderStream as BoxStream;
use futures::{StreamExt, stream};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Semaphore, oneshot};
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::{AbortHandle, JoinHandle};
#[cfg(target_arch = "wasm32")]
use futures::future::{AbortHandle, Abortable};

mod timer;
pub(crate) use timer::{sleep_millis, timeout_until};

#[cfg(all(test, target_arch = "wasm32"))]
mod browser_tests;

#[cfg(not(target_arch = "wasm32"))]
type LiveJoin<T> = JoinHandle<Outcome<T>>;

#[cfg(target_arch = "wasm32")]
struct LiveJoin<T> {
    outcome: oneshot::Receiver<Outcome<T>>,
    abort: AbortHandle,
}
#[cfg(target_arch = "wasm32")]
impl<T> LiveJoin<T> {
    fn abort(&self) { self.abort.abort(); }
    fn abort_handle(&self) -> AbortHandle { self.abort.clone() }
}

fn spawn_live<T, F>(task: F) -> LiveJoin<T>
where
    T: Send + 'static,
    F: Future<Output = Outcome<T>> + acyclic_stream::ProviderTask + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    { tokio::spawn(task) }
    #[cfg(target_arch = "wasm32")]
    {
        let (abort, registration) = AbortHandle::new_pair();
        let (sender, outcome) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = Abortable::new(task, registration).await.unwrap_or(Outcome::Cancelled);
            let _ = sender.send(result);
        });
        LiveJoin { outcome, abort }
    }
}

struct ActiveGuard {
    id: OperationId,
    group: Arc<GroupState>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.group
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&self.id);
    }
}

struct GroupAdmission {
    closed: bool,
    active: BTreeMap<OperationId, AbortHandle>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct GroupChildCustody {
    _previous: Option<Box<GroupChildCustody>>,
    allocation: acyclic_fs::NativeAllocation,
}

struct GroupState {
    semaphore: Arc<Semaphore>,
    admission: Mutex<GroupAdmission>,
    children: Mutex<Vec<Weak<GroupState>>>,
    _parent: Option<Arc<GroupState>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    children_custody: Mutex<Option<Box<GroupChildCustody>>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Controls admission and lifetime for related live tasks.
#[derive(Clone)]
pub struct TaskGroup {
    state: Arc<GroupState>,
}

impl TaskGroup {
    /// Creates a task group with bounded concurrency.
    pub fn new(max_concurrency: usize, work: NativeWork<'_>) -> crate::Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = if let Some(work) = work {
            let bytes = (std::mem::size_of::<GroupState>() + std::mem::size_of::<Semaphore>()) as u64;
            let custody = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: 2, ..Default::default()
            }).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
            Some(custody)
        } else { None };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        Ok(Self {
            state: Arc::new(GroupState {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                admission: Mutex::new(GroupAdmission { closed: false, active: BTreeMap::new() }),
                children: Mutex::new(Vec::new()),
                _parent: None,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                children_custody: Mutex::new(None),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                _native_custody: custody,
            }),
        })
    }

    /// Creates an independently bounded child retained in this cancellation tree.
    pub fn child(
        &self, max_concurrency: usize, work: NativeWork<'_>,
    ) -> crate::Result<Self> {
        let mut child = Self::new(max_concurrency, work)?;
        Arc::get_mut(&mut child.state).expect("new unattached task group")._parent =
            Some(Arc::clone(&self.state));
        let admission = self
            .state
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut children = self.state.children.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            if children.len() == children.capacity() {
                let old = children.capacity();
                let target = old.checked_mul(2).unwrap_or(usize::MAX).max(4);
                let bytes = target.checked_sub(old)
                    .and_then(|slots| slots.checked_mul(std::mem::size_of::<Weak<GroupState>>()))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<GroupChildCustody>()))
                    .ok_or(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
                        custody: None,
                    })? as u64;
                let custody = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
                work.charge(acyclic_fs::WorkCounters {
                    allocation_operations: 1, ..Default::default()
                }).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
                let mut retained = self.state.children_custody.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let previous = retained.take();
                *retained = Some(Box::new(GroupChildCustody { _previous: previous, allocation: custody }));
                let growth = work.charge(acyclic_fs::WorkCounters {
                    allocation_operations: 1, ..Default::default()
                }).map_err(|cause| crate::Error::NativeWork { cause, custody: None })
                    .and_then(|()| {
                        let extra = target - children.len();
                        children.try_reserve_exact(extra).map_err(|cause| crate::Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
                            custody: None,
                        })
                    });
                if let Err(error) = growth {
                    let mut unused = retained.take().expect("installed child-vector custody");
                    *retained = unused._previous.take();
                    drop(unused);
                    return Err(error);
                }
                let actual = ((children.capacity() - old) * std::mem::size_of::<Weak<GroupState>>()
                    + std::mem::size_of::<GroupChildCustody>()) as u64;
                retained.as_mut().expect("installed child-vector custody").allocation
                    .reconcile_capacity(actual)
                    .map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
            }
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        children.push(Arc::downgrade(&child.state));
        drop(children);
        let closed = admission.closed;
        drop(admission);
        if closed {
            child.state.admission.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).closed = true;
        }
        Ok(child)
    }

    /// Admits one live operation under its caller-retained identity.
    pub async fn spawn<T, F>(
        &self,
        id: OperationId,
        future: F,
    ) -> Admission<TaskHandle<T>>
    where
        T: Send + 'static,
        F: Future<Output = T> + acyclic_stream::ProviderTask + 'static,
    {
        let mut admission = self
            .state
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.closed || admission.active.contains_key(&id) {
            return Admission::Rejected {
                reason: if admission.closed {
                    "task group is closed"
                } else {
                    "operation is already active"
                }
                .into(),
            };
        }
        let semaphore = Arc::clone(&self.state.semaphore);
        let guard = ActiveGuard {
            id,
            group: Arc::clone(&self.state),
        };
        let (start, admitted) = oneshot::channel();
        let task = async move {
            let _guard = guard;
            let _ = admitted.await;
            match semaphore.acquire_owned().await {
                Ok(_permit) => Outcome::Succeeded(future.await),
                Err(_) => Outcome::Failed {
                    message: "task group closed".into(),
                },
            }
        };
        let join = spawn_live(task);
        admission.active.insert(id, join.abort_handle());
        drop(admission);
        let _ = start.send(());
        Admission::Accepted(TaskHandle {
            id,
            join,
        })
    }

    /// Closes admission and requests cancellation of this group and descendants.
    pub fn cancel(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            let active = {
                let mut admission = group
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                admission.closed = true;
                std::mem::take(&mut admission.active)
            };
            for handle in active.values() {
                handle.abort();
            }
            let children = {
                let mut retained = group
                    .children
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let children = retained
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>();
                retained.retain(|child| child.strong_count() > 0);
                children
            };
            pending.extend(children);
        }
    }

    /// Closes admission throughout the group tree while accepted work drains.
    pub fn close(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            group
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed = true;
            let mut retained = group
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.extend(retained.iter().filter_map(Weak::upgrade));
            retained.retain(|child| child.strong_count() > 0);
        }
    }

}

/// Addressable handle for an admitted live task.
pub struct TaskHandle<T> {
    id: OperationId,
    join: LiveJoin<T>,
}

impl<T> TaskHandle<T> {
    /// Returns the operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        self.join.abort();
    }

    /// Waits for a terminal outcome, cancelling this task if the wait is dropped.
    pub fn result(self) -> impl Future<Output = Outcome<T>> {
        struct CancelOnDrop(Option<AbortHandle>);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                if let Some(handle) = &self.0 {
                    handle.abort();
                }
            }
        }
        let guard = CancelOnDrop(Some(self.join.abort_handle()));
        async move {
            let mut guard = guard;
            #[cfg(not(target_arch = "wasm32"))]
            let outcome = match self.join.await {
                Ok(outcome) => outcome,
                Err(error) if error.is_cancelled() => Outcome::Cancelled,
                Err(error) => Outcome::Failed { message: error.to_string() },
            };
            #[cfg(target_arch = "wasm32")]
            let outcome = match self.join.outcome.await {
                Ok(outcome) => outcome,
                Err(error) => Outcome::Failed { message: error.to_string() },
            };
            guard.0 = None;
            outcome
        }
    }
}

/// Collects outcomes in input order.
pub async fn join_all<T>(handles: Vec<TaskHandle<T>>) -> Vec<Outcome<T>> {
    stream::iter(handles)
        .then(TaskHandle::result)
        .collect()
        .await
}

/// Streams outcomes in completion order.
pub fn completion_stream<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
) -> BoxStream<'static, (OperationId, Outcome<T>)> {
    let concurrency = handles.len().max(1);
    stream::iter(handles)
        .map(|handle| async move {
            let id = handle.id;
            (id, handle.result().await)
        })
        .buffer_unordered(concurrency)
        .boxed()
}

/// Folds terminal outcomes in admission order regardless of completion order.
pub async fn ordered_reduce<T, A, F>(handles: Vec<TaskHandle<T>>, initial: A, reducer: F) -> A
where
    F: FnMut(A, Outcome<T>) -> A,
{
    join_all(handles).await.into_iter().fold(initial, reducer)
}

/// Returns the first success or all failures.
pub async fn first_success<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
) -> std::result::Result<T, Vec<Outcome<T>>> {
    let mut completions = completion_stream(handles);
    let mut failures = Vec::new();
    while let Some((_id, outcome)) = completions.next().await {
        match outcome {
            Outcome::Succeeded(value) => return Ok(value),
            outcome => failures.push(outcome),
        }
    }
    Err(failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MarkDropped(Arc<AtomicBool>);
    impl Drop for MarkDropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn grandchildren_retain_the_original_cancellation_parent() {
        let root = TaskGroup::new(2, None).unwrap();
        let parent = root.child(2, None).unwrap();
        let grandchild = parent.child(1, None).unwrap();
        drop(parent);
        root.cancel();
        assert!(matches!(grandchild.spawn(OperationId::new(), async { 1_u64 }).await,
            Admission::Rejected { .. }));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_groups_pay_before_allocation_and_keep_parent_vector_custody() -> crate::Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            allocation_operations: 64, peak_allocation_bytes: 65_536,
            ..Default::default()
        };
        let denied = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget { peak_allocation_bytes: 0, ..budget }, &cancellation,
        ).map_err(|failure| crate::Error::NativeWork { cause: failure.error, custody: None })?;
        let root = TaskGroup::new(2, None).unwrap();
        assert!(root.child(1, Some(&denied)).is_err());
        assert_eq!(denied.receipt().allocation_operations, 0);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|failure| crate::Error::NativeWork { cause: failure.error, custody: None })?;
        let parent = root.child(2, Some(&work))?;
        let grandchild = parent.child(1, Some(&work))?;
        drop(parent);
        root.cancel();
        assert!(matches!(grandchild.spawn(OperationId::new(), async { 1_u64 }).await,
            Admission::Rejected { .. }));
        drop(grandchild);
        assert!(work.live_allocation_bytes() > 0);
        drop(root);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn named_admission_rejects_active_duplicates_and_allows_settled_retry() {
        let group = TaskGroup::new(2, None).unwrap();
        let operation = OperationId::new();
        let Admission::Accepted(handle) = group
            .spawn(operation, std::future::pending::<u64>())
            .await
        else {
            panic!("first admission rejected")
        };
        assert_eq!(*handle.id(), operation);
        assert!(matches!(
            group
                .spawn(operation, async { 99_u64 })
                .await,
            Admission::Rejected { .. }
        ));
        handle.cancel();
        assert_eq!(handle.result().await, Outcome::Cancelled);
        let Admission::Accepted(retried) = group
            .spawn(operation, async { 7_u64 })
            .await
        else {
            panic!("settled retry rejected")
        };
        assert_eq!(*retried.id(), operation);
        assert_eq!(retried.result().await, Outcome::Succeeded(7));
        group.close();
        assert!(matches!(
            group
                .spawn(operation, async { 9_u64 })
                .await,
            Admission::Rejected { .. }
        ));
    }

    #[tokio::test]
    async fn dropping_an_owned_wait_cancels_only_its_task() -> Result<(), Box<dyn std::error::Error>>
    {
        let group = TaskGroup::new(2, None).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&dropped);
        let (started, observed) = tokio::sync::oneshot::channel();
        let Admission::Accepted(handle) = group
            .spawn(OperationId::new(), async move {
                let _guard = MarkDropped(flag);
                let _ = started.send(());
                std::future::pending::<u64>().await
            })
            .await else { panic!("task rejected"); };
        observed.await?;
        let Admission::Accepted(other) = group.spawn(OperationId::new(), std::future::pending::<u64>()).await
        else { panic!("task rejected"); };
        // Cancellation must also work when the returned wait was never polled.
        drop(handle.result());
        for _ in 0..16 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::SeqCst));
        let abort_other = other.join.abort_handle();
        let other_result = other.result();
        futures::pin_mut!(other_result);
        assert!(futures::poll!(&mut other_result).is_pending());
        abort_other.abort();
        assert_eq!(other_result.await, Outcome::Cancelled);
        let Admission::Accepted(next) = group.spawn(OperationId::new(), async { 11_u64 }).await
        else { panic!("task rejected"); };
        assert_eq!(
            next.result().await,
            Outcome::Succeeded(11)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_is_explicit() {
        let Admission::Accepted(handle) = TaskGroup::new(1, None).unwrap()
            .spawn(OperationId::new(), std::future::pending::<u64>()).await
        else { panic!("task rejected"); };
        handle.cancel();
        assert_eq!(handle.result().await, Outcome::Cancelled);
    }

    #[tokio::test]
    async fn first_success_observes_without_implicitly_cancelling_losers()
    -> Result<(), Box<dyn std::error::Error>> {
        let dropped = Arc::new(AtomicBool::new(false));
        let (started, observed) = tokio::sync::oneshot::channel();
        let flag = Arc::clone(&dropped);
        let group = TaskGroup::new(2, None).unwrap();
        let Admission::Accepted(loser) = group
            .spawn(OperationId::new(), async move {
                let _guard = MarkDropped(flag);
                let _ = started.send(());
                std::future::pending::<u64>().await
            })
            .await else { panic!("task rejected"); };
        observed.await?;
        let Admission::Accepted(winner) = group.spawn(OperationId::new(), async { 7_u64 }).await
        else { panic!("task rejected"); };
        assert_eq!(first_success(vec![loser, winner]).await, Ok(7));
        assert!(!dropped.load(Ordering::SeqCst));
        group.cancel();
        for _ in 0..16 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn ordered_reduction_uses_admission_order() -> Result<(), Box<dyn std::error::Error>> {
        let group = TaskGroup::new(2, None).unwrap();
        let (release, wait) = tokio::sync::oneshot::channel();
        let Admission::Accepted(first) = group
            .spawn(OperationId::new(), async move {
                let _ = wait.await;
                1_u64
            })
            .await else { panic!("task rejected"); };
        let Admission::Accepted(second) = group.spawn(OperationId::new(), async { 2_u64 }).await
        else { panic!("task rejected"); };
        release.send(()).map_err(|_| "first task stopped waiting")?;
        let values = ordered_reduce(vec![first, second], Vec::new(), |mut values, outcome| {
            values.push(outcome);
            values
        })
        .await;
        assert_eq!(values, vec![Outcome::Succeeded(1), Outcome::Succeeded(2)]);
        Ok(())
    }
}
