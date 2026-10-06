//! Browser-local bounded live execution.
//!
//! Browser futures are intentionally local: JavaScript promises and DOM
//! callbacks are not `Send`, and wasm has one executor.  This module mirrors
//! the native live-task API while retaining cancellation and group admission
//! semantics through `AbortHandle`.

use crate::{Admission, OperationId, Outcome};
use futures::{
    Future, StreamExt,
    future::{AbortHandle, Abortable},
    stream::{self, LocalBoxStream},
};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::{Rc, Weak},
};
use wasm_bindgen_futures::spawn_local;

struct GroupState {
    closed: bool,
    active: BTreeMap<OperationId, AbortHandle>,
    children: Vec<Weak<RefCell<GroupState>>>,
}

/// Controls admission and lifetime for related browser-local tasks.
#[derive(Clone)]
pub struct TaskGroup {
    state: Rc<RefCell<GroupState>>,
}

impl TaskGroup {
    /// Creates a task group.  Browser execution is cooperative; the capacity
    /// argument is retained for API parity and future scheduler integration.
    #[must_use]
    pub fn new(_max_concurrency: usize) -> Self {
        Self {
            state: Rc::new(RefCell::new(GroupState {
                closed: false,
                active: BTreeMap::new(),
                children: Vec::new(),
            })),
        }
    }

    /// Creates a child retained in this cancellation tree.
    #[must_use]
    pub fn child(&self, max_concurrency: usize) -> Self {
        let child = Self::new(max_concurrency);
        let closed = self.state.borrow().closed;
        self.state
            .borrow_mut()
            .children
            .push(Rc::downgrade(&child.state));
        if closed {
            child.cancel();
        }
        child
    }

    /// Admits one browser-local live task.
    pub async fn spawn<T, F>(&self, future: F) -> TaskHandle<T>
    where
        T: 'static,
        F: Future<Output = T> + 'static,
    {
        match self.try_spawn(future).await {
            Admission::Accepted(handle) => handle,
            Admission::Rejected { reason } => TaskHandle::failed(OperationId::new(), reason),
            Admission::Indeterminate { operation_id } => TaskHandle::failed(
                operation_id,
                "task admission became indeterminate".into(),
            ),
        }
    }

    /// Returns an explicit rejection when cancellation has closed admission.
    pub async fn try_spawn<T, F>(&self, future: F) -> Admission<TaskHandle<T>>
    where
        T: 'static,
        F: Future<Output = T> + 'static,
    {
        let id = OperationId::new();
        if self.state.borrow().closed {
            return Admission::Rejected {
                reason: "task group is closed".into(),
            };
        }
        let (sender, receiver) = futures::channel::oneshot::channel();
        let (abort, registration) = AbortHandle::new_pair();
        self.state.borrow_mut().active.insert(id, abort.clone());
        let state = Rc::clone(&self.state);
        spawn_local(async move {
            let outcome = match Abortable::new(future, registration).await {
                Ok(value) => Outcome::Succeeded(value),
                Err(_) => Outcome::Cancelled,
            };
            let _ = sender.send(outcome);
            state.borrow_mut().active.remove(&id);
        });
        Admission::Accepted(TaskHandle {
            id,
            receiver: Some(receiver),
            abort: Some(abort),
        })
    }

    /// Closes admission and requests cancellation of this group and children.
    pub fn cancel(&self) {
        let (active, children) = {
            let mut state = self.state.borrow_mut();
            state.closed = true;
            (
                std::mem::take(&mut state.active),
                state.children.iter().filter_map(Weak::upgrade).collect::<Vec<_>>(),
            )
        };
        for abort in active.values() {
            abort.abort();
        }
        for child in children {
            Self { state: child }.cancel();
        }
    }

    /// Closes admission while accepted work drains.
    pub fn close(&self) {
        self.state.borrow_mut().closed = true;
        let children = self
            .state
            .borrow()
            .children
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for child in children {
            Self { state: child }.close();
        }
    }

    /// Admits many independent tasks.
    pub async fn spawn_many<T, F, I>(&self, futures: I) -> Vec<TaskHandle<T>>
    where
        T: 'static,
        F: Future<Output = T> + 'static,
        I: IntoIterator<Item = F>,
    {
        stream::iter(futures)
            .then(|future| self.spawn(future))
            .collect()
            .await
    }

    /// Preserves one admission result per input.
    pub async fn admit_many<T, F, I>(&self, futures: I) -> Vec<Admission<TaskHandle<T>>>
    where
        T: 'static,
        F: Future<Output = T> + 'static,
        I: IntoIterator<Item = F>,
    {
        stream::iter(futures)
            .then(|future| self.try_spawn(future))
            .collect()
            .await
    }
}

/// Addressable handle for a browser-local live task.
pub struct TaskHandle<T> {
    id: OperationId,
    receiver: Option<futures::channel::oneshot::Receiver<Outcome<T>>>,
    abort: Option<AbortHandle>,
}

impl<T> TaskHandle<T> {
    fn failed(id: OperationId, message: String) -> Self {
        let (sender, receiver) = futures::channel::oneshot::channel();
        let _ = sender.send(Outcome::Failed { message });
        Self {
            id,
            receiver: Some(receiver),
            abort: None,
        }
    }

    /// Returns the operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }

    /// Waits for a terminal outcome.
    pub async fn result(mut self) -> Outcome<T> {
        match self.receiver.take() {
            Some(receiver) => receiver.await.unwrap_or(Outcome::Cancelled),
            None => Outcome::Failed {
                message: "task handle has no result".into(),
            },
        }
    }
}

/// Collects outcomes in input order.
pub async fn join_all<T: 'static>(handles: Vec<TaskHandle<T>>) -> Vec<Outcome<T>> {
    stream::iter(handles)
        .then(TaskHandle::result)
        .collect()
        .await
}

/// Streams outcomes in completion order.
pub fn completion_stream<T: 'static>(
    handles: Vec<TaskHandle<T>>,
) -> LocalBoxStream<'static, (OperationId, Outcome<T>)> {
    let concurrency = handles.len().max(1);
    stream::iter(handles)
        .map(|handle| async move {
            let id = handle.id.clone();
            (id, handle.result().await)
        })
        .buffer_unordered(concurrency)
        .boxed_local()
}

/// Returns the first terminal task.
pub async fn race<T: 'static>(
    handles: Vec<TaskHandle<T>>,
) -> Option<(OperationId, Outcome<T>)> {
    completion_stream(handles).next().await
}

/// Folds terminal outcomes in admission order.
pub async fn ordered_reduce<T: 'static, A, F>(
    handles: Vec<TaskHandle<T>>,
    initial: A,
    reducer: F,
) -> A
where
    F: FnMut(A, Outcome<T>) -> A,
{
    join_all(handles).await.into_iter().fold(initial, reducer)
}

/// Returns the first success or all failures.
pub async fn first_success<T: 'static>(
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

/// Collects the first required successes.
pub async fn quorum<T: 'static>(
    handles: Vec<TaskHandle<T>>,
    required: usize,
) -> std::result::Result<Vec<T>, Vec<Outcome<T>>> {
    if required == 0 {
        return Ok(Vec::new());
    }
    let mut completions = completion_stream(handles);
    let mut successes = Vec::with_capacity(required);
    let mut failures = Vec::new();
    while let Some((_id, outcome)) = completions.next().await {
        match outcome {
            Outcome::Succeeded(value) => {
                successes.push(value);
                if successes.len() == required {
                    return Ok(successes);
                }
            }
            outcome => failures.push(outcome),
        }
    }
    Err(failures)
}

/// Recursively sums input with balanced live fork-join decomposition.
pub async fn recursive_sum(group: TaskGroup, values: Vec<u64>, leaf_size: usize) -> Outcome<u64> {
    if values.len() <= leaf_size.max(1) {
        return group
            .spawn(async move { values.into_iter().sum() })
            .await
            .result()
            .await;
    }
    let midpoint = values.len() / 2;
    let (left, right) = values.split_at(midpoint);
    let (left, right) = (left.to_vec(), right.to_vec());
    let (left_result, right_result) = futures::join!(
        recursive_sum_boxed(group.clone(), left, leaf_size),
        recursive_sum_boxed(group, right, leaf_size),
    );
    match (left_result, right_result) {
        (Outcome::Succeeded(left), Outcome::Succeeded(right)) => Outcome::Succeeded(left + right),
        (Outcome::Failed { message }, _) | (_, Outcome::Failed { message }) => {
            Outcome::Failed { message }
        }
        (Outcome::Indeterminate { operation_id }, _)
        | (_, Outcome::Indeterminate { operation_id }) => Outcome::Indeterminate { operation_id },
        _ => Outcome::Cancelled,
    }
}

fn recursive_sum_boxed(
    group: TaskGroup,
    values: Vec<u64>,
    leaf_size: usize,
) -> futures::future::LocalBoxFuture<'static, Outcome<u64>> {
    Box::pin(recursive_sum(group, values, leaf_size))
}
