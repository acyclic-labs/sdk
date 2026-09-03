//! Disposable authority-head notification acceleration.
//!
//! Notifications are hints only. They can suppress unnecessary authority
//! polls or wake a tracking checkout, but cannot establish authority, fill a
//! replay gap, or permit publication. Every consumer must authenticate the
//! indicated head through [`crate::AsyncAuthorityStore`].

use crate::cancellation::CancellationToken;
use crate::foundation::{AuthorityId, Head};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use thiserror::Error;

/// Result of polling a disposable notification cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotificationPoll {
    /// No newer hint is currently visible; callers may poll authority directly.
    Unchanged,
    /// A possibly newer authority head should be authenticated and replayed.
    Advanced(Head),
}

/// Notification adapter failures never alter filesystem truth.
#[derive(Debug, Error)]
pub enum NotificationError {
    /// Cooperative cancellation occurred before adapter access.
    #[error("notification operation was cancelled")]
    Cancelled,
    /// Adapter synchronization state is unavailable.
    #[error("notification adapter state is unavailable")]
    Unavailable,
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Receipt-bearing notification result.
pub type NotificationResult<T> = Result<OperationReceipt<T>, OperationFailure<NotificationError>>;

/// Optional synchronous authority-head hint adapter.
///
/// Implementations may coalesce, duplicate, delay, or lose hints. They must
/// never invent a head not supplied to [`Self::publish`], and returning
/// [`NotificationPoll::Unchanged`] must never be interpreted as proof that the
/// authority head is unchanged.
pub trait NotificationStore {
    /// Publishes a disposable monotonic hint.
    ///
    /// # Errors
    ///
    /// Returns typed adapter or work-budget failure without changing truth.
    fn publish(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
    ) -> NotificationResult<()>;

    /// Polls for a hint strictly newer than `after` in epoch/sequence order.
    ///
    /// # Errors
    ///
    /// Returns typed adapter or work-budget failure without changing truth.
    fn poll_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
    ) -> NotificationResult<NotificationPoll>;
}

/// Runtime-neutral notification adapter suitable for browser and remote APIs.
pub trait AsyncNotificationStore {
    /// Asynchronously publishes one disposable head hint.
    ///
    /// # Errors
    ///
    /// Returns cancellation, adapter, or exact work-budget failure.
    fn publish_hint(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = NotificationResult<()>>;

    /// Asynchronously polls for a possibly newer hint.
    ///
    /// # Errors
    ///
    /// Returns cancellation, adapter, or exact work-budget failure.
    fn poll_hint_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = NotificationResult<NotificationPoll>>;
}

/// Explicit opt-in for synchronous notification stores that complete inline.
pub trait ImmediateNotificationStore: NotificationStore {}

impl<T: ImmediateNotificationStore + ?Sized> AsyncNotificationStore for T {
    async fn publish_hint(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> NotificationResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(NotificationError::Cancelled))?;
        NotificationStore::publish(self, authority_id, head, budget)
    }

    async fn poll_hint_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> NotificationResult<NotificationPoll> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(NotificationError::Cancelled))?;
        NotificationStore::poll_after(self, authority_id, after, budget)
    }
}

/// Deterministic process-local coalescing notification adapter.
#[derive(Default)]
pub struct MemoryNotificationStore {
    heads: Mutex<HashMap<AuthorityId, Head>>,
}

impl MemoryNotificationStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl NotificationStore for MemoryNotificationStore {
    fn publish(
        &self,
        authority_id: AuthorityId,
        head: Head,
        budget: WorkBudget,
    ) -> NotificationResult<()> {
        let work = WorkCounters {
            backend_write_operations: 1,
            items_examined: 1,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut heads = self
            .heads
            .lock()
            .map_err(|_| OperationFailure::new(NotificationError::Unavailable, work))?;
        match heads.get(&authority_id) {
            Some(current) if !head_is_newer(head, *current) => {}
            _ => {
                heads.insert(authority_id, head);
            }
        }
        Ok(OperationReceipt { value: (), work })
    }

    fn poll_after(
        &self,
        authority_id: AuthorityId,
        after: Head,
        budget: WorkBudget,
    ) -> NotificationResult<NotificationPoll> {
        let work = WorkCounters {
            backend_read_operations: 1,
            items_examined: 1,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let heads = self
            .heads
            .lock()
            .map_err(|_| OperationFailure::new(NotificationError::Unavailable, work))?;
        let value = heads
            .get(&authority_id)
            .copied()
            .filter(|head| head_is_newer(*head, after))
            .map_or(NotificationPoll::Unchanged, NotificationPoll::Advanced);
        Ok(OperationReceipt { value, work })
    }
}

impl ImmediateNotificationStore for MemoryNotificationStore {}

fn head_is_newer(candidate: Head, current: Head) -> bool {
    candidate.epoch > current.epoch
        || (candidate.epoch == current.epoch && candidate.sequence > current.sequence)
}

#[cfg(test)]
#[path = "tests/notification.rs"]
mod tests;
