//! Child task views, indexed apart from any checkout guard.
//!
//! The index lock is synchronous and never held across an await. Each task has
//! its own construction gate, so a child's first call waits only for that
//! child's construction or retirement, never for its parent's running command.
//! Settled children are retired under the same gate, so a view is constructed
//! again only after the previous one is physically retired. A view whose
//! retirement failed is never leased again; its retirement is retried, from
//! whatever state it reached, before that child's next view is constructed.
use super::*;
use crate::sync::SyncMutex;
use crate::sync::owned::lock_owned;
use std::collections::BTreeMap;

/// Views under construction or constructed and not yet physically retired,
/// across this process, so the scale test can report the task-view mounts
/// live at its peak.
#[cfg(test)]
static LIVE_IN_PROCESS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Child views of every owner in this process that are not yet retired.
#[cfg(test)]
pub(in crate::local) fn live_in_process() -> usize {
    LIVE_IN_PROCESS.load(std::sync::atomic::Ordering::Relaxed)
}

/// A view's construction, counted live until it completes or is abandoned.
#[cfg(test)]
struct Constructing;

#[cfg(test)]
impl Constructing {
    fn start() -> Self {
        LIVE_IN_PROCESS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}

#[cfg(test)]
impl Drop for Constructing {
    fn drop(&mut self) {
        LIVE_IN_PROCESS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(in crate::local) struct ChildViews<T> {
    index: SyncMutex<Index<T>>,
}

struct Index<T> {
    entries: BTreeMap<TaskId, Entry<T>>,
    // Views ever constructed, so tests see that children really had views.
    #[cfg(test)]
    constructed: usize,
    // Children that settled while a call still held their view.
    deferred: Vec<TaskId>,
}

struct Entry<T> {
    gate: StateArc<AsyncMutex<()>>,
    view: Option<Arc<T>>,
    // A view whose retirement failed: never leased, and retried before a
    // replacement is constructed and at each settlement.
    failed: Option<Arc<T>>,
    users: usize,
    // Settled while in use: retire once the last call releases it.
    retired: bool,
    // Retirement holds the gate with the view taken; keep the entry so every
    // later caller waits on this same gate.
    retiring: bool,
}

impl<T> Default for ChildViews<T> {
    fn default() -> Self {
        Self {
            index: SyncMutex::new(Index {
                entries: BTreeMap::new(),
                #[cfg(test)]
                constructed: 0,
                deferred: Vec::new(),
            }),
        }
    }
}

/// One call's use of a child view. The view is retired only after every
/// lease taken before its child settled is released.
pub(in crate::local) struct ChildLease<T> {
    _claim: Claim<T>,
    view: Arc<T>,
}

impl<T> std::ops::Deref for ChildLease<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.view
    }
}

struct Claim<T> {
    views: StateArc<ChildViews<T>>,
    task: TaskId,
}

impl<T> Drop for Claim<T> {
    fn drop(&mut self) {
        let mut index = self.views.lock();
        let Index {
            entries, deferred, ..
        } = &mut *index;
        // A claim keeps its entry in the index until this release.
        let Some(entry) = entries.get_mut(&self.task) else {
            return;
        };
        entry.users -= 1;
        if entry.users == 0 && !entry.retiring {
            if entry.failed.is_some() || (entry.retired && entry.view.is_some()) {
                deferred.push(self.task);
            } else if entry.view.is_none() {
                entries.remove(&self.task);
            }
        }
    }
}

impl<T> ChildViews<T> {
    fn lock(&self) -> impl std::ops::DerefMut<Target = Index<T>> + '_ {
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Number of children whose view is not yet physically retired.
    #[cfg(test)]
    pub(in crate::local) fn live(&self) -> usize {
        self.lock()
            .entries
            .values()
            .filter(|entry| entry.view.is_some() || entry.failed.is_some())
            .count()
    }

    /// Views constructed so far.
    #[cfg(test)]
    pub(in crate::local) fn constructed(&self) -> usize {
        self.lock().constructed
    }

    /// Whether `task` has an index entry, live or under construction.
    pub(in crate::local) fn contains(&self, task: TaskId) -> bool {
        self.lock().entries.contains_key(&task)
    }

    /// Leases `task`'s installed view without waiting.
    pub(in crate::local) fn get(views: &StateArc<Self>, task: TaskId) -> Option<ChildLease<T>> {
        let mut index = views.lock();
        let entry = index.entries.get_mut(&task)?;
        let view = Arc::clone(entry.view.as_ref()?);
        entry.users += 1;
        entry.retired = false;
        Some(ChildLease {
            _claim: Claim {
                views: StateArc::clone(views),
                task,
            },
            view,
        })
    }

    /// Returns `task`'s view, constructing it once outside every checkout
    /// guard. Concurrent first calls for one task share one construction,
    /// which first finishes retiring the task's failed view, if any.
    pub(in crate::local) async fn acquire(
        views: &StateArc<Self>,
        task: TaskId,
        create: impl AsyncFnOnce() -> Result<T>,
        retire: impl AsyncFnOnce(&T) -> Result<()>,
    ) -> Result<ChildLease<T>> {
        let gate = {
            let mut index = views.lock();
            let entry = index.entries.entry(task).or_insert_with(|| Entry {
                gate: StateArc::new(AsyncMutex::new(())),
                view: None,
                failed: None,
                users: 0,
                retired: false,
                retiring: false,
            });
            entry.users += 1;
            entry.retired = false;
            let claim = Claim {
                views: StateArc::clone(views),
                task,
            };
            if let Some(view) = &entry.view {
                return Ok(ChildLease {
                    _claim: claim,
                    view: Arc::clone(view),
                });
            }
            (StateArc::clone(&entry.gate), claim)
        };
        let (gate, claim) = gate;
        let _gate = lock_owned(gate).await;
        let (installed, failed) = views
            .lock()
            .entries
            .get_mut(&task)
            .map(|entry| (entry.view.clone(), entry.failed.take()))
            .unwrap_or_default();
        if let Some(view) = installed {
            return Ok(ChildLease {
                _claim: claim,
                view,
            });
        }
        if let Some(failed) = failed
            && let Err(error) = retire(&failed).await
        {
            if let Some(entry) = views.lock().entries.get_mut(&task) {
                entry.failed = Some(failed);
            }
            return Err(error);
        }
        // A view counts as live from the start of its construction, while
        // its mount is being set up, until its retirement completes.
        #[cfg(test)]
        let constructing = Constructing::start();
        let view = Arc::new(create().await?);
        #[cfg(test)]
        std::mem::forget(constructing);
        let mut index = views.lock();
        #[cfg(test)]
        {
            index.constructed += 1;
        }
        if let Some(entry) = index.entries.get_mut(&task) {
            entry.view = Some(Arc::clone(&view));
        }
        drop(index);
        Ok(ChildLease {
            _claim: claim,
            view,
        })
    }

    /// Retires `task`'s view once no call holds it, any view whose child
    /// settled while in use, and any view whose retirement failed. A failed
    /// retirement withdraws its view from use; the next settlement or
    /// construction for that child retries it.
    pub(in crate::local) async fn settle(
        views: &StateArc<Self>,
        settled: TaskId,
        retire: impl AsyncFn(&T) -> Result<()>,
    ) -> Result<()> {
        let mut pending = {
            let mut index = views.lock();
            let mut pending = std::mem::take(&mut index.deferred);
            pending.push(settled);
            pending
        };
        pending.sort_unstable();
        pending.dedup();
        let mut result = Ok(());
        for task in pending {
            if let Err(error) = Self::retire(views, task, task == settled, &retire).await
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    }

    async fn retire(
        views: &StateArc<Self>,
        task: TaskId,
        settled: bool,
        retire: &impl AsyncFn(&T) -> Result<()>,
    ) -> Result<()> {
        let gate = {
            let mut index = views.lock();
            let Some(entry) = index.entries.get_mut(&task) else {
                return Ok(());
            };
            // A deferred child that was used again after settling stays live.
            if !(settled || entry.retired || entry.failed.is_some()) {
                return Ok(());
            }
            if entry.users != 0 {
                entry.retired = true;
                return Ok(());
            }
            StateArc::clone(&entry.gate)
        };
        let _gate = lock_owned(gate).await;
        let view = {
            let mut index = views.lock();
            let Some(entry) = index.entries.get_mut(&task) else {
                return Ok(());
            };
            if !(settled || entry.retired || entry.failed.is_some()) {
                return Ok(());
            }
            if entry.users != 0 {
                entry.retired = true;
                return Ok(());
            }
            let Some(view) = entry.view.take().or_else(|| entry.failed.take()) else {
                index.entries.remove(&task);
                return Ok(());
            };
            entry.retiring = true;
            view
        };
        let retired = retire(&view).await;
        #[cfg(test)]
        if retired.is_ok() {
            LIVE_IN_PROCESS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        let mut index = views.lock();
        let Index {
            entries, deferred, ..
        } = &mut *index;
        // A retiring entry stays indexed until this point.
        let Some(entry) = entries.get_mut(&task) else {
            return retired;
        };
        entry.retiring = false;
        entry.retired = false;
        if retired.is_err() {
            // Keep the owner of a view whose physical retirement failed, out
            // of use; the next settlement or construction retries it.
            entry.failed = Some(view);
            if entry.users == 0 {
                deferred.push(task);
            }
        } else if entry.users == 0 {
            entries.remove(&task);
        }
        retired
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
#[path = "child_views/models.rs"]
mod models;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
#[path = "child_views/tests.rs"]
mod tests;
