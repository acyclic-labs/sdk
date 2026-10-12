//! Native source change notifications.

use crate::demand::{
    DemandError, DemandWork, NativeSourceWork, OriginalDemandWork, SourceDemandReadWork,
};
use crate::{CancellationToken, WorkBudget, WorkCounters};
use std::sync::{Arc, Mutex};

mod callback_work;
pub(crate) use callback_work::CallbackWork;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(windows)]
mod directory_changes;
#[cfg(target_os = "macos")]
mod fsevents;
#[cfg(target_os = "linux")]
mod inotify;

#[cfg(windows)]
pub(crate) use directory_changes::NativeSourceWatch;
#[cfg(target_os = "macos")]
pub(crate) use fsevents::NativeSourceWatch;
#[cfg(target_os = "linux")]
pub(crate) use inotify::NativeSourceWatch;

/// One change the host reported beneath a watched root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HostChange<'a> {
    /// This root-relative path may bind another entry.
    Rebound(std::borrow::Cow<'a, std::path::Path>),
    /// This root-relative entry may have changed in place.
    Altered(std::borrow::Cow<'a, std::path::Path>),
    /// A Windows report borrowed from the completed read buffer.
    #[cfg(windows)]
    WidePath { action: u32, path: &'a [u16] },
    /// This volume node may have changed through any name.
    #[cfg(windows)]
    File(u64),
    /// Notifications were lost: anything may have changed.
    Everything,
    /// Delivery may be incomplete without known notification loss.
    #[cfg(any(target_os = "macos", windows))]
    Unconfirmed,
}

/// Receives host changes on the watch's original accounting context.
pub(crate) trait HostChangeSink: Send + Sync {
    fn logical_owned_bytes(&self) -> u64;
    fn host_changed(
        &self,
        changes: &[HostChange<'_>],
        work: &mut CallbackWork<'_>,
    ) -> Result<(), DemandError>;
}

/// The destination and accounting context for native notifications.
pub(crate) struct Delivery {
    sink: Arc<dyn HostChangeSink>,
    exact: AtomicBool,
    budget: WorkBudget,
    cancellation: CancellationToken,
    original: Option<OriginalDemandWork>,
    failure: Mutex<Option<(DemandError, Option<WorkCounters>)>>,
    allocation: Option<crate::NativeAllocation>,
}

impl Delivery {
    fn new<W: SourceDemandReadWork>(
        sink: Arc<dyn HostChangeSink>,
        work: &mut W,
    ) -> Result<Arc<Self>, DemandError> {
        work.check()?;
        let original = work
            .native_source_work()
            .map(|work| work.original_demand_work().allocation_work());
        let (budget, cancellation) =
            work.with_original_leaf(|work| (work.budget, work.cancellation().clone()));
        let bytes = (size_of::<Self>() + 2 * size_of::<usize>()) as u64;
        let allocation = if let Some(source) = work.native_source_work() {
            Some(
                source
                    .materialization_work()?
                    .claim(bytes, 1, WorkCounters::UNCHARGED)
                    .map_err(crate::native_name::original_name_error)?,
            )
        } else {
            work.allocate(bytes)?;
            None
        };
        let answer = Arc::new(Self {
            sink,
            exact: AtomicBool::new(true),
            budget,
            cancellation,
            original,
            failure: Mutex::new(None),
            allocation,
        });
        if !work.is_native_original() {
            work.release(bytes)?;
        }
        Ok(answer)
    }

    fn with_work<T>(
        &self,
        invoke: impl FnOnce(&mut CallbackWork<'_>) -> Result<T, DemandError>,
    ) -> Result<T, ()> {
        let mut ordinary = DemandWork::new(self.budget, &self.cancellation);
        let mut work = match &self.original {
            Some(original) => {
                CallbackWork::Original(NativeSourceWork::from_original_work(original))
            }
            None => CallbackWork::Ordinary(&mut ordinary),
        };
        invoke(&mut work).map_err(|error| {
            self.exact.store(false, Ordering::Release);
            let prior = match &work {
                CallbackWork::Ordinary(work) => Some(work.work),
                CallbackWork::Original(_) => None,
            };
            let mut failure = self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if failure.is_none() {
                *failure = Some((error, prior));
            }
        })
    }

    fn deliver(&self, changes: &[HostChange<'_>]) {
        if !changes.is_empty() {
            let _ = self.with_work(|work| self.sink.host_changed(changes, work));
        }
    }

    fn birth_matches<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        let Some(birth) = &self.original else {
            return false;
        };
        let Some(current) = work.native_source_work() else {
            return false;
        };
        let current = current.original_demand_work();
        Arc::ptr_eq(&birth.state, &current.state)
            && match (&birth.resource, &current.resource) {
                (None, None) => true,
                (Some(birth), Some(current)) => Arc::ptr_eq(birth, current),
                _ => false,
            }
    }

    fn take_failure<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        if let Some((error, prior)) = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            if let Some(prior) = prior {
                work.merge(prior)?;
            }
            return Err(error);
        }
        Ok(())
    }

    fn abandon(&self) {
        if self.exact.swap(false, Ordering::AcqRel) {
            self.deliver(&[HostChange::Everything]);
        }
    }
    fn is_exact(&self) -> bool {
        self.exact.load(Ordering::Acquire)
    }
}

impl NativeSourceWatch {
    pub(crate) fn start<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot,
        sink: Arc<dyn HostChangeSink>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_driver(directory, Delivery::new(sink, work)?, work)
    }
    pub(crate) fn is_exact(&self) -> bool {
        self.delivery().is_exact()
    }
}

#[cfg(test)]
mod tests;
