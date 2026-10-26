//! How every backend meters and authorizes a callback.
//!
//! A view records, when it is built, the source's original invocation and
//! its callback guard ([`install`]). Each callback then charges the meter
//! of the invocation running now, else that original, else the backend's
//! own fallback ([`select`]), and fails as [`NativeMountError::ControlPending`]
//! once the source's invocation or guard is no longer the one installed. A
//! [`NativeMountWorkScope`] holds an invocation's meter active for the
//! callbacks it causes, on every backend through one type.

use crate::native_mount::view_ledger::same_work;
use crate::native_mount::{MountFilesystem, NativeMountCallbackGuard, NativeMountError};
use crate::{NativeMaterializationWork as Work, WorkCounters};
use std::sync::Arc;

pub(crate) type Guard = Arc<dyn NativeMountCallbackGuard>;

/// The source's original invocation, which must be `candidate`'s if both
/// exist, admitted.
pub(crate) fn source_original(
    source: &dyn MountFilesystem,
    candidate: Option<&Work>,
) -> Result<Option<Work>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if candidate.is_some_and(|work| !same_work(original, work)) {
            return Err(NativeMountError::ControlPending);
        }
        original.admit(WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

/// Records a view's original invocation and guard as it is built under
/// `work`, verifying the guard once.
pub(crate) fn install(
    source: &dyn MountFilesystem,
    work: &Work,
) -> Result<(Option<Work>, Option<Guard>), NativeMountError> {
    let original = source_original(source, Some(work))?;
    let guard = source.native_callback_guard();
    verify(guard.as_ref(), original.as_ref(), false)?;
    Ok((original, guard))
}

fn verify(guard: Option<&Guard>, work: Option<&Work>, write: bool) -> Result<(), NativeMountError> {
    if let Some(guard) = guard {
        let work = work.ok_or(NativeMountError::ControlPending)?;
        let callbacks = crate::native_mount::adapter::CallbackRuntime::existing()?;
        if write {
            callbacks.verify_callback(work, || guard.check_write(work))?;
        } else {
            callbacks.verify_callback(work, || guard.check(work))?;
        }
    }
    Ok(())
}

/// The guard the source holds now, if it is still the installed one.
fn current_guard<'a>(
    installed: Option<&'a Guard>,
    source: &dyn MountFilesystem,
) -> Result<Option<&'a Guard>, NativeMountError> {
    match (installed, source.native_callback_guard()) {
        (None, None) => Ok(None),
        (Some(installed), Some(current)) if Arc::ptr_eq(installed, &current) => Ok(Some(installed)),
        _ => Err(NativeMountError::ControlPending),
    }
}

/// The meter a callback charges: `active` (the invocation running now),
/// else the installed original, else `fallback`. Fails once the source's
/// invocation or guard differs from the installed ones; a guard checks the
/// callback, as a write when `write`.
pub(crate) fn select(
    source: &dyn MountFilesystem,
    active: Option<Work>,
    installed_original: Option<&Work>,
    installed_guard: Option<&Guard>,
    fallback: Option<Work>,
    write: bool,
) -> Result<Option<Work>, NativeMountError> {
    let selected = active.or_else(|| installed_original.cloned()).or(fallback);
    let current = source_original(source, selected.as_ref())?;
    match (installed_original, &current) {
        (None, None) => {}
        (Some(installed), Some(current)) if same_work(installed, current) => {}
        _ => return Err(NativeMountError::ControlPending),
    }
    if let Some(work) = &selected {
        work.admit(WorkCounters::UNCHARGED)?;
    }
    verify(
        current_guard(installed_guard, source)?,
        selected.as_ref(),
        write,
    )?;
    Ok(selected)
}

/// Checks a callback's write against the installed guard. `ProjFS` checks an
/// authored callback's write as part of [`select`] instead.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn verify_write(
    source: &dyn MountFilesystem,
    installed_guard: Option<&Guard>,
    work: &Work,
) -> Result<(), NativeMountError> {
    verify(current_guard(installed_guard, source)?, Some(work), true)
}

/// A view whose callbacks charge an active invocation's meter.
pub(crate) trait MeteredView: Send + Sync {
    /// Makes `work` the active meter; returns its activation generation.
    fn activate(&self, work: &Work) -> Result<u64, NativeMountError>;
    /// Ends the activation `activate` returned.
    fn deactivate(&self, work: &Work, generation: u64);
}

/// Retains an invocation's meter as the view's active one for accepted
/// native I/O; dropping it deactivates the meter.
#[must_use]
pub struct NativeMountWorkScope {
    view: Arc<dyn MeteredView>,
    work: Work,
    generation: u64,
}

impl NativeMountWorkScope {
    pub(crate) fn new<V: MeteredView + 'static>(
        view: &Arc<V>,
        work: &Work,
    ) -> Result<Self, NativeMountError> {
        let view: Arc<dyn MeteredView> = Arc::<V>::clone(view);
        Self::enter(view, work)
    }

    fn enter(view: Arc<dyn MeteredView>, work: &Work) -> Result<Self, NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        let generation = view.activate(work)?;
        Ok(Self {
            view,
            work: work.clone(),
            generation,
        })
    }

    /// Retains the same invocation for one physical I/O operation.
    pub fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::enter(Arc::clone(&self.view), &self.work)
    }
}

impl Drop for NativeMountWorkScope {
    fn drop(&mut self) {
        self.view.deactivate(&self.work, self.generation);
    }
}
