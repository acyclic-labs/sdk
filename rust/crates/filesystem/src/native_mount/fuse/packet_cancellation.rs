//! One original-funded cancellation registration for a physical packet owner.

use super::NativeMountError;
use crate::{Cancelled, NativeMaterializationWork, WorkCounters};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

pub(super) fn register<T: Wake + Send + Sync + 'static>(
    work: &NativeMaterializationWork,
    owner: &Arc<T>,
) -> Result<Cancelled<'static>, NativeMountError> {
    let mut cancellation = work.cancellation_wait()?;
    let waker = Waker::from(Arc::clone(owner));
    if Pin::new(&mut cancellation)
        .poll(&mut Context::from_waker(&waker))
        .is_ready()
    {
        waker.wake_by_ref();
        // Return the original typed terminal cause; errno alone is only a
        // fallback if an owner supplied an already cancelled registration.
        work.admit(WorkCounters::UNCHARGED)?;
        return Err(libc::ECANCELED.into());
    }
    Ok(cancellation)
}
