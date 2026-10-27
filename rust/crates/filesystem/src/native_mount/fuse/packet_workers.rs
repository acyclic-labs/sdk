//! One physical stop and positive join boundary for packet transports.

use super::{NativeMountError, fuse_vec};
use crate::{NativeMaterializationWork, WorkCounters};
use std::num::NonZeroU32;

pub(super) trait PacketWorkers: Sync {
    /// Interrupt every physical wait before attempting protocol retirement.
    fn stop(&self);
    fn run_worker(&self, index: u32) -> Result<(), NativeMountError>;
}

struct Stop<'a, T: PacketWorkers>(&'a T);

impl<T: PacketWorkers> Drop for Stop<'_, T> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// All started workers are joined, including after partial spawn or panic.
/// The caller retains its source, buffers and cancellation registration until
/// this returns. No detached worker can turn an uncertain callback into drain.
pub(super) fn run<T: PacketWorkers>(
    owner: &T,
    count: NonZeroU32,
    work: &NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let _stop = Stop(owner);
    std::thread::scope(|scope| {
        let mut workers = fuse_vec(count.get() as usize, work)?;
        let mut result = Ok(());
        let mut panic = None;
        for index in 0..count.get() {
            match std::thread::Builder::new().spawn_scoped(scope, move || {
                let _stop = Stop(owner);
                owner.run_worker(index)
            }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    owner.stop();
                    result = Err(error.into());
                    break;
                }
            }
        }
        for worker in workers.drain(..) {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) if result.is_ok() => result = Err(error),
                Ok(Err(_)) => {}
                Err(payload) if panic.is_none() => panic = Some(payload),
                Err(_) => {}
            }
        }
        if let Some(payload) = panic {
            std::panic::resume_unwind(payload);
        }
        result.and_then(|()| work.admit(WorkCounters::UNCHARGED).map_err(Into::into))
    })
}
