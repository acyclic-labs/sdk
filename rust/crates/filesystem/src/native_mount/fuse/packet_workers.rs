//! One physical stop and positive join boundary for packet transports.

use super::NativeMountError;
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
pub(super) fn run<T: PacketWorkers>(owner: &T, count: NonZeroU32) -> Result<(), NativeMountError> {
    let _stop = Stop(owner);
    std::thread::scope(|scope| {
        // The private callers select fixed host/device worker counts. Keep
        // their handles in these stack frames rather than allocating another
        // vector after startup, when original cancellation may already hold.
        let (result, panic) = spawn_and_join(scope, owner, 0, count.get());
        if let Some(payload) = panic {
            std::panic::resume_unwind(payload);
        }
        result
    })
}

type Joined = (
    Result<(), NativeMountError>,
    Option<Box<dyn std::any::Any + Send>>,
);

fn spawn_and_join<'scope, 'env, T: PacketWorkers>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    owner: &'scope T,
    index: u32,
    count: u32,
) -> Joined {
    if index == count {
        return (Ok(()), None);
    }
    let worker = match std::thread::Builder::new().spawn_scoped(scope, move || {
        let _stop = Stop(owner);
        owner.run_worker(index)
    }) {
        Ok(worker) => worker,
        Err(error) => {
            owner.stop();
            return (Err(error.into()), None);
        }
    };
    let (mut result, mut panic) = spawn_and_join(scope, owner, index + 1, count);
    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) if result.is_ok() => result = Err(error),
        Ok(Err(_)) => {}
        Err(payload) if panic.is_none() => panic = Some(payload),
        Err(_) => {}
    }
    (result, panic)
}
