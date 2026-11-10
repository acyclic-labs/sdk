//! Locks whose storage is included in their owner's retained allocation.
//!
//! Futexes and Windows SRW locks/condition variables retain their storage
//! inline, including the first contended wait. The shared parking service can
//! allocate its process-wide table on that wait, after operation admission.
#[cfg(any(target_os = "linux", windows))]
pub use std::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(any(target_os = "linux", windows)))]
#[path = "portable.rs"]
mod portable;
#[cfg(not(any(target_os = "linux", windows)))]
pub use portable::{Condvar, Mutex, MutexGuard};

#[cfg(test)]
use std::sync::TryLockError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condition_reacquisition_preserves_original_poison_and_value() {
        let mut mutex = Mutex::new(0);
        let changed = Condvar::new();
        std::thread::scope(|scope| {
            let mut guard = mutex.lock().unwrap();
            assert!(matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));
            let producer = scope.spawn(|| {
                let mut guard = mutex.lock().unwrap();
                *guard = 7;
                changed.notify_all();
                panic!("original locked producer failed");
            });
            let mut observed_poison = false;
            while *guard == 0 {
                guard = match changed.wait(guard) {
                    Ok(next) => next,
                    Err(error) => {
                        observed_poison = true;
                        error.into_inner()
                    }
                };
            }
            assert!(observed_poison);
            assert_eq!(*guard, 7);
            drop(guard);
            assert!(producer.join().is_err());
        });
        assert!(matches!(mutex.try_lock(), Err(TryLockError::Poisoned(_))));
        assert_eq!(*mutex.lock().unwrap_err().into_inner(), 7);
        assert_eq!(*mutex.get_mut().unwrap_err().into_inner(), 7);
        assert_eq!(mutex.into_inner().unwrap_err().into_inner(), 7);

        // Acquiring a lock while unwinding must not newly poison it.
        struct DuringUnwind<'a>(&'a Mutex<u8>);
        impl Drop for DuringUnwind<'_> {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = 9;
            }
        }
        let clean = Mutex::new(0);
        let failed = std::panic::catch_unwind(|| {
            let _during = DuringUnwind(&clean);
            panic!("original outer failure");
        });
        assert!(failed.is_err());
        assert_eq!(*clean.lock().unwrap(), 9);
    }
}
