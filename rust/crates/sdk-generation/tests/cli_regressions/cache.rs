//! Shared immutable fixture initialization with explicit owner failure.

#[cfg(loom)]
pub use loom::sync::{Arc, Mutex};
#[cfg(not(loom))]
pub use std::sync::{Arc, Mutex};

// Lock order: each initialization slot is a leaf. An initializer must not
// acquire another slot or re-enter its own. No slot lock survives this call;
// callers use immutable data after releasing it. Package construction never
// resolves Rustdoc, and Rustdoc resolution never enters the package slot.

/// Failure to initialize or acquire an immutable fixture slot.
#[derive(Debug, PartialEq, Eq)]
pub enum InitError<E> {
    /// An earlier initializer unwound while holding the slot.
    Poisoned,
    /// The initializer failed without publishing a value; its error is retained.
    Initialize(E),
}

/// Share one complete value, or report the original initialization/owner failure.
///
/// An initializer error leaves the slot empty and available to another caller.
/// A poisoned slot is refused without running another initializer.
///
/// # Panics
///
/// Propagates a panic from `initialize`; later calls report [`InitError::Poisoned`].
pub fn get_or_try_init<T, E>(
    slot: &Mutex<Option<Arc<T>>>,
    initialize: impl FnOnce() -> Result<T, E>,
) -> Result<Arc<T>, InitError<E>> {
    let mut value = slot.lock().map_err(|_| InitError::Poisoned)?;
    if let Some(value) = value.as_ref() {
        return Ok(Arc::clone(value));
    }
    let initialized = Arc::new(initialize().map_err(InitError::Initialize)?);
    *value = Some(Arc::clone(&initialized));
    Ok(initialized)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::{get_or_try_init, Arc, InitError, Mutex};

    #[test]
    fn lost_initializer_owner_refuses_the_slot_without_reinitializing() {
        let slot = Arc::new(Mutex::new(None));
        let owner_slot = Arc::clone(&slot);
        let owner = std::thread::spawn(move || {
            get_or_try_init::<usize, ()>(&owner_slot, || {
                std::panic::resume_unwind(Box::new("original initializer owner failed"));
            })
        });
        assert!(owner.join().is_err(), "original initializer must unwind");

        let mut initialized = false;
        let observed = get_or_try_init(&slot, || {
            initialized = true;
            Ok::<_, ()>(17)
        });
        assert_eq!(observed, Err(InitError::Poisoned));
        assert!(
            !initialized,
            "a poisoned owner must not admit another initializer"
        );
    }
}
