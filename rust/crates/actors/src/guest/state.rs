use super::GuestError;
use std::cell::Cell;
use ts_rs::TS;

#[cfg(loom)]
use loom::{
    sync::{Arc, Mutex},
    thread_local,
};
#[cfg(not(loom))]
use std::{
    sync::{Arc, Mutex},
    thread_local,
};

#[cfg(loom)]
thread_local! { static IN_MUTATION: Cell<bool> = Cell::new(false); }
#[cfg(not(loom))]
thread_local! { static IN_MUTATION: Cell<bool> = const { Cell::new(false) }; }

struct MutationScope;
impl Drop for MutationScope {
    fn drop(&mut self) {
        IN_MUTATION.with(|held| held.set(false));
    }
}

/// Shared guest state. Lock order: no nested mutation locks, including distinct states.
/// The synchronous closure cannot borrow state into an async future. No lock survives
/// the operation; other subscriptions and Invoke handlers may run across awaits.
#[derive(TS)]
#[ts(
    bound = "State: TS",
    type = "{ with: (mutate: (state: State) => undefined) => void }"
)]
pub struct GuestState<State> {
    value: Arc<Mutex<State>>,
}

impl<State> Clone for GuestState<State> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
        }
    }
}

impl<State> GuestState<State> {
    /// Wrap restored state once; handlers receive clones of this shared handle.
    pub fn new(value: State) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
        }
    }

    /// Mutate synchronously. Nested operations fail before attempting another lock.
    pub fn with(&self, mutate: impl FnOnce(&mut State)) -> Result<(), GuestError> {
        if IN_MUTATION.with(Cell::get) {
            return Err(GuestError::NestedMutation);
        }
        let mut state = self
            .value
            .lock()
            .map_err(|_| GuestError::Handler("poisoned guest state".into()))?;
        IN_MUTATION.with(|held| held.set(true));
        let _scope = MutationScope;
        mutate(&mut state);
        Ok(())
    }
}

#[cfg(test)]
#[cfg(loom)]
mod tests {
    use super::*;

    #[test]
    fn two_handlers_interleave_without_torn_state_or_lost_progress() {
        loom::model(|| {
            let state = GuestState::new((0u8, 0u8));
            let handlers: Vec<_> = (0..2)
                .map(|_| {
                    let state = state.clone();
                    loom::thread::spawn(move || {
                        assert!(
                            state
                                .with(|value| {
                                    value.0 += 1;
                                    value.1 += 1;
                                })
                                .is_ok()
                        );
                        // An await boundary: no state lock is held while another handler runs.
                        loom::thread::yield_now();
                        assert!(
                            state
                                .with(|value| {
                                    assert_eq!(value.0, value.1);
                                    value.0 += 1;
                                    value.1 += 1;
                                })
                                .is_ok()
                        );
                    })
                })
                .collect();
            for handler in handlers {
                assert!(handler.join().is_ok());
            }
            assert!(state.with(|value| assert_eq!(*value, (4, 4))).is_ok());
        });
    }

    #[test]
    fn lock_order_checker_rejects_same_and_different_state_nesting() {
        loom::model(|| {
            let first = GuestState::new(0);
            let second = GuestState::new(0);
            assert!(
                first
                    .with(|_| {
                        assert_eq!(first.with(|_| {}), Err(GuestError::NestedMutation));
                        assert_eq!(second.with(|_| {}), Err(GuestError::NestedMutation));
                    })
                    .is_ok()
            );
            assert!(second.with(|value| *value += 1).is_ok());
        });
    }

    // Negative control: deliberately split a read/modify/write across an await.
    // It exercises the same state shim, but loses atomic exclusion between steps.
    #[test]
    #[should_panic(expected = "lost update")]
    fn model_detects_broken_mutation_exclusion() {
        loom::model(|| {
            let state = GuestState::new(0u8);
            let handlers: Vec<_> = (0..2)
                .map(|_| {
                    let state = state.clone();
                    loom::thread::spawn(move || {
                        let mut snapshot = 0;
                        assert!(state.with(|value| snapshot = *value).is_ok());
                        loom::thread::yield_now();
                        assert!(state.with(|value| *value = snapshot + 1).is_ok());
                    })
                })
                .collect();
            for handler in handlers {
                assert!(handler.join().is_ok());
            }
            assert!(
                state
                    .with(|value| assert_eq!(*value, 2, "lost update"))
                    .is_ok()
            );
        });
    }

    #[test]
    #[should_panic(expected = "lock order violation")]
    fn model_detects_disabled_lock_order_checker() {
        loom::model(|| {
            let first = GuestState::new(0u8);
            let second = GuestState::new(0u8);
            assert!(
                first
                    .with(|_| {
                        // Negative control: deliberately erase the held-lock marker.
                        IN_MUTATION.with(|held| held.set(false));
                        assert_eq!(
                            second.with(|_| {}),
                            Err(GuestError::NestedMutation),
                            "lock order violation"
                        );
                    })
                    .is_ok()
            );
        });
    }
}
