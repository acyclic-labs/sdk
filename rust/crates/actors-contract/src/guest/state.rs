use super::GuestError;
use std::cell::Cell;
use ts_rs::TS;

#[cfg(all(loom, not(target_arch = "wasm32")))]
use loom::{
    sync::{Arc, Mutex},
    thread_local,
};
#[cfg(any(not(loom), target_arch = "wasm32"))]
use std::{
    sync::{Arc, Mutex},
    thread_local,
};

#[cfg(all(loom, not(target_arch = "wasm32")))]
thread_local! { static IN_MUTATION: Cell<bool> = Cell::new(false); }
#[cfg(any(not(loom), target_arch = "wasm32"))]
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

#[cfg(all(test, loom, not(target_arch = "wasm32")))]
mod tests;
