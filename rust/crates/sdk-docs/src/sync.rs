#[cfg(acyclic_sdk_docs_loom)]
pub use loom::sync::{Arc, Mutex};
#[cfg(not(acyclic_sdk_docs_loom))]
pub use std::sync::{Arc, Mutex};

// Lock order: each initialization slot is a leaf. An initializer must not
// acquire another slot or re-enter its own. No slot lock survives this call;
// callers use immutable data after releasing it. Package construction never
// resolves Rustdoc, and Rustdoc resolution never enters the package slot.
pub fn get_or_try_init<T, E>(
    slot: &Mutex<Option<Arc<T>>>,
    initialize: impl FnOnce() -> Result<T, E>,
) -> Result<Arc<T>, E> {
    let mut value = slot.lock().unwrap();
    if value.is_none() {
        *value = Some(Arc::new(initialize()?));
    }
    Ok(Arc::clone(value.as_ref().unwrap()))
}
