#[cfg(acyclic_sdk_docs_loom)]
pub use loom::sync::Mutex;
use std::collections::BTreeMap;
#[cfg(not(acyclic_sdk_docs_loom))]
pub use std::sync::Mutex;

pub type Versions<K, S, T = String> = Mutex<BTreeMap<K, (S, T)>>;

// Leaf lock: observation and the concrete executable probe acquire no other
// SDK cache lock. Keep lookup, probe and publication in this one critical section.
pub fn query<K: Ord, S: Eq, T: Clone, E>(
    versions: &Versions<K, S, T>,
    key: K,
    mut observe: impl FnMut() -> Result<S, E>,
    probe: impl FnOnce() -> Result<T, E>,
    changed: impl FnOnce() -> E,
) -> Result<T, E> {
    let before = observe()?;
    let mut versions = versions.lock().unwrap();
    let value = match versions.get(&key) {
        Some((stamp, value)) if stamp == &before => value.clone(),
        _ => probe()?,
    };
    if observe()? != before {
        return Err(changed());
    }
    versions.insert(key, (before, value.clone()));
    Ok(value)
}

#[cfg(all(test, not(acyclic_sdk_docs_loom)))]
#[path = "concrete_versions/tests.rs"]
mod tests;
