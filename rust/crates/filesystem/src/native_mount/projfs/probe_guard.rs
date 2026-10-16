use crate::sync::Mutex;
use std::collections::HashMap;
use std::hash::Hash;

// Lock order: only the probe map is locked, and it is released before I/O or
// callbacks. A live guard retains a registration, never a mutex guard.
pub(super) struct ProbeGuard<'a, P: Eq + Hash> {
    probes: &'a Mutex<HashMap<P, usize>>,
    path: &'a P,
}

impl<'a, P: Clone + Eq + Hash> ProbeGuard<'a, P> {
    pub(super) fn enter(probes: &'a Mutex<HashMap<P, usize>>, path: &'a P) -> Self {
        let mut active = probes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active.entry(path.clone()).or_default() += 1;
        Self { probes, path }
    }

    pub(super) fn is_active(
        probes: &Mutex<HashMap<P, usize>>,
        path: &P,
        equivalent: impl Fn(&P, &P) -> bool,
    ) -> bool {
        probes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .any(|active| equivalent(active, path))
    }
}

impl<P: Eq + Hash> Drop for ProbeGuard<'_, P> {
    fn drop(&mut self) {
        let mut active = self
            .probes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(count) = active.get_mut(self.path) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            active.remove(self.path);
        }
    }
}

pub(super) fn with_guard<G, T>(guard: impl FnOnce() -> G, operation: impl FnOnce() -> T) -> T {
    let _guard = guard();
    operation()
}
