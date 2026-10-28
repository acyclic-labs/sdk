//! Platform ownership only; validity and refresh rules live once in the cache.
use super::CredentialLease;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(super) struct Slot(arc_swap::ArcSwapOption<super::CredentialSnapshot>);

#[cfg(not(target_arch = "wasm32"))]
impl Slot {
    pub(super) fn load(&self) -> Option<CredentialLease> {
        self.0.load_full()
    }
    pub(super) fn store(&self, value: Option<CredentialLease>) {
        self.0.store(value);
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Default)]
pub(super) struct Slot(std::cell::RefCell<Option<CredentialLease>>);

#[cfg(target_arch = "wasm32")]
impl Slot {
    pub(super) fn load(&self) -> Option<CredentialLease> {
        self.0.borrow().clone()
    }
    pub(super) fn store(&self, value: Option<CredentialLease>) {
        *self.0.borrow_mut() = value;
    }
}

impl Slot {
    pub(super) fn matches(&self, value: &CredentialLease) -> bool {
        self.load()
            .is_some_and(|current| super::Shared::ptr_eq(&current, value))
    }
}
