use std::cell::Cell;

thread_local! {
    static OMIT: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::native_mount::adapter) fn omitted() -> bool {
    OMIT.get()
}

pub(super) struct Omit(bool);

impl Omit {
    pub(super) fn enter(value: bool) -> Self {
        Self(OMIT.replace(value))
    }
}

impl Drop for Omit {
    fn drop(&mut self) {
        OMIT.set(self.0);
    }
}

#[cfg(any(not(unit_test_split), unit_test_mount_lazy))]
pub(in crate::native_mount) fn with_omitted<T>(broken: bool, run: impl FnOnce() -> T) -> T {
    let _old = Omit::enter(broken);
    run()
}
