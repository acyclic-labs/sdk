//! Serialises the tests that take the process-wide terminal owner.

static TERMINAL_OWNER: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn terminal_owner() -> std::sync::MutexGuard<'static, ()> {
    match TERMINAL_OWNER.lock() {
        Ok(owner) => owner,
        Err(poisoned) => poisoned.into_inner(),
    }
}
