//! Opt-in native async future size and phase diagnostics.
//!
//! This is intentionally inert unless `ACYCLIC_STACK_DIAGNOSTICS` is set.
//! It exists to distinguish a large concrete future from recursive polling
//! when qualifying native compositions; it is not runtime behavior.

use std::sync::OnceLock;

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ACYCLIC_STACK_DIAGNOSTICS").is_some())
}

pub(crate) fn marker(label: &str) {
    if enabled() {
        eprintln!("[acyclic-stack] {label}");
    }
}

pub(crate) fn future_size<T: ?Sized>(label: &str, future: &T) {
    if enabled() {
        eprintln!("[acyclic-stack] future={label} bytes={}", std::mem::size_of_val(future));
    }
}
