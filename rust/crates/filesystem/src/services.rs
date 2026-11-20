//! Proof that process-global native frameworks are loaded before any root work ledger.

/// Proof that process-global native frameworks have loaded.
///
/// A root work ledger requires this proof. Loading a framework while an
/// original ledger exists would charge process bootstrap to one invocation,
/// so the proof can only come from the one initializer that loads them.
#[derive(Debug)]
pub struct ServicesInitialized(());

impl ServicesInitialized {
    /// The gate behind [`crate::watch::native_services`], the one initializer
    /// that loads every process-global framework. Nothing else may call it.
    #[doc(hidden)]
    pub(crate) const fn __after_framework_init() -> Self {
        Self(())
    }

    /// A proof for the kernel layer's tests and fuzzers. It is sound because
    /// that layer loads no native framework, so none can load under its ledgers.
    #[cfg(any(test, feature = "test-support", feature = "fuzzing"))]
    pub(crate) const fn for_kernel_tests() -> Self {
        Self(())
    }
}
