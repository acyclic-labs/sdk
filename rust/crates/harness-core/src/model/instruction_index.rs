//! Exact instruction source keys and original accounting observations.
use super::*;

pub(super) fn source_key(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    with_history_io(io, |work| source_key_history(volume, path, work))
}

pub(super) fn source_key_history(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 32]> {
    // Keys are request-local; the unobserved path hashes the borrowed wire.
    if work.is_none() {
        return crate::executor::json::serialized_json_digest(&(volume, path));
    }
    #[cfg(feature = "filesystem")]
    let maximum = work
        .as_deref()
        .map(|work| work.remaining().map(|b| b.bytes_encoded))
        .transpose()?
        .unwrap_or(u64::MAX);
    #[cfg(not(feature = "filesystem"))]
    let maximum = u64::MAX;
    crate::executor::json::canonical_json_digest(&(volume, path), Some(maximum), work)
}

pub(super) fn history_step_traced(work: &mut Option<&mut dyn HistoryIo>, items: u64) -> Result<()> {
    history_step(work, items)?;
    trace_step(items);
    Ok(())
}

pub(super) fn step(io: Option<&PreparationIo>, items: u64) -> Result<()> {
    charge_items(io, items)?;
    trace_step(items);
    Ok(())
}
fn trace_step(items: u64) {
    trace_operation("instruction-step", items);
}
pub(super) fn trace_operation(name: &'static str, items: u64) {
    let _ = (name, items);
    #[cfg(all(
        test,
        feature = "test-support",
        feature = "filesystem",
        not(target_arch = "wasm32")
    ))]
    TRACE_STEPS.with(|active| {
        if active.get() {
            acyclic_test_support::differential::record((name, items));
        }
    });
}

#[cfg(all(
    test,
    feature = "test-support",
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
thread_local! { static TRACE_STEPS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(all(
    test,
    feature = "test-support",
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
pub(super) fn traced<T>(run: impl FnOnce() -> T) -> T {
    TRACE_STEPS.with(|active| assert!(!active.replace(true)));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TRACE_STEPS.with(|active| active.set(false));
        }
    }
    let _reset = Reset;
    run()
}
