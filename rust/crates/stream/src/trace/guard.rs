//! Trace custody for an original guard, including acquisition and ownership.
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
use super::Outcome;
use super::{OwnedRecording, Request};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
};

/// The original guard. Its diagnostic terminal follows the actual unlock,
/// rather than reporting settlement as soon as acquisition returns.
#[must_use = "dropping this value releases the original guard"]
pub struct ObservedGuard<G> {
    // Fields drop in declaration order: original unlock precedes settlement.
    value: G,
    _recording: GuardRecording,
}

impl<G: Deref> Deref for ObservedGuard<G> {
    type Target = G::Target;
    fn deref(&self) -> &Self::Target {
        self.value.deref()
    }
}

impl<G: DerefMut> DerefMut for ObservedGuard<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.deref_mut()
    }
}

struct GuardRecording(Option<OwnedRecording>);

impl Drop for GuardRecording {
    fn drop(&mut self) {
        // A panicking original unlock leaves the recording unsettled through
        // its existing drop custody; it cannot publish a successful unlock.
        if std::thread::panicking() {
            return;
        }
        let recording = self.0.take();
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        if let Some(recording) = recording {
            let _completion = recording.finish_submitted(Some(0), Outcome::Succeeded);
        }
        #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
        drop(recording);
    }
}

/// Acquire the original guard under the current causal owner, and retain the
/// same span through guard release. Without an admitted owner this performs no
/// diagnostic allocation, clock read or IO. Dropped acquisition stays unsettled.
pub async fn observe_guard<G>(request: Request, work: impl Future<Output = G>) -> ObservedGuard<G> {
    let (value, recording) = OwnedRecording::accompany(super::context::child(request), work).await;
    ObservedGuard {
        value,
        _recording: GuardRecording(recording),
    }
}
