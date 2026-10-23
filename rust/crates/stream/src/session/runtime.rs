//! Session tracing context and stream boxing.
#[cfg(not(target_arch = "wasm32"))]
pub(super) use acyclic_native_runtime::obs::OperationSpan;
#[cfg(target_arch = "wasm32")]
pub(super) use context::OperationSpan;
#[cfg(any(test, target_arch = "wasm32"))]
#[path = "runtime/context.rs"]
mod context;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn boxed<S: futures::Stream + Send + 'static>(
    source: S,
) -> crate::BoxProviderStream<'static, S::Item> {
    Box::pin(source)
}
#[cfg(target_arch = "wasm32")]
pub(super) fn boxed<S: futures::Stream + 'static>(
    source: S,
) -> crate::BoxProviderStream<'static, S::Item> {
    Box::pin(source)
}
