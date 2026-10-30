//! Session tracing context and stream boxing.
#[cfg(not(target_arch = "wasm32"))]
pub(super) use acyclic_native_runtime::obs::OperationSpan;
#[cfg(not(target_arch = "wasm32"))]
pub(super) use acyclic_native_runtime::obs::waiting;
#[cfg(target_arch = "wasm32")]
pub(super) use context::OperationSpan;

/// Browser sessions install no wait report; the future runs unchanged.
#[cfg(target_arch = "wasm32")]
pub(super) fn waiting<F: std::future::Future>(
    primitive: &'static str,
    key: impl std::fmt::Display,
    future: F,
) -> F {
    let _ = (primitive, key);
    future
}
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
