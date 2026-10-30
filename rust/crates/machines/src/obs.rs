//! Small RPC field adapter over the shared operation lifecycle.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Span = tracing::Span;
#[cfg(target_arch = "wasm32")]
pub(crate) type Span = ();

macro_rules! operation {
    ($name:expr, $span:ident, $work:expr) => {
        $crate::obs::operation!(@tracing::Level::INFO, $name, $span, $work)
    };
    (@$level:path, $name:expr, $span:ident, $work:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            let $span = acyclic_native_runtime::obs::OperationSpan::new(tracing::span!(
                $level,
                $name,
                outcome = tracing::field::Empty,
                rpc.code = tracing::field::Empty,
                error.kind = tracing::field::Empty,
            ));
            $span
                .scope(acyclic_observability::observe_output(
                    &$span,
                    $work,
                    |result: &Result<_, crate::ProviderError>| {
                        if result.is_ok() {
                            $span.record("rpc.code", 0);
                        }
                        result.as_ref().err().map(crate::ProviderError::kind)
                    },
                ))
                .await
        }
        #[cfg(target_arch = "wasm32")]
        {
            let $span = ();
            ($work).await
        }
    }};
}
pub(crate) use operation;

pub(crate) fn failed_rpc(_span: &Span, _status: &tonic::Status) {
    #[cfg(not(target_arch = "wasm32"))]
    _span.record("rpc.code", i32::from(_status.code()));
}
