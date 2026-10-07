//! Tracing glue for `docs/observability.md`. Spans come from
//! `#[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(..))]`; these
//! helpers compile to no-ops on wasm32, which never links `tracing`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// Records `outcome` and `error.kind` on the current span, then returns `result`.
pub(crate) fn outcome<T>(result: crate::Result<T>) -> crate::Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span
                .record("outcome", "err")
                .record("error.kind", error.kind()),
        };
    }
    result
}

/// Records extra fields on the current span; expands to nothing on wasm32.
macro_rules! obs_record {
    ($($field:literal = $value:expr),+ $(,)?) => {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = ::tracing::Span::current();
            $(span.record($field, $value);)+
        }
    };
}
pub(crate) use obs_record;
