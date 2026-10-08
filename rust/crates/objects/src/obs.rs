//! Span helpers for `docs/observability.md`. Each expands to nothing on wasm32,
//! which never links `tracing`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::{Span, field::Empty};
/// wasm32 records nothing.
#[cfg(target_arch = "wasm32")]
pub(crate) struct Span;

/// A result with a stable, path-free `error.kind` for a failure.
#[cfg_attr(
    target_arch = "wasm32",
    expect(dead_code, reason = "wasm32 records no outcome")
)]
pub(crate) trait Outcome {
    fn failure(&self) -> Option<&'static str>;
}
impl<T> Outcome for Result<T, crate::Error> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|error| error.code.as_str_name())
    }
}
/// A native batch fails with its first failed item.
impl<T> Outcome for Vec<Result<T, crate::Error>> {
    fn failure(&self) -> Option<&'static str> {
        self.iter().find_map(Outcome::failure)
    }
}
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<T> Outcome for Result<T, crate::LocalOpenError> {
    fn failure(&self) -> Option<&'static str> {
        use crate::LocalOpenError::{AlreadyOwned, Corrupt, Invalid, Io, Unavailable};
        self.as_ref().err().map(|error| match error {
            Invalid => "invalid",
            Corrupt => "corrupt",
            AlreadyOwned => "already_owned",
            Unavailable => "unavailable",
            Io(_) => "io",
        })
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<T> Outcome for std::io::Result<T> {
    fn failure(&self) -> Option<&'static str> {
        self.as_ref().err().map(|_| "io")
    }
}

/// Records `outcome` and, on failure, `error.kind` on the current span.
#[cfg(not(target_arch = "wasm32"))]
fn finish<R: Outcome>(result: R) -> R {
    let span = Span::current();
    match result.failure() {
        None => span.record("outcome", "ok"),
        Some(kind) => span.record("outcome", "err").record("error.kind", kind),
    };
    result
}

/// Runs `future` inside `span` and records its outcome there.
pub(crate) async fn traced<R: Outcome>(span: Span, future: impl Future<Output = R>) -> R {
    #[cfg(not(target_arch = "wasm32"))]
    return tracing::Instrument::instrument(async move { finish(future.await) }, span).await;
    #[cfg(target_arch = "wasm32")]
    {
        let Span = span;
        future.await
    }
}

/// Runs `body` inside `span` and records its outcome there.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) fn scoped<R: Outcome>(span: &Span, body: impl FnOnce() -> R) -> R {
    span.in_scope(|| finish(body()))
}

/// Opens a span at `level` with `outcome`/`error.kind` slots for [`traced`];
/// a unit [`Span`] on wasm32, where the field values are not evaluated.
macro_rules! span {
    ($level:ident, $name:literal $(, $($field:tt)+)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let span = ::tracing::span!(
            ::tracing::Level::$level,
            $name,
            $($($field)+,)?
            outcome = ::tracing::field::Empty,
            error.kind = ::tracing::field::Empty,
        );
        #[cfg(target_arch = "wasm32")]
        let span = crate::obs::Span;
        span
    }};
}
pub(crate) use span;

/// Records fields on the current span; only borrows the values on wasm32.
macro_rules! record {
    ($($field:literal = $value:expr),+ $(,)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = ::tracing::Span::current();
            $(span.record($field, $value);)+
        }
        #[cfg(target_arch = "wasm32")]
        {
            $(let _ = &$value;)+
        }
    }};
}
pub(crate) use record;
