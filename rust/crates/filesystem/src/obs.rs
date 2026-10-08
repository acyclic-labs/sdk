//! Tracing glue for `docs/observability.md`. Spans come from
//! `#[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(..))]`; these
//! helpers compile to no-ops on wasm32, which never links `tracing`.
//!
//! Only [`Observe::observe`] emits the `acyclic.work` event, and only facade
//! operations returning an [`OperationReceipt`] call it, while no observed
//! facade operation calls another. Summing those events therefore counts each
//! unit of work once; nested spans record just the `work.*` summary fields.

use crate::performance::{MeasuredResult, OperationReceipt, WorkCounters};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// A failure with a stable, path-free name to record as `error.kind`: its
/// variant name, from `strum::IntoStaticStr`.
pub(crate) trait ErrorKind {
    #[cfg_attr(
        target_arch = "wasm32",
        allow(dead_code, reason = "wasm32 records no spans")
    )]
    fn kind(&self) -> &'static str;
}

impl<E> ErrorKind for E
where
    for<'a> &'a E: Into<&'static str>,
{
    fn kind(&self) -> &'static str {
        self.into()
    }
}

/// Records `outcome` and `error.kind` on the current span, then returns `result`.
pub(crate) fn outcome<T, E: ErrorKind>(result: Result<T, E>) -> Result<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    record_outcome(
        &tracing::Span::current(),
        result.as_ref().err().map(E::kind),
    );
    result
}

/// Like [`outcome`], also recording the `work.*` summary of the receipt that
/// `work` selects or of the failure.
#[cfg_attr(
    target_arch = "wasm32",
    allow(unused_variables, reason = "wasm32 records no spans")
)]
pub(crate) fn measured<T, E: ErrorKind>(
    result: MeasuredResult<T, E>,
    work: impl FnOnce(&T) -> &WorkCounters,
) -> MeasuredResult<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let span = tracing::Span::current();
        if !span.is_disabled() {
            let (work, kind) = match &result {
                Ok(value) => (work(value), None),
                Err(failure) => (&*failure.work, Some(failure.error.kind())),
            };
            record_outcome(&span, kind);
            let bytes = work
                .object_bytes_read
                .saturating_add(work.object_bytes_written)
                .saturating_add(work.authority_bytes_written);
            span.record("work.items", work.items_examined)
                .record("work.bytes", bytes)
                .record("work.durability", work.durability_operations);
        }
    }
    result
}

/// Closes a facade operation: records [`measured`] on its span and emits its
/// receipt through [`WorkCounters::emit`].
pub(crate) trait Observe {
    fn observe(self, op: &'static str) -> Self;
}

impl<T, E: ErrorKind> Observe for MeasuredResult<OperationReceipt<T>, E> {
    #[cfg_attr(
        target_arch = "wasm32",
        allow(unused_variables, reason = "wasm32 records no spans")
    )]
    fn observe(self, op: &'static str) -> Self {
        let result = measured(self, |receipt| &receipt.work);
        #[cfg(not(target_arch = "wasm32"))]
        match &result {
            Ok(receipt) => receipt.work.emit(op, "ok"),
            Err(failure) => failure.work.emit(op, "err"),
        }
        result
    }
}

/// An id field value: `bytes` as lowercase hex, formatted only when recorded.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn hex(bytes: &[u8]) -> tracing::field::DisplayValue<Hex<'_>> {
    tracing::field::display(Hex(bytes))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Hex<'a>(&'a [u8]);

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for Hex<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn record_outcome(span: &tracing::Span, kind: Option<&'static str>) {
    match kind {
        None => span.record("outcome", "ok"),
        Some(kind) => span.record("outcome", "err").record("error.kind", kind),
    };
}
