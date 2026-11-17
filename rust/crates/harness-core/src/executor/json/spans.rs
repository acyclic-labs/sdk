//! Runtime classification over the shared original-byte span cursor.
pub(crate) use acyclic_harness_json::spans::{SpanEvent, SpanKind};
pub(crate) type JsonSpans = acyclic_harness_json::spans::JsonSpans<super::RuntimeJson>;

#[cfg(test)]
#[path = "span_depth.rs"]
mod depth;
