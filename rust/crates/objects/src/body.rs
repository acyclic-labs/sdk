//! Private immutable body storage for the logical Objects recovery engine.
use futures::future::BoxFuture;
#[cfg(feature = "local")]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum BodyError {
    #[error("object body storage is unavailable")]
    Unavailable,
}

/// Equality is structural: the same bytes or the same physical references.
#[derive(Clone, PartialEq)]
pub(crate) enum StoredBody {
    Memory(bytes::Bytes),
    Composite {
        parts: Arc<[StoredBody]>,
        length: usize,
    },
    #[cfg(feature = "local")]
    Local {
        root: Arc<crate::physical::LocalRoot>,
        digest: [u8; 32],
        length: usize,
        location: LocalBodyLocation,
    },
}

impl StoredBody {
    pub(crate) fn memory(body: bytes::Bytes) -> Self {
        Self::Memory(body)
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Memory(body) => body.len(),
            Self::Composite { length, .. } => *length,
            #[cfg(feature = "local")]
            Self::Local { length, .. } => *length,
        }
    }

    #[cfg(feature = "local")]
    pub(crate) fn local_references(&self, output: &mut BTreeSet<LocalBodyReference>) {
        match self {
            Self::Memory(_) => {}
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.local_references(output);
                }
            }
            Self::Local {
                digest,
                length,
                location,
                ..
            } => {
                output.insert(LocalBodyReference {
                    digest: *digest,
                    length: *length,
                    location: location.clone(),
                });
            }
        }
    }

    /// This body with every local leaf found in `relocations` moved, if any leaf moves.
    #[cfg(feature = "local")]
    pub(crate) fn relocated(&self, relocations: &LocalBodyRelocations) -> Option<Self> {
        match self {
            Self::Memory(_) => None,
            Self::Composite { parts, length } => {
                let moved = parts
                    .iter()
                    .map(|part| part.relocated(relocations))
                    .collect::<Vec<_>>();
                moved.iter().any(Option::is_some).then(|| Self::Composite {
                    parts: moved
                        .into_iter()
                        .zip(parts.iter())
                        .map(|(moved, part)| moved.unwrap_or_else(|| part.clone()))
                        .collect(),
                    length: *length,
                })
            }
            Self::Local {
                root,
                digest,
                length,
                location,
            } => relocations
                .get(&(location.clone(), *digest))
                .map(|destination| Self::Local {
                    root: Arc::clone(root),
                    digest: *digest,
                    length: *length,
                    location: destination.clone(),
                }),
        }
    }

    pub(crate) fn read_async(
        &self,
        start: usize,
        end: usize,
    ) -> BoxFuture<'_, Result<bytes::Bytes, BodyError>> {
        let span = crate::obs::span!(
            TRACE,
            "acyclic.objects.body.read",
            backend = self.backend(),
            bytes = end.saturating_sub(start),
        );
        Box::pin(crate::obs::traced(span, self.read_inner_async(start, end)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn backend(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Composite { .. } => "composite",
            #[cfg(feature = "local")]
            Self::Local { location, .. } => match location {
                LocalBodyLocation::Segment { .. } => "segment",
                LocalBodyLocation::Journal { .. } => "journal",
            },
        }
    }

    // Composite recursion shares one logical read span instead of emitting a span
    // for every constituent body.
    async fn read_inner_async(&self, start: usize, end: usize) -> Result<bytes::Bytes, BodyError> {
        if start > end || end > self.len() {
            return Err(BodyError::Unavailable);
        }
        match self {
            Self::Memory(body) => Ok(body.slice(start..end)),
            Self::Composite { parts, .. } => {
                let mut output = Vec::with_capacity(end.saturating_sub(start));
                let mut offset = 0usize;
                for part in parts.iter() {
                    let part_end = offset
                        .checked_add(part.len())
                        .ok_or(BodyError::Unavailable)?;
                    if part_end > start && offset < end {
                        let selected_start = start.saturating_sub(offset).min(part.len());
                        let selected_end = end.saturating_sub(offset).min(part.len());
                        output.extend_from_slice(
                            &Box::pin(part.read_inner_async(selected_start, selected_end)).await?,
                        );
                    }
                    offset = part_end;
                }
                if output.len() != end.saturating_sub(start) {
                    return Err(BodyError::Unavailable);
                }
                Ok(output.into())
            }
            #[cfg(feature = "local")]
            Self::Local {
                root,
                digest,
                length,
                location,
            } => crate::physical::read_body_at_async(root, digest, *length, location, start, end)
                .await
                .map_err(|_| BodyError::Unavailable),
        }
    }
}

#[cfg(feature = "local")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum LocalBodyLocation {
    /// A body record inside one immutable, content-addressed segment file.
    Segment { id: [u8; 32], offset: u64 },
    /// Bytes carried by the journal frame that committed the body.
    Journal { offset: u64 },
}

/// Physical moves of local bodies, keyed by current location and digest: an empty body
/// shares its journal offset with the inline body that follows it.
#[cfg(feature = "local")]
pub(crate) type LocalBodyRelocations = BTreeMap<(LocalBodyLocation, [u8; 32]), LocalBodyLocation>;

#[cfg(feature = "local")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LocalBodyReference {
    pub(crate) digest: [u8; 32],
    pub(crate) length: usize,
    pub(crate) location: LocalBodyLocation,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

    #[derive(Clone, Debug, Default)]
    pub(crate) struct CapturedSpan {
        pub(crate) name: &'static str,
        pub(crate) level: Option<tracing::Level>,
        pub(crate) id: u64,
        pub(crate) parent_id: Option<u64>,
        pub(crate) fields: BTreeMap<String, String>,
    }
    impl tracing::field::Visit for CapturedSpan {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().into(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields.insert(field.name().into(), value.into());
        }
    }
    #[derive(Clone, Default)]
    pub(crate) struct Capture(
        pub(crate) Arc<Mutex<Vec<CapturedSpan>>>,
        Arc<Mutex<BTreeMap<u64, CapturedSpan>>>,
    );
    impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::Id,
            ctx: Context<'_, S>,
        ) {
            let span = ctx.span(id).unwrap_or_else(|| unreachable!());
            let mut captured = CapturedSpan {
                name: attrs.metadata().name(),
                level: Some(*attrs.metadata().level()),
                id: id.into_u64(),
                parent_id: span.parent().map(|parent| parent.id().into_u64()),
                ..CapturedSpan::default()
            };
            attrs.record(&mut captured);
            self.1
                .lock()
                .unwrap_or_else(|_| unreachable!())
                .insert(captured.id, captured.clone());
            span.extensions_mut().insert(captured);
        }
        fn on_record(
            &self,
            id: &tracing::Id,
            values: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            let span = ctx.span(id).unwrap_or_else(|| unreachable!());
            let mut extensions = span.extensions_mut();
            let captured = extensions
                .get_mut::<CapturedSpan>()
                .unwrap_or_else(|| unreachable!());
            values.record(captured);
            self.1
                .lock()
                .unwrap_or_else(|_| unreachable!())
                .insert(captured.id, captured.clone());
        }
        fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
            let span = ctx.span(&id).unwrap_or_else(|| unreachable!());
            let captured = span
                .extensions_mut()
                .remove::<CapturedSpan>()
                .unwrap_or_else(|| unreachable!());
            self.1
                .lock()
                .unwrap_or_else(|_| unreachable!())
                .remove(&captured.id);
            self.0
                .lock()
                .unwrap_or_else(|_| unreachable!())
                .push(captured);
        }
    }
    impl Capture {
        /// Includes live parents retained by worker-owned child spans.
        #[cfg(feature = "local")]
        pub(crate) fn recorded_spans(&self, name: &str) -> Vec<CapturedSpan> {
            let mut spans = self.spans(name);
            spans.extend(
                self.1
                    .lock()
                    .unwrap_or_else(|_| unreachable!())
                    .values()
                    .filter(|span| span.name == name)
                    .cloned(),
            );
            spans
        }
        pub(crate) fn spans(&self, name: &str) -> Vec<CapturedSpan> {
            self.0
                .lock()
                .unwrap_or_else(|_| unreachable!())
                .iter()
                .filter(|span| span.name == name)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn body_reads_trace_one_logical_span_and_terminal_outcomes() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            let body = StoredBody::Composite {
                parts: vec![
                    StoredBody::memory(bytes::Bytes::from_static(b"ab")),
                    StoredBody::memory(bytes::Bytes::from_static(b"cd")),
                ]
                .into(),
                length: 4,
            };
            let bytes = futures::executor::block_on(body.read_async(1, 3))
                .unwrap_or_else(|_| unreachable!());
            assert_eq!(bytes.as_ref(), b"bc");
            assert!(futures::executor::block_on(body.read_async(3, 2)).is_err());
            let malformed = StoredBody::Composite {
                parts: vec![StoredBody::memory(bytes::Bytes::from_static(b"ab"))].into(),
                length: 4,
            };
            assert!(futures::executor::block_on(malformed.read_async(0, 4)).is_err());
            drop(body.read_async(0, 4));
        });
        let spans = capture.spans("acyclic.objects.body.read");
        assert_eq!(
            spans.len(),
            4,
            "composite leaves must not add logical spans"
        );
        assert!(
            spans
                .iter()
                .all(|span| span.level == Some(tracing::Level::TRACE))
        );
        let outcomes = spans
            .iter()
            .map(|span| {
                (
                    span.fields.get("outcome").map(String::as_str),
                    span.fields.get("error.kind").map(String::as_str),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                (Some("ok"), None),
                (Some("err"), Some("unavailable")),
                (Some("err"), Some("unavailable")),
                (Some("err"), Some("cancelled"))
            ]
        );
        assert!(spans.iter().all(|span| {
            span.fields
                .keys()
                .all(|key| matches!(key.as_str(), "backend" | "bytes" | "outcome" | "error.kind"))
        }));
    }
}
