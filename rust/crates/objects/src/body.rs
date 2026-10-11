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

#[derive(Clone)]
enum BodyReadMode {
    Ordinary,
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<dyn acyclic_native_runtime::NativeReadMeter>),
}
enum BodyReadError {
    Unavailable,
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::v1::NativeObjectsError),
}
impl From<BodyError> for BodyReadError {
    fn from(_: BodyError) -> Self { Self::Unavailable }
}
#[cfg(not(target_arch = "wasm32"))]
impl From<crate::v1::NativeObjectsError> for BodyReadError {
    fn from(error: crate::v1::NativeObjectsError) -> Self { Self::Native(error) }
}

enum BodyReadBuffer {
    Ordinary(Vec<u8>),
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        storage: acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    },
}
impl BodyReadBuffer {
    fn new(length: usize, mode: &BodyReadMode) -> Result<Self, BodyReadError> {
        match mode {
            BodyReadMode::Ordinary => Ok(Self::Ordinary(Vec::with_capacity(length))),
            #[cfg(not(target_arch = "wasm32"))]
            BodyReadMode::Native(meter) => Ok(Self::Native {
                storage: prepare_native_bytes(length, meter)?,
                meter: Arc::clone(meter),
            }),
        }
    }
    fn append(&mut self, written: usize, bytes: &[u8]) -> Result<(), BodyReadError> {
        match self {
            Self::Ordinary(output) => output.extend_from_slice(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { storage, meter } => {
                let end = written.checked_add(bytes.len()).ok_or(BodyReadError::Unavailable)?;
                let target = storage.writable().get_mut(written..end).ok_or(BodyReadError::Unavailable)?;
                meter.admit_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
                target.copy_from_slice(bytes);
                meter.record_codec(0, bytes.len() as u64, 0)
                    .map_err(crate::v1::NativeObjectsError::from)?;
            }
        }
        Ok(())
    }
    fn freeze(self, written: usize) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        match self {
            Self::Ordinary(output) => Ok(output.into()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { storage, .. } => storage.freeze(written).map_err(|_| BodyReadError::Unavailable),
        }
    }
}

struct OwnedBodyRead<'a> {
    future: BoxFuture<'a, Result<acyclic_stream::OwnedBytes, BodyReadError>>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl Future for OwnedBodyRead<'_> {
    type Output = Result<acyclic_stream::OwnedBytes, BodyReadError>;
    fn poll(self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}
fn box_body_read<'a, F>(
    future: F,
    mode: &BodyReadMode,
) -> Result<OwnedBodyRead<'a>, BodyReadError>
where
    F: Future<Output = Result<acyclic_stream::OwnedBytes, BodyReadError>> + Send + 'a,
{
    #[cfg(not(target_arch = "wasm32"))]
    let allocation = match mode {
        BodyReadMode::Ordinary => None,
        BodyReadMode::Native(meter) => Some(acyclic_native_runtime::ReadAllocation::new(
            meter, std::mem::size_of::<F>(),
        ).map_err(crate::v1::NativeObjectsError::from)?),
    };
    #[cfg(target_arch = "wasm32")]
    let _ = mode;
    Ok(OwnedBodyRead {
        future: Box::pin(future),
        #[cfg(not(target_arch = "wasm32"))]
        _allocation: allocation,
    })
}

#[derive(Clone)]
pub(crate) struct BodyParts(Arc<BodyPartsData>);
struct BodyPartsData {
    parts: Vec<StoredBody>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    _metadata: Option<acyclic_native_runtime::ReadAllocation>,
}
impl std::ops::Deref for BodyParts {
    type Target = [StoredBody];
    fn deref(&self) -> &Self::Target { &self.0.parts }
}
impl PartialEq for BodyParts {
    fn eq(&self, other: &Self) -> bool { (**self).eq(&**other) }
}
impl From<Vec<StoredBody>> for BodyParts {
    fn from(parts: Vec<StoredBody>) -> Self {
        Self(Arc::new(BodyPartsData {
            parts,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
            #[cfg(not(target_arch = "wasm32"))]
            _metadata: None,
        }))
    }
}
impl FromIterator<StoredBody> for BodyParts {
    fn from_iter<T: IntoIterator<Item = StoredBody>>(parts: T) -> Self {
        Self::from(Vec::from_iter(parts))
    }
}

/// Equality is structural: the same bytes or the same physical references.
#[derive(Clone, PartialEq)]
pub(crate) enum StoredBody {
    Memory(acyclic_stream::OwnedBytes),
    Composite {
        parts: BodyParts,
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
    pub(crate) fn memory(body: acyclic_stream::OwnedBytes) -> Self {
        // Establish any ordinary bytes refcount control at ingress, never on a
        // later native read. Prepared guarded carriers already have this control.
        drop(body.clone());
        Self::Memory(body)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn composite_measured(
        parts: Vec<StoredBody>,
        length: usize,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, crate::v1::NativeObjectsError> {
        let bytes = parts.capacity().checked_mul(std::mem::size_of::<StoredBody>());
        if bytes.and_then(|bytes| u64::try_from(bytes).ok()) != Some(allocation.bytes()) {
            drop(parts);
            drop(allocation);
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData).into());
        }
        let metadata = match acyclic_native_runtime::ReadAllocation::new(
            meter, std::mem::size_of::<BodyPartsData>(),
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                drop(parts);
                drop(allocation);
                return Err(error.into());
            }
        };
        Ok(Self::Composite {
            parts: BodyParts(Arc::new(BodyPartsData {
                parts, _allocation: Some(allocation), _metadata: Some(metadata),
            })),
            length,
        })
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
        self.relocated_with_mode(
            &|location, digest| relocations.get(&(location.clone(), *digest)).cloned(),
            &BodyReadMode::Ordinary,
        ).expect("ordinary relocation performs no fallible admission")
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn relocated_measured(
        &self,
        relocations: &LocalBodyRelocations,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Option<Self>, crate::v1::NativeObjectsError> {
        self.relocated_measured_with(
            &|location, digest| relocations.get(&(location.clone(), *digest)).cloned(),
            meter,
        )
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn relocated_measured_with(
        &self,
        lookup: &impl Fn(&LocalBodyLocation, &[u8; 32]) -> Option<LocalBodyLocation>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Option<Self>, crate::v1::NativeObjectsError> {
        meter.check()?;
        self.relocated_with_mode(lookup, &BodyReadMode::Native(Arc::clone(meter))).map_err(|error| {
            match error {
                BodyReadError::Native(error) => error,
                BodyReadError::Unavailable =>
                    crate::v1::Error::from(crate::v1::wire::ErrorCode::Unavailable).into(),
            }
        })
    }

    #[cfg(feature = "local")]
    fn has_relocation(
        &self,
        lookup: &impl Fn(&LocalBodyLocation, &[u8; 32]) -> Option<LocalBodyLocation>,
    ) -> bool {
        match self {
            Self::Memory(_) => false,
            Self::Composite { parts, .. } => parts.iter().any(|part| part.has_relocation(lookup)),
            Self::Local { location, digest, .. } => lookup(location, digest).is_some(),
        }
    }

    #[cfg(feature = "local")]
    fn relocated_with_mode(
        &self,
        lookup: &impl Fn(&LocalBodyLocation, &[u8; 32]) -> Option<LocalBodyLocation>,
        mode: &BodyReadMode,
    ) -> Result<Option<Self>, BodyReadError> {
        match self {
            Self::Memory(_) => Ok(None),
            Self::Composite { parts, length } => {
                if !parts.iter().any(|part| part.has_relocation(lookup)) { return Ok(None); }
                #[cfg(not(target_arch = "wasm32"))]
                let mut allocation = match mode {
                    BodyReadMode::Ordinary => None,
                    BodyReadMode::Native(meter) => Some(acyclic_native_runtime::ReadAllocation::new(
                        meter, parts.len().checked_mul(std::mem::size_of::<StoredBody>())
                            .ok_or(BodyReadError::Unavailable)?,
                    ).map_err(crate::v1::NativeObjectsError::from)?),
                };
                let mut moved = match mode {
                    BodyReadMode::Ordinary => Vec::with_capacity(parts.len()),
                    #[cfg(not(target_arch = "wasm32"))]
                    BodyReadMode::Native(meter) => {
                        let mut moved = Vec::new();
                        moved.try_reserve_exact(parts.len())
                            .map_err(|error| crate::v1::NativeObjectsError::from(meter.allocation_failed(error)))?;
                        allocation.as_mut().expect("native vector has original custody")
                            .reconcile_capacity(moved.capacity() * std::mem::size_of::<StoredBody>())
                            .map_err(crate::v1::NativeObjectsError::from)?;
                        moved
                    }
                };
                for part in parts.iter() {
                    let relocated = part.relocated_with_mode(lookup, mode)?;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let BodyReadMode::Native(meter) = mode {
                        meter.admit_codec(0, std::mem::size_of::<StoredBody>() as u64, 0)
                            .map_err(crate::v1::NativeObjectsError::from)?;
                    }
                    moved.push(relocated.unwrap_or_else(|| part.clone()));
                    #[cfg(not(target_arch = "wasm32"))]
                    if let BodyReadMode::Native(meter) = mode {
                        meter.record_codec(0, std::mem::size_of::<StoredBody>() as u64, 0)
                            .map_err(crate::v1::NativeObjectsError::from)?;
                    }
                }
                match mode {
                    BodyReadMode::Ordinary => Ok(Some(Self::Composite { parts: moved.into(), length: *length })),
                    #[cfg(not(target_arch = "wasm32"))]
                    BodyReadMode::Native(meter) => Ok(Some(Self::composite_measured(
                        moved, *length, meter, allocation.expect("native vector has original custody"),
                    )?)),
                }
            }
            Self::Local { root, digest, length, location } => Ok(lookup(location, digest).map(|location| {
                Self::Local { root: Arc::clone(root), digest: *digest, length: *length, location }
            })),
        }
    }

    pub(crate) fn read_async(
        &self,
        start: usize,
        end: usize,
    ) -> BoxFuture<'_, Result<acyclic_stream::OwnedBytes, BodyError>> {
        let span = crate::obs::span!(
            TRACE,
            "acyclic.objects.body.read",
            backend = self.backend(),
            bytes = end.saturating_sub(start),
        );
        Box::pin(crate::obs::traced(span, async move {
            self.read_inner_async(start, end, BodyReadMode::Ordinary).await
                .map_err(|_| BodyError::Unavailable)
        }))
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
    async fn read_inner_async(
        &self,
        start: usize,
        end: usize,
        mode: BodyReadMode,
    ) -> Result<acyclic_stream::OwnedBytes, BodyReadError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let BodyReadMode::Native(meter) = &mode {
            meter.check().map_err(crate::v1::NativeObjectsError::from)?;
        }
        if start > end || end > self.len() { return Err(BodyReadError::Unavailable); }
        match self {
            Self::Memory(body) => Ok(body.slice(start..end)),
            Self::Composite { parts, .. } => {
                let mut offset = 0usize;
                for part in parts.iter() {
                    let part_end = offset.checked_add(part.len()).ok_or(BodyReadError::Unavailable)?;
                    if start >= offset && end <= part_end {
                        return box_body_read(part.read_inner_async(start - offset, end - offset, mode.clone()), &mode)?.await;
                    }
                    offset = part_end;
                }
                let mut output = BodyReadBuffer::new(end - start, &mode)?;
                let mut written = 0usize;
                let mut offset = 0usize;
                for part in parts.iter() {
                    let part_end = offset.checked_add(part.len()).ok_or(BodyReadError::Unavailable)?;
                    if part_end > start && offset < end {
                        let bytes = box_body_read(part.read_inner_async(
                            start.saturating_sub(offset).min(part.len()),
                            end.saturating_sub(offset).min(part.len()),
                            mode.clone(),
                        ), &mode)?.await?;
                        output.append(written, bytes.as_slice())?;
                        written = written.checked_add(bytes.len()).ok_or(BodyReadError::Unavailable)?;
                    }
                    offset = part_end;
                }
                if written != end - start { return Err(BodyReadError::Unavailable); }
                output.freeze(written)
            }
            #[cfg(feature = "local")]
            Self::Local { root, digest, length, location } => match mode {
                BodyReadMode::Ordinary => crate::physical::read_body_at_async(
                    root, digest, *length, location, start, end,
                ).await.map_err(Into::into),
                #[cfg(not(target_arch = "wasm32"))]
                BodyReadMode::Native(meter) => crate::physical::read_body_at_native(
                    root, digest, *length, location, start, end, meter,
                ).await.map_err(Into::into),
            },
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn read_native(
        &self,
        start: usize,
        end: usize,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<acyclic_stream::OwnedBytes, crate::v1::NativeObjectsError> {
        let span = crate::obs::span!(
            TRACE,
            "acyclic.objects.body.read",
            backend = self.backend(),
            bytes = end.saturating_sub(start),
        );
        crate::obs::traced(span, async move {
            self.read_inner_async(start, end, BodyReadMode::Native(meter)).await.map_err(|error| {
                match error {
                    BodyReadError::Native(error) => error,
                    BodyReadError::Unavailable =>
                        crate::v1::Error::from(crate::v1::wire::ErrorCode::Unavailable).into(),
                }
            })
        }).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn prepare_native_bytes(
    length: usize,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>, crate::v1::NativeObjectsError> {
    use acyclic_native_runtime::ReadAllocation;
    use acyclic_stream::PreparedOwnedBytes;
    let mut allocation = ReadAllocation::new(meter, length)?;
    let mut data = Vec::new();
    data.try_reserve_exact(length)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    data.resize(data.capacity(), 0);
    PreparedOwnedBytes::new(data, allocation, Arc::clone(meter)).map_err(|failure| {
        crate::v1::native_preparation_error(failure.into_error())
    })
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
    fn resident_ranges_share_original_known_capacity_even_when_empty() {
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(b"abcdef");
        let capacity = bytes.capacity();
        let body = StoredBody::memory(bytes.into());
        let full = futures::executor::block_on(body.read_async(0, 6))
            .unwrap_or_else(|_| unreachable!());
        let selected = futures::executor::block_on(body.read_async(2, 4))
            .unwrap_or_else(|_| unreachable!());
        let empty = futures::executor::block_on(body.read_async(3, 3))
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(full.retained_capacity(), Some(capacity));
        assert_eq!(selected.retained_capacity(), Some(capacity));
        assert_eq!(empty.retained_capacity(), Some(capacity));
        assert_eq!(selected.as_slice().as_ptr(), full.as_slice()[2..].as_ptr());
        drop(body);
        drop(full);
        assert_eq!(selected.as_slice(), b"cd");
        assert!(empty.is_empty());
    }

    #[test]
    fn body_reads_trace_one_logical_span_and_terminal_outcomes() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            let body = StoredBody::Composite {
                parts: vec![
                    StoredBody::memory(acyclic_stream::OwnedBytes::from_static(b"ab")),
                    StoredBody::memory(acyclic_stream::OwnedBytes::from_static(b"cd")),
                ]
                .into(),
                length: 4,
            };
            let bytes = futures::executor::block_on(body.read_async(1, 3))
                .unwrap_or_else(|_| unreachable!());
            assert_eq!(bytes.as_ref(), b"bc");
            assert!(futures::executor::block_on(body.read_async(3, 2)).is_err());
            let malformed = StoredBody::Composite {
                parts: vec![StoredBody::memory(acyclic_stream::OwnedBytes::from_static(b"ab"))].into(),
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
