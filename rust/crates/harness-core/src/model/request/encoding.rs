//! Canonical request continuations retain the hash state at the message cut.
//! Only a validated new suffix can extend that cut.
use super::*;

/// Exact immutable input observed by a consumer. Only its prepared owner can
/// issue this anchor; continuations retain the original configuration and cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputAnchor {
    digest: [u8; 32],
    messages: usize,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct InputStats {
    extra: u64,
    native: bool,
    expanding: bool,
}

impl InputStats {
    fn extend(&mut self, messages: &[ModelMessage], io: Option<&PreparationIo>) -> Result<()> {
        for message in messages {
            charge_items(io, 1)?;
            #[cfg(any(test, feature = "test-support"))]
            acyclic_differential::record(("input footprint messages", 1u64));
            self.expanding |=
                matches!(&message.content, ModelContent::Text(text) if text.trim().is_empty());
            for part in message.content.parts() {
                charge_items(io, 1)?;
                self.expanding |= match part {
                    ModelContentPart::Notices { .. } | ModelContentPart::File { .. } => true,
                    ModelContentPart::Text { text, .. } => text.trim().is_empty(),
                    _ => false,
                };
            }
            for (file, policy) in message.content.file_parts() {
                charge_items(io, 1)?;
                let bytes = policy.projected_bytes(file, u64::MAX)?;
                self.native |= matches!(policy, FileProjectionPolicy::Native(_));
                if !matches!(policy, FileProjectionPolicy::Reference) {
                    self.extra = bytes
                        .checked_mul(6)
                        .and_then(|bytes| self.extra.checked_add(bytes))
                        .ok_or_else(|| invalid(io, "input footprint size overflows"))?;
                }
            }
        }
        Ok(())
    }
}

/// Fixed-size facts about the same immutable cut as the canonical encoding.
/// Cold readers may compute independently; publication never waits on them.
#[derive(Clone, Debug, Default)]
pub(super) struct Input {
    stats: std::sync::OnceLock<InputStats>,
    observed: std::sync::OnceLock<InputAnchor>,
    inherited: Option<InputAnchor>,
}

impl Input {
    fn stats(&self, request: &ModelRequest) -> Result<InputStats> {
        if let Some(stats) = self.stats.get() {
            return Ok(*stats);
        }
        let mut stats = InputStats::default();
        stats.extend(&request.messages, None)?;
        let _ = self.stats.set(stats);
        Ok(stats)
    }

    pub(super) fn extend(
        self,
        suffix: &[ModelMessage],
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let inherited = self.observed.get().copied().or(self.inherited);
        let stats = std::sync::OnceLock::new();
        if let Some(mut prior) = self.stats.into_inner() {
            prior.extend(suffix, io)?;
            let _ = stats.set(prior);
        }
        Ok(Self {
            stats,
            observed: Default::default(),
            inherited,
        })
    }
}

impl PreparedModelRequest {
    /// Total admitted source bytes, including instruction framing, bounded by
    /// the reader's aggregate credit before any file I/O. Reference-only
    /// attachments contribute zero. Warm requests never revisit history.
    pub fn projected_file_bytes(&self, maximum: u64) -> Result<u64> {
        let bytes = self.input.stats(&self.request)?.extra / 6;
        if bytes > maximum {
            return Err(invalid(
                None,
                "model files exceed projection aggregate byte limit",
            ));
        }
        Ok(bytes)
    }

    /// Canonical input plus a conservative JSON expansion of pinned bodies.
    /// Reads no files and never serializes or hashes admitted messages.
    pub fn input_bytes(&self) -> Result<u64> {
        (self.bytes.len() as u64)
            .checked_add(self.input.stats(&self.request)?.extra)
            .ok_or_else(|| invalid(None, "input footprint size overflows"))
    }

    /// Whether compressed native media prevents a byte-based token bound.
    pub fn input_has_native_media(&self) -> Result<bool> {
        Ok(self.input.stats(&self.request)?.native)
    }

    /// Whether an appended byte bound can omit provider rendering work.
    /// Blank runs and expandable boundary items require actual rendering.
    pub fn input_has_byte_bound(&self) -> Result<bool> {
        let stats = self.input.stats(&self.request)?;
        Ok(!stats.native && !stats.expanding)
    }

    /// Retains this exact input as an observation for future continuations.
    /// Provider token totals remain owned by the observing provider.
    pub fn observe_input(&self) -> Result<InputAnchor> {
        let anchor = InputAnchor {
            digest: self.request_digest,
            messages: self.request.messages.len(),
            bytes: self.input_bytes()?,
        };
        let _ = self.input.observed.set(anchor);
        Ok(anchor)
    }

    /// Added bytes after an exact observed prefix. A retained continuation
    /// checks fixed-size custody; independently prepared input verifies its
    /// cold prefix once per query using the complete canonical configuration.
    pub fn input_since(&self, anchor: InputAnchor) -> Result<Option<u64>> {
        let matches =
            if self.request_digest == anchor.digest || self.input.inherited == Some(anchor) {
                true
            } else {
                let Some(messages) = self.request.messages.get(..anchor.messages) else {
                    return Ok(None);
                };
                #[cfg(any(test, feature = "test-support"))]
                acyclic_differential::record(("input prefix rebuilds", 1u64));
                crate::executor::json::canonical_json_digest(
                    &Envelope::new(&self.request, messages),
                    u64::MAX,
                    &mut None,
                )? == anchor.digest
            };
        Ok(if matches {
            self.input_bytes()?.checked_sub(anchor.bytes)
        } else {
            None
        })
    }
}

pub(super) type MessageDigests = ([u8; 32], Option<[u8; 32]>);
type Encoded = (acyclic_stream::OwnedBytes, [u8; 32], Vec<EncodingPrefix>);

fn invalid(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, reason)
}

#[derive(Clone, Debug)]
pub(super) struct EncodingPrefix {
    request: blake3::Hasher,
    messages: blake3::Hasher,
    end: usize,
    count: usize,
    previous: Option<(usize, [u8; 32])>,
}

#[derive(Serialize)]
struct Envelope<'a> {
    compose_tools: bool,
    max_output_tokens: Option<u32>,
    messages: &'a [ModelMessage],
    model: &'a Model,
    #[serde(skip_serializing_if = "crate::model::ModelSampling::is_default")]
    sampling: crate::model::ModelSampling,
    summary: bool,
    tools: &'a [crate::tool::ToolDefinition],
}

impl<'a> Envelope<'a> {
    fn new(request: &'a ModelRequest, messages: &'a [ModelMessage]) -> Self {
        Self {
            summary: request.summary,
            compose_tools: request.compose_tools,
            sampling: request.sampling,
            max_output_tokens: request.max_output_tokens,
            messages,
            model: &request.model,
            tools: &request.tools,
        }
    }
}

fn encode<T: Serialize>(
    value: &T,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    match io {
        #[cfg(feature = "filesystem")]
        Some(io) => io.encode(value, io.remaining()?.bytes_encoded, true),
        _ => Ok(crate::contract::canonical_json_bytes(value)?.into()),
    }
}

fn hash(digest: &mut blake3::Hasher, bytes: &[u8], io: Option<&PreparationIo>) -> Result<()> {
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(bytes.len() as u64)?;
        }
        digest.update(bytes);
        Ok(())
    })
}

fn storage_error(error: std::io::Error, io: Option<&PreparationIo>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
        return work.runtime_error(error).into();
    }
    let _ = io;
    Error::Storage(error.to_string())
}

fn envelope(
    request: &ModelRequest,
    io: Option<&PreparationIo>,
) -> Result<(acyclic_stream::OwnedBytes, usize)> {
    let bytes = encode(&Envelope::new(request, &[]), io)?;
    // The fixed top-level delimiter precedes model and tool strings.
    let marker = b"\"messages\":[]";
    let open = bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .and_then(|start| start.checked_add(marker.len() - 1))
        .ok_or_else(|| invalid(io, "canonical request envelope lacks messages"))?;
    Ok((bytes, open))
}

fn fresh(request: &ModelRequest, io: Option<&PreparationIo>) -> Result<Encoded> {
    let bytes = encode(request, io)?;
    #[cfg(feature = "filesystem")]
    if let Some(hold) = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten() {
        hold.into_retained()?;
    }
    let digest = hash_bytes(&bytes, io)?;
    Ok((bytes, digest, Vec::new()))
}

enum Buffer {
    Unique(acyclic_stream::UniqueOwnedBytes),
    #[cfg(feature = "filesystem")]
    Finite(Vec<u8>),
}

impl Buffer {
    fn new(
        bytes: acyclic_stream::OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> std::result::Result<Self, acyclic_stream::OwnedBytes> {
        #[cfg(feature = "filesystem")]
        if io.is_some_and(|io| {
            #[cfg(not(target_arch = "wasm32"))]
            {
                io.original_native_work().is_none()
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = io;
                true
            }
        }) {
            return bytes.try_into_ordinary_vec().map(Self::Finite);
        }
        let _ = io;
        bytes.try_into_unique().map(Self::Unique)
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Unique(bytes) => bytes.capacity(),
            #[cfg(feature = "filesystem")]
            Self::Finite(bytes) => bytes.capacity(),
        }
    }

    fn reserve(&mut self, capacity: usize, io: Option<&PreparationIo>) -> Result<()> {
        match self {
            Self::Unique(bytes) => bytes
                .reserve_capacity(capacity)
                .map_err(|error| storage_error(error, io)),
            #[cfg(feature = "filesystem")]
            Self::Finite(bytes) => crate::executor::stock_vec_reserve(bytes, capacity, io),
        }
    }

    fn replace(&mut self, prefix: usize, suffix: &[u8], io: Option<&PreparationIo>) -> Result<()> {
        match self {
            Self::Unique(bytes) => bytes
                .replace_suffix(prefix, suffix)
                .map_err(|error| storage_error(error, io)),
            #[cfg(feature = "filesystem")]
            Self::Finite(bytes) => with_history_io(io, |work| {
                if let Some(work) = work.as_deref_mut() {
                    work.admit_copy(suffix.len() as u64)?;
                }
                bytes.truncate(prefix);
                bytes.extend_from_slice(suffix);
                if let Some(work) = work.as_deref_mut() {
                    work.copy_bytes(suffix.len() as u64)?;
                }
                Ok(())
            }),
        }
    }

    fn freeze(self) -> acyclic_stream::OwnedBytes {
        match self {
            Self::Unique(bytes) => bytes.freeze(),
            #[cfg(feature = "filesystem")]
            Self::Finite(bytes) => bytes.into(),
        }
    }
}

impl EncodingPrefix {
    /// `old_bytes` and `old_count` belong to the immutable admitted prefix.
    /// The caller has validated every message following that prefix.
    pub(super) fn append(
        old_bytes: acyclic_stream::OwnedBytes,
        mut state: Vec<Self>,
        old_count: usize,
        request: &ModelRequest,
        io: Option<&PreparationIo>,
        projection_nodes: &mut usize,
    ) -> Result<Encoded> {
        let (envelope, open) = envelope(request, io)?;
        #[cfg(feature = "filesystem")]
        let _envelope_hold = io
            .map(|io| io.guard_bytes(&envelope))
            .transpose()?
            .flatten();
        let trailer = envelope
            .get(open..)
            .ok_or_else(|| invalid(io, "canonical request envelope is truncated"))?;
        if state.is_empty() {
            let prefix = Self::capture(&old_bytes, old_count, open, &envelope, io)?;
            crate::executor::stock_vec_push(&mut state, prefix, io)?;
        }
        let prefix = state
            .first_mut()
            .filter(|prefix| prefix.count == old_count)
            .ok_or_else(|| invalid(io, "canonical request cut differs from its messages"))?;
        let suffix = request
            .messages
            .get(old_count..)
            .ok_or_else(|| invalid(io, "canonical request prefix exceeds messages"))?;
        if suffix.is_empty() {
            let mut complete = prefix.request.clone();
            hash(&mut complete, trailer, io)?;
            return Ok((old_bytes, *complete.finalize().as_bytes(), state));
        }
        prefix.previous = Some((old_count, prefix.messages_digest(io)?));
        let encoded = encode(&suffix, io)?;
        #[cfg(feature = "filesystem")]
        let _encoded_hold = io.map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
        let appended = encoded
            .get(1..encoded.len().saturating_sub(1))
            .ok_or_else(|| invalid(io, "canonical request suffix is not an array"))?;
        if *projection_nodes != 0 {
            // The unchanged envelope and prior messages retain their count.
            // The scanner's initial node is framing, not part of this suffix.
            let added = crate::executor::json::encoded_json_shape(appended).1 - 1;
            *projection_nodes = projection_nodes
                .checked_add(added)
                .and_then(|nodes| nodes.checked_add(usize::from(old_count != 0)))
                .ok_or_else(|| invalid(io, "canonical request shape overflows"))?;
        }
        let Ok(bytes) = Buffer::new(old_bytes, io) else {
            // An explicit PreparedModelRequest clone shares its immutable
            // encoding. Give the clone new storage; never mutate its peer.
            return fresh(request, io);
        };
        let (bytes, digest) =
            prefix.extend(bytes, appended, trailer, request.messages.len(), io)?;
        Ok((bytes, digest, state))
    }

    fn capture(
        old_bytes: &[u8],
        count: usize,
        open: usize,
        envelope: &[u8],
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let trailer = envelope
            .get(open..)
            .ok_or_else(|| invalid(io, "canonical request envelope is truncated"))?;
        let end = old_bytes
            .len()
            .checked_sub(trailer.len())
            .filter(|end| *end >= open)
            .ok_or_else(|| invalid(io, "canonical request prefix is truncated"))?;
        let head = envelope
            .get(..open)
            .ok_or_else(|| invalid(io, "canonical request envelope is truncated"))?;
        if !old_bytes.starts_with(head) || !old_bytes.ends_with(trailer) {
            return Err(invalid(io, "canonical request envelope changed"));
        }
        let mut prefix = Self {
            request: blake3::Hasher::new(),
            messages: blake3::Hasher::new(),
            end,
            count,
            previous: None,
        };
        let request = old_bytes
            .get(..end)
            .ok_or_else(|| invalid(io, "canonical request prefix is truncated"))?;
        let array_start = open
            .checked_sub(1)
            .ok_or_else(|| invalid(io, "canonical request messages lack an array"))?;
        let messages = old_bytes
            .get(array_start..end)
            .ok_or_else(|| invalid(io, "canonical request messages are truncated"))?;
        hash(&mut prefix.request, request, io)?;
        hash(&mut prefix.messages, messages, io)?;
        Ok(prefix)
    }

    fn extend(
        &mut self,
        mut bytes: Buffer,
        appended: &[u8],
        trailer: &[u8],
        count: usize,
        io: Option<&PreparationIo>,
    ) -> Result<(acyclic_stream::OwnedBytes, [u8; 32])> {
        let separator: &[u8] = if self.count == 0 { b"" } else { b"," };
        let end = self
            .end
            .checked_add(separator.len())
            .and_then(|end| end.checked_add(appended.len()))
            .ok_or_else(|| invalid(io, "canonical request size overflows"))?;
        let length = end
            .checked_add(trailer.len())
            .ok_or_else(|| invalid(io, "canonical request size overflows"))?;
        if length > bytes.capacity() {
            let capacity = bytes
                .capacity()
                .checked_mul(2)
                .unwrap_or(length)
                .max(length);
            bytes.reserve(capacity, io)?;
        }
        bytes.replace(self.end, separator, io)?;
        bytes.replace(self.end + separator.len(), appended, io)?;
        bytes.replace(end, trailer, io)?;
        hash(&mut self.request, separator, io)?;
        hash(&mut self.request, appended, io)?;
        hash(&mut self.messages, separator, io)?;
        hash(&mut self.messages, appended, io)?;
        self.end = end;
        self.count = count;
        let mut complete = self.request.clone();
        hash(&mut complete, trailer, io)?;
        Ok((bytes.freeze(), *complete.finalize().as_bytes()))
    }

    pub(super) fn messages_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        let mut closed = self.messages.clone();
        hash(&mut closed, b"]", io)?;
        Ok(*closed.finalize().as_bytes())
    }

    pub(super) fn digests(
        &self,
        prefix: Option<usize>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<MessageDigests>> {
        let content = self.messages_digest(io)?;
        let selected = match prefix {
            None => None,
            Some(count) if count == self.count => Some(content),
            Some(count) => match self.previous.filter(|(previous, _)| *previous == count) {
                Some((_, digest)) => Some(digest),
                None => return Ok(None),
            },
        };
        Ok(Some((content, selected)))
    }
}

#[cfg(test)]
mod tests;
