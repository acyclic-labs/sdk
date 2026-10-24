//! Canonical request continuations retain the hash state at the message cut.
//! Only a validated new suffix can extend that cut.
use super::*;

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
    max_output_tokens: Option<u32>,
    messages: &'a [ModelMessage],
    model: &'a Model,
    tools: &'a [crate::tool::ToolDefinition],
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
    let bytes = encode(
        &Envelope {
            max_output_tokens: request.max_output_tokens,
            messages: &[],
            model: &request.model,
            tools: &request.tools,
        },
        io,
    )?;
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
