//! Complete terminal diagnostics, split into ordinary Streams records. A
//! terminal publication conditions on the complete staged prefix; interrupted
//! staging cannot expose a partial diagnostic as a completed agent result.

use super::{AgentPath, Error};
use acyclic_stream::{ReadRequest, RecordStream, StreamPath, StreamProvider};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

// Packing threshold, not an admission limit. Even six-byte JSON escapes leave
// room for the terminal record's metadata. Larger text uses raw record chunks.
pub(super) const INLINE_BYTES: usize = super::MAX_MESSAGE_BYTES;

/// An immutable, complete diagnostic on the failed agent's own stream subtree.
/// Consumers read only the requested record interval; no live-agent inspection
/// or reconstruction of a later result is involved.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct Diagnostic {
    /// Original failed agent.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    #[schemars(with = "String")]
    pub agent: AgentPath,
    /// Digest of the complete original UTF-8 text.
    pub digest: [u8; 32],
    /// Complete byte length, independent of record and page sizes.
    pub bytes: u64,
}

impl Diagnostic {
    pub(super) fn new(agent: &AgentPath, text: &str) -> Self {
        Self {
            agent: agent.clone(),
            digest: *blake3::hash(text.as_bytes()).as_bytes(),
            bytes: text.len() as u64,
        }
    }

    /// Original stream containing raw text chunks, in byte order. The original
    /// agent identity keeps inherited ancestor diagnostics in a distinct slot.
    pub fn path(&self) -> Result<StreamPath, Error> {
        Self::stream_path(&self.agent, &self.digest)
    }

    /// Only this original agent's diagnostic slot, excluding inherited slots.
    pub fn directory(agent: &AgentPath) -> Result<StreamPath, Error> {
        StreamPath::new(format!("{}/diagnostics/{}", agent, agent.id())).map_err(Error::Stream)
    }

    /// One immutable diagnostic stream, also used by sans-IO client demands.
    pub fn stream_path(agent: &AgentPath, digest: &[u8; 32]) -> Result<StreamPath, Error> {
        StreamPath::new(format!(
            "{}/diagnostics/{}/{}",
            agent,
            agent.id(),
            blake3::Hash::from_bytes(*digest).to_hex()
        ))
        .map_err(Error::Stream)
    }

    /// Number of records in this immutable prefix.
    #[must_use]
    pub const fn records(&self) -> u64 {
        self.bytes.div_ceil(acyclic_stream::MAX_RECORD_BYTES as u64)
    }

    /// Reads at most `budget` original chunks, starting at `from`. A zero
    /// budget or a position outside this diagnostic is refused before IO.
    pub async fn page<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        from: u64,
        budget: u32,
    ) -> Result<RecordStream, Error> {
        if budget == 0 || from >= self.records() {
            return Err(Error::Record(
                "diagnostic page is outside its retained prefix",
            ));
        }
        let count = (self.records() - from).min(u64::from(budget)) as u32;
        let records = provider
            .read(ReadRequest {
                key: None,
                path: self.path()?,
                from,
                limit: count,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let end = from + u64::from(count);
        let bytes = self.bytes;
        Ok(records.map_cursor(|records| {
            Box::pin(futures::stream::try_unfold(
                (records, from),
                move |(mut records, next)| async move {
                    if next == end {
                        return Ok(None);
                    }
                    let record = records
                        .try_next()
                        .await?
                        .ok_or(acyclic_stream::StreamError::InvalidResponse)?;
                    let length = (bytes - next * acyclic_stream::MAX_RECORD_BYTES as u64)
                        .min(acyclic_stream::MAX_RECORD_BYTES as u64);
                    if record.sequence != next || record.value.len() as u64 != length {
                        return Err(acyclic_stream::StreamError::InvalidResponse);
                    }
                    Ok(Some((record, (records, next + 1))))
                },
            ))
        }))
    }

    /// Stages only bounded records. Exact retries use the original per-chunk
    /// identity; failures propagate without an independent retry or tail probe.
    pub(super) async fn stage<P: crate::store::JournalProvider + ?Sized>(
        &self,
        provider: &std::sync::Arc<P>,
        text: &str,
    ) -> Result<(), Error> {
        let path = self.path()?;
        crate::store::operations::stage_chunks(provider, &path, text.as_bytes(), None, |index| {
            super::intent_key("diagnostic", &[path.as_str(), &index.to_string()])
                .map(|key| (key, 0))
                .map_err(crate::Error::from)
        })
        .await
        .map_err(|cause| Error::Turn(Box::new(cause)))
    }
}

/// Immutable file-query prefix consumed by the existing read tool.
pub const QUERY_PREFIX: &str = ".volumes/system/diagnostics/";

/// One bounded UTF-8 interval of the original diagnostic.
#[derive(Debug, PartialEq, Eq)]
pub struct DiagnosticRead {
    /// Original bytes selected, before any presentation encoding.
    pub text: String,
    /// Inclusive byte offset.
    pub start: u64,
    /// Exclusive byte offset; always an original UTF-8 boundary.
    pub end: u64,
    /// Original complete byte length.
    pub total: u64,
}

/// One shared range validator for ordinary, funded and cached record readers.
pub struct DiagnosticRange<'a> {
    diagnostic: &'a Diagnostic,
    start: u64,
    end: u64,
    first: u64,
    next: u64,
    through: u64,
}

impl Diagnostic {
    /// Immutable model-visible file query. The digest binds continuation reads.
    pub fn query_path(&self) -> String {
        format!(
            "{QUERY_PREFIX}{}/{}",
            self.agent,
            blake3::Hash::from_bytes(self.digest).to_hex()
        )
    }

    /// Decode only the query identity; the published outcome supplies its size.
    pub fn query_target(path: &str) -> Result<Option<(AgentPath, [u8; 32])>, Error> {
        let Some(path) = path.strip_prefix(QUERY_PREFIX) else {
            return Ok(None);
        };
        let (agent, digest) = path
            .rsplit_once('/')
            .ok_or(Error::Record("invalid diagnostic query"))?;
        let hash = blake3::Hash::from_hex(digest)
            .map_err(|_| Error::Record("invalid diagnostic digest"))?;
        if hash.to_hex().as_str() != digest {
            return Err(Error::Record("noncanonical diagnostic digest"));
        }
        Ok(Some((
            AgentPath::try_from(agent.to_owned())?,
            *hash.as_bytes(),
        )))
    }

    /// Plan at most one physical record's bytes, with a second record only
    /// when this interval crosses a chunk edge. This is paging, never a size cap.
    pub fn range(&self, from: u64, maximum_bytes: u64) -> DiagnosticRange<'_> {
        let from = from.min(self.bytes);
        let chunk = acyclic_stream::MAX_RECORD_BYTES as u64;
        let end = from
            .saturating_add(maximum_bytes.min(chunk))
            .min(self.bytes);
        let first = from / chunk;
        DiagnosticRange {
            diagnostic: self,
            start: from,
            end,
            first,
            next: first,
            through: if from == end {
                first
            } else {
                end.div_ceil(chunk)
            },
        }
    }

    /// Bounded read for a session or ordinary provider. Native model reads use
    /// the same range validator with their original funded record producer.
    pub async fn read_text_with<F, R>(
        &self,
        from: u64,
        maximum_bytes: u64,
        read: F,
    ) -> Result<DiagnosticRead, Error>
    where
        F: FnOnce(ReadRequest) -> R,
        R: std::future::Future<Output = Result<RecordStream, acyclic_stream::StreamError>>,
    {
        let mut range = self.range(from, maximum_bytes);
        let mut bytes = Vec::with_capacity(range.capacity());
        if range.records() != 0 {
            let mut records = read(range.request()?).await.map_err(Error::Stream)?;
            while !range.complete() {
                let record = records
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .ok_or(Error::Record("diagnostic prefix is incomplete"))?;
                bytes.extend_from_slice(range.take_record(&record)?);
            }
        }
        range.finish(bytes)
    }
}

impl DiagnosticRange<'_> {
    /// Exact maximum payload retained for this page.
    pub fn capacity(&self) -> usize {
        (self.end - self.start) as usize
    }
    /// First original chunk.
    pub fn first(&self) -> u64 {
        self.first
    }
    /// Number of original chunks; at most two.
    pub fn records(&self) -> u32 {
        (self.through - self.first) as u32
    }
    /// All selected original chunks have been consumed.
    pub fn complete(&self) -> bool {
        self.next == self.through
    }
    /// Exact immutable finite read; callers attach their existing credential.
    pub fn request(&self) -> Result<ReadRequest, Error> {
        Ok(ReadRequest {
            key: None,
            path: self.diagnostic.path()?,
            from: self.first,
            limit: self.records(),
            at_least: Default::default(),
        })
    }
    /// Validate the original ordinal and chunk size, then borrow only its
    /// selected interval. The record retains its producer's original custody.
    pub fn take_record<'a>(
        &mut self,
        record: &'a acyclic_stream::Record,
    ) -> Result<&'a [u8], Error> {
        self.take_chunk(record.sequence, &record.value)
    }
    /// The same validation over a client's immutable record cache.
    pub fn take_chunk<'a>(&mut self, sequence: u64, value: &'a [u8]) -> Result<&'a [u8], Error> {
        let chunk = acyclic_stream::MAX_RECORD_BYTES as u64;
        if self.complete() || sequence != self.next {
            return Err(Error::Record("diagnostic chunk ordinal differs"));
        }
        let offset = self.next * chunk;
        let expected = (self.diagnostic.bytes - offset).min(chunk);
        if value.len() as u64 != expected {
            return Err(Error::Record("diagnostic chunk length differs"));
        }
        let from = self.start.saturating_sub(offset) as usize;
        let to = (self.end - offset).min(expected) as usize;
        self.next += 1;
        value
            .get(from..to)
            .ok_or(Error::Record("invalid diagnostic interval"))
    }
    /// Preserve complete characters at page edges, so the returned end is the
    /// next exact byte cursor and no Unicode byte is replaced or skipped.
    pub fn finish(self, mut bytes: Vec<u8>) -> Result<DiagnosticRead, Error> {
        if !self.complete() || bytes.len() != self.capacity() {
            return Err(Error::Record("diagnostic page is incomplete"));
        }
        if let Err(error) = std::str::from_utf8(&bytes) {
            if self.end < self.diagnostic.bytes && error.error_len().is_none() {
                bytes.truncate(error.valid_up_to());
            } else {
                return Err(Error::Record(
                    "diagnostic range requires an original UTF-8 boundary",
                ));
            }
        }
        if bytes.is_empty() && self.start != self.end {
            return Err(Error::Record(
                "diagnostic page cannot hold its next character",
            ));
        }
        let text =
            String::from_utf8(bytes).map_err(|_| Error::Record("diagnostic text is not UTF-8"))?;
        Ok(DiagnosticRead {
            end: self.start + text.len() as u64,
            text,
            start: self.start,
            total: self.diagnostic.bytes,
        })
    }
}
