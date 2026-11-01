//! Complete terminal diagnostics, split into ordinary Streams records. A
//! terminal publication conditions on the complete staged prefix; interrupted
//! staging cannot expose a partial diagnostic as a completed agent result.

use super::{AgentPath, Error};
use acyclic_stream::{
    AppendOutcome, AppendRequest, ReadRequest, RecordStream, StreamPath, StreamProvider,
    StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

// Packing threshold, not an admission limit. Even six-byte JSON escapes leave
// room for the terminal record's metadata. Larger text uses raw record chunks.
pub(super) const INLINE_BYTES: usize = acyclic_stream::MAX_RECORD_BYTES / 8;

/// An immutable, complete diagnostic on the failed agent's own stream subtree.
/// Consumers read only the requested record interval; no live-agent inspection
/// or reconstruction of a later result is involved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct Diagnostic {
    /// Original failed agent.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
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
        StreamPath::new(format!(
            "{}/diagnostics/{}/{}",
            self.agent.as_str(),
            self.agent.id().as_str(),
            blake3::Hash::from_bytes(self.digest).to_hex()
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
        Ok(Box::pin(futures::stream::try_unfold(
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
        )))
    }

    /// Stages only bounded records. Exact retries use the original per-chunk
    /// identity; failures propagate without an independent retry or tail probe.
    pub(super) async fn stage<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        text: &str,
    ) -> Result<(), Error> {
        let path = self.path()?;
        for (index, chunk) in text
            .as_bytes()
            .chunks(acyclic_stream::MAX_RECORD_BYTES)
            .enumerate()
        {
            let request = AppendRequest {
                path: path.clone(),
                records: vec![chunk.to_vec().into()],
                if_tail: Some(index as u64),
                idempotency_key: super::intent_key(
                    "diagnostic",
                    &[path.as_str(), &index.to_string()],
                )?,
            };
            match provider.append(request).await.map_err(Error::Stream)? {
                AppendOutcome::Committed(_) => {}
                AppendOutcome::TailConflict { .. } => {
                    return Err(Error::Record("diagnostic prefix differs"));
                }
            }
        }
        Ok(())
    }
}
