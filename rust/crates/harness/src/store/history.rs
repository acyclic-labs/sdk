//! Pinned, bounded reads from the existing authoritative aggregate Stream.

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event},
    wire_codec::decode_event,
};
use acyclic_stream::{Stream, StreamClient, StreamProvider};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

/// Explicit archival position. Revisions are inclusive; later appends are excluded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCursor {
    /// Aggregate whose admission attestations must verify.
    pub authority: Authority,
    /// Last event already consumed (zero starts at the first event).
    pub after_revision: u64,
    /// Fixed committed boundary for this traversal.
    pub through_revision: u64,
}

/// Finite work and output bounds for one history read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryReadLimits {
    /// Maximum number of decoded events.
    pub maximum_events: u32,
    /// Maximum total encoded record bytes, checked before decoding each record.
    pub maximum_bytes: u64,
}

/// One verified page and its resumable position. An empty page is terminal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage {
    /// Ordered, gapless events attested by this aggregate's owner.
    pub events: Vec<Event>,
    /// Same pinned boundary, advanced only over returned events.
    pub cursor: HistoryCursor,
}

/// An owner-bound Stream handle, with no reducer, history cache or replay on open.
pub struct HistoryReader<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    verifier: AuthorityVerifier,
}

impl<P: StreamProvider> HistoryReader<P> {
    /// Binds the existing canonical Stream without reading or restoring history.
    pub fn new(
        client: &StreamClient<P>,
        authority: &Authority,
        verifier: AuthorityVerifier,
    ) -> Result<Self> {
        verifier.verify_audience(authority)?;
        Ok(Self {
            client: client.clone(),
            stream: client.stream(authority.stream_path()?)?,
            verifier,
        })
    }

    /// Resolves a globally unique admitted operation without restoring its aggregate.
    /// This identity lookup is independent of a paginated traversal's pinned boundary.
    pub async fn operation_event(&self, operation: crate::OperationId) -> Result<Option<Event>> {
        super::operations::find_operation(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            operation,
        )
        .await
    }

    /// Resolves one message at the pinned boundary using its atomic locator.
    /// Reads at most one locator and one canonical record; no history scan or
    /// reducer hydration occurs. The byte budget includes both encoded records.
    pub async fn conversation_message(
        &self,
        cursor: &HistoryCursor,
        message_id: uuid::Uuid,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        self.verifier.verify_audience(&cursor.authority)?;
        if cursor.after_revision > cursor.through_revision
            || cursor.through_revision > self.committed_tail().await?
        {
            return Err(Error::Invalid("message lookup cursor is invalid".into()));
        }
        super::operations::find_message(
            &self.client,
            &cursor.authority,
            &self.verifier,
            message_id,
            cursor.through_revision,
            maximum_bytes,
        )
        .await
    }

    /// Captures a committed boundary once for explicit archival traversal.
    pub async fn pin(&self, after_revision: u64) -> Result<HistoryCursor> {
        let through_revision = self.committed_tail().await?;
        if after_revision > through_revision {
            return Err(Error::Invalid(
                "history cursor exceeds committed tail".into(),
            ));
        }
        Ok(HistoryCursor {
            authority: self.verifier.audience().clone(),
            after_revision,
            through_revision,
        })
    }

    /// Reads at most one caller-bounded page; a failure leaves the supplied cursor intact.
    pub async fn read_page(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
    ) -> Result<HistoryPage> {
        self.verifier.verify_audience(&cursor.authority)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "history read bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail().await? {
            return Err(Error::Invalid(
                "history cursor exceeds committed tail".into(),
            ));
        }
        let mut next = cursor.clone();
        let count = u32::try_from(
            (cursor.through_revision - cursor.after_revision).min(u64::from(limits.maximum_events)),
        )
        .map_err(|_| Error::Invalid("history page count exceeds platform bounds".into()))?;
        let mut events = Vec::new();
        if count == 0 {
            return Ok(HistoryPage {
                events,
                cursor: next,
            });
        }
        let mut records = self.stream.read(cursor.after_revision, count).await?;
        let mut bytes = 0_u64;
        while let Some(record) = records.try_next().await? {
            if events.len() >= count as usize || record.sequence != next.after_revision {
                return Err(Error::Storage(
                    "history page is not bounded and contiguous".into(),
                ));
            }
            bytes = bytes
                .checked_add(record.value.len() as u64)
                .filter(|total| *total <= limits.maximum_bytes)
                .ok_or_else(|| Error::Invalid("history page exceeds byte bound".into()))?;
            let event = verify_history_record(
                &self.verifier,
                &cursor.authority,
                next.after_revision,
                &record,
            )?;
            next.after_revision = event.revision;
            events.push(event);
        }
        if events.is_empty() {
            return Err(Error::Storage(
                "history page stopped before its pinned boundary".into(),
            ));
        }
        Ok(HistoryPage {
            events,
            cursor: next,
        })
    }

    async fn committed_tail(&self) -> Result<u64> {
        match self.stream.tail().await {
            Ok(tail) => Ok(tail),
            Err(acyclic_stream::StreamError::NotFound) => Ok(0),
            Err(error) => Err(error.into()),
        }
    }
}

pub(crate) fn verify_history_record(
    verifier: &AuthorityVerifier,
    authority: &Authority,
    sequence: u64,
    record: &acyclic_stream::Record,
) -> Result<Event> {
    if record.sequence != sequence || record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Storage(
            "history record position or byte bound is invalid".into(),
        ));
    }
    let (record_authority, event) = decode_event(&record.value)?;
    let revision = sequence
        .checked_add(1)
        .ok_or_else(|| Error::Storage("history event revision overflows".into()))?;
    if &record_authority != authority || event.revision != revision {
        return Err(Error::Storage("history event binding is invalid".into()));
    }
    verifier.verify_audience(authority)?;
    verifier.verify_event(&event)?;
    Ok(event)
}
