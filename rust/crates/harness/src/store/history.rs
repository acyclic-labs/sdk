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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct HistoryReadLimits {
    /// Maximum number of decoded events.
    pub maximum_events: u32,
    /// Maximum total encoded record bytes, checked before decoding each record.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
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

    /// Resolves an attested operation under a finite encoded-byte allowance.
    /// The returned count includes its atomic locator and canonical event, so
    /// a composite read can deduct both before loading another history range.
    /// Zero rejects before I/O; exhaustion at the locator avoids the event read.
    /// Stream reads transfer whole protocol-bounded records. This meters their
    /// accepted encoded bytes before decode, not provider allocation or transfer;
    /// tail metadata calls are outside the encoded-byte count.
    pub async fn operation_event_bounded(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
    ) -> Result<(Option<Event>, u64)> {
        super::operations::find_operation_bounded(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            operation,
            maximum_bytes,
        )
        .await
    }

    /// Reads one explicit page of a child's published parent-history cut.
    /// The publication proof and message records share the supplied encoded-byte
    /// and event budgets. Later parent appends are excluded. Continue with the
    /// last returned message sequence and the same seed; an empty page is terminal.
    /// This reads canonical records, not model text, and grants no file access.
    /// Normal turns must reuse their admitted context/checkpoint rather than
    /// traverse this archival/bootstrap interface on every request.
    pub async fn inherited_conversation_page(
        &self,
        seed: &crate::fork::ForkSeed,
        child: crate::AgentId,
        after_sequence: u64,
        limits: HistoryReadLimits,
    ) -> Result<Vec<crate::conversation::ConversationMessage>> {
        self.verifier.verify_audience(&seed.parent)?;
        if child != seed.child_agent {
            return Err(Error::Unauthorized(
                "inherited history belongs to another child".into(),
            ));
        }
        if limits.maximum_bytes == 0
            || limits.maximum_events == 0
            || after_sequence > seed.inherited_through_sequence
            || (after_sequence < seed.inherited_through_sequence && limits.maximum_events < 2)
        {
            return Err(Error::Invalid(
                "inherited history page bounds are invalid".into(),
            ));
        }
        crate::contract::validate_json_byte_bound(seed, limits.maximum_bytes)?;
        seed.validate()?;
        let publication_revision = crate::contract::next_revision(seed.parent_revision)?;
        let (publication, consumed) = self
            .operation_event_bounded(seed.operation_id, limits.maximum_bytes)
            .await?;
        let publication = publication
            .ok_or_else(|| Error::Conflict("inherited history has no published fork".into()))?;
        if publication.revision != publication_revision
            || !matches!(&publication.payload,
                crate::core::EventPayload::ForkPublished { seed: published }
                if published.as_ref() == seed)
        {
            return Err(Error::Conflict(
                "inherited history differs from the published fork".into(),
            ));
        }
        if after_sequence == seed.inherited_through_sequence {
            return Ok(Vec::new());
        }
        let remaining_bytes = limits
            .maximum_bytes
            .checked_sub(consumed)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Invalid("inherited history page exceeds byte bound".into()))?;
        let count = (seed.inherited_through_sequence - after_sequence)
            .min(u64::from(limits.maximum_events - 1));
        // The count is at most the distance to the pinned cut, so this cannot overflow.
        let through_sequence = after_sequence + count;
        self.conversation_range(
            &HistoryCursor {
                authority: seed.parent.clone(),
                after_revision: 0,
                through_revision: seed.parent_revision,
            },
            after_sequence,
            through_sequence,
            HistoryReadLimits {
                maximum_events: limits.maximum_events - 1,
                maximum_bytes: remaining_bytes,
            },
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

    /// Reads an exact contiguous canonical message range at one event cutoff.
    /// All message kinds count toward the explicit work bound. The byte bound
    /// includes every locator and canonical record, checked before decoding.
    /// Missing or future records fail; no aggregate replay or scan is attempted.
    pub async fn conversation_range(
        &self,
        cursor: &HistoryCursor,
        after_sequence: u64,
        through_sequence: u64,
        limits: HistoryReadLimits,
    ) -> Result<Vec<crate::conversation::ConversationMessage>> {
        self.verifier.verify_audience(&cursor.authority)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || after_sequence > through_sequence
            || through_sequence - after_sequence > u64::from(limits.maximum_events)
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "conversation range bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail().await? {
            return Err(Error::Invalid(
                "conversation range exceeds committed event cutoff".into(),
            ));
        }
        let mut messages = Vec::new();
        let mut consumed = 0_u64;
        if after_sequence == through_sequence {
            return Ok(messages);
        }
        for sequence in after_sequence + 1..=through_sequence {
            let remaining = limits
                .maximum_bytes
                .checked_sub(consumed)
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| Error::Invalid("conversation range exceeds byte bound".into()))?;
            let (message, bytes) = super::operations::find_message_sequence(
                &self.client,
                &cursor.authority,
                &self.verifier,
                sequence,
                cursor.through_revision,
                remaining,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("conversation range byte count overflows".into()))?;
            messages.push(message.ok_or_else(|| {
                Error::Storage("conversation range is incomplete at its pinned cutoff".into())
            })?);
        }
        Ok(messages)
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
