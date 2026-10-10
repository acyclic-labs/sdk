//! Pinned, bounded reads from the existing authoritative aggregate Stream.

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event},
    wire_codec::decode_event,
};
use acyclic_stream::{Stream, StreamClient, StreamProvider};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

#[cfg(feature = "filesystem")]
mod measured;
#[cfg(feature = "filesystem")]
pub(crate) use measured::HistoryWork;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::native_work_io;

// The portable reader remains independent of the optional Filesystem SDK.
// A receipt observer instruments this same engine, never another reader.
pub(crate) trait HistoryIo: Send {
    fn check_cancellation(&self) -> Result<()>;
    fn begin_call(&mut self) -> Result<()>;
    fn begin_write(&mut self) -> Result<()>;
    fn receive_record(&mut self, bytes: u64) -> Result<()>;
    fn release_record(&mut self, bytes: u64) -> Result<()>;
    fn retain_owned(&mut self, bytes: u64) -> Result<()>;
    fn admit_owned(&self, bytes: u64) -> Result<()>;
    fn release_owned(&mut self, bytes: u64) -> Result<()>;
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority>;
    fn clone_text(&mut self, value: &str) -> Result<String>;
    fn reserve_events(&mut self, events: &mut Vec<Event>, maximum: usize) -> Result<()>;
    fn reserve_records(&mut self, records: &mut Vec<acyclic_stream::Record>, maximum: usize) -> Result<()>;
    fn reserve_messages(&mut self, messages: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()>;
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()>;
    fn reserve_bytes(&mut self, bytes: &mut Vec<u8>, capacity: usize) -> Result<()>;
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()>;
    fn admit_allocation(&mut self, bytes: u64) -> Result<()>;
    fn copy_bytes(&mut self, bytes: u64) -> Result<()>;
    fn release_bytes(&mut self, bytes: u64) -> Result<()> { self.release_owned(bytes) }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()>;
    fn admit_encoding(&self, bytes: u64) -> Result<()>;
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()>;
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()>;
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()>;
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()>;
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()>;
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()>;
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()>;
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> crate::conversation::ContentFuture<'a, Result<()>>;
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> crate::conversation::ContentFuture<'a, Result<Vec<u8>>>;
    #[cfg(feature = "filesystem")]
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget>;
    #[cfg(feature = "filesystem")]
    fn cancellation(&self) -> &acyclic_fs::CancellationToken;
    #[cfg(feature = "filesystem")]
    fn receive_work(&mut self, work: acyclic_fs::WorkCounters) -> Result<()>;
}

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

pub(super) struct CountedHistoryPage {
    pub(super) page: HistoryPage,
    pub(super) records: Vec<acyclic_stream::Record>,
    pub(super) bytes: u64,
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

    /// Explicitly hydrates one finite logical prefix for fork grant capture.
    /// Normal turns never call this archival operation. Every message's atomic
    /// locator and canonical event share the supplied input allowance.
    pub async fn fork_history(
        &self,
        parent: &crate::core::Reducer,
        through_sequence: u64,
        limits: HistoryReadLimits,
    ) -> Result<crate::fork::PreparedForkHistory> {
        self.verifier.verify_audience(parent.authority())?;
        let source = parent.conversation()
            .ok_or_else(|| Error::Invalid("fork archive parent is unbound".into()))?;
        if through_sequence > source.logical_revision() {
            return Err(Error::Invalid("fork archive exceeds the logical parent cut".into()));
        }
        let messages = self.conversation_range(
            &HistoryCursor {
                authority: parent.authority().clone(),
                after_revision: 0,
                through_revision: parent.revision(),
            },
            0,
            through_sequence,
            limits,
        ).await?;
        let mut conversation = crate::conversation::ConversationState::selected_view(messages)?;
        conversation.agent = source.agent;
        if through_sequence >= source.resident_after_sequence()
            && conversation.history_prefix_digest(through_sequence)?
                != source.history_prefix_digest(through_sequence)?
        {
            return Err(Error::Storage("fork archive differs from the authenticated parent prefix".into()));
        }
        #[cfg(feature = "filesystem")]
        {
            let proof = self.verifier.fork_history_selection_proof(
                parent.revision(), through_sequence, conversation.agent,
                conversation.history_prefix_digest(through_sequence)?,
            )?;
            Ok(crate::fork::PreparedForkHistory::from_archive(
                parent.authority().clone(), parent.revision(), conversation, proof,
            ))
        }
        #[cfg(not(feature = "filesystem"))]
        {
            Ok(crate::fork::PreparedForkHistory::from_archive(
                parent.authority().clone(), parent.revision(), conversation,
            ))
        }
    }

    /// Captures an admitted Summary fork through cold authenticated indexes.
    /// Parent publication and the child's original causal binding share the
    /// supplied history allowance: two canonical events and four bounded records.
    /// The signed receiving scope must already grant every inherited file read.
    /// Content reads use the separate admitted context/file limits. No aggregate
    /// is restored, file authority granted or model operation dispatched.
    pub async fn summary_fork_stage(
        &self,
        parent: &Self,
        seed: &crate::fork::ForkSeed,
        scope: &crate::core::Scope,
        reader: std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
        limits: crate::conversation::Limits,
        history_limits: HistoryReadLimits,
    ) -> Result<crate::context::PinnedContextStage> {
        if history_limits.maximum_events < 2 || history_limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "summary fork history allowance is insufficient".into(),
            ));
        }
        crate::contract::validate_json_byte_bound(seed, history_limits.maximum_bytes)?;
        self.verifier.verify(scope)?;
        if parent.verifier.audience() != &seed.parent
            || self.verifier.audience() != &seed.child
            || scope.agent() != Some(seed.child_agent)
        {
            return Err(Error::Unauthorized(
                "summary source is not this admitted child fork".into(),
            ));
        }
        let capture = seed
            .summary
            .as_ref()
            .ok_or_else(|| Error::Invalid("fork has no Summary projection".into()))?;
        let limits = crate::context::restrict_context_limits(capture.selection.limits, limits)?;
        for file in capture.inherited_files().chain(&capture.references) {
            limits.validate_file(file)?;
            crate::conversation::ContentGrant::verify_read(&self.verifier, scope, file)?;
        }
        self.verify_published_child_with_work(parent, seed, history_limits, &mut None).await?;
        crate::context::PinnedContextStage::capture(
            "fork-summary",
            "1",
            capture.context.clone(),
            reader,
            crate::context::ContextPlacement::Prepend,
            limits,
        )
        .await
    }

    /// Authenticates the original publication and initial child binding only.
    /// This has no dependency on a current publication lease or Summary policy.
    pub(crate) async fn verify_published_child_with_work(
        &self, parent: &Self, seed: &crate::fork::ForkSeed,
        limits: HistoryReadLimits, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if limits.maximum_events < 2 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid("published child verification requires two bounded canonical events".into()));
        }
        seed.validate_with_work(work)?;
        if parent.verifier.audience() != &seed.parent || self.verifier.audience() != &seed.child {
            return Err(Error::Unauthorized("published child differs from the original parent or child audience".into()));
        }
        let child_cut = self.pin_with_work(0,
            work.as_mut().map(|work| &mut **work as &mut dyn HistoryIo)).await?;
        if child_cut.through_revision == 0 {
            return Err(Error::Unauthorized("published child is not bound".into()));
        }
        let child_cursor_bytes = child_cut.authority.id.capacity() as u64;
        let cut = child_cut.through_revision;
        drop(child_cut);
        if let Some(work) = work.as_deref_mut() { work.release_owned(child_cursor_bytes)?; }
        let (operation, causal) = super::fork_child_binding_with_work(seed, work)?;
        let (publication, consumed) = super::operations::find_operation_bounded_with_work(
            &parent.client, parent.verifier.audience(), &parent.verifier,
            seed.operation_id, limits.maximum_bytes, work).await?;
        let publication = publication.ok_or_else(|| Error::Conflict("child has no original parent publication".into()))?;
        if publication.revision != causal.revision
            || !matches!(&publication.payload, crate::core::EventPayload::ForkPublished { seed: published } if published.as_ref() == seed)
        {
            return Err(Error::Conflict("child differs from the original parent publication".into()));
        }
        drop(publication);
        let remaining = limits.maximum_bytes.checked_sub(consumed).filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Invalid("published child verification exceeds byte allowance".into()))?;
        let (binding, _) = super::operations::find_operation_bounded_with_work(
            &self.client, self.verifier.audience(), &self.verifier, operation, remaining, work).await?;
        let binding = binding.ok_or_else(|| Error::Conflict("child lacks its original fork binding".into()))?;
        if binding.revision != 1 || binding.revision > cut
            || binding.scope.agent() != Some(seed.child_agent)
            || binding.causal_parent.as_ref() != Some(&causal)
            || !matches!(binding.payload, crate::core::EventPayload::ConversationBound { agent } if agent == seed.child_agent)
        {
            return Err(Error::Conflict("child lacks its original fork binding".into()));
        }
        Ok(())
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

    /// Captures the latest canonical message using one head locator and one
    /// owner-attested event. The byte allowance covers both encoded records.
    /// The head is pinned before reading; a concurrent later append is excluded.
    /// This is a new head observation, not a continuation of an older cursor.
    /// Empty conversations return `None`; missing/corrupt indexed records fail.
    pub async fn latest_conversation_message(
        &self,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        super::operations::latest_message(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            maximum_bytes,
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

    /// Resolves only the declared selection through authenticated atomic indexes.
    /// Each identity costs at most two single-record reads; all identities share
    /// the supplied byte allowance. This read-only view grants no admission or
    /// authority, and the caller must bind the selection to its original operation.
    pub async fn selected_conversation(
        &self,
        cursor: &HistoryCursor,
        selection: &crate::conversation::ModelContextSelection,
        limits: HistoryReadLimits,
    ) -> Result<crate::conversation::ConversationState> {
        self.verifier.verify_audience(&cursor.authority)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || selection.message_ids.len() as u64 > u64::from(limits.maximum_events)
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "selected history bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail().await? {
            return Err(Error::Invalid(
                "selected history exceeds committed cutoff".into(),
            ));
        }
        let mut consumed = 0_u64;
        let mut messages = Vec::with_capacity(selection.message_ids.len());
        for id in &selection.message_ids {
            let remaining = limits
                .maximum_bytes
                .checked_sub(consumed)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| Error::Invalid("selected history exceeds byte bound".into()))?;
            let (message, bytes) = super::operations::find_message_counted(
                &self.client,
                &cursor.authority,
                &self.verifier,
                *id,
                cursor.through_revision,
                remaining,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("selected history byte count overflows".into()))?;
            messages.push(message.ok_or_else(|| {
                Error::Storage("selected history message is missing at its pinned cutoff".into())
            })?);
        }
        let view = crate::conversation::ConversationState::selected_view(messages)?;
        crate::projection::validate_model_context_selection_at_revision(
            &view,
            selection,
            selection.conversation_revision,
        )?;
        Ok(view)
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
        self.conversation_range_with_work(cursor, after_sequence, through_sequence, limits, &mut None).await
    }

    pub(crate) async fn conversation_range_with_work(
        &self,
        cursor: &HistoryCursor,
        after_sequence: u64,
        through_sequence: u64,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
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
        if cursor.through_revision > self.committed_tail_with_work(work).await? {
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
            let (message, bytes) = super::operations::find_message_sequence_with_work(
                &self.client,
                &cursor.authority,
                &self.verifier,
                sequence,
                cursor.through_revision,
                remaining,
                work,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("conversation range byte count overflows".into()))?;
            if let Some(work) = work.as_deref_mut() {
                work.reserve_messages(&mut messages, limits.maximum_events as usize)?;
            }
            messages.push(message.ok_or_else(|| {
                Error::Storage("conversation range is incomplete at its pinned cutoff".into())
            })?);
        }
        Ok(messages)
    }

    /// Reconstructs one exact effect at the pinned canonical cut. Only that
    /// effect's atomically indexed transitions are read; all locator and event
    /// bytes share the declared allowance. This read grants no execution authority.
    pub async fn effect(
        &self,
        cursor: &HistoryCursor,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
    ) -> Result<Option<crate::core::EffectState>> {
        self.effect_with_work(cursor, effect, limits, None).await
    }

    pub(crate) async fn effect_with_work(
        &self, cursor: &HistoryCursor, effect: crate::EffectId,
        limits: HistoryReadLimits, mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<Option<crate::core::EffectState>> {
        let events = super::effects::read_with_work(
            &self.client, &self.verifier, cursor, effect, limits, &mut work,
        ).await?;
        if events.is_empty() {
            return Ok(None);
        }
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(&cursor.authority)?,
            None => cursor.authority.clone(),
        };
        let verifier = self.verifier.clone_with_work(&mut work)?;
        let event_owned = if work.is_some() {
            events.iter().try_fold(
                events.capacity() as u64 * std::mem::size_of::<Event>() as u64,
                |bytes, event| bytes.checked_add(crate::core::event_clone_reservation(event)?)
                    .ok_or_else(|| Error::Invalid("effect source custody overflows".into())),
            )?
        } else { 0 };
        let result = crate::core::Reducer::project_effect_with_work(
            authority, verifier, effect, &events, &mut work,
        );
        drop(events);
        if let Some(work) = work.as_deref_mut() {
            let released = work.release_owned(event_owned);
            if result.is_ok() { released?; }
        }
        result.map(Some)
    }

    /// Captures a committed boundary once for explicit archival traversal.
    pub async fn pin(&self, after_revision: u64) -> Result<HistoryCursor> {
        self.pin_with_work(after_revision, None).await
    }

    pub(crate) async fn pin_with_work(
        &self,
        after_revision: u64,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<HistoryCursor> {
        let through_revision = self.committed_tail_with_work(&mut work).await?;
        if after_revision > through_revision {
            return Err(Error::Invalid(
                "history cursor exceeds committed tail".into(),
            ));
        }
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(self.verifier.audience())?,
            None => self.verifier.audience().clone(),
        };
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        Ok(HistoryCursor {
            authority,
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
        self.read_page_with_work(cursor, limits, false, None)
            .await
            .map(|read| read.page)
    }

    pub(super) async fn read_page_counted(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
    ) -> Result<CountedHistoryPage> {
        self.read_page_with_work(cursor, limits, true, None).await
    }

    pub(crate) async fn read_page_with_work(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        retain_records: bool,
        mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<CountedHistoryPage> {
        self.verifier.verify_audience(&cursor.authority)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "history read bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail_with_work(&mut work).await? {
            return Err(Error::Invalid(
                "history cursor exceeds committed tail".into(),
            ));
        }
        let mut next = match work.as_deref_mut() {
            Some(work) => HistoryCursor {
                authority: work.clone_authority(&cursor.authority)?,
                after_revision: cursor.after_revision,
                through_revision: cursor.through_revision,
            },
            None => cursor.clone(),
        };
        let count = u32::try_from(
            (cursor.through_revision - cursor.after_revision).min(u64::from(limits.maximum_events)),
        )
        .map_err(|_| Error::Invalid("history page count exceeds platform bounds".into()))?;
        let mut events = Vec::new();
        if count == 0 {
            if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
            return Ok(CountedHistoryPage {
                page: HistoryPage {
                    events,
                    cursor: next,
                },
                records: Vec::new(),
                bytes: 0,
            });
        }
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        let mut records = self.stream.read(cursor.after_revision, count).await?;
        let mut retained_records = Vec::new();
        let mut bytes = 0_u64;
        loop {
            if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
            let Some(record) = records.try_next().await? else { break; };
            let record_bytes = record.value.len() as u64;
            if let Some(work) = work.as_deref_mut() {
                work.receive_record(record_bytes)?;
                work.check_cancellation()?;
            }
            if events.len() >= count as usize || record.sequence != next.after_revision {
                return Err(Error::Storage(
                    "history page is not bounded and contiguous".into(),
                ));
            }
            bytes = bytes
                .checked_add(record.value.len() as u64)
                .filter(|total| *total <= limits.maximum_bytes)
                .ok_or_else(|| Error::Invalid("history page exceeds byte bound".into()))?;
            let event = verify_history_record_with_work(
                &self.verifier,
                &cursor.authority,
                next.after_revision,
                &record,
                &mut work,
            )?;
            if let Some(work) = work.as_deref_mut() {
                work.admit_output(bytes, events.len() as u64 + 1)?;
                work.reserve_events(&mut events, count as usize)?;
            }
            next.after_revision = event.revision;
            events.push(event);
            if retain_records {
                if let Some(work) = work.as_deref_mut() {
                    work.reserve_records(&mut retained_records, count as usize)?;
                }
                retained_records.push(record);
            } else {
                drop(record);
                if let Some(work) = work.as_deref_mut() { work.release_record(record_bytes)?; }
            }
        }
        if events.is_empty() {
            return Err(Error::Storage(
                "history page stopped before its pinned boundary".into(),
            ));
        }
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
            work.stage_output(bytes, events.len() as u64)?;
        }
        Ok(CountedHistoryPage {
            page: HistoryPage {
                events,
                cursor: next,
            },
            records: retained_records,
            bytes,
        })
    }

    async fn committed_tail(&self) -> Result<u64> {
        self.committed_tail_with_work(&mut None).await
    }

    async fn committed_tail_with_work(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<u64> {
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        let result = self.stream.tail().await;
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        match result {
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
    verify_history_record_with_work(verifier, authority, sequence, record, &mut None)
}

pub(crate) fn verify_history_record_with_work(
    verifier: &AuthorityVerifier,
    authority: &Authority,
    sequence: u64,
    record: &acyclic_stream::Record,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Event> {
    if record.sequence != sequence || record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Storage(
            "history record position or byte bound is invalid".into(),
        ));
    }
    let (record_authority, event) = crate::wire_codec::decode_event_with_work(&record.value, work)?;
    let owned_event = if work.is_some() { crate::core::event_clone_reservation(&event)? } else { 0 };
    let verification = (|| {
        let revision = sequence.checked_add(1)
            .ok_or_else(|| Error::Storage("history event revision overflows".into()))?;
        if &record_authority != authority || event.revision != revision {
            return Err(Error::Storage("history event binding is invalid".into()));
        }
        verifier.verify_audience(authority)?;
        verifier.verify_event_with_work(&event, work)
    })();
    let authority_bytes = record_authority.id.capacity() as u64;
    drop(record_authority);
    if let Err(error) = verification {
        drop(event);
        if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(authority_bytes + owned_event); }
        return Err(error);
    }
    if let Some(work) = work.as_deref_mut() { work.release_owned(authority_bytes)?; }
    Ok(event)
}

/// The original canonical codec, admitting logical storage before growth.
/// Opaque allocator/map-node overhead is outside this codec contract.
pub(crate) fn canonical_bytes_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let Some(work) = work.as_deref_mut() else {
        return crate::contract::canonical_json_bytes(value);
    };
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(Error::Invalid("canonical encoder allowance must be finite and positive".into()));
    }
    let bytes = crate::contract::canonical_json_bytes_bounded(
        value,
        crate::contract::JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes.saturating_add(1),
            maximum_depth: 256,
        },
        &mut |charge| work.codec_charge(charge),
    )?;
    if let Err(error) = work.retain_owned((bytes.capacity() - bytes.len()) as u64) {
        let size = bytes.len() as u64;
        drop(bytes);
        let _ = work.release_owned(size);
        return Err(error);
    }
    Ok(bytes)
}
