//! Pinned, bounded reads from the existing authoritative aggregate Stream.

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{Stream, StreamClient, StreamProvider};
use serde::{Deserialize, Serialize};

#[cfg(feature = "filesystem")]
mod measured;
#[cfg(feature = "filesystem")]
pub(crate) use measured::HistoryWork;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native::{AcceptedCompletionIo, adopt_native_sdk_output, native_sdk_result, native_work_io};

/// The native branch owns its original pre-funded frame outside the boxed future.
pub(crate) enum HistoryContentFuture<'a, T> {
    Ordinary {
        future: crate::conversation::ContentFuture<'a, Result<T>>,
        frame_bytes: u64,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(crate::NativeFutureBundle<'a, T>),
}

impl<T> Future for HistoryContentFuture<'_, T> {
    type Output = Result<T>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ordinary { future, .. } => future.as_mut().poll(cx),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(future) => std::pin::Pin::new(future).poll(cx),
        }
    }
}

impl<T> HistoryContentFuture<'_, T> {
    fn ordinary_frame_bytes(&self) -> u64 {
        match self {
            Self::Ordinary { frame_bytes, .. } => *frame_bytes,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(_) => 0,
        }
    }
}

pub(crate) fn content_frame_bytes<A, F>(_: &impl FnOnce(A) -> F) -> u64 {
    std::mem::size_of::<F>() as u64
}

pub(crate) async fn verify_content_with_work(
    observer: &mut dyn HistoryIo,
    verifier: &dyn crate::conversation::ContentResidencyVerifier,
    reference: &crate::conversation::FileRef,
) -> Result<()> {
    let future = observer.verify_content(verifier, reference)?;
    let frame_bytes = future.ordinary_frame_bytes();
    let result = future.await;
    let released = if frame_bytes == 0 { Ok(()) } else { observer.release_owned(frame_bytes) };
    match result {
        Err(error) => Err(error),
        Ok(()) => released,
    }
}

/// Returns the original carrier and only its separately retained observer claim.
pub(crate) async fn read_content_with_work(
    observer: &mut dyn HistoryIo,
    verifier: &dyn crate::conversation::ContentResidencyVerifier,
    reference: &crate::conversation::FileRef,
) -> Result<(acyclic_stream::OwnedBytes, u64)> {
    let future = observer.read_content(verifier, reference)?;
    let frame_bytes = future.ordinary_frame_bytes();
    let result = future.await;
    let released = if frame_bytes == 0 { Ok(()) } else { observer.release_owned(frame_bytes) };
    match result {
        Err(error) => Err(error),
        Ok((bytes, retained_bytes)) => match released {
            Ok(()) => Ok((bytes, retained_bytes)),
            Err(error) => {
                drop(bytes);
                if retained_bytes != 0 {
                    let _ = observer.release_owned(retained_bytes);
                }
                Err(error)
            }
        },
    }
}

pub(crate) trait HistoryIo: Send {
    fn check_cancellation(&self) -> Result<()>;
    fn begin_call(&mut self) -> Result<()>;
    fn begin_write(&mut self) -> Result<()>;
    fn receive_record(&mut self, bytes: u64) -> Result<()>;
    /// Records the SDK-returned authority prefix when the genuine native SDK already owns raw buffers and accounts its source IO and item visits.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn receive_native_record(&mut self, bytes: u64) -> Result<()> {
        let Some(original) = self.original_native_work() else {
            return self.receive_record(bytes);
        };
        original.spent(acyclic_fs::WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: bytes,
            ..acyclic_fs::WorkCounters::default()
        }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        original.remaining()
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }
    fn release_record(&mut self, bytes: u64) -> Result<()>;
    fn retain_owned(&mut self, bytes: u64) -> Result<()>;
    fn admit_owned(&self, bytes: u64) -> Result<()>;
    fn release_owned(&mut self, bytes: u64) -> Result<()>;
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        Error::Storage(cause.to_string())
    }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority>;
    fn clone_text(&mut self, value: &str) -> Result<String>;
    fn reserve_events(&mut self, events: &mut Vec<Event>, maximum: usize) -> Result<()>;
    fn reserve_records(&mut self, records: &mut Vec<super::operations::ObservedRecord>, maximum: usize) -> Result<()>;
    fn reserve_messages(&mut self, messages: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()>;
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()>;
    fn reserve_bytes(&mut self, bytes: &mut Vec<u8>, capacity: usize) -> Result<()>;
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()>;
    fn admit_allocation(&mut self, bytes: u64) -> Result<()>;
    fn admit_copy(&self, bytes: u64) -> Result<()>;
    fn copy_bytes(&mut self, bytes: u64) -> Result<()>;
    fn release_bytes(&mut self, bytes: u64) -> Result<()> { self.release_owned(bytes) }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()>;
    fn admit_encoding(&self, bytes: u64) -> Result<()>;
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()>;
    fn decode_json_value(&mut self, _bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        Err(Error::Unsupported("this history observer has no guarded JSON value decoder".into()))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> { None }
    /// Identity alone cannot dispatch another native call for an accepted completion; its caller must supply the sealed pre-action SDK token.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn requires_prepared_native_dispatch(&self) -> bool { false }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(incoming),
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn take_native_custody(&mut self, _bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
        })
    }
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
    ) -> Result<HistoryContentFuture<'a, ()>>;
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>;
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
    pub(super) records: Vec<super::operations::ObservedRecord>,
    pub(super) bytes: u64,
    /// Original decoded-event claims, excluding all page/header/vector backing.
    pub(super) events_retained_bytes: u64,
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
        Self::new_owned(client, authority, verifier, &mut None)
    }

    pub(crate) fn new_with_work(
        client: &StreamClient<P>,
        authority: &Authority,
        verifier: &AuthorityVerifier,
        work: Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut work = work;
        verifier.verify_audience_with_work(authority, &mut work)?;
        let verifier = verifier.clone_with_work(&mut work)?;
        Self::new_owned(client, authority, verifier, &mut work)
    }

    /// Constructs the original authenticated reader with pre-admitted issuer and path copies on the caller's exact native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        client: &StreamClient<P>,
        authority: &Authority,
        verifier: &AuthorityVerifier,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let reader = Self::new_with_work(client, authority, verifier, Some(&mut io))?;
        Ok((reader, io.into_custody()))
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.verifier.logical_owned_bytes()?
            .checked_add(self.stream.path().as_str().len() as u64)
            .ok_or_else(|| Error::Invalid("history reader metadata byte count overflows".into()))
    }

    pub(super) fn new_owned(
        client: &StreamClient<P>,
        authority: &Authority,
        verifier: AuthorityVerifier,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        verifier.verify_audience_with_work(authority, work)?;
        let path = authority.stream_path_with_work(work)?;
        let temporary_bytes = path.capacity() as u64;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, temporary_bytes);
        let path_bytes = path.len() as u64;
        let stream: Result<_> = (|| {
            if let Some(work) = quota.work().as_deref_mut() {
                work.admit_owned(path_bytes)?;
                work.retain_owned(path_bytes)?;
                quota.adopt(path_bytes)?;
                let work = quota.work().as_deref_mut().expect("path observer");
                work.admit_allocation(path_bytes)?;
                work.copy_bytes(path_bytes)?;
            }
            client.stream(&path).map_err(|error|
                crate::store::operations::stream_error_with_work(error, quota.work()))
        })();
        drop(path);
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => return Err(error),
        };
        if let Err(error) = quota.relinquish(temporary_bytes) {
            drop(stream);
            return Err(error);
        }
        quota.transfer();
        Ok(Self { client: client.clone(), stream, verifier })
    }

    /// Explicitly hydrates one finite logical prefix for fork grant capture.
    pub async fn fork_history(
        &self,
        parent: &crate::core::Reducer,
        through_sequence: u64,
        limits: HistoryReadLimits,
    ) -> Result<crate::fork::PreparedForkHistory> {
        self.fork_history_with_work(parent, through_sequence, limits, &mut None).await
    }

    pub(crate) async fn fork_history_with_work(
        &self,
        parent: &crate::core::Reducer,
        through_sequence: u64,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<crate::fork::PreparedForkHistory> {
        self.verifier.verify_audience_with_work(parent.authority(), work)?;
        let source = parent.conversation().ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
                return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "fork archive parent is unbound" };
            }
            Error::Invalid("fork archive parent is unbound".into())
        })?;
        if through_sequence > source.logical_revision() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "fork archive exceeds the logical parent cut" });
            }
            return Err(Error::Invalid("fork archive exceeds the logical parent cut".into()));
        }
        let cursor_authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(parent.authority())?,
            None => parent.authority().clone(),
        };
        let cursor = HistoryCursor {
            authority: cursor_authority,
            after_revision: 0,
            through_revision: parent.revision(),
        };
        let cursor_bytes = cursor.authority.id.capacity() as u64;
        let messages = self.conversation_range_with_work(
            &cursor, 0, through_sequence, limits, work,
        ).await;
        drop(cursor);
        if let Some(work) = work.as_deref_mut() {
            let released = work.release_owned(cursor_bytes);
            if messages.is_ok() { released?; }
        }
        let (mut conversation, _) =
            crate::conversation::ConversationState::selected_view_with_work(messages?, work)?;
        conversation.agent = source.agent;
        if through_sequence >= source.resident_after_sequence()
            && conversation.history_prefix_digest_with_work(through_sequence, work)?
                != source.history_prefix_digest_with_work(through_sequence, work)?
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "fork archive differs from the authenticated parent prefix" });
            }
            return Err(Error::Storage("fork archive differs from the authenticated parent prefix".into()));
        }
        #[cfg(feature = "filesystem")]
        {
            let proof = self.verifier.fork_history_selection_proof_with_work(
                parent.revision(), through_sequence, conversation.agent,
                conversation.history_prefix_digest_with_work(through_sequence, work)?, work,
            )?;
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(parent.authority())?,
                None => parent.authority().clone(),
            };
            Ok(crate::fork::PreparedForkHistory::from_archive(
                authority, parent.revision(), conversation, proof,
            ))
        }
        #[cfg(not(feature = "filesystem"))]
        {
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(parent.authority())?,
                None => parent.authority().clone(),
            };
            Ok(crate::fork::PreparedForkHistory::from_archive(
                authority, parent.revision(), conversation,
            ))
        }
    }

    /// Captures an admitted Summary fork through cold authenticated indexes.
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
    pub(crate) async fn verify_published_child_with_work(
        &self, parent: &Self, seed: &crate::fork::ForkSeed,
        limits: HistoryReadLimits, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if limits.maximum_events < 2 || limits.maximum_bytes == 0 {
            return Err(history_read_invalid(work, "published child verification requires two bounded canonical events"));
        }
        seed.validate_with_work(work)?;
        if parent.verifier.audience() != &seed.parent || self.verifier.audience() != &seed.child {
            return Err(history_verification_error(work, crate::wire::ErrorCode::Unauthorized,
                Error::Unauthorized, "published child differs from the original parent or child audience"));
        }
        let child_cut = self.pin_with_work(0,
            work.as_mut().map(|work| &mut **work as &mut dyn HistoryIo)).await?;
        if child_cut.through_revision == 0 {
            return Err(history_verification_error(work, crate::wire::ErrorCode::Unauthorized,
                Error::Unauthorized, "published child is not bound"));
        }
        let child_cursor_bytes = child_cut.authority.id.capacity() as u64;
        let cut = child_cut.through_revision;
        drop(child_cut);
        if let Some(work) = work.as_deref_mut() { work.release_owned(child_cursor_bytes)?; }
        let (operation, causal) = super::fork_child_binding_with_work(seed, work)?;
        let (publication, consumed) = super::operations::bounded_operation_with_retained_quota(
            &parent.client, parent.verifier.audience(), &parent.verifier,
            seed.operation_id, limits.maximum_bytes, false, work).await?;
        let (publication, publication_owned) = publication
            .ok_or_else(|| history_verification_error(work, crate::wire::ErrorCode::Conflict,
                Error::Conflict, "child has no original parent publication"))?;
        let publication_matches = publication.revision == causal.revision
            && matches!(&publication.payload, crate::core::EventPayload::ForkPublished { seed: published } if published.as_ref() == seed);
        drop(publication);
        let released_publication = match work.as_deref_mut() {
            Some(work) => work.release_owned(publication_owned),
            None => Ok(()),
        };
        if !publication_matches {
            let _ = released_publication;
            return Err(history_verification_error(work, crate::wire::ErrorCode::Conflict,
                Error::Conflict, "child differs from the original parent publication"));
        }
        released_publication?;
        let remaining = limits.maximum_bytes.checked_sub(consumed).filter(|bytes| *bytes > 0)
            .ok_or_else(|| history_read_invalid(work, "published child verification exceeds byte allowance"))?;
        let (binding, _) = super::operations::bounded_operation_with_retained_quota(
            &self.client, self.verifier.audience(), &self.verifier, operation, remaining, false, work).await?;
        let (binding, binding_owned) = binding
            .ok_or_else(|| history_verification_error(work, crate::wire::ErrorCode::Conflict,
                Error::Conflict, "child lacks its original fork binding"))?;
        let binding_matches = binding.revision == 1 && binding.revision <= cut
            && binding.scope.agent() == Some(seed.child_agent)
            && binding.causal_parent.as_ref() == Some(&causal)
            && matches!(&binding.payload, crate::core::EventPayload::ConversationBound { agent } if *agent == seed.child_agent);
        let released_owned = if work.is_some() {
            binding_owned
                .checked_add(causal.authority.id.capacity() as u64)
                .ok_or_else(|| history_read_invalid(work, "child verification custody overflows"))?
        } else { 0 };
        drop(binding);
        drop(causal);
        let released_binding = match work.as_deref_mut() {
            Some(work) => work.release_owned(released_owned),
            None => Ok(()),
        };
        if !binding_matches {
            let _ = released_binding;
            return Err(history_verification_error(work, crate::wire::ErrorCode::Conflict,
                Error::Conflict, "child lacks its original fork binding"));
        }
        released_binding
    }

    /// Resolves a globally unique admitted operation without restoring its aggregate.
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
    pub async fn operation_event_bounded(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
    ) -> Result<(Option<Event>, u64)> {
        self.operation_event_bounded_with_work(operation, maximum_bytes, &mut None).await
    }

    pub(crate) async fn operation_event_bounded_with_work(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Option<Event>, u64)> {
        super::operations::find_operation_bounded_with_work(
            &self.client, self.verifier.audience(), &self.verifier,
            operation, maximum_bytes, true, work,
        ).await
    }

    /// Reads one explicit page of a child's published parent-history cut.
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

    /// Captures the latest canonical message using one head locator and one owner-attested event.
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
    pub async fn conversation_message(
        &self,
        cursor: &HistoryCursor,
        message_id: uuid::Uuid,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        self.verifier.verify_audience(&cursor.authority)?;
        if cursor.after_revision > cursor.through_revision
            || cursor.through_revision > self.committed_tail_with_work(&mut None).await?
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
        if cursor.through_revision > self.committed_tail_with_work(&mut None).await? {
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
        let view = crate::conversation::ConversationState::selected_view_with_work(messages, &mut None).map(|(state, _)| state)?;
        crate::projection::validate_model_context_selection_at_revision(
            &view,
            selection,
            selection.conversation_revision,
        )?;
        Ok(view)
    }

    /// Reads an exact contiguous canonical message range at one event cutoff.
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

    /// Reconstructs one exact effect at the pinned canonical cut.
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
        limits: HistoryReadLimits, work: Option<&mut dyn HistoryIo>,
    ) -> Result<Option<crate::core::EffectState>> {
        self.effect_with_retained_quota(cursor, effect, limits, work).await
            .map(|effect| effect.map(|(state, _)| state))
    }

    pub(crate) async fn effect_with_retained_quota(
        &self, cursor: &HistoryCursor, effect: crate::EffectId,
        limits: HistoryReadLimits, mut work: Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(crate::core::EffectState, u64)>> {
        let (events, event_owned) = super::effects::read_with_retained_quota(
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
        let result = crate::core::Reducer::project_effect_with_retained_quota(
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
            return Err(history_read_invalid(&work, "history cursor exceeds committed tail"));
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
        self.verifier.verify_audience_with_work(&cursor.authority, &mut work)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
        {
            return Err(history_read_invalid(&work, "history read bounds or cursor are invalid"));
        }
        if cursor.through_revision > self.committed_tail_with_work(&mut work).await? {
            return Err(history_read_invalid(&work, "history cursor exceeds committed tail"));
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
        .map_err(|_| history_read_invalid(&work, "history page count exceeds platform bounds"))?;
        let mut events = Vec::new();
        let mut events_retained_bytes = 0_u64;
        if count == 0 {
            if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
            return Ok(CountedHistoryPage {
                page: HistoryPage {
                    events,
                    cursor: next,
                },
                records: Vec::new(),
                bytes: 0,
                events_retained_bytes: 0,
            });
        }
        let mut records = super::operations::read_records_with_work(
            &self.client, &self.stream, cursor.after_revision, count, &mut work,
        ).await?.map_err(|error| crate::distributed::sdk_error(error, &work))?;
        let mut retained_records = Vec::new();
        let mut bytes = 0_u64;
        loop {
            if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
            let Some(record) = records.next(&mut work).await? else { break; };
            let record_bytes = super::operations::sdk_record_retained_bytes(&record, &work);
            if let Some(work) = work.as_deref_mut() {
                work.check_cancellation()?;
            }
            if events.len() >= count as usize || record.sequence != next.after_revision {
                return Err(history_record_boundary(&work, "history page is not bounded and contiguous"));
            }
            bytes = bytes
                .checked_add(record.value.len() as u64)
                .filter(|total| *total <= limits.maximum_bytes)
                .ok_or_else(|| history_read_invalid(&work, "history page exceeds byte bound"))?;
            let (event, event_retained) = verify_history_record_with_retained_quota(
                &self.verifier,
                &cursor.authority,
                next.after_revision,
                &record,
                &mut work,
            )?;
            events_retained_bytes = events_retained_bytes.checked_add(event_retained)
                .ok_or_else(|| history_record_boundary(&work, "history event custody overflows"))?;
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
            return Err(history_record_boundary(&work, "history page stopped before its pinned boundary"));
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
            events_retained_bytes,
        })
    }

    async fn committed_tail_with_work(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(super::operations::original_native_decode_mode) {
            let tail = super::operations::tail_optional_with_work(
                &self.client, self.stream.path(), work,
            ).await?;
            work.as_deref().expect("native tail observer").check_cancellation()?;
            return Ok(tail.unwrap_or(0));
        }
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



pub(crate) fn verify_history_record_with_retained_quota(
    verifier: &AuthorityVerifier,
    authority: &Authority,
    sequence: u64,
    record: &acyclic_stream::Record,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Event, u64)> {
    if record.sequence != sequence || record.value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(history_record_boundary(work,
            "history record position or byte bound is invalid"));
    }
    let ((record_authority, event), owned_event) =
        crate::wire_codec::decode_event_with_work_and_custody(&record.value, work)?;
    let verification = (|| {
        let revision = sequence.checked_add(1)
            .ok_or_else(|| history_record_boundary(work, "history event revision overflows"))?;
        if &record_authority != authority || event.revision != revision {
            return Err(history_record_boundary(work, "history event binding is invalid"));
        }
        verifier.verify_audience(authority)?;
        verifier.verify_event(&event, work)
    })();
    let authority_bytes = record_authority.id.capacity() as u64;
    drop(record_authority);
    if let Err(error) = verification {
        drop(event);
        if let Some(work) = work.as_deref_mut() { let _ = work.release_owned(authority_bytes + owned_event); }
        return Err(error);
    }
    if let Some(work) = work.as_deref_mut() { work.release_owned(authority_bytes)?; }
    Ok((event, owned_event))
}

fn history_read_invalid(work: &Option<&mut dyn HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(super::operations::original_native_decode_mode) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

fn history_record_boundary(
    work: &Option<&mut dyn HistoryIo>, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().and_then(|observer| observer.original_native_work()).is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason };
    }
    let _ = work;
    Error::Storage(reason.into())
}

fn history_verification_error(
    work: &Option<&mut dyn HistoryIo>,
    code: crate::wire::ErrorCode,
    ordinary: fn(String) -> Error,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(super::operations::original_native_decode_mode) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = (work, code);
    ordinary(reason.into())
}

/// The original canonical codec, admitting logical storage before growth.
pub(crate) fn canonical_bytes_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    if work.is_none() { return crate::contract::canonical_json_bytes(value); }
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "canonical encoder allowance must be finite and positive",
            });
        }
        return Err(Error::Invalid("canonical encoder allowance must be finite and positive".into()));
    }
    canonical_bytes_for_bounds_with_work(value, crate::contract::JsonBounds {
        maximum_bytes, maximum_items: maximum_bytes.saturating_add(1), maximum_depth: 256,
    }, work)
}

pub(crate) fn canonical_bytes_for_bounds_with_work<T: Serialize + ?Sized>(
    value: &T,
    bounds: crate::contract::JsonBounds,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let Some(work) = work.as_deref_mut() else {
        return crate::contract::canonical_json_bytes(value);
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let bytes = if work.original_native_work().is_some() {
        crate::contract::canonical_json_bytes_bounded(
            value, bounds, Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| work.codec_charge(charge),
        )?
    } else {
        crate::contract::canonical_json_bytes_bounded(value, bounds, None, &mut |charge| work.codec_charge(charge))?
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let bytes = crate::contract::canonical_json_bytes_bounded(
        value, bounds, None, &mut |charge| work.codec_charge(charge),
    )?;
    retain_encoded_capacity(bytes, work)
}

fn retain_encoded_capacity(
    bytes: Vec<u8>,
    work: &mut dyn HistoryIo,
) -> Result<Vec<u8>> {
    if let Err(error) = work.retain_owned((bytes.capacity() - bytes.len()) as u64) {
        let size = bytes.len() as u64;
        drop(bytes);
        let _ = work.release_owned(size);
        return Err(error);
    }
    Ok(bytes)
}
