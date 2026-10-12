//! Typed capture and publication of a child environment.

use crate::conversation::{
    ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    decode_complete_attachment_manifest,
};
use crate::core::Authority;
use crate::resources::{
    ArtifactRef, CheckpointRef, ContextRef, GenerationRef, ProviderRef, ResourceRef, StreamRef,
};
use crate::{AgentId, Capabilities, Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Largest portable count representable by the contract.
pub const MAX_FORK_AGENTS: usize = crate::conversation::MAX_PORTABLE_COUNT;
/// Maximum selected resources in one fork request or seed.
pub const MAX_FORK_RESOURCES: usize = crate::conversation::MAX_PORTABLE_COUNT;
/// Maximum references or grants in one fork.
pub const MAX_FORK_REFERENCES: usize = u32::MAX as usize;
/// Maximum aggregate attachment manifest bytes in one fork seed.
pub const MAX_FORK_ATTACHMENT_MANIFEST_BYTES: u64 = u64::MAX;
/// Maximum inherited context bytes in one fork preparation.
pub const MAX_FORK_INHERITED_BYTES: u64 = u64::MAX;
/// Maximum aggregate referenced file bytes in one fork seed.
pub const MAX_FORK_REFERENCE_BYTES: u64 = u64::MAX;
/// Largest sequence representable by the contract.
pub const MAX_FORK_INHERITED_MESSAGES: u64 = u64::MAX;

mod summary;
pub use summary::*;

type DirectManifestReads = BTreeSet<([u8; 32], AgentId)>;
type ManifestMemberGrants = BTreeMap<[u8; 32], BTreeMap<[u8; 32], BTreeSet<AgentId>>>;

/// Child-owned pointer to one immutable revision of authoritative parent history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedConversationPrefix {
    /// Version of this direct-parent reference contract.
    pub format_version: u32,
    /// Parent conversation from which this prefix was selected.
    pub parent: Authority,
    /// Exact parent revision observed during selection.
    pub parent_revision: u64,
    /// Agent bound to the parent conversation.
    pub parent_agent: AgentId,
    /// Inclusive final sequence selected for inheritance.
    pub through_sequence: u64,
    /// Readers bound to this preparation retry identity; grants live in the seed.
    pub attached_agents: Vec<AgentId>,
    /// Version 3 domain-separated chain of canonical per-message digests.
    pub message_digest: [u8; 32],
}

impl InheritedConversationPrefix {
    /// Binds one contiguous bound parent prefix without retaining its message list.
    pub fn select(
        parent: Authority,
        parent_revision: u64,
        through_sequence: u64,
        attached_agents: &[AgentId],
        conversation: &crate::conversation::ConversationState,
    ) -> Result<Self> {
        Self::select_with_work(parent, parent_revision, through_sequence, attached_agents, conversation, &mut None)
    }

    pub(crate) fn select_with_work(
        parent: Authority,
        parent_revision: u64,
        through_sequence: u64,
        attached_agents: &[AgentId],
        conversation: &crate::conversation::ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut owned = ForkValidationWork::new(work)?;
        let parent_agent = conversation
            .agent
            .ok_or_else(|| Error::Invalid("inherited prefix parent is unbound".into()))?;
        if through_sequence > conversation.logical_revision() {
            return Err(Error::Invalid(
                "inherited prefix exceeds parent history".into(),
            ));
        }
        if attached_agents.len() as u64 > MAX_FORK_AGENTS as u64 {
            return Err(Error::Invalid(
                "too many attached agents for inherited context".into(),
            ));
        }
        let mut readers = BTreeSet::new();
        for reader in attached_agents {
            owned.checkpoint()?;
            if !readers.contains(reader) {
                owned.slots::<AgentId>(1)?;
                readers.insert(*reader);
            }
        }
        if readers.len() != attached_agents.len() {
            return Err(Error::Invalid(
                "inherited attached agents are invalid".into(),
            ));
        }
        let message_digest = conversation.history_prefix_digest(through_sequence)?;
        let attached_bytes = attached_agents.len() as u64 * std::mem::size_of::<AgentId>() as u64;
        if !attached_agents.is_empty() && let Some(observer) = owned.work().as_deref_mut() {
            observer.admit_allocation(attached_bytes)?;
            observer.copy_bytes(attached_bytes)?;
        }
        let attached_agents = attached_agents.to_vec();
        if let Some(observer) = owned.work().as_deref_mut() {
            observer.retain_owned(attached_agents.capacity() as u64 * std::mem::size_of::<AgentId>() as u64)?;
        }
        Ok(Self {
            format_version: 3,
            parent,
            parent_revision,
            parent_agent,
            through_sequence,
            attached_agents,
            message_digest,
        })
    }

    /// Stable JSON descriptor input shared by staging and reducer admission.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(self)
    }
}

/// A finite, authenticated archive selection used only during explicit fork preparation.
#[derive(Debug)]
pub struct PreparedForkHistory {
    parent: Authority,
    parent_revision: u64,
    conversation: crate::conversation::ConversationState,
    #[cfg(feature = "filesystem")]
    attestation: [u8; 32],
}

impl PreparedForkHistory {
    pub(crate) fn from_archive(
        parent: Authority,
        parent_revision: u64,
        conversation: crate::conversation::ConversationState,
        #[cfg(feature = "filesystem")]
        attestation: [u8; 32],
    ) -> Self {
        Self {
            parent, parent_revision, conversation,
            #[cfg(feature = "filesystem")]
            attestation,
        }
    }

    /// Original parent whose indexed records were authenticated.
    #[must_use]
    pub const fn parent(&self) -> &Authority { &self.parent }

    /// Exact canonical event cut of the archive selection.
    #[must_use]
    pub const fn parent_revision(&self) -> u64 { self.parent_revision }

    /// Inclusive logical message cut, independent of parent residency.
    #[must_use]
    pub fn through_sequence(&self) -> u64 { self.conversation.logical_revision() }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn conversation(&self) -> &crate::conversation::ConversationState {
        &self.conversation
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify(&self, verifier: &crate::core::AuthorityVerifier) -> Result<()> {
        self.verify_with_work(verifier, &mut None)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_with_work(
        &self, verifier: &crate::core::AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify_audience_with_work(&self.parent, work)?;
        let digest = self.conversation.history_prefix_digest_with_work(self.through_sequence(), work)?;
        if self.attestation != verifier.fork_history_selection_proof_with_work(
            self.parent_revision, self.through_sequence(), self.conversation.agent, digest, work,
        )? {
            #[cfg(not(target_arch = "wasm32"))]
            if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork archive belongs to another issuer",
                });
            }
            return Err(Error::Unauthorized("fork archive belongs to another issuer".into()));
        }
        Ok(())
    }
}

/// Provider-owned asynchronous fence guarding one fork publication.
pub type ForkFenceFuture<'a> = BoxFuture<'a, Result<Box<dyn ForkPublicationGuard>>>;

/// One exact provider's admission barrier for state prepared before publication.
pub trait ForkSeedVerifier: acyclic_stream::ProviderPlatform {
    /// Identity of the provider whose resources this verifier can prove.
    fn provider(&self) -> &ProviderRef;
    /// Maximum aggregate descriptor bytes verified for this provider during one publication, including members discovered through manifests.
    fn maximum_reference_bytes(&self) -> u64 {
        MAX_FORK_REFERENCE_BYTES
    }
    /// Checks every selected revision owned by this provider.
    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        unsupported_native_fork(work, "native fork provider verification is unavailable")
    }
    /// Executes the same provider proof with a finite SDK allowance.
    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self,
        _seed: &'a ForkSeed,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        unsupported_fork_measurement("fork provider verification is not measured")
    }
    /// Acquires the child-private provider's durable write fence before any fork verification reads its mutable head.
    fn acquire_private_fence<'a>(&'a self, _seed: &'a ForkSeed) -> ForkFenceFuture<'a> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fork private publication fence is not bound".into(),
            ))
        })
    }
    /// Returns the concrete fence before its allocation guard, which also survives the Box's physical deallocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn acquire_private_fence_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Box<dyn ForkPublicationGuard>, acyclic_fs::NativeAllocation)>> {
        unsupported_native_fork(work, "native fork private publication fence is unavailable")
    }
    /// Acquires the original durable fence with its actual SDK receipt.
    #[cfg(feature = "filesystem")]
    fn acquire_private_fence_measured<'a>(
        &'a self,
        _seed: &'a ForkSeed,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Box<dyn ForkPublicationGuard>>, Error>> {
        unsupported_fork_measurement("fork private publication fence is not measured")
    }
    /// Reads a complete, pinned attachment manifest from this provider.
    fn read_manifest<'a>(&'a self, _manifest: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fork attachment manifest reader is not bound".into(),
            ))
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_manifest_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, _manifest: &'a FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        unsupported_native_fork(work, "native fork attachment manifest reader is unavailable")
    }
    /// Reads the original pinned manifest with its receipt and exact separate observer claim, observed by the producer before converting its storage.
    #[cfg(feature = "filesystem")]
    fn read_manifest_measured<'a>(
        &'a self,
        _manifest: &'a FileRef,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<(acyclic_stream::OwnedBytes, u64)>, Error>> {
        unsupported_fork_measurement("fork attachment manifest reader is not measured")
    }
    /// Proves one immutable member is resident at its owning provider.
    fn verify_file<'a>(&'a self, _file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("fork file verifier is not bound".into())) })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_file_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, _file: &'a FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        unsupported_native_fork(work, "native fork file verifier is unavailable")
    }
    /// Verifies the original immutable member with its actual SDK receipt.
    #[cfg(feature = "filesystem")]
    fn verify_file_measured<'a>(
        &'a self,
        _file: &'a FileRef,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        unsupported_fork_measurement("fork file verifier is not measured")
    }
    /// Checks a cross-provider boundary claim, if this provider issues one.
    fn verify_boundary<'a>(&'a self, _boundary: &'a AttestedBoundary) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "provider does not attest fork boundaries".into(),
            ))
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_boundary_with_native_work<'a>(
        &'a self, _boundary: &'a AttestedBoundary, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        unsupported_native_fork(work, "native fork boundary verifier is unavailable")
    }
    /// Verifies the original provider-issued claim with its actual SDK receipt.
    #[cfg(feature = "filesystem")]
    fn verify_boundary_measured<'a>(
        &'a self,
        _boundary: &'a AttestedBoundary,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        unsupported_fork_measurement("fork boundary verifier is not measured")
    }
}

/// An exact provider-owned write fence retained through Stream publication or reconciliation.
pub trait ForkPublicationGuard: acyclic_stream::ProviderPlatform {
    /// Idempotently releases the exact fence after a terminal publication result.
    fn release<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn release_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        unsupported_native_fork(work, "native fork private publication fence release is unavailable")
    }
    /// Releases that same gate only when the concrete provider supports receipts.
    #[cfg(feature = "filesystem")]
    fn release_measured<'a>(
        &'a self,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        unsupported_fork_measurement("fork private publication fence release is not measured")
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn unsupported_native_fork<'a, T: 'a>(
    work: &'a acyclic_fs::NativeMaterializationWork,
    reason: &'static str,
) -> Result<crate::NativeFutureBundle<'a, T>> {
    work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason })
}

fn fork_provider_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Unsupported(reason.into()),
    }
}

#[cfg(feature = "filesystem")]
fn unsupported_fork_measurement<'a, T: 'a>(
    message: &'static str,
) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>> {
    Box::pin(async move {
        Err(acyclic_fs::OperationFailure::new(
            Error::Unsupported(message.into()),
            acyclic_fs::WorkCounters::default(),
        ))
    })
}

#[cfg(feature = "filesystem")]
fn receive_fork_work<T>(
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>,
    observer: &mut dyn crate::store::history::HistoryIo,
) -> Result<T> {
    match result {
        Ok(receipt) => {
            observer.receive_work(receipt.work)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            // Preserve the real provider error even if accounting rejects its spent prefix.
            let _ = observer.receive_work(*failure.work);
            Err(failure.error)
        }
    }
}

pub(crate) async fn release_fork_guard_with_work(
    guard: &dyn ForkPublicationGuard,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else {
        return guard.release().await;
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = observer.original_native_work() {
        return guard.release_with_native_work(native)?.await;
    }
    #[cfg(feature = "filesystem")]
    {
        let receipt = guard.release_measured(observer.remaining()?, observer.cancellation()).await;
        receive_fork_work(receipt, observer)
    }
    #[cfg(not(feature = "filesystem"))]
    {
        let _ = observer;
        Err(Error::Unsupported("measured fork publication requires Filesystem".into()))
    }
}

macro_rules! fork_provider_dispatcher {
    ($name:ident, $ordinary:ident, $measured:ident, $native:ident, $input:ident : $input_type:ty, $output:ty $(, $context:ident : $context_type:ty)?) => {
        async fn $name(
            verifier: &dyn ForkSeedVerifier,
            $($context: &$context_type,)?
            $input: &$input_type,
            work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
        ) -> Result<$output> {
            $(let _ = $context;)?
            let Some(observer) = work.as_deref_mut() else {
                return verifier.$ordinary($input).await;
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = observer.original_native_work() {
                return verifier.$native($($context,)? $input, native)?.await;
            }
            #[cfg(feature = "filesystem")]
            {
                let receipt = verifier.$measured(
                    $input, observer.remaining()?, observer.cancellation(),
                ).await;
                receive_fork_work(receipt, observer)
            }
            #[cfg(not(feature = "filesystem"))]
            {
                let _ = observer;
                Err(Error::Unsupported("measured fork publication requires Filesystem".into()))
            }
        }
    };
}

pub(crate) struct ObservedForkPublicationGuard {
    guard: Box<dyn ForkPublicationGuard>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _allocation: Option<acyclic_fs::NativeAllocation>,
}

impl std::ops::Deref for ObservedForkPublicationGuard {
    type Target = dyn ForkPublicationGuard;
    fn deref(&self) -> &Self::Target { self.guard.as_ref() }
}

async fn acquire_fork_fence(
    verifier: &dyn ForkSeedVerifier, seed: &ForkSeed,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<ObservedForkPublicationGuard> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_ref().and_then(|observer| observer.original_native_work()) {
        let (guard, allocation) = verifier.acquire_private_fence_with_native_work(seed, native)?.await?;
        if !allocation.work().shares_ledger(native) {
            drop(guard);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(allocation),
            });
        }
        return Ok(ObservedForkPublicationGuard { guard, _allocation: Some(allocation) });
    }
    let guard = if let Some(observer) = work.as_deref_mut() {
        #[cfg(feature = "filesystem")]
        {
            let receipt = verifier.acquire_private_fence_measured(
                seed, observer.remaining()?, observer.cancellation(),
            ).await;
            receive_fork_work(receipt, observer)?
        }
        #[cfg(not(feature = "filesystem"))]
        {
            let _ = observer;
            return Err(Error::Unsupported("measured fork publication requires Filesystem".into()));
        }
    } else {
        verifier.acquire_private_fence(seed).await?
    };
    Ok(ObservedForkPublicationGuard {
        guard,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        _allocation: None,
    })
}
fork_provider_dispatcher!(verify_fork_provider_with_work, verify,
    verify_measured, verify_with_native_work, seed: ForkSeed, ());
fork_provider_dispatcher!(verify_fork_file_with_work, verify_file,
    verify_file_measured, verify_file_with_native_work, file: FileRef, (), seed: ForkSeed);
fork_provider_dispatcher!(verify_fork_boundary_with_work, verify_boundary,
    verify_boundary_measured, verify_boundary_with_native_work, boundary: AttestedBoundary, ());

async fn read_fork_manifest(
    verifier: &dyn ForkSeedVerifier,
    seed: &ForkSeed,
    manifest: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(acyclic_stream::OwnedBytes, u64)> {
    let Some(observer) = work.as_deref_mut() else {
        return verifier.read_manifest(manifest).await.map(|bytes| (bytes, 0));
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = observer.original_native_work() {
        let bytes = verifier.read_manifest_with_native_work(seed, manifest, native)?.await?;
        return Ok((bytes, 0));
    }
    #[cfg(feature = "filesystem")]
    {
        let receipt = verifier.read_manifest_measured(
            manifest, observer.remaining()?, observer.cancellation(),
        ).await;
        let (bytes, retained) = receive_fork_work(receipt, observer)?;
        observer.admit_owned(retained)?;
        observer.retain_owned(retained)?;
        Ok((bytes, retained))
    }
    #[cfg(not(feature = "filesystem"))]
    {
        let _ = observer;
        Err(Error::Unsupported("measured fork publication requires Filesystem".into()))
    }
}

/// Exhaustive provider dispatcher.
pub struct CompositeForkVerifier {
    providers: BTreeMap<String, std::sync::Arc<dyn ForkSeedVerifier>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn admit_native_verifier_share(
    metadata: Option<&acyclic_fs::NativeAllocation>,
    work: &acyclic_fs::NativeMaterializationWork, inline_bytes: usize,
) -> Result<()> {
    if !metadata.is_some_and(|metadata| work.shares_ledger(metadata.work())) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native verifier sharing requires its original metadata custody" });
    }
    work.admit_retained(inline_bytes as u64)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    work.charge(acyclic_fs::WorkCounters { allocation_operations: 1,
        ..acyclic_fs::WorkCounters::default() })
        .map_err(|cause| Error::NativeWork { cause, custody: None })
}

impl CompositeForkVerifier {
    /// Rejects duplicate registrations for one exact provider contract.
    pub fn new(verifiers: Vec<std::sync::Arc<dyn ForkSeedVerifier>>) -> Result<Self> {
        Self::new_with_work(verifiers.into_iter(), &mut None)
    }

    fn new_with_work(
        verifiers: impl Iterator<Item = std::sync::Arc<dyn ForkSeedVerifier>>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut owned = ForkValidationWork::new(work)?;
        let mut providers = BTreeMap::new();
        for verifier in verifiers {
            owned.checkpoint()?;
            verifier.provider().validate_with_work(owned.work())?;
            let key = provider_key(verifier.provider(), &mut owned)?;
            if providers.contains_key(&key) {
                return Err(owned.invalid("fork provider verifier is registered twice"));
            }
            owned.slots::<(String, std::sync::Arc<dyn ForkSeedVerifier>)>(1)?;
            providers.insert(key, verifier);
        }
        owned.quota.transfer();
        Ok(Self {
            providers,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata: None,
        })
    }

    /// Registers the same original provider handles on the caller's native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        verifiers: &[std::sync::Arc<dyn ForkSeedVerifier>],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut verifier = Self::new_with_work(verifiers.iter().cloned(), &mut Some(&mut io))?;
        verifier._native_metadata = Some(io.into_custody());
        Ok(verifier)
    }

    /// Shares original provider registrations, not their authority or private backing.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_native_shared(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        admit_native_verifier_share(self._native_metadata.as_ref(), work, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(self))
    }

    /// Fences the child-private writer, then verifies all selected providers.
    pub async fn activate(&self, seed: &ForkSeed) -> Result<Box<dyn ForkPublicationGuard>> {
        self.activate_with_work(seed, &mut None).await.map(|observed| observed.guard)
    }

    pub(crate) async fn activate_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ObservedForkPublicationGuard> {
        seed.validate_with_work(work)?;
        let private = self.provider(seed.child_private_volume.provider(), &mut ForkValidationWork::new(work)?)?;
        let guard = acquire_fork_fence(private.as_ref(), seed, work).await?;
        if let Err(error) = self.verify_with_work(seed, work).await {
            let released = release_fork_guard_with_work(&*guard, work).await;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let preserve_primary = work.as_ref().is_some_and(|work| work.original_native_work().is_some());
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let preserve_primary = false;
            if released.is_err() && !preserve_primary {
                return Err(Error::Indeterminate(seed.operation_id));
            }
            return Err(error);
        }
        Ok(guard)
    }

    /// Recovers and releases a previously held private fence only after an exact Stream idempotency observation proves publication terminal.
    pub async fn release_private_fence(&self, seed: &ForkSeed) -> Result<()> {
        self.release_private_fence_with_work(seed, &mut None).await
    }

    pub(crate) async fn release_private_fence_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate_with_work(work)?;
        let private = self.provider(seed.child_private_volume.provider(), &mut ForkValidationWork::new(work)?)?;
        match acquire_fork_fence(private.as_ref(), seed, work).await {
            Ok(guard) => release_fork_guard_with_work(&*guard, work).await,
            // A different operation can acquire the gate after this fork's terminal release.
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Verifies every source, child, private, inherited, and attested provider.
    pub async fn verify(&self, seed: &ForkSeed) -> Result<()> {
        self.verify_with_work(seed, &mut None).await
    }

    pub(crate) async fn verify_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate_with_work(work)?;
        let mut owned = ForkValidationWork::new(work)?;
        // Logical registered identity entries, not allocator-node estimates.
        owned.slots::<(String, std::sync::Arc<dyn ForkSeedVerifier>)>(self.providers.len())?;
        for key in self.providers.keys() {
            owned.retain(key.capacity() as u64)?;
        }
        let mut required = BTreeSet::new();
        let providers = std::iter::once(seed.child_private_volume.provider())
            .chain(seed.inherited_context.iter().map(|file| file.volume().provider()))
            .chain(seed.reference_grants.iter().map(|grant| grant.file.volume().provider()))
            .chain(seed.attachment_manifests.iter().map(|file| file.volume().provider()))
            .chain(seed.resources.iter().flat_map(|capture| [
                capture.source.provider(), capture.revision.provider(),
            ]))
            .chain(seed.boundary.iter().map(|boundary| &boundary.provider));
        for provider in providers {
            let key = provider_key(provider, &mut owned)?;
            if required.contains(&key) {
                let capacity = key.capacity() as u64;
                drop(key);
                owned.release(capacity)?;
            } else {
                owned.slots::<String>(1)?;
                required.insert(key);
            }
        }
        for key in &required {
            let verifier = self.providers.get(key).ok_or_else(|| {
                Error::Unsupported("fork resource has no exact provider verifier".into())
            })?;
            verify_fork_provider_with_work(verifier.as_ref(), seed, owned.work()).await?;
            if let Some(boundary) = &seed.boundary
                && &boundary.provider == verifier.provider()
            {
                verify_fork_boundary_with_work(verifier.as_ref(), boundary, owned.work()).await?;
            }
        }
        self.verify_references(seed, &mut owned).await
    }

    fn provider<'a>(
        &'a self,
        provider: &ProviderRef,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<&'a std::sync::Arc<dyn ForkSeedVerifier>> {
        let key = provider_key(provider, owned)?;
        let verifier = self.providers.get(&key)
            .ok_or_else(|| Error::Unsupported("fork resource has no exact provider verifier".into()));
        let capacity = key.capacity() as u64;
        drop(key);
        owned.release(capacity)?;
        verifier
    }

    /// Verifies direct references and complete attachment manifests after provider-wide resource capture has been attested.
    async fn verify_references(
        &self,
        seed: &ForkSeed,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        let mut verified_files = BTreeSet::new();
        let mut reference_bytes = BTreeMap::new();
        let (direct_manifest_reads, member_grants) = collect_manifest_grants(seed, owned)?;
        let mut volume_readers = ForkVolumeReaders::new(seed, owned)?;
        self.verify_reference_files(seed, &mut verified_files, &mut reference_bytes, owned)
            .await?;
        self.verify_attachment_manifests(
            seed,
            &mut verified_files,
            &mut reference_bytes,
            &mut volume_readers,
            &direct_manifest_reads,
            &member_grants,
            owned,
        )
        .await
    }

    async fn verify_reference_files<'a>(
        &'a self,
        seed: &ForkSeed,
        verified_files: &mut BTreeSet<[u8; 32]>,
        reference_bytes: &mut BTreeMap<&'a ProviderRef, u64>,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        for file in seed
            .inherited_context
            .iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
        {
            let capability = owned.file_digest(file)?;
            if verified_files.contains(&capability) {
                continue;
            }
            owned.slots::<[u8; 32]>(1)?;
            verified_files.insert(capability);
            let verifier = self.provider(file.volume().provider(), owned)?;
            reserve_reference_bytes(reference_bytes, verifier.as_ref(), file, owned)?;
            verify_fork_file_with_work(verifier.as_ref(), seed, file, owned.work()).await?;
        }
        Ok(())
    }

    async fn verify_attachment_manifests<'a>(
        &'a self,
        seed: &ForkSeed,
        verified_files: &mut BTreeSet<[u8; 32]>,
        reference_bytes: &mut BTreeMap<&'a ProviderRef, u64>,
        volume_readers: &mut ForkVolumeReaders,
        direct_manifest_reads: &DirectManifestReads,
        member_grants: &ManifestMemberGrants,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        let mut manifest_members = 0_usize;
        for manifest in &seed.attachment_manifests {
            let manifest_capability = owned.file_digest(manifest)?;
            let verifier = self.provider(manifest.volume().provider(), owned)?;
            if !verified_files.contains(&manifest_capability) {
                owned.slots::<[u8; 32]>(1)?;
                verified_files.insert(manifest_capability);
                reserve_reference_bytes(reference_bytes, verifier.as_ref(), manifest, owned)?;
                verify_fork_file_with_work(verifier.as_ref(), seed, manifest, owned.work()).await?;
            }
            let (bytes, retained) = read_fork_manifest(
                verifier.as_ref(), seed, manifest, owned.work()).await?;
            if let Err(error) = owned.adopt(retained) {
                drop(bytes);
                if let Some(observer) = owned.work().as_deref_mut() {
                    let _ = observer.release_owned(retained);
                }
                return Err(error);
            }
            let items = decode_fork_manifest(manifest, bytes.as_ref(), owned);
            drop(bytes);
            owned.release(retained)?;
            let items = items?;
            manifest_members = manifest_members
                .checked_add(items.len())
                .ok_or_else(|| Error::Invalid("fork manifest membership count overflow".into()))?;
            if manifest_members as u64 > MAX_FORK_REFERENCES as u64 {
                return Err(Error::Invalid(
                    "fork manifests exceed aggregate member limit".into(),
                ));
            }
            Self::verify_manifest_grants(
                manifest,
                &items,
                volume_readers,
                direct_manifest_reads,
                member_grants,
                owned,
            )?;
            for item in items {
                let item_capability = owned.file_digest(&item.file)?;
                if !verified_files.contains(&item_capability) {
                    owned.slots::<[u8; 32]>(1)?;
                    verified_files.insert(item_capability);
                    let member_verifier = self.provider(item.file.volume().provider(), owned)?;
                    reserve_reference_bytes(reference_bytes, member_verifier.as_ref(), &item.file, owned)?;
                    verify_fork_file_with_work(member_verifier.as_ref(), seed, &item.file, owned.work()).await?;
                }
            }

        }
        Ok(())
    }

    fn verify_manifest_grants(
        manifest: &FileRef,
        items: &[crate::conversation::Attachment],
        volume_readers: &mut ForkVolumeReaders,
        direct_manifest_reads: &DirectManifestReads,
        member_grants: &ManifestMemberGrants,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        let manifest_capability = owned.file_digest(manifest)?;
        let mut members = BTreeSet::new();
        for item in items {
            let capability = owned.file_digest(&item.file)?;
            if !members.contains(&capability) {
                owned.slots::<[u8; 32]>(1)?;
                members.insert(capability);
            }
        }
        let grants = member_grants.get(&manifest_capability);
        if grants.is_some_and(|grants| grants.keys().any(|file| !members.contains(file))) {
            return Err(Error::Invalid(
                "fork reference is not in its published manifest".into(),
            ));
        }
        let implicit_readers = volume_readers.implicit_readers(manifest.volume(), owned)?;
        let mut manifest_readers = BTreeSet::new();
        if let Some(grants) = grants {
            for readers in grants.values() {
                for reader in readers {
                    if !manifest_readers.contains(reader) {
                        owned.slots::<AgentId>(1)?;
                        manifest_readers.insert(*reader);
                    }
                }
            }
        }
        for reader in manifest_readers {
            if !implicit_readers.contains(&reader)
                && !direct_manifest_reads.iter().any(|(file, agent)| {
                    file == &manifest_capability && *agent == reader
                })
            {
                return Err(Error::Invalid(
                    "fork omits a manifest attachment read grant".into(),
                ));
            }
        }
        Ok(())
    }
}

fn collect_manifest_grants(
    seed: &ForkSeed,
    owned: &mut ForkValidationWork<'_, '_>,
) -> Result<(DirectManifestReads, ManifestMemberGrants)> {
    let mut direct_manifest_reads = BTreeSet::new();
    let mut member_grants = ManifestMemberGrants::new();
    for grant in &seed.reference_grants {
        if let Some(manifest) = &grant.attachment_manifest {
            let manifest_capability = owned.file_digest(manifest)?;
            let file_capability = owned.file_digest(&grant.file)?;
            if manifest_capability == file_capability {
                owned.slots::<([u8; 32], AgentId)>(1)?;
                direct_manifest_reads.insert((file_capability, grant.reader));
                continue;
            }
            owned.slots::<([u8; 32], BTreeMap<[u8; 32], BTreeSet<AgentId>>)>(1)?;
            owned.slots::<([u8; 32], BTreeSet<AgentId>)>(1)?;
            owned.slots::<AgentId>(1)?;
            member_grants
                .entry(manifest_capability)
                .or_default()
                .entry(file_capability)
                .or_default()
                .insert(grant.reader);
        } else {
            let file_capability = owned.file_digest(&grant.file)?;
            owned.slots::<([u8; 32], AgentId)>(1)?;
            direct_manifest_reads.insert((file_capability, grant.reader));
        }
    }
    Ok((direct_manifest_reads, member_grants))
}

/// Precomputed reader authority for a fork.
struct ForkVolumeReaders {
    all: BTreeSet<AgentId>,
    selected_projects: BTreeSet<[u8; 32]>,
    shared: BTreeMap<[u8; 32], BTreeSet<AgentId>>,
    implicit: BTreeMap<[u8; 32], BTreeSet<AgentId>>,
}

impl ForkVolumeReaders {
    fn new(seed: &ForkSeed, owned: &mut ForkValidationWork<'_, '_>) -> Result<Self> {
        let mut all = BTreeSet::new();
        for agent in std::iter::once(seed.child_agent).chain(seed.attached_agents.iter().copied()) {
            if !all.contains(&agent) {
                owned.slots::<AgentId>(1)?;
                all.insert(agent);
            }
        }
        let mut selected_projects = BTreeSet::new();
        for resource in &seed.resources {
            if let ResourceRevision::Project { volume, .. } = &resource.revision {
                let key = owned.digest(volume)?;
                if !selected_projects.contains(&key) {
                    owned.slots::<[u8; 32]>(1)?;
                    selected_projects.insert(key);
                }
            }
        }
        let mut shared = BTreeMap::<[u8; 32], BTreeSet<AgentId>>::new();
        for grant in &seed.shared_grants {
            if grant.operations.contains(&VolumeOperation::Read) {
                let key = owned.digest(&grant.volume)?;
                if let Some(readers) = shared.get_mut(&key) {
                    if !readers.contains(&grant.child_agent) {
                        owned.slots::<AgentId>(1)?;
                        readers.insert(grant.child_agent);
                    }
                } else {
                    owned.slots::<([u8; 32], BTreeSet<AgentId>)>(1)?;
                    owned.slots::<AgentId>(1)?;
                    shared.insert(key, BTreeSet::from([grant.child_agent]));
                }
            }
        }
        Ok(Self {
            all,
            selected_projects,
            shared,
            implicit: BTreeMap::new(),
        })
    }

    /// Readers that can resolve a file through a whole-volume grant rather than through a direct exact-file reference.
    fn implicit_readers(
        &mut self,
        volume: &VolumeRef,
        owned: &mut ForkValidationWork<'_, '_>,
    ) -> Result<&BTreeSet<AgentId>> {
        let key = owned.digest(volume)?;
        if self.implicit.contains_key(&key) {
            let readers = self.implicit.get(&key)
                .ok_or_else(|| Error::Invalid("fork reader cache entry disappeared".into()))?;
            return Ok(readers);
        }
        owned.slots::<([u8; 32], BTreeSet<AgentId>)>(1)?;
        let mut implicit_readers = BTreeSet::new();
        match volume.class() {
            VolumeClass::AgentPrivate => {
                if let VolumeOwner::Agent(owner) = volume.owner()
                    && self.all.contains(owner)
                {
                    owned.slots::<AgentId>(1)?;
                    implicit_readers.insert(*owner);
                }
            }
            VolumeClass::Project if self.selected_projects.contains(&key) => {
                owned.slots::<AgentId>(self.all.len())?;
                implicit_readers.extend(self.all.iter().copied());
            }
            VolumeClass::SessionShared => {
                if let Some(granted) = self.shared.get(&key) {
                    owned.slots::<AgentId>(granted.len())?;
                    implicit_readers.extend(granted.iter().copied());
                }
            }
            VolumeClass::Project => {}
        }
        Ok(self.implicit.entry(key).or_insert(implicit_readers))
    }
}

fn reserve_reference_bytes<'a>(
    totals: &mut BTreeMap<&'a ProviderRef, u64>,
    verifier: &'a dyn ForkSeedVerifier,
    file: &FileRef,
    owned: &mut ForkValidationWork<'_, '_>,
) -> Result<()> {
    let key = verifier.provider();
    if !totals.contains_key(key) { owned.slots::<(&ProviderRef, u64)>(1)?; }
    let total = totals.entry(key).or_default();
    *total = total
        .checked_add(file.descriptor().byte_length())
        .ok_or_else(|| Error::Invalid("fork reference bytes overflow".into()))?;
    if *total > verifier.maximum_reference_bytes() {
        return Err(Error::Invalid(
            "fork reference bytes exceed aggregate limit".into(),
        ));
    }
    Ok(())
}

fn provider_key(
    provider: &ProviderRef,
    owned: &mut ForkValidationWork<'_, '_>,
) -> Result<String> {
    let length = provider.namespace().len().checked_add(provider.family().len())
        .and_then(|length| length.checked_add(provider.version().len()))
        .and_then(|length| length.checked_add(2))
        .ok_or_else(|| owned.invalid("fork provider identity length overflows"))?;
    let mut key = String::new();
    if let Some(observer) = owned.work().as_deref_mut() {
        observer.reserve_text(&mut key, length)?;
    } else {
        key.reserve_exact(length);
    }
    owned.adopt(key.capacity() as u64)?;
    for part in [provider.namespace(), "\0", provider.family(), "\0", provider.version()] {
        if let Some(observer) = owned.work().as_deref_mut() { observer.admit_copy(part.len() as u64)?; }
        key.push_str(part);
        if let Some(observer) = owned.work().as_deref_mut() { observer.copy_bytes(part.len() as u64)?; }
    }
    Ok(key)
}

fn decode_fork_manifest(
    reference: &FileRef,
    bytes: &[u8],
    owned: &mut ForkValidationWork<'_, '_>,
) -> Result<Vec<crate::conversation::Attachment>> {
    if owned.work().is_none() {
        return decode_complete_attachment_manifest(reference, bytes);
    }
    reference.validate()?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(Error::Invalid("attachment manifest metadata is invalid".into()));
    }
    if bytes.len() as u64 == reference.descriptor().byte_length()
        && let Some(observer) = owned.work().as_deref_mut()
    {
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    reference.descriptor().verify(bytes)?;
    let maximum = crate::store::default_projection_read_limits().maximum_bytes;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid("attachment manifest exceeds finite codec allowance".into()));
    }
    let schema_bytes = (bytes.len() as u64).max(8).checked_mul(2)
        .and_then(|count| count.checked_mul(
            (std::mem::size_of::<crate::conversation::Attachment>()
                + std::mem::size_of::<String>() + 1) as u64,
        ))
        .ok_or_else(|| Error::Invalid("attachment manifest schema storage overflows".into()))?;
    owned.retain(schema_bytes)?;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Attachment", "file", "label", "FileRefWire", "volume", "path", "version",
            "descriptor", "display_name", "FileDescriptorWire", "sha256", "byte_length",
            "media_type", "VolumeRefWire", "provider", "id", "class", "owner",
            "ProviderRefWire", "namespace", "family", "project", "agent_private",
            "session_shared", "kind", "agent", "session",
            "SHA-256 digest must contain exactly 32 bytes", "provider identity is invalid"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native = owned.work().as_deref().is_some_and(|observer| observer.original_native_work().is_some());
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let native = false;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: maximum,
        maximum_items: maximum.saturating_add(1),
        maximum_depth: 256,
    };
    let mut observed_failure = false;
    let mut charge = |charge| {
        let result = match charge {
            crate::contract::JsonCodecCharge::Admission { .. } => {
                owned.retain(diagnostic_bytes).and_then(|()| owned.checkpoint())
            }
            _ => match owned.work().as_deref_mut() {
                Some(observer) => observer.codec_charge(charge),
                None => Ok(()),
            },
        };
        observed_failure |= result.is_err();
        result
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut decoded = if native {
        crate::contract::json_from_slice_bounded_native_trusted::<Vec<crate::conversation::Attachment>, _>(
            bytes, bounds, schema_bytes, crate::wire::ErrorCode::Invalid,
            crate::contract::JsonValueSchema::None, &mut charge,
        )
    } else {
        crate::contract::json_from_slice_bounded_trusted(bytes, bounds, schema_bytes, &mut charge)
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let decoded: Result<Vec<crate::conversation::Attachment>> =
        crate::contract::json_from_slice_bounded_trusted(bytes, bounds, schema_bytes, &mut charge);
    drop(charge);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeDecode { custody, .. }) = &mut decoded
        && custody.is_none()
    {
        let reservation = schema_bytes.checked_add(diagnostic_bytes)
            .expect("original admitted manifest reservation");
        let guard = owned.work().as_deref_mut().expect("original manifest observer")
            .take_native_custody(reservation).expect("original manifest reservation custody");
        let retained = owned.quota.retained().checked_sub(reservation)
            .expect("original manifest reservation belongs to the proof custodian");
        owned.quota.transfer();
        owned.quota.adopt(retained)?;
        *custody = Some(guard);
    }
    let items = decoded.map_err(|error| {
        if native || observed_failure { error }
        else { Error::Invalid(format!("attachment manifest is invalid: {error}")) }
    })?;
    owned.release(diagnostic_bytes)?;
    if items.len() as u64 > crate::conversation::MAX_LIMIT_ATTACHMENTS as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in &items {
        item.validate()?;
    }
    let canonical = crate::contract::json_bytes_bounded(&items, bounds, None, &mut |charge| {
        match charge {
            crate::contract::JsonCodecCharge::Retain { bytes } => owned.retain(bytes),
            crate::contract::JsonCodecCharge::Release { bytes } => owned.release(bytes),
            _ => match owned.work().as_deref_mut() {
                Some(observer) => observer.codec_charge(charge),
                None => Ok(()),
            },
        }
    })?;
    owned.retain((canonical.capacity() - canonical.len()) as u64)?;
    let canonical_capacity = canonical.capacity() as u64;
    let matches = canonical == bytes;
    drop(canonical);
    owned.release(canonical_capacity)?;
    if !matches {
        return Err(Error::Invalid("attachment manifest is not canonical or complete".into()));
    }
    Ok(items)
}

/// Fork barrier for immutable file-only providers such as Objects.
pub struct ContentForkVerifier {
    provider: ProviderRef,
    content: std::sync::Arc<dyn ContentResidencyVerifier>,
}

impl ContentForkVerifier {
    /// Binds one exact provider identity to its authenticated content reader.
    pub fn new(
        provider: ProviderRef,
        content: std::sync::Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        provider.validate()?;
        Ok(Self { provider, content })
    }

    async fn verify_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate_with_work(work)?;
        if seed.child_private_volume.provider() == &self.provider
            || seed.resources.iter().any(|capture| {
                capture.source.provider() == &self.provider
                    || capture.revision.provider() == &self.provider
            })
        {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Unsupported,
                "content-only provider cannot attest mutable fork resources"));
        }
        let mut owned = ForkValidationWork::new(work)?;
        let mut checked = BTreeSet::new();
        for file in seed.inherited_context.iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
            .chain(seed.attachment_manifests.iter())
        {
            if file.volume().provider() != &self.provider {
                continue;
            }
            let capability = owned.file_digest(file)?;
            if checked.contains(&capability) {
                continue;
            }
            owned.slots::<[u8; 32]>(1)?;
            checked.insert(capability);
            self.verify_file_with_work(file, owned.work()).await?;
        }
        Ok(())
    }

    async fn read_manifest_with_work(
        &self,
        manifest: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(acyclic_stream::OwnedBytes, u64)> {
        if manifest.volume().provider() != &self.provider {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "fork manifest belongs to another provider"));
        }
        match work.as_deref_mut() {
            Some(observer) => crate::store::history::read_content_with_work(
                observer, self.content.as_ref(), manifest).await,
            None => self.content.read(manifest).await.map(|bytes| (bytes, 0)),
        }
    }

    async fn verify_file_with_work(
        &self,
        file: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if file.volume().provider() != &self.provider {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "fork file belongs to another provider"));
        }
        match work.as_deref_mut() {
            Some(observer) => crate::store::history::verify_content_with_work(
                observer, self.content.as_ref(), file).await,
            None => self.content.verify(file).await,
        }
    }
}

impl ForkSeedVerifier for ContentForkVerifier {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_with_work(seed, &mut None).await })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(native, || async move {
            let mut io = crate::store::history::native_work_io(native)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.verify_with_work(seed, &mut Some(&mut io)).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_manifest_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, manifest: &'a FileRef,
        native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(native, || async move {
            let mut io = crate::store::history::native_work_io(native)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let (bytes, _retained) = self.read_manifest_with_work(manifest, &mut Some(&mut io)).await?;
            // The native carrier itself retains original byte custody.
            drop(io);
            Ok(bytes)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_file_with_native_work<'a>(
        &'a self, _seed: &'a ForkSeed, file: &'a FileRef,
        native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(native, || async move {
            let mut io = crate::store::history::native_work_io(native)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.verify_file_with_work(file, &mut Some(&mut io)).await
        })
    }

    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self,
        seed: &'a ForkSeed,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
            let result = async {
                work.preflight()?;
                self.verify_with_work(seed, &mut Some(&mut work)).await
            }.await;
            work.finish(result)
        })
    }

    fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            self.read_manifest_with_work(manifest, &mut None).await.map(|(bytes, _)| bytes)
        })
    }

    #[cfg(feature = "filesystem")]
    fn read_manifest_measured<'a>(
        &'a self,
        manifest: &'a FileRef,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<(acyclic_stream::OwnedBytes, u64)>, Error>> {
        Box::pin(async move {
            let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
            let result = async {
                work.preflight()?;
                // The source receipt records cumulative work and its observed peak, not a physical storage lease.
                self.read_manifest_with_work(manifest, &mut Some(&mut work)).await
            }.await;
            work.finish(result)
        })
    }

    fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_file_with_work(file, &mut None).await })
    }

    #[cfg(feature = "filesystem")]
    fn verify_file_measured<'a>(
        &'a self,
        file: &'a FileRef,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
            let result = async {
                work.preflight()?;
                self.verify_file_with_work(file, &mut Some(&mut work)).await
            }.await;
            work.finish(result)
        })
    }
}

/// Stream history proof scoped to the parent aggregate already opened by the admission path. `ForkSeed::validate` checks its exact path and revision.
pub struct StreamHistoryForkVerifier {
    provider: ProviderRef,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl StreamHistoryForkVerifier {
    /// Registers the exact Stream provider used for parent history refs.
    pub fn new(provider: ProviderRef) -> Result<Self> {
        provider.validate()?;
        if provider.family() != "stream" {
            return Err(Error::Invalid(
                "history verifier requires a Stream provider".into(),
            ));
        }
        Ok(Self {
            provider,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata: None,
        })
    }

    /// Copies only this original provider identity under the native caller's work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        provider: &ProviderRef, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut observed = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        provider.validate_with_work(quota.work())?;
        if provider.family() != "stream" {
            return Err(fork_provider_boundary(quota.work(), crate::wire::ErrorCode::Invalid,
                "history verifier requires a Stream provider"));
        }
        let provider = crate::core::copy_provider(provider, &mut quota)?;
        quota.transfer();
        drop(quota);
        Ok(Self { provider, _native_metadata: Some(io.into_custody()) })
    }

    /// Shares the same funded provider identity; Arc's private backing is opaque.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_native_shared(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        admit_native_verifier_share(self._native_metadata.as_ref(), work, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(self))
    }

    async fn verify_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate_with_work(work)?;
        for capture in &seed.resources {
            for revision in [&capture.source, &capture.revision] {
                if revision.provider() == &self.provider
                    && !matches!(revision, ResourceRevision::History(_))
                {
                    return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Unsupported,
                        "Stream history verifier cannot prove this resource"));
                }
            }
        }
        Ok(())
    }
}

impl ForkSeedVerifier for StreamHistoryForkVerifier {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_with_work(seed, &mut None).await })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(native, || async move {
            let mut io = crate::store::history::native_work_io(native)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.verify_with_work(seed, &mut Some(&mut io)).await
        })
    }

    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self,
        seed: &'a ForkSeed,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
            let result = async {
                work.preflight()?;
                self.verify_with_work(seed, &mut Some(&mut work)).await
            }.await;
            work.finish(result)
        })
    }
}

/// One exact retained resource revision selected for a child.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "reference", rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ResourceRevision {
    /// Immutable Stream history prefix.
    History(#[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))] StreamRef),
    /// Immutable project workspace generation.
    Project {
        /// Project volume selected for the child.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
        volume: VolumeRef,
        /// Exact immutable generation in that volume.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
        generation: GenerationRef,
    },
    /// Direct parent's pinned scratch.
    PrivateVolume {
        /// Parent or captured child scratch identity.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
        volume: VolumeRef,
        /// Immutable source or child generation.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
        generation: GenerationRef,
        /// Explicit scratch paths, excluding reserved runtime state.
        #[cfg_attr(feature = "wasm", tsify(type = "readonly string[]"))]
        paths: Vec<String>,
    },
    /// Selected model context revision.
    Context(#[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))] ContextRef),
    /// Qualified process checkpoint.
    Process(#[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))] CheckpointRef),
    /// Immutable retained artifact.
    Artifact(#[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))] ArtifactRef),
    /// Session-shared volume reference.
    SharedVolume(#[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))] VolumeRef),
    /// Namespaced extension state with pinned implementation version.
    Extension {
        /// Namespaced state identity.
        name: String,
        /// Immutable extension implementation version.
        version: u32,
        /// Digest of the exact extension implementation selected by the parent.
        #[cfg_attr(feature = "wasm", tsify(type = "readonly number[]"))]
        implementation_digest: [u8; 32],
        /// Provider-owned retained state reference.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
        reference: ResourceRef,
    },
}

impl ResourceRevision {
    /// Exact provider contract owning this revision.
    #[must_use]
    pub fn provider(&self) -> &ProviderRef {
        match self {
            Self::History(value) => value.as_resource().provider(),
            Self::Project { volume, .. }
            | Self::PrivateVolume { volume, .. }
            | Self::SharedVolume(volume) => volume.provider(),
            Self::Context(value) => value.as_resource().provider(),
            Self::Process(value) => value.as_resource().provider(),
            Self::Artifact(value) => value.as_resource().provider(),
            Self::Extension { reference, .. } => reference.provider(),
        }
    }

    /// Checks provider families and the semantic resource class.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        match self {
            Self::PrivateVolume {
                volume,
                generation,
                paths,
            } if volume.class() == VolumeClass::AgentPrivate
                && generation.as_resource().provider() == volume.provider()
                && volume.provider().family() == "filesystem" =>
            {
                volume.validate_with_work(work)?;
                generation.validate_with_work(work)?;
                if paths.len() as u64 > MAX_FORK_RESOURCES as u64 {
                    return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                        "too many scratch paths"));
                }
                for path in paths {
                    let relative = path
                        .strip_prefix('/')
                        .ok_or_else(|| fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                            "scratch path must be absolute"))?;
                    crate::conversation::validate_content_path_with_work(relative, work)?;
                    if relative.split('/').any(|segment| segment == ".system") {
                        return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Unauthorized,
                            "scratch selection includes reserved runtime state"));
                    }
                }
                Ok(())
            }
            Self::History(value) if value.as_resource().provider().family() == "stream" => {
                value.validate_with_work(work)
            }
            Self::Project { volume, generation }
                if generation.as_resource().provider() == volume.provider()
                    && volume.provider().family() == "filesystem" =>
            {
                volume.validate_with_work(work)?;
                generation.validate_with_work(work)
            }
            Self::Context(value) => value.validate_with_work(work),
            Self::Process(value) if value.as_resource().provider().family() == "machines" => {
                value.validate_with_work(work)
            }
            Self::Artifact(value) => value.validate_with_work(work),
            Self::SharedVolume(value) if value.class() == VolumeClass::SessionShared => {
                value.validate_with_work(work)
            }
            Self::Extension {
                name,
                version,
                implementation_digest,
                reference,
            } if name.contains('.')
                && !name.chars().any(char::is_control)
                && *version > 0
                && *implementation_digest != [0; 32] =>
            {
                reference.validate_with_work(work)
            }
            _ => Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "fork resource has an invalid kind or provider")),
        }
    }
}

/// Required or optional exact resource selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkSelection {
    /// Required selections must capture successfully before publication.
    pub required: bool,
    /// Exact source revision.
    pub revision: ResourceRevision,
}

/// Provider evidence of a consistent boundary across selected resources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct AttestedBoundary {
    /// Provider qualified to attest this boundary.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmProviderRefWire"))]
    pub provider: ProviderRef,
    /// Opaque bounded proof, interpreted only by that provider.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly number[]"))]
    pub evidence: Vec<u8>,
}

impl AttestedBoundary {
    /// Validates an attestation envelope without claiming to verify its proof.
    pub(crate) fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.provider.validate_with_work(work)?;
        if self.evidence.is_empty() {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "fork boundary evidence is invalid"));
        }
        Ok(())
    }
}

/// Idempotent request to capture selected resources for a new child.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkRequest {
    /// Stable identity used for preparation and publication reconciliation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Publishing parent aggregate.
    pub parent: Authority,
    /// Exact parent revision before publication.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub parent_revision: u64,
    /// Fresh child aggregate.
    pub child: Authority,
    /// Fresh child agent identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child_agent: AgentId,
    /// Additional agents allowed to attach to the child environment as readers.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly string[]"))]
    pub attached_agents: Vec<AgentId>,
    /// Exact child allocation and bounded inherited prefix chosen before preparation.
    pub preparation: ForkPreparation,
    /// Ordered required and optional resource selections.
    #[cfg_attr(feature = "wasm", tsify(type = "readonly ForkSelection[]"))]
    pub selections: Vec<ForkSelection>,
    /// Present only when one provider attests a common capture boundary.
    pub boundary: Option<AttestedBoundary>,
    /// Operation-sealed workspace read; never grants source transcript access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
}

/// Selection of authoritative logical history before a fork is admitted.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ForkHistoryPolicy {
    /// Pins the direct parent's entire logical history at the requested revision.
    #[default]
    Fork,
    /// Explicitly starts without inherited conversation messages.
    Fresh,
    /// Keeps the full logical parent cut with an explicitly selected checkpoint projection.
    Summary(Box<SummaryForkSelection>),
}

/// Immutable provider allocation and context bounds for one fork operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkPreparation {
    /// Fresh project workspace derived from the selected project generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub child_project_volume: VolumeRef,
    /// Fresh private workspace owned by the child agent.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub child_private_volume: VolumeRef,
    /// Optional child-owned unmounted result namespace, distinct from scratch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire | null"))]
    pub child_result_volume: Option<VolumeRef>,
    /// Inclusive final parent conversation sequence; zero selects none.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub inherited_through_sequence: u64,
    /// Deployment limit, no greater than the protocol ceiling.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub maximum_inherited_messages: u64,
    /// Maximum bytes in the child-owned inherited-context file.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub maximum_inherited_bytes: u64,
    /// Maximum retained references in the inherited prefix.
    pub maximum_inherited_references: u32,
    /// Optional immutable Summary projection selected before fork admission.
    pub summary: Option<SummaryForkSelection>,
}

impl ForkPreparation {
    fn validate(
        &self, child_agent: AgentId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(selection) = &self.summary {
            selection.validate_with_work(work)?;
            if self.inherited_through_sequence == 0 {
                return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                    "summary fork requires a nonempty logical parent cut"));
            }
        }
        self.child_project_volume.validate_with_work(work)?;
        self.child_private_volume.validate_with_work(work)?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.owner() != &VolumeOwner::Agent(child_agent)
            || self.child_project_volume.class() != VolumeClass::Project
            || self.child_private_volume.provider() != self.child_project_volume.provider()
            || self.child_private_volume.provider().family() != "filesystem"
            || self.maximum_inherited_messages == 0
            || self.inherited_through_sequence > self.maximum_inherited_messages
            || self.maximum_inherited_bytes == 0
            || self.maximum_inherited_references == 0
        {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "fork preparation is invalid"));
        }
        if let Some(result) = &self.child_result_volume {
            validate_child_result_volume(result, &self.child_private_volume, child_agent, work)?;
            if result == &self.child_project_volume {
                return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                    "child result volume cannot alias the project volume"));
            }
        }
        Ok(())
    }
}

fn validate_child_result_volume(
    result: &VolumeRef, scratch: &VolumeRef, child: AgentId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    result.validate_with_work(work)?;
    if result.class() != VolumeClass::AgentPrivate
        || result.owner() != &VolumeOwner::Agent(child)
        || result.provider() != scratch.provider()
        || result == scratch
    {
        return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
            "child result volume must be child-owned and distinct from scratch"));
    }
    Ok(())
}

fn validate_child_result_generation(
    result: Option<&VolumeRef>, generation: Option<&GenerationRef>,
    scratch: &VolumeRef, child: AgentId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    match (result, generation) {
        (None, None) => Ok(()),
        (Some(result), Some(generation)) => {
            validate_child_result_volume(result, scratch, child, work)?;
            generation.validate_with_work(work)?;
            if generation.as_resource().provider() != result.provider() {
                return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                    "child result generation belongs to another provider"));
            }
            Ok(())
        }
        _ => Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
            "child result volume and generation must be captured together")),
    }
}

/// Provider-neutral owner boundary for an idempotent multi-resource fork preparation.
pub trait ForkPreparer: acyclic_stream::ProviderPlatform {
    /// Exact immutable parent projection this preparer was constructed from.
    fn parent_snapshot(&self) -> (&Authority, u64);

    /// Captures or returns the existing report for the exact request identity.
    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>>;

    /// Observes an uncertain preparation for the exact original request.
    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>>;
}

/// Provider-owned capture of one selected revision during parent-controlled fork preparation.
pub trait ForkCaptureProvider: acyclic_stream::ProviderPlatform {
    /// Exact provider identity whose resource revisions this adapter owns.
    fn provider(&self) -> &ProviderRef;

    /// Captures one immutable child-visible revision or reports an explicit unavailable state.
    fn capture<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>>;

    /// Observes a capture whose start was durably recorded but whose result may have been lost. `None` means unresolved, not permission to repeat the side effect.
    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>>;

    /// Captures the same immutable selection on the supplied original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn capture_with_native_work<'a>(
        &'a self, _request: &'a ForkRequest, _selection: &'a ForkSelection,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Capture, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "fork capture provider has no original native callback" })
    }

    /// Reconciles the original capture operation without repeating uncertain effects or replacing its pinned selection.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self, _request: &'a ForkRequest, _selection: &'a ForkSelection,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Capture>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "fork capture provider has no original native reconciliation callback" })
    }
}

// Logical key/value slots and owned identity buffers, not allocator/node overhead.
pub(super) struct ForkValidationWork<'a, 'b> {
    quota: crate::wire_codec::CodecQuota<'a, 'b>,
}

impl<'a, 'b> ForkValidationWork<'a, 'b> {
    pub(super) fn new(
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        Ok(Self { quota: crate::wire_codec::CodecQuota::from_retained(work, 0) })
    }

    pub(super) fn work(&mut self) -> &mut Option<&'b mut dyn crate::store::history::HistoryIo> {
        self.quota.work()
    }

    fn invalid(&mut self, reason: &'static str) -> Error {
        fork_provider_boundary(self.work(), crate::wire::ErrorCode::Invalid, reason)
    }

    pub(super) fn checkpoint(&mut self) -> Result<()> {
        if let Some(observer) = self.work().as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        Ok(())
    }

    pub(super) fn retain(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() { return Ok(()); }
        let retained = self.quota.retained().checked_add(bytes)
            .ok_or_else(|| self.invalid("fork validation storage overflows"))?;
        self.quota.replace(retained)
    }

    /// Adopts buffers already retained by the same observer without charging their allocation again; the existing guard releases them on every exit.
    pub(super) fn adopt(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_some() {
            self.quota.adopt(bytes).map_err(|_| self.invalid("fork custody storage overflows"))?;
        }
        Ok(())
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() { return Ok(()); }
        self.quota.retained().checked_sub(bytes)
            .ok_or_else(|| self.invalid("fork validation storage underflows"))?;
        self.quota.relinquish(bytes)
    }

    pub(super) fn slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = (count as u64).checked_mul(std::mem::size_of::<T>() as u64)
            .ok_or_else(|| self.invalid("fork validation slot count overflows"))?;
        self.retain(bytes)
    }

    pub(super) fn canonical<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<Vec<u8>> {
        let bytes = crate::store::history::canonical_bytes_with_work(
            value, crate::store::default_projection_read_limits().maximum_bytes, self.work(),
        )?;
        self.adopt(bytes.capacity() as u64)?;
        Ok(bytes)
    }

    pub(super) fn digest<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<[u8; 32]> {
        let bytes = self.canonical(value)?;
        let capacity = bytes.capacity() as u64;
        if let Some(observer) = self.work().as_deref_mut() {
            observer.hashed_bytes(bytes.len() as u64)?;
        }
        let digest = *blake3::hash(&bytes).as_bytes();
        drop(bytes);
        self.release(capacity)?;
        Ok(digest)
    }

    fn capability<T: Serialize + ?Sized>(&mut self, prefix: &str, value: &T) -> Result<String> {
        let digest = self.digest(value)?;
        let length = prefix.len().checked_add(64)
            .ok_or_else(|| Error::Invalid("fork capability storage overflows".into()))?;
        if let Some(observer) = self.work().as_deref_mut() {
            observer.admit_allocation(length as u64)?;
        }
        self.retain(length as u64)?;
        let mut capability = String::new();
        capability.try_reserve_exact(length).map_err(|error| Error::Invalid(error.to_string()))?;
        if let Some(observer) = self.work().as_deref_mut() {
            observer.copy_bytes(length as u64)?;
        }
        capability.push_str(prefix);
        capability.push_str(blake3::Hash::from_bytes(digest).to_hex().as_str());
        self.retain((capability.capacity() - length) as u64)?;
        Ok(capability)
    }

    pub(super) fn file_capability(&mut self, file: &FileRef) -> Result<String> {
        file.validate_with_work(self.work())?;
        self.capability("file:read:", file)
    }

    fn file_digest(&mut self, file: &FileRef) -> Result<[u8; 32]> {
        file.validate_with_work(self.work())?;
        self.digest(file)
    }

    pub(super) fn volume_capability(&mut self, volume: &VolumeRef) -> Result<String> {
        self.capability("volume:read:", volume)
    }

    fn stream_path(&mut self, authority: &Authority) -> Result<String> {
        let path = authority.stream_path_with_work(self.work())?;
        self.adopt(path.capacity() as u64)?;
        Ok(path)
    }
}

impl ForkRequest {
    /// Resolves the context policy against the exact parent before preparation.
    pub fn with_history_policy(
        self,
        parent: &crate::core::Reducer,
        policy: ForkHistoryPolicy,
    ) -> Result<Self> {
        self.with_history_policy_with_work(parent, policy, &mut None)
    }

    pub(crate) fn with_history_policy_with_work(
        mut self,
        parent: &crate::core::Reducer,
        policy: ForkHistoryPolicy,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_deref().is_some_and(|observer| observer.original_native_work().is_some());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        let policy_error = |code, reason: &'static str| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native { return Error::NativeBoundary { code, reason }; }
            let _ = native;
            match code {
                crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
                _ => Error::Invalid(reason.into()),
            }
        };
        if &self.parent != parent.authority() || self.parent_revision != parent.revision() {
            return Err(policy_error(crate::wire::ErrorCode::Conflict,
                "fork context policy requires the exact requested parent revision"));
        }
        let conversation = parent.conversation().ok_or_else(|| policy_error(
            crate::wire::ErrorCode::Invalid, "fork context policy requires a parent conversation"))?;
        if conversation.agent.is_none() {
            return Err(policy_error(crate::wire::ErrorCode::Invalid,
                "fork context policy requires a bound parent agent"));
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let full_cut = conversation.logical_revision();
        let (through_sequence, summary) = match policy {
            ForkHistoryPolicy::Fork => (full_cut, None),
            ForkHistoryPolicy::Fresh => (0, None),
            ForkHistoryPolicy::Summary(selection) => (full_cut, Some(*selection)),
        };
        self.preparation.inherited_through_sequence = through_sequence;
        self.preparation.summary = summary;
        self.validate_with_work(work)?;
        Ok(self)
    }

    /// Moves a funded selection into a request being built before admission.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn add_selection_with_native_work(
        &mut self,
        selection: ForkSelection,
        request_custody: &mut acyclic_fs::NativeAllocation,
        mut selection_custody: acyclic_fs::NativeAllocation,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        use crate::store::history::HistoryIo as _;
        // Keep the owned input outside all fallible work, so its fields always die before the incoming guard, even when validation or reserve fails.
        let mut selection = Some(selection);
        let mut merged_input = 0;
        let native_error = |cause| Error::NativeWork { cause, custody: None };
        let result = (|| {
            let old_backing = self.selections.capacity()
                .checked_mul(std::mem::size_of::<ForkSelection>())
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow)))? as u64;
            if !request_custody.matches_work(original)
                || !selection_custody.matches_work(original)
                || request_custody.bytes() == 0
                || request_custody.bytes() < old_backing
                || selection_custody.bytes() == 0
            {
                return Err(native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
            }
            request_custody.bytes().checked_add(selection_custody.bytes())
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow)))?;
            // Absorb also checks the SDK's private claim-time capture context.
            let input_bytes = selection_custody.bytes();
            request_custody.absorb(&mut selection_custody).map_err(native_error)?;
            merged_input = input_bytes;
            let count = self.selections.len().checked_add(1)
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow)))?;
            let mut io = crate::store::history::native_work_io(original).map_err(native_error)?;
            self.validate_selection_view(
                self.selections.iter().chain(selection.iter()), count, &mut Some(&mut io))?;
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            let full = self.selections.len() == self.selections.capacity();
            // These are actual inline moves, not clones of revision payloads.
            let moved = if full { count } else { 1 };
            let copied = moved.checked_mul(std::mem::size_of::<ForkSelection>())
                .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow)))? as u64;
            io.admit_copy(copied)?;
            if !full {
                io.copy_bytes(copied)?;
                self.selections.push(selection.take().expect("funded selection"));
                merged_input = 0;
                return Ok(());
            }
            let capacity = self.selections.capacity().saturating_mul(2)
                .max(count).min(MAX_FORK_RESOURCES as usize);
            let mut replacement = {
                let mut observed = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
                let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
                let value = crate::core::reserve_copy_vector::<ForkSelection>(capacity, &mut quota)?;
                quota.transfer();
                value
            };
            let mut backing_custody = io.into_custody();
            let new_backing = backing_custody.bytes();
            if let Err(cause) = request_custody.absorb(&mut backing_custody) {
                drop(replacement);
                drop(backing_custody);
                return Err(native_error(cause));
            }
            if let Err(cause) = original.charge(acyclic_fs::WorkCounters {
                bytes_copied: copied, ..acyclic_fs::WorkCounters::default()
            }) {
                drop(replacement);
                request_custody.release(new_backing).map_err(native_error)?;
                return Err(native_error(cause));
            }
            // No fallible work separates the recorded inline copy from moves.
            let old = std::mem::replace(&mut self.selections, Vec::new());
            for value in old {
                replacement.push(value);
            }
            replacement.push(selection.take().expect("funded selection"));
            self.selections = replacement;
            merged_input = 0;
            request_custody.release(old_backing).map_err(native_error)
        })();
        drop(selection);
        drop(selection_custody);
        if result.is_err() && merged_input != 0 {
            request_custody.release(merged_input).map_err(native_error)?;
        }
        result
    }

    /// Checks identities, resource selections and optional attestation metadata.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    /// Checks the borrowed original selection using the same validator and native ledger; this does not authorize a source read or publication.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.validate_with_work(&mut Some(&mut observer))
    }

    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_selection_view(self.selections.iter(), self.selections.len(), work)
    }

    fn validate_selection_view<'a>(
        &self,
        selections: impl Iterator<Item = &'a ForkSelection> + Clone,
        selection_count: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut work = ForkValidationWork::new(work)?;
        if self.attached_agents.len() as u64 > MAX_FORK_AGENTS as u64
            || selection_count as u64 > MAX_FORK_RESOURCES as u64
        {
            return Err(work.invalid("fork request exceeds protocol limits"));
        }
        if self.parent == self.child {
            return Err(work.invalid("fork child cannot equal its parent"));
        }
        if self.parent.kind != crate::core::AggregateKind::Conversation
            || self.child.kind != crate::core::AggregateKind::Conversation
        {
            return Err(work.invalid("fork must link parent and child conversations"));
        }
        let parent_path = work.stream_path(&self.parent)?;
        let _child_path = work.stream_path(&self.child)?;
        self.preparation.validate(self.child_agent, work.work())?;
        if let Some(source) = &self.authorized_source {
            source.validate_with_work(work.work())?;
            if source.operation_id != self.operation_id
                || !selections.clone().any(|selection| matches!(
                    &selection.revision,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation))
                || (source.source_task != source.caller_task
                    && (self.preparation.inherited_through_sequence != 0
                        || self.preparation.summary.is_some()))
            {
                return Err(fork_provider_boundary(work.work(), crate::wire::ErrorCode::Unauthorized,
                    "authorized source must select its exact project and history-free cross-task fork"));
            }
        }
        work.slots::<AgentId>(self.attached_agents.len())?;
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(work.invalid("fork attached agents are not unique"));
        }
        work.slots::<Vec<u8>>(selection_count)?;
        let mut selected = BTreeSet::new();
        let mut histories = 0_usize;
        let mut projects = 0_usize;
        let mut private = 0_usize;
        let mut contexts = 0_usize;
        let mut processes = 0_usize;
        for selection in selections {
            work.checkpoint()?;
            selection.revision.validate_with_work(work.work())?;
            let identity = work.canonical(&selection.revision)?;
            if !selected.insert(identity) {
                return Err(work.invalid("fork resource is selected more than once"));
            }
            match &selection.revision {
                ResourceRevision::History(reference) => {
                    histories += 1;
                    if !selection.required
                        || reference.as_resource().key() != parent_path.as_bytes()
                        || reference
                            .as_resource()
                            .version()
                            .and_then(|version| version.parse::<u64>().ok())
                            != Some(self.parent_revision)
                    {
                        return Err(work.invalid("fork history is not the required exact parent prefix"));
                    }
                }
                ResourceRevision::Project { volume, .. }
                | ResourceRevision::PrivateVolume { volume, .. } => {
                    let (destination, count) = match &selection.revision {
                        ResourceRevision::Project { .. } => {
                            (&self.preparation.child_project_volume, &mut projects)
                        }
                        _ => (&self.preparation.child_private_volume, &mut private),
                    };
                    *count += 1;
                    if !selection.required
                        || volume == destination
                        || volume.provider() != destination.provider()
                    {
                        return Err(work.invalid("fork selected volume does not match child allocation"));
                    }
                }
                ResourceRevision::Context(_) => contexts += 1,
                ResourceRevision::Process(_) => processes += 1,
                _ => {}
            }
        }
        if histories != 1 || projects != 1 || private != 1 || contexts > 1 || processes > 1 {
            return Err(work.invalid("fork requires one history, project and scratch selection"));
        }
        if let Some(boundary) = &self.boundary {
            boundary.validate(work.work())?;
        }
        Ok(())
    }
}

/// Result of one selected provider capture.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "capture variants preserve the published v2 serde shape"
)]
pub enum Capture {
    /// Exact immutable resource revision was captured.
    Captured(CapturedResource),
    /// Provider does not support this requested capture.
    Unsupported(String),
    /// Capture must wait for an existing operation.
    InFlight(OperationId),
    /// The existing operation must be reconciled.
    Indeterminate(OperationId),
}

/// A source revision and the corresponding child-visible retained revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedResource {
    /// Exact parent revision selected for capture.
    pub source: ResourceRevision,
    /// Child-visible revision prepared by the provider.
    pub revision: ResourceRevision,
}

/// An optional resource that was not captured, with its exact reconciliation identity retained in the published child seed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkOmission {
    /// Exact optional selection requested by the caller.
    pub selection: ForkSelection,
    /// Unsupported, in-flight, or indeterminate provider outcome.
    pub outcome: Capture,
}

/// Explicit child-bound operations for one selected session-shared volume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedGrant {
    /// Exact shared volume selected in the fork seed.
    pub volume: VolumeRef,
    /// Child or attached agent to whom the parent delegates these operations.
    pub child_agent: AgentId,
    /// Nonempty set of authorized operations.
    pub operations: BTreeSet<VolumeOperation>,
}

/// Parent-authorized, exact-version read access to a reference for a child agent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceGrant {
    /// Pinned file identity whose bytes may be resolved.
    pub file: FileRef,
    /// Child or explicitly attached agent allowed to request owner-mediated reads.
    pub reader: AgentId,
    /// Published attachment manifest proving a member that is not directly in an event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_manifest: Option<FileRef>,
}

impl ReferenceGrant {
    /// Validates the standalone owner-mediated reference envelope.
    pub(crate) fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.file.validate_with_work(work)?;
        if let Some(manifest) = &self.attachment_manifest {
            manifest.validate_with_work(work)?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
            {
                return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                    "reference grant manifest has an invalid media type"));
            }
        }
        Ok(())
    }

    /// Exact, per-file capability the owner may issue to this reader.
    pub fn capability(&self) -> Result<String> {
        self.validate(&mut None)?;
        self.file.read_capability()
    }
}

impl SharedGrant {
    /// Validates the grant envelope; the seed checks selection and child binding.
    pub(crate) fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate_with_work(work)?;
        if self.volume.class() != VolumeClass::SessionShared || self.operations.is_empty() {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "invalid fork shared-volume grant"));
        }
        Ok(())
    }
}

impl ForkOmission {
    /// Required captures and successful captures cannot become omissions.
    pub(crate) fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.selection.revision.validate_with_work(work)?;
        if self.selection.required || matches!(self.outcome, Capture::Captured(_)) {
            return Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "fork omission must be an uncaptured optional selection"));
        }
        Ok(())
    }
}

impl CapturedResource {
    /// Only project capture may create a new revision and volume identity.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.source.validate_with_work(work)?;
        self.revision.validate_with_work(work)?;
        match (&self.source, &self.revision) {
            (
                ResourceRevision::Project { volume: source, .. },
                ResourceRevision::Project { volume: child, .. },
            ) if source.provider() == child.provider() && source != child => Ok(()),
            (
                ResourceRevision::PrivateVolume {
                    volume: source,
                    paths: selected,
                    ..
                },
                ResourceRevision::PrivateVolume {
                    volume: child,
                    paths: captured,
                    ..
                },
            ) if source.provider() == child.provider()
                && source != child
                && selected == captured =>
            {
                Ok(())
            }
            (ResourceRevision::PrivateVolume { .. }, _)
            | (_, ResourceRevision::PrivateVolume { .. }) => Err(fork_provider_boundary(
                work, crate::wire::ErrorCode::Invalid, "scratch capture changed its selection")),
            (ResourceRevision::Project { .. }, _) | (_, ResourceRevision::Project { .. }) => Err(
                fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                    "project capture has an invalid child volume"),
            ),
            (
                ResourceRevision::Extension {
                    name: source_name,
                    version: source_version,
                    implementation_digest: source_digest,
                    reference: source_reference,
                },
                ResourceRevision::Extension {
                    name: child_name,
                    version: child_version,
                    implementation_digest: child_digest,
                    reference: child_reference,
                },
            ) if source_name == child_name
                && source_version == child_version
                && source_digest == child_digest
                && source_reference.provider() == child_reference.provider() =>
            {
                Ok(())
            }
            (source, revision) if source == revision => Ok(()),
            _ => Err(fork_provider_boundary(work, crate::wire::ErrorCode::Invalid,
                "capture changed an immutable selected revision")),
        }
    }
}

/// Prepared capture report; no child is visible until its seed is published.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkReport {
    /// The original immutable request.
    pub request: ForkRequest,
    /// One result per selection in the same order.
    pub captures: Vec<Capture>,
    /// Newly created empty child-owned private volume.
    pub child_private_volume: VolumeRef,
    /// Exact private generation after inherited files were materialized.
    pub child_private_generation: GenerationRef,
    /// Optional actual result allocation; never selected by private-volume order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_result_volume: Option<VolumeRef>,
    /// Immutable original result allocation generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_result_generation: Option<GenerationRef>,
    /// Bounded parent context materialized as child-owned files.
    pub inherited_context: Vec<FileRef>,
    /// Exact optional Summary capture; its selection and files are seed-bound.
    pub summary: Option<SummaryForkCapture>,
    /// Inclusive conversation message sequence inherited by the child; zero selects none.
    pub inherited_through_sequence: u64,
    /// Explicit child-bound access to selected session-shared volumes.
    pub shared_grants: Vec<SharedGrant>,
    /// Exact parent-owned references retained for child and attached readers.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Original issuer's proof of complete archived reference/grant capture.
    pub history_witness: Option<[u8; 32]>,
    /// Published manifests whose complete private members must remain readable.
    pub attachment_manifests: Vec<FileRef>,
}

impl ForkReport {
    /// Validates a prepared report even when a required capture is still unavailable; only `into_seed` demands all required captures succeed.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    /// Applies the complete report policy using the caller's original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.validate_with_work(&mut Some(&mut io))
    }

    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.request.validate_with_work(work)?;
        let mut work = ForkValidationWork::new(work)?;
        if self.child_result_volume != self.request.preparation.child_result_volume {
            return Err(work.invalid("fork report changed the child result allocation"));
        }
        validate_child_result_generation(
            self.child_result_volume.as_ref(), self.child_result_generation.as_ref(),
            &self.child_private_volume, self.request.child_agent, work.work())?;
        if self.summary.as_ref().map(|capture| &capture.selection)
            != self.request.preparation.summary.as_ref()
        {
            return Err(work.invalid("fork report changed its summary selection"));
        }
        if let Some(summary) = &self.summary {
            summary.validate_with_work(&self.child_private_volume, work.work())?;
        }
        if self.inherited_context.len() as u64 > MAX_FORK_RESOURCES as u64
            || self.shared_grants.len() as u64 > MAX_FORK_REFERENCES as u64
            || self.reference_grants.len() as u64 > MAX_FORK_REFERENCES as u64
            || self.attachment_manifests.len() as u64 > MAX_FORK_RESOURCES as u64
        {
            return Err(work.invalid("fork report exceeds protocol limits"));
        }
        if self.inherited_through_sequence != self.request.preparation.inherited_through_sequence
            || self.child_private_volume != self.request.preparation.child_private_volume
            || self.reference_grants.len()
                > self.request.preparation.maximum_inherited_references as usize
        {
            return Err(work.invalid("fork report changed the requested child allocation or prefix"));
        }
        let inherited_bytes = self
            .inherited_context
            .iter()
            .try_fold(0_u64, |total, file| {
                work.checkpoint()?;
                total
                    .checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| {
                        work.invalid("fork inherited context byte count overflow")
                    })
            })?;
        if inherited_bytes > self.request.preparation.maximum_inherited_bytes {
            return Err(work.invalid("fork inherited context exceeds requested byte limit"));
        }
        if self.captures.len() != self.request.selections.len() {
            return Err(work.invalid("fork capture count does not match selections"));
        }
        for (selection, capture) in self.request.selections.iter().zip(&self.captures) {
            work.checkpoint()?;
            match capture {
                Capture::Captured(resource) if resource.source == selection.revision => {
                    resource.validate_with_work(work.work())?;
                    if let ResourceRevision::Project { volume, .. } = &resource.revision
                        && volume != &self.request.preparation.child_project_volume
                    {
                        return Err(work.invalid("fork report changed the requested project volume"));
                    }
                }
                Capture::Captured(_) => {
                    return Err(work.invalid("fork capture source does not match selection"));
                }
                Capture::Unsupported(reason) if reason.is_empty() => {
                    return Err(work.invalid("fork capture reason is invalid"));
                }
                _ => {}
            }
        }
        self.child_private_volume.validate_with_work(work.work())?;
        self.child_private_generation.as_resource().validate_with_work(work.work())?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.request.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(work.invalid("fork report private volume is not child owned"));
        }
        if self.child_result_volume != self.request.preparation.child_result_volume {
            return Err(Error::Invalid("fork report changed result allocation".into()));
        }
        for file in &self.inherited_context {
            work.checkpoint()?;
            file.validate_with_work(work.work())?;
        }
        for grant in &self.shared_grants {
            work.checkpoint()?;
            grant.validate(work.work())?;
        }
        for grant in &self.reference_grants {
            work.checkpoint()?;
            grant.validate(work.work())?;
        }
        for manifest in &self.attachment_manifests {
            work.checkpoint()?;
            manifest.validate_with_work(work.work())?;
        }
        Ok(())
    }

    /// Produces a publishable seed only when every required capture succeeded.
    pub fn into_seed(self) -> Result<ForkSeed> {
        self.into_seed_with_work(&mut None)
    }

    pub(crate) fn into_seed_with_work(
        self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ForkSeed> {
        let mut owned = ForkValidationWork::new(work)?;
        self.validate_with_work(owned.work())?;
        let (mut resources, mut omissions) = if owned.work().is_some() {
            let resource_count = self.captures.iter()
                .filter(|capture| matches!(capture, Capture::Captured(_))).count();
            let omission_count = self.captures.len() - resource_count;
            let (resources, omissions, retained) = {
                let mut quota = crate::wire_codec::CodecQuota::new(owned.work(), 0)?;
                let resources = crate::core::reserve_copy_vector::<CapturedResource>(
                    resource_count, &mut quota)?;
                let omissions = crate::core::reserve_copy_vector::<ForkOmission>(
                    omission_count, &mut quota)?;
                let retained = quota.retained();
                quota.transfer();
                (resources, omissions, retained)
            };
            // Both real Vec capacities were admitted before their allocator attempts.
            owned.quota.adopt(retained)?;
            (resources, omissions)
        } else {
            (Vec::new(), Vec::new())
        };
        for (selection, capture) in self.request.selections.into_iter().zip(self.captures) {
            owned.checkpoint()?;
            match capture {
                Capture::Captured(resource) if resource.source == selection.revision => {
                    resource.validate_with_work(owned.work())?;
                    resources.push(resource);
                }
                Capture::Captured(_) => {
                    return Err(owned.invalid("fork capture source does not match selection"));
                }
                Capture::Unsupported(_) | Capture::InFlight(_) | Capture::Indeterminate(_)
                    if selection.required =>
                {
                    return Err(fork_provider_boundary(owned.work(), crate::wire::ErrorCode::Conflict,
                        "required fork capture is unavailable"));
                }
                outcome => omissions.push(ForkOmission {
                    selection,
                    outcome,
                }),
            }
        }
        let seed = ForkSeed {
            operation_id: self.request.operation_id,
            parent: self.request.parent,
            parent_revision: self.request.parent_revision,
            authorized_source: self.request.authorized_source,
            child: self.request.child,
            child_agent: self.request.child_agent,
            attached_agents: self.request.attached_agents,
            resources,
            omissions,
            child_private_volume: self.child_private_volume,
            child_private_generation: self.child_private_generation,
            child_result_volume: self.child_result_volume,
            child_result_generation: self.child_result_generation,

            inherited_context: self.inherited_context,
            summary: self.summary,
            inherited_through_sequence: self.inherited_through_sequence,
            shared_grants: self.shared_grants,
            reference_grants: self.reference_grants,
            history_witness: self.history_witness,
            attachment_manifests: self.attachment_manifests,
            boundary: self.request.boundary,
        };
        seed.validate_with_work(owned.work())?;
        // Transfer these actual newly allocated vector slots with the returned seed; failures drop the vectors before the existing custody guard.
        owned.quota.transfer();
        Ok(seed)
    }
}

/// Immutable, validated child environment published by the parent aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSeed {
    /// Stable publication identity.
    pub operation_id: OperationId,
    /// Publishing parent.
    pub parent: Authority,
    /// Exact predecessor revision.
    pub parent_revision: u64,
    /// Retained operation-sealed workspace source evidence, not transcript access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
    /// Fresh child aggregate.
    pub child: Authority,
    /// Fresh child agent.
    pub child_agent: AgentId,
    /// Other agents attached to this fork without rebinding its conversation.
    pub attached_agents: Vec<AgentId>,
    /// Exact captured provider revisions.
    pub resources: Vec<CapturedResource>,
    /// Exact optional captures that did not resolve before publication.
    pub omissions: Vec<ForkOmission>,
    /// Empty child-owned private workspace; never merged.
    pub child_private_volume: VolumeRef,
    /// Exact immutable child-private view admitted at fork publication.
    pub child_private_generation: GenerationRef,
    /// Optional distinct unmounted result allocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_result_volume: Option<VolumeRef>,
    /// Exact original result allocation generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_result_generation: Option<GenerationRef>,
    /// Child-owned inherited conversation files.
    pub inherited_context: Vec<FileRef>,
    /// Exact optional Summary capture; its selection and files are seed-bound.
    pub summary: Option<SummaryForkCapture>,
    /// Inclusive bounded parent conversation prefix; zero selects no messages.
    pub inherited_through_sequence: u64,
    /// Explicit child-bound access to selected session-shared volumes.
    pub shared_grants: Vec<SharedGrant>,
    /// Parent-approved read-only access to pinned references.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Mandatory issuer proof when the selected prefix is no longer resident.
    pub history_witness: Option<[u8; 32]>,
    /// Exact published attachment lists.
    pub attachment_manifests: Vec<FileRef>,
    /// Optional provider-attested common boundary.
    pub boundary: Option<AttestedBoundary>,
}

impl ForkSeed {
    /// Validates pinned scratch lineage, singleton selections, and resource references.
    #[allow(
        clippy::too_many_lines,
        reason = "validates the complete fork seed contract"
    )]
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    #[allow(clippy::too_many_lines, reason = "validates the complete fork seed contract")]
    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut work = ForkValidationWork::new(work)?;
        if self.attached_agents.len() as u64 > MAX_FORK_AGENTS as u64
            || self.resources.len().saturating_add(self.omissions.len()) as u64
                > MAX_FORK_RESOURCES as u64
            || self.reference_grants.len() as u64 > MAX_FORK_REFERENCES as u64
            || self.shared_grants.len() as u64 > MAX_FORK_REFERENCES as u64
            || self.inherited_context.len() as u64 > MAX_FORK_RESOURCES as u64
            || self.attachment_manifests.len() as u64 > MAX_FORK_RESOURCES as u64
        {
            return Err(work.invalid("fork seed exceeds protocol limits"));
        }
        if self.parent == self.child {
            return Err(work.invalid("fork child cannot equal its parent"));
        }
        if self.parent.kind != crate::core::AggregateKind::Conversation
            || self.child.kind != crate::core::AggregateKind::Conversation
        {
            return Err(work.invalid("fork must link parent and child conversations"));
        }
        let parent_path = work.stream_path(&self.parent)?;
        let _child_path = work.stream_path(&self.child)?;
        work.slots::<AgentId>(self.attached_agents.len())?;
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(work.invalid("fork attached agents are not unique"));
        }
        self.child_private_volume.validate_with_work(work.work())?;
        self.child_private_generation.as_resource().validate_with_work(work.work())?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(work.invalid("fork private volume is not child owned"));
        }
        validate_child_result_generation(
            self.child_result_volume.as_ref(), self.child_result_generation.as_ref(),
            &self.child_private_volume, self.child_agent, work.work())?;
        if let Some(source) = &self.authorized_source {
            source.validate_with_work(work.work())?;
            if source.operation_id != self.operation_id
                || !self.resources.iter().any(|capture| matches!(
                    &capture.source,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation))
                || (source.source_task != source.caller_task
                    && (self.inherited_through_sequence != 0 || self.summary.is_some()))
            {
                return Err(fork_provider_boundary(work.work(), crate::wire::ErrorCode::Unauthorized,
                    "authorized source must select its exact project and history-free cross-task fork"));
            }
        }
        let mut history = 0;
        let mut project = 0;
        let mut private = 0;
        let mut context = 0;
        let mut process = 0;
        work.slots::<Vec<u8>>(self.resources.len())?;
        work.slots::<Vec<u8>>(self.resources.len().saturating_add(self.omissions.len()))?;
        work.slots::<Vec<u8>>(self.resources.len())?;
        let mut unique = BTreeSet::new();
        let mut selected = BTreeSet::new();
        let mut selected_shared = BTreeSet::new();
        for resource in &self.resources {
            work.checkpoint()?;
            resource.validate_with_work(work.work())?;
            let source = work.canonical(&resource.source)?;
            if !selected.insert(source) {
                return Err(work.invalid("fork selection appears twice"));
            }
            match &resource.revision {
                ResourceRevision::History(reference) => {
                    history += 1;
                    if reference.as_resource().key() != parent_path.as_bytes()
                        || reference
                            .as_resource()
                            .version()
                            .and_then(|version| version.parse::<u64>().ok())
                            != Some(self.parent_revision)
                    {
                        return Err(work.invalid("fork history is not the exact parent prefix"));
                    }
                }
                ResourceRevision::Project { .. } => project += 1,
                ResourceRevision::PrivateVolume { volume, .. } => {
                    private += 1;
                    if volume != &self.child_private_volume {
                        return Err(work.invalid("scratch capture names another child volume"));
                    }
                }
                ResourceRevision::Context(_) => context += 1,
                ResourceRevision::Process(_) => process += 1,
                ResourceRevision::SharedVolume(volume) => {
                    selected_shared.insert(work.canonical(volume)?);
                }
                _ => {}
            }
            let encoded = work.canonical(&resource.revision)?;
            if !unique.insert(encoded) {
                return Err(work.invalid("fork resource appears twice"));
            }
        }
        for omission in &self.omissions {
            work.checkpoint()?;
            omission.validate(work.work())?;
            let encoded = work.canonical(&omission.selection.revision)?;
            if !selected.insert(encoded) {
                return Err(work.invalid("fork selection appears twice"));
            }
        }
        self.validate_shared_grants(&selected_shared, &attached, &mut work)?;
        self.validate_reference_grants(&attached, &mut work)?;
        work.slots::<[u8; 32]>(self.attachment_manifests.len())?;
        let mut manifest_ids = BTreeSet::new();
        let mut manifest_bytes = 0_u64;
        for manifest in &self.attachment_manifests {
            work.checkpoint()?;
            manifest.validate_with_work(work.work())?;
            manifest_bytes = manifest_bytes
                .checked_add(manifest.descriptor().byte_length())
                .ok_or_else(|| work.invalid("fork attachment manifest bytes overflow"))?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
                || !manifest_ids.insert(work.file_digest(manifest)?)
            {
                return Err(work.invalid("fork attachment manifest is invalid or duplicated"));
            }
        }
        for grant in &self.reference_grants {
            work.checkpoint()?;
            if let Some(manifest) = &grant.attachment_manifest
                && !manifest_ids.contains(&work.file_digest(manifest)?)
            {
                return Err(work.invalid("reference grant names an unselected attachment manifest"));
            }
        }
        let (direct_manifest_reads, member_grants) = collect_manifest_grants(self, &mut work)?;
        let mut volume_readers = ForkVolumeReaders::new(self, &mut work)?;
        for manifest in &self.attachment_manifests {
            work.checkpoint()?;
            let capability = work.file_digest(manifest)?;
            let implicit_readers = volume_readers.implicit_readers(manifest.volume(), &mut work)?;
            if let Some(grants) = member_grants.get(&capability) {
                for reader in grants.values().flat_map(|readers| readers.iter()) {
                    work.checkpoint()?;
                    if !implicit_readers.contains(reader)
                        && !direct_manifest_reads.contains(&(capability, *reader))
                    {
                        return Err(work.invalid("fork omits a manifest file read grant"));
                    }
                }
            }
        }
        if history != 1 || project != 1 || private != 1 || context > 1 || process > 1 {
            return Err(work.invalid("fork needs one history, project, and scratch revision"));
        }
        self.validate_inherited_context(&mut work)?;
        if let Some(boundary) = &self.boundary {
            boundary.validate(work.work())?;
        }
        Ok(())
    }

    fn validate_shared_grants(
        &self,
        selected_shared: &BTreeSet<Vec<u8>>,
        attached: &BTreeSet<AgentId>,
        work: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        work.slots::<&Vec<u8>>(self.shared_grants.len())?;
        work.slots::<(&Vec<u8>, AgentId)>(self.shared_grants.len())?;
        let mut granted_shared = BTreeSet::new();
        let mut granted_pairs = BTreeSet::new();
        for grant in &self.shared_grants {
            work.checkpoint()?;
            grant.validate(work.work())?;
            if grant.child_agent != self.child_agent && !attached.contains(&grant.child_agent) {
                return Err(work.invalid("shared grant belongs to an unattached agent"));
            }
            let volume = work.canonical(&grant.volume)?;
            let selected_volume = selected_shared.get(&volume)
                .ok_or_else(|| work.invalid("shared grant was not selected"))?;
            let capacity = volume.capacity() as u64;
            drop(volume);
            work.release(capacity)?;
            if !granted_pairs.insert((selected_volume, grant.child_agent)) {
                return Err(work.invalid("shared volume is granted twice to one agent"));
            }
            if grant.child_agent == self.child_agent {
                granted_shared.insert(selected_volume);
            }
        }
        if !selected_shared.iter().eq(granted_shared.iter().copied()) {
            return Err(work.invalid("shared-volume selections need exact child grants"));
        }
        Ok(())
    }

    fn validate_reference_grants(
        &self,
        attached: &BTreeSet<AgentId>,
        work: &mut ForkValidationWork<'_, '_>,
    ) -> Result<()> {
        work.slots::<([u8; 32], AgentId, Option<[u8; 32]>)>(self.reference_grants.len())?;
        let mut unique = BTreeSet::new();
        for grant in &self.reference_grants {
            work.checkpoint()?;
            grant.validate(work.work())?;
            if grant.reader != self.child_agent && !attached.contains(&grant.reader) {
                return Err(work.invalid("reference reader is not attached to fork"));
            }
            if grant.file.volume().class() == VolumeClass::AgentPrivate
                && grant.file.volume().owner() == &VolumeOwner::Agent(grant.reader)
            {
                return Err(work.invalid("owner needs no inherited reference grant"));
            }
            let identity = (
                work.file_digest(&grant.file)?,
                grant.reader,
                grant
                    .attachment_manifest
                    .as_ref()
                    .map(|file| work.file_digest(file))
                    .transpose()?,
            );
            if !unique.insert(identity) {
                return Err(work.invalid("reference grant appears twice"));
            }
        }
        Ok(())
    }

    fn validate_inherited_context(&self, work: &mut ForkValidationWork<'_, '_>) -> Result<()> {
        if let Some(summary) = &self.summary {
            summary.validate_with_work(&self.child_private_volume, work.work())?;
            if self.inherited_through_sequence == 0 {
                return Err(work.invalid("summary fork has no inherited logical history"));
            }
        }
        let prefix = if self.inherited_through_sequence > 0 {
            Some(self
                .inherited_context
                .first()
                .filter(|file| {
                    file.path() == ".system/inherited-conversation/prefix.json"
                        && file.descriptor().media_type()
                            == "application/vnd.acyclic.harness.inherited-conversation+json"
                })
                .ok_or_else(|| {
                    work.invalid("inherited context omitted its selected prefix")
                })?)
        } else {
            None
        };
        let expected = prefix.into_iter()
            .chain(self.summary.iter().flat_map(SummaryForkCapture::inherited_files));
        if !expected.eq(self.inherited_context.iter()) {
            return Err(work.invalid("inherited context does not match its selected capture"));
        }
        work.slots::<&str>(self.inherited_context.len())?;
        let mut inherited_paths = BTreeSet::new();
        for file in &self.inherited_context {
            work.checkpoint()?;
            file.validate_with_work(work.work())?;
            if file.volume() != &self.child_private_volume
                || !file.path().starts_with(".system/inherited-conversation/")
            {
                return Err(work.invalid("inherited context is not child owned"));
            }
            if !inherited_paths.insert(file.path()) {
                return Err(work.invalid("inherited context path appears twice"));
            }
        }
        Ok(())
    }

    /// Capabilities the child issuer may sign after admitting this exact seed.
    pub fn shared_capabilities(&self) -> Result<Capabilities> {
        self.shared_capabilities_for(self.child_agent)
    }

    /// Exact shared operations issuable to an attached reader or owning child.
    pub fn shared_capabilities_for(&self, reader: AgentId) -> Result<Capabilities> {
        self.validate()?;
        if reader != self.child_agent && !self.attached_agents.contains(&reader) {
            return Err(Error::Unauthorized("agent is not attached to fork".into()));
        }
        let mut values = Vec::new();
        for grant in &self.shared_grants {
            if grant.child_agent == reader {
                for operation in &grant.operations {
                    values.push(grant.volume.capability(*operation)?);
                }
            }
        }
        Ok(Capabilities::new(values))
    }

    /// Exact read capabilities issuable to one attached agent.
    pub fn reference_capabilities(&self, reader: AgentId) -> Result<Capabilities> {
        self.validate()?;
        if reader != self.child_agent && !self.attached_agents.contains(&reader) {
            return Err(Error::Unauthorized("agent is not attached to fork".into()));
        }
        let mut values = Vec::new();
        for grant in &self.reference_grants {
            if grant.reader == reader {
                values.push(grant.capability()?);
            }
        }
        // Inherited files are pinned child-owned content.
        for file in &self.inherited_context {
            values.push(file.read_capability()?);
        }
        Ok(Capabilities::new(values))
    }

    /// Read-only environment capabilities for one attached agent.
    pub fn attached_read_capabilities(&self, reader: AgentId) -> Result<Capabilities> {
        let references = self.reference_capabilities(reader)?;
        let mut values = references.iter().map(str::to_owned).collect::<Vec<_>>();
        for grant in &self.shared_grants {
            if grant.child_agent == reader && grant.operations.contains(&VolumeOperation::Read) {
                values.push(grant.volume.capability(VolumeOperation::Read)?);
            }
        }
        for resource in &self.resources {
            if let ResourceRevision::Project { volume, .. } = &resource.revision {
                values.push(volume.capability(VolumeOperation::Read)?);
            }
        }
        Ok(Capabilities::new(values))
    }
}

mod physical_clones {
    use super::*;
    use crate::store::history::HistoryIo;
    use crate::wire_codec::CodecQuota;

    fn item(quota: &mut CodecQuota<'_, '_>) -> Result<()> {
        quota.work().as_deref_mut().expect("observed fork clone")
            .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })
    }

    fn text(source: &str, quota: &mut CodecQuota<'_, '_>) -> Result<String> {
        let value = quota.work().as_deref_mut().expect("observed fork clone").clone_text(source)?;
        quota.adopt(value.capacity() as u64)?;
        Ok(value)
    }

    fn slots<T>(length: usize, quota: &mut CodecQuota<'_, '_>) -> Result<Vec<T>> {
        let mut value = Vec::new();
        let bytes = (length as u64).checked_mul(std::mem::size_of::<T>() as u64)
            .ok_or_else(|| Error::Storage("fork clone slot count overflows".into()))?;
        if bytes != 0 {
            let observer = quota.work().as_deref_mut().expect("observed fork clone");
            observer.admit_owned(bytes)?;
            observer.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            quota.work().as_deref_mut().expect("observed fork clone").admit_allocation(bytes)?;
            value.try_reserve_exact(length).map_err(|error|
                quota.work().as_deref_mut().expect("observed fork clone").allocation_failure(error))?;
            let actual = value.capacity() as u64 * std::mem::size_of::<T>() as u64;
            if actual > bytes {
                quota.work().as_deref_mut().expect("observed fork clone").retain_owned(actual - bytes)?;
                quota.adopt(actual - bytes)?;
            }
        }
        Ok(value)
    }

    fn bytes(source: &[u8], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<u8>> {
        let mut value = slots::<u8>(source.len(), quota)?;
        quota.work().as_deref_mut().expect("observed fork clone").copy_bytes(source.len() as u64)?;
        value.extend_from_slice(source);
        Ok(value)
    }

    fn agents(source: &[AgentId], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<AgentId>> {
        let mut value = slots::<AgentId>(source.len(), quota)?;
        quota.work().as_deref_mut().expect("observed fork clone").copy_bytes(
            source.len() as u64 * std::mem::size_of::<AgentId>() as u64)?;
        for agent in source {
            item(quota)?;
            value.push(*agent);
        }
        Ok(value)
    }

    fn authority(source: &Authority, quota: &mut CodecQuota<'_, '_>) -> Result<Authority> {
        let value = quota.work().as_deref_mut().expect("observed fork clone").clone_authority(source)?;
        quota.adopt(value.id.capacity() as u64)?;
        Ok(value)
    }

    fn provider(source: &ProviderRef, quota: &mut CodecQuota<'_, '_>) -> Result<ProviderRef> {
        ProviderRef::new_with_work(
            text(source.namespace(), quota)?,
            text(source.family(), quota)?,
            text(source.version(), quota)?,
            quota.work(),
        )
    }

    fn volume(source: &VolumeRef, quota: &mut CodecQuota<'_, '_>) -> Result<VolumeRef> {
        source.clone_in_quota(quota)
    }

    fn resource(source: &ResourceRef, quota: &mut CodecQuota<'_, '_>) -> Result<ResourceRef> {
        ResourceRef::new_with_work(
            source.kind(),
            provider(source.provider(), quota)?,
            bytes(source.key(), quota)?,
            source.version().map(|value| text(value, quota)).transpose()?,
            quota.work(),
        )
    }

    fn file(source: &FileRef, quota: &mut CodecQuota<'_, '_>) -> Result<FileRef> {
        let value = source.clone_with_work(quota.work())?;
        quota.adopt(value.logical_owned_bytes()?)?;
        Ok(value)
    }

    fn files(source: &[FileRef], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<FileRef>> {
        let mut value = slots::<FileRef>(source.len(), quota)?;
        for entry in source {
            item(quota)?;
            value.push(file(entry, quota)?);
        }
        Ok(value)
    }

    fn revision(source: &ResourceRevision, quota: &mut CodecQuota<'_, '_>) -> Result<ResourceRevision> {
        Ok(match source {
            ResourceRevision::History(value) => ResourceRevision::History(
                StreamRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Project { volume: source_volume, generation } => ResourceRevision::Project {
                volume: volume(source_volume, quota)?,
                generation: GenerationRef::from_resource_with_work(resource(generation.as_resource(), quota)?, quota.work())?,
            },
            ResourceRevision::PrivateVolume { volume: source_volume, generation, paths } => {
                let volume = volume(source_volume, quota)?;
                let generation = GenerationRef::from_resource_with_work(
                    resource(generation.as_resource(), quota)?, quota.work())?;
                let mut copied_paths = slots::<String>(paths.len(), quota)?;
                for path in paths {
                    item(quota)?;
                    copied_paths.push(text(path, quota)?);
                }
                ResourceRevision::PrivateVolume { volume, generation, paths: copied_paths }
            },
            ResourceRevision::Context(value) => ResourceRevision::Context(
                ContextRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Process(value) => ResourceRevision::Process(
                CheckpointRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Artifact(value) => ResourceRevision::Artifact(
                ArtifactRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::SharedVolume(value) => ResourceRevision::SharedVolume(volume(value, quota)?),
            ResourceRevision::Extension { name, version, implementation_digest, reference } =>
                ResourceRevision::Extension {
                    name: text(name, quota)?,
                    version: *version,
                    implementation_digest: *implementation_digest,
                    reference: resource(reference, quota)?,
                },
        })
    }

    fn captured(source: &CapturedResource, quota: &mut CodecQuota<'_, '_>) -> Result<CapturedResource> {
        Ok(CapturedResource {
            source: revision(&source.source, quota)?,
            revision: revision(&source.revision, quota)?,
        })
    }

    fn capture(source: &Capture, quota: &mut CodecQuota<'_, '_>) -> Result<Capture> {
        Ok(match source {
            Capture::Captured(value) => Capture::Captured(captured(value, quota)?),
            Capture::Unsupported(value) => Capture::Unsupported(text(value, quota)?),
            Capture::InFlight(value) => Capture::InFlight(*value),
            Capture::Indeterminate(value) => Capture::Indeterminate(*value),
        })
    }

    fn selection(source: &ForkSelection, quota: &mut CodecQuota<'_, '_>) -> Result<ForkSelection> {
        Ok(ForkSelection { required: source.required, revision: revision(&source.revision, quota)? })
    }

    fn boundary(source: &AttestedBoundary, quota: &mut CodecQuota<'_, '_>) -> Result<AttestedBoundary> {
        Ok(AttestedBoundary {
            provider: provider(&source.provider, quota)?,
            evidence: bytes(&source.evidence, quota)?,
        })
    }

    fn summary_selection(source: &SummaryForkSelection, quota: &mut CodecQuota<'_, '_>) -> Result<SummaryForkSelection> {
        Ok(SummaryForkSelection {
            checkpoint: file(&source.checkpoint, quota)?,
            limits: source.limits,
            history_limits: source.history_limits,
        })
    }

    fn summary_capture(source: &SummaryForkCapture, quota: &mut CodecQuota<'_, '_>) -> Result<SummaryForkCapture> {
        Ok(SummaryForkCapture {
            selection: summary_selection(&source.selection, quota)?,
            context: file(&source.context, quota)?,
            payloads: files(&source.payloads, quota)?,
            references: files(&source.references, quota)?,
        })
    }

    fn preparation(source: &ForkPreparation, quota: &mut CodecQuota<'_, '_>) -> Result<ForkPreparation> {
        Ok(ForkPreparation {
            child_project_volume: volume(&source.child_project_volume, quota)?,
            child_private_volume: volume(&source.child_private_volume, quota)?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| volume(value, quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            maximum_inherited_messages: source.maximum_inherited_messages,
            maximum_inherited_bytes: source.maximum_inherited_bytes,
            maximum_inherited_references: source.maximum_inherited_references,
            summary: source.summary.as_ref().map(|value| summary_selection(value, quota)).transpose()?,
        })
    }

    fn shared(source: &[SharedGrant], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<SharedGrant>> {
        let mut value = slots::<SharedGrant>(source.len(), quota)?;
        for grant in source {
            item(quota)?;
            let volume = volume(&grant.volume, quota)?;
            let mut operations = BTreeSet::new();
            // Entry payload is explicit; opaque std node/control allocation is not observed.
            let payload = grant.operations.len() as u64 * std::mem::size_of::<VolumeOperation>() as u64;
            quota.work().as_deref_mut().expect("observed fork clone").admit_owned(payload)?;
            quota.work().as_deref_mut().expect("observed fork clone").retain_owned(payload)?;
            quota.adopt(payload)?;
            for operation in &grant.operations {
                item(quota)?;
                operations.insert(*operation);
            }
            value.push(SharedGrant { volume, child_agent: grant.child_agent, operations });
        }
        Ok(value)
    }

    fn references(source: &[ReferenceGrant], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<ReferenceGrant>> {
        let mut value = slots::<ReferenceGrant>(source.len(), quota)?;
        for grant in source {
            item(quota)?;
            value.push(ReferenceGrant {
                file: file(&grant.file, quota)?,
                reader: grant.reader,
                attachment_manifest: grant.attachment_manifest.as_ref().map(|entry| file(entry, quota)).transpose()?,
            });
        }
        Ok(value)
    }

    fn source_authorization(
        source: Option<&crate::source_authorization::ProjectSourceAuthorization>,
        quota: &mut CodecQuota<'_, '_>,
    ) -> Result<Option<crate::source_authorization::ProjectSourceAuthorization>> {
        source.map(|source| {
            let (value, retained) = source.clone_with_work_and_custody(quota.work())?;
            quota.adopt(retained)?;
            Ok(value)
        }).transpose()
    }

    fn request(source: &ForkRequest, quota: &mut CodecQuota<'_, '_>) -> Result<ForkRequest> {
        let parent = authority(&source.parent, quota)?;
        let child = authority(&source.child, quota)?;
        let attached_agents = agents(&source.attached_agents, quota)?;
        let preparation = preparation(&source.preparation, quota)?;
        let mut selections = slots::<ForkSelection>(source.selections.len(), quota)?;
        for entry in &source.selections {
            item(quota)?;
            selections.push(selection(entry, quota)?);
        }
        Ok(ForkRequest {
            operation_id: source.operation_id,
            parent,
            parent_revision: source.parent_revision,
            child,
            child_agent: source.child_agent,
            attached_agents,
            preparation,
            selections,
            boundary: source.boundary.as_ref().map(|value| boundary(value, quota)).transpose()?,
            authorized_source: source_authorization(source.authorized_source.as_ref(), quota)?,
        })
    }

    fn report(source: &ForkReport, quota: &mut CodecQuota<'_, '_>) -> Result<ForkReport> {
        let request = request(&source.request, quota)?;
        let mut captures = slots::<Capture>(source.captures.len(), quota)?;
        for entry in &source.captures {
            item(quota)?;
            captures.push(capture(entry, quota)?);
        }
        Ok(ForkReport {
            request,
            captures,
            child_private_volume: volume(&source.child_private_volume, quota)?,
            child_private_generation: GenerationRef::from_resource_with_work(
                resource(source.child_private_generation.as_resource(), quota)?, quota.work())?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| volume(value, quota)).transpose()?,
            child_result_generation: source.child_result_generation.as_ref()
                .map(|value| GenerationRef::from_resource_with_work(
                    resource(value.as_resource(), quota)?, quota.work())).transpose()?,
            inherited_context: files(&source.inherited_context, quota)?,
            summary: source.summary.as_ref().map(|value| summary_capture(value, quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            shared_grants: shared(&source.shared_grants, quota)?,
            reference_grants: references(&source.reference_grants, quota)?,
            history_witness: source.history_witness,
            attachment_manifests: files(&source.attachment_manifests, quota)?,
        })
    }

    fn seed(source: &ForkSeed, quota: &mut CodecQuota<'_, '_>) -> Result<ForkSeed> {
        let parent = authority(&source.parent, quota)?;
        let child = authority(&source.child, quota)?;
        let attached_agents = agents(&source.attached_agents, quota)?;
        let mut resources = slots::<CapturedResource>(source.resources.len(), quota)?;
        for entry in &source.resources {
            item(quota)?;
            resources.push(captured(entry, quota)?);
        }
        let mut omissions = slots::<ForkOmission>(source.omissions.len(), quota)?;
        for entry in &source.omissions {
            item(quota)?;
            omissions.push(ForkOmission {
                selection: selection(&entry.selection, quota)?,
                outcome: capture(&entry.outcome, quota)?,
            });
        }
        Ok(ForkSeed {
            operation_id: source.operation_id,
            parent,
            parent_revision: source.parent_revision,
            authorized_source: source_authorization(source.authorized_source.as_ref(), quota)?,
            child,
            child_agent: source.child_agent,
            attached_agents,
            resources,
            omissions,
            child_private_volume: volume(&source.child_private_volume, quota)?,
            child_private_generation: GenerationRef::from_resource_with_work(
                resource(source.child_private_generation.as_resource(), quota)?, quota.work())?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| volume(value, quota)).transpose()?,
            child_result_generation: source.child_result_generation.as_ref()
                .map(|value| GenerationRef::from_resource_with_work(
                    resource(value.as_resource(), quota)?, quota.work())).transpose()?,
            inherited_context: files(&source.inherited_context, quota)?,
            summary: source.summary.as_ref().map(|value| summary_capture(value, quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            shared_grants: shared(&source.shared_grants, quota)?,
            reference_grants: references(&source.reference_grants, quota)?,
            history_witness: source.history_witness,
            attachment_manifests: files(&source.attachment_manifests, quota)?,
            boundary: source.boundary.as_ref().map(|value| boundary(value, quota)).transpose()?,
        })
    }

    impl ForkReport {
        pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
            self.clone_with_work_and_custody(work).map(|(value, _)| value)
        }
        pub(crate) fn clone_with_work_and_custody(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
            if work.is_none() { return Ok((self.clone(), 0)); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = report(self, &mut quota)?;
            let held = quota.retained();
            quota.transfer();
            Ok((value, held))
        }
    }

    impl ForkRequest {
        pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
            self.clone_with_work_and_custody(work).map(|(value, _)| value)
        }
        pub(crate) fn clone_with_work_and_custody(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
            if work.is_none() { return Ok((self.clone(), 0)); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = request(self, &mut quota)?;
            let held = quota.retained();
            quota.transfer();
            Ok((value, held))
        }
    }

    impl ForkSeed {
        pub(crate) fn clone_with_work_and_custody(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
            if work.is_none() { return Ok((self.clone(), 0)); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = seed(self, &mut quota)?;
            let held = quota.retained();
            quota.transfer();
            Ok((value, held))
        }
    }

    impl ResourceRevision {
        pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
            if work.is_none() { return Ok(self.clone()); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = revision(self, &mut quota)?;
            quota.transfer();
            Ok(value)
        }
    }

    impl Capture {
        /// Copies this closed capture on the original native ledger.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        pub fn clone_with_native_work(
            &self, original: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
            let mut io = crate::store::history::native_work_io(original)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            let value = self.clone_with_work(&mut Some(&mut io))?;
            Ok((value, io.into_custody()))
        }
        pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
            if work.is_none() { return Ok(self.clone()); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = capture(self, &mut quota)?;
            quota.transfer();
            Ok(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{conversation::FileDescriptor, core::AggregateKind};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn inherited_prefix_accepts_more_than_a_thousand_readers() -> Result<()> {
        let readers = (0_u64..=1_024)
            .map(|index| {
                let mut bytes = [0_u8; 16];
                bytes[..8].copy_from_slice(&index.to_le_bytes());
                AgentId::from_bytes(bytes)
            })
            .collect::<Vec<_>>();
        let mut conversation = crate::conversation::ConversationState::default();
        conversation.bind(AgentId::from_bytes([255; 16]))?;
        assert!(
            InheritedConversationPrefix::select(
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "parent".into()
                },
                0,
                0,
                &readers,
                &conversation,
            )
            .is_ok()
        );
        Ok(())
    }

    struct AcceptingVerifier(ProviderRef);

    struct MissingFileVerifier(ProviderRef);

    struct CountingFileVerifier {
        provider: ProviderRef,
        count: std::sync::Arc<AtomicUsize>,
    }

    struct ManifestVerifier {
        provider: ProviderRef,
        bytes: acyclic_stream::OwnedBytes,
        reads: std::sync::Arc<AtomicUsize>,
    }

    impl ForkSeedVerifier for ManifestVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.provider
        }

        fn verify<'a>(&'a self, _seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if file.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                file.descriptor().verify(&self.bytes)
            })
        }

        fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
            Box::pin(async move {
                if manifest.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                self.reads.fetch_add(1, Ordering::Relaxed);
                Ok(self.bytes.clone())
            })
        }
    }

    #[test]
    fn v2_fork_seed_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/fork-seed.json").trim();
        let seed: ForkSeed =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        seed.validate()?;
        let encoded =
            serde_json::to_string(&seed).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn child_result_capture_rejects_alias_partial_and_foreign_generation() -> Result<()> {
        let mut seed: ForkSeed = serde_json::from_str(
            include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let provider = seed.child_private_volume.provider().clone();
        seed.child_result_volume = Some(VolumeRef::new(
            provider.clone(), "independent-child-result", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(seed.child_agent))?);
        seed.child_result_generation = Some(GenerationRef::new(provider.clone(), [73; 32], None)?);
        seed.validate()?;
        let mut mutation = seed.clone();
        mutation.child_result_generation = None;
        assert_eq!(mutation.validate().unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        mutation = seed.clone();
        mutation.child_result_volume = Some(seed.child_private_volume.clone());
        assert_eq!(mutation.validate().unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        mutation = seed;
        mutation.child_result_generation = Some(GenerationRef::new(
            ProviderRef::new(provider.namespace(), provider.family(), "foreign-result-provider")?, [73; 32], None)?);
        assert_eq!(mutation.validate().unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn physical_fork_clone_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 100_000,
            peak_allocation_bytes: 16_777_216,
            bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216,
            items_examined: 100_000,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn funded_selection_request(
        source: &ForkRequest, native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ForkRequest, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.retain_owned(std::mem::size_of::<ForkRequest>() as u64)?;
        let (request, _) = source.clone_with_work_and_custody(&mut Some(&mut io))?;
        Ok((request, io.into_custody()))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn funded_selection(
        source: &ForkSelection, native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ForkSelection, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.retain_owned(std::mem::size_of::<ForkSelection>() as u64)?;
        let revision = source.revision.clone_with_work(&mut Some(&mut io))?;
        Ok((ForkSelection { required: source.required, revision }, io.into_custody()))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn additional_selection(index: u64) -> Result<ForkSelection> {
        Ok(ForkSelection {
            required: false,
            revision: ResourceRevision::Artifact(crate::resources::ArtifactRef::new(
                ProviderRef::new("test", "objects", "2")?,
                index.to_le_bytes().to_vec(), None)?),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_selection_builder_rejects_foreign_missing_and_cancelled_custody_before_mutation() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let fixture: ForkRequest = serde_json::from_str(include_str!("../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        for mode in 0..5 {
            let cancellation = CancellationToken::new();
            let foreign_cancellation = CancellationToken::new();
            let native = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancellation)
                .expect("original request ledger");
            let foreign = NativeMaterializationWork::new(physical_fork_clone_budget(), &foreign_cancellation)
                .expect("distinct foreign ledger");
            let request_work = if mode == 1 { &foreign } else { &native };
            let selection_work = if mode == 0 { &foreign } else { &native };
            let (mut request, mut custody) = funded_selection_request(&fixture, request_work)?;
            let (selection, mut incoming) = funded_selection(&additional_selection(77)?, selection_work)?;
            // Empty real guards represent missing custody; keep the funded payload guards live separately until the rejected fields die.
            let mut held = None;
            if mode == 2 {
                held = Some(incoming);
                incoming = native.retain_owned(0)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            }
            if mode == 3 {
                held = Some(custody);
                custody = native.retain_owned(0)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            }
            let before = native.receipt();
            let old_bytes = custody.bytes();
            let pointer = request.selections.as_ptr();
            let capacity = request.selections.capacity();
            if mode == 4 { cancellation.cancel(); }
            assert!(matches!(request.add_selection_with_native_work(
                selection, &mut custody, incoming, &native), Err(Error::NativeWork { custody: None, .. })));
            assert_eq!(request, fixture);
            assert_eq!((request.selections.as_ptr(), request.selections.capacity(), custody.bytes()),
                (pointer, capacity, old_bytes));
            assert_eq!(native.receipt().allocation_operations, before.allocation_operations);
            assert_eq!(native.receipt().bytes_copied, before.bytes_copied);
            drop(request);
            drop(custody);
            drop(held);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_selection_builder_preserves_canonical_request_policy_and_denied_copy_state() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let fixture: ForkRequest = serde_json::from_str(include_str!("../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        for mode in 0..4 {
            let cancellation = CancellationToken::new();
            let native = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancellation)
                .expect("canonical mutation ledger");
            let mut changed = fixture.clone();
            match mode {
                0 => changed.selections[0].required = false,
                1 => changed.parent_revision += 1,
                2 => { changed.selections.remove(0); },
                _ => changed.boundary = Some(AttestedBoundary {
                    provider: ProviderRef::new("test", "filesystem", "2")?, evidence: Vec::new(),
                }),
            }
            let (mut request, mut custody) = funded_selection_request(&changed, &native)?;
            let (selection, incoming) = funded_selection(&additional_selection(1)?, &native)?;
            let bytes = custody.bytes();
            let capacity = request.selections.capacity();
            assert_eq!(request.add_selection_with_native_work(selection, &mut custody, incoming, &native)
                .expect_err("unchanged canonical policy").code(), changed.validate().expect_err("invalid control").code());
            assert_eq!(request, changed);
            assert_eq!((custody.bytes(), request.selections.capacity()), (bytes, capacity));
            drop(request);
            drop(custody);
        }
        let cancellation = CancellationToken::new();
        let sizing = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancellation)
            .expect("actual setup scalar ledger");
        let (request, guard) = funded_selection_request(&fixture, &sizing)?;
        let (selection, input) = funded_selection(&additional_selection(1)?, &sizing)?;
        let setup_copies = sizing.receipt().bytes_copied;
        drop((request, selection));
        drop((guard, input));
        let denied = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            bytes_copied: setup_copies, ..physical_fork_clone_budget()
        }, &cancellation).expect("original copy-denied ledger");
        let (mut request, mut guard) = funded_selection_request(&fixture, &denied)?;
        let (selection, input) = funded_selection(&additional_selection(1)?, &denied)?;
        let bytes = guard.bytes();
        let capacity = request.selections.capacity();
        assert!(matches!(request.add_selection_with_native_work(selection, &mut guard, input, &denied),
            Err(Error::NativeWork { custody: None, .. })));
        assert_eq!(request, fixture);
        assert_eq!((guard.bytes(), request.selections.capacity()), (bytes, capacity));
        drop(request);
        drop(guard);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_capture_validation_keeps_original_policy_without_allocating_a_diagnostic() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let seed: ForkSeed = serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancellation)
            .expect("original validation ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let original = &seed.resources[0];
        original.validate()?;
        original.validate_with_work(&mut Some(&mut io))?;
        let mut changed = original.clone();
        changed.revision = seed.resources[1].revision.clone();
        assert_eq!(changed.validate().expect_err("changed capture").code(), crate::wire::ErrorCode::Invalid);
        let before = native.receipt();
        let error = changed.validate_with_work(&mut Some(&mut io))
            .expect_err("native changed capture");
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(matches!(error, Error::NativeBoundary { .. }));
        assert_eq!(native.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(native.receipt().bytes_copied, before.bytes_copied);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_capture_public_copy_keeps_funded_fields_and_rejects_copy_denial() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let seed: ForkSeed = serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let original = Capture::Captured(seed.resources[0].clone());
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancellation)
            .expect("original capture copy ledger");
        let (copied, custody) = original.clone_with_native_work(&native)?;
        assert_eq!(copied, original);
        assert!(custody.bytes() >= std::mem::size_of::<Capture>() as u64);
        assert!(native.receipt().bytes_copied > 0);
        assert_eq!(native.receipt().bytes_encoded, 0);
        drop(copied);
        drop(custody);

        let mut denied_budget = physical_fork_clone_budget();
        denied_budget.bytes_copied = 0;
        let denied = NativeMaterializationWork::new(denied_budget, &cancellation)
            .expect("original denied capture ledger");
        assert!(matches!(original.clone_with_native_work(&denied), Err(Error::NativeWork { .. })));
        assert_eq!(denied.receipt().bytes_copied, 0);
        assert_eq!(original, Capture::Captured(seed.resources[0].clone()));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn physical_fork_clone_partial_denial_releases_only_new_fields_and_cancels_before_copy() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        use crate::store::history::HistoryIo as _;
        let request: ForkRequest = serde_json::from_str(include_str!("../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let cancellation = CancellationToken::new();
        let mut budget = physical_fork_clone_budget();
        budget.bytes_copied = 4 + request.parent.id.len() as u64;
        let native = NativeMaterializationWork::new(budget, &cancellation).expect("partial fork-copy ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("partial fork-copy observer");
        let held = io.clone_text("held")?;
        let before = native.receipt();
        assert!(matches!(request.clone_with_work(&mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_copied", .. }), .. })));
        assert_eq!(native.receipt().bytes_copied - before.bytes_copied, request.parent.id.len() as u64);
        let guard = io.into_custody();
        assert_eq!(guard.bytes(), held.capacity() as u64);
        drop(held);
        drop(guard);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        native.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let cancelled_token = CancellationToken::new();
        let cancelled = NativeMaterializationWork::new(physical_fork_clone_budget(), &cancelled_token)
            .expect("original fork ledger before cancellation");
        let mut cancelled_io = crate::store::history::native_work_io(&cancelled).expect("cancelled fork observer");
        let before = cancelled.receipt();
        cancelled_token.cancel();
        assert!(request.clone_with_work(&mut Some(&mut cancelled_io)).is_err());
        assert_eq!(cancelled.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(cancelled.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(cancelled_io.into_custody().bytes(), 0);
        Ok(())
    }

    #[test]
    fn fork_volume_read_index_tracks_implicit_volume_readers() -> Result<()> {
        let mut seed: ForkSeed =
            serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let attached = AgentId::from_bytes([7; 16]);
        seed.attached_agents.push(attached);
        let project_resource = seed
            .resources
            .get(1)
            .ok_or_else(|| Error::Invalid("fixture has no project resource".into()))?;
        let (parent_project, child_project) = match project_resource {
            CapturedResource {
                source: ResourceRevision::Project { volume: parent, .. },
                revision: ResourceRevision::Project { volume: child, .. },
                ..
            } => (parent.clone(), child.clone()),
            _ => unreachable!("fixture selects a project"),
        };
        let mut observer = None;
        let mut owned = ForkValidationWork::new(&mut observer)?;
        let mut readers = ForkVolumeReaders::new(&seed, &mut owned)?;
        assert_eq!(
            readers.implicit_readers(&child_project, &mut owned)?,
            &BTreeSet::from([seed.child_agent, attached])
        );
        assert_eq!(
            readers.implicit_readers(&seed.child_private_volume, &mut owned)?,
            &BTreeSet::from([seed.child_agent])
        );
        assert_eq!(readers.implicit_readers(&parent_project, &mut owned)?, &BTreeSet::new());
        Ok(())
    }

    impl ForkSeedVerifier for AcceptingVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.0
        }

        fn verify<'a>(&'a self, _seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if file.volume().provider() != &self.0 {
                    return Err(Error::Unauthorized("wrong file provider".into()));
                }
                Ok(())
            })
        }
    }

    impl ForkSeedVerifier for MissingFileVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.0
        }

        fn verify<'a>(&'a self, _seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(&'a self, _file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::NotFound("pinned file".into())) })
        }
    }

    impl ForkSeedVerifier for CountingFileVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.provider
        }

        fn verify<'a>(&'a self, _seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(&'a self, _file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    #[test]
    fn project_capture_requires_new_volume_with_independent_owner() -> Result<()> {
        let provider = ProviderRef::new("test", "filesystem", "2")?;
        let parent = VolumeRef::new(
            provider.clone(),
            "parent",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        let child = VolumeRef::new(
            provider.clone(),
            "child",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        let source = ResourceRevision::Project {
            volume: parent,
            generation: GenerationRef::new(provider.clone(), [1; 32], None)?,
        };
        let revision = ResourceRevision::Project {
            volume: child,
            generation: GenerationRef::new(provider.clone(), [2; 32], None)?,
        };
        CapturedResource {
            source: source.clone(),
            revision,
        }
        .validate()?;
        assert!(
            CapturedResource {
                source: source.clone(),
                revision: source.clone()
            }
            .validate()
            .is_err()
        );
        let foreign = ResourceRevision::Project {
            volume: VolumeRef::new(
                provider.clone(),
                "foreign",
                VolumeClass::Project,
                VolumeOwner::Project("other".into()),
            )?,
            generation: GenerationRef::new(provider, [3; 32], None)?,
        };
        assert!(
            CapturedResource {
                source,
                revision: foreign
            }
            .validate()
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn fork_history_must_be_exact_parent_prefix() -> Result<()> {
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "parent".into(),
        };
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: "child".into(),
        };
        let filesystem = ProviderRef::new("test", "filesystem", "2")?;
        let stream = ProviderRef::new("test", "stream", "2")?;
        let source_project = VolumeRef::new(
            filesystem.clone(),
            "source-project",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        let child_project = VolumeRef::new(
            filesystem.clone(),
            "child-project",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        let history = ResourceRevision::History(StreamRef::new(
            stream,
            parent.stream_path()?.into_bytes(),
            Some("3".into()),
        )?);
        let mut seed = ForkSeed {
            authorized_source: None,
            child_result_volume: None,
            child_result_generation: None,
            summary: None,
            history_witness: None,
            operation_id: OperationId::from_bytes([1; 16]),
            parent,
            parent_revision: 3,
            child,
            child_agent: AgentId::from_bytes([2; 16]),
            attached_agents: Vec::new(),
            resources: vec![
                CapturedResource {
                    source: history.clone(),
                    revision: history,
                },
                CapturedResource {
                    source: ResourceRevision::Project {
                        volume: source_project,
                        generation: GenerationRef::new(filesystem.clone(), [3; 32], None)?,
                    },
                    revision: ResourceRevision::Project {
                        volume: child_project,
                        generation: GenerationRef::new(filesystem.clone(), [4; 32], None)?,
                    },
                },
                CapturedResource {
                    source: ResourceRevision::PrivateVolume {
                        volume: VolumeRef::new(
                            filesystem.clone(),
                            "parent-scratch",
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                        )?,
                        generation: GenerationRef::new(filesystem.clone(), [98; 32], None)?,
                        paths: Vec::new(),
                    },
                    revision: ResourceRevision::PrivateVolume {
                        volume: VolumeRef::new(
                            filesystem.clone(),
                            "scratch",
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
                        )?,
                        generation: GenerationRef::new(filesystem.clone(), [99; 32], None)?,
                        paths: Vec::new(),
                    },
                },
            ],
            omissions: Vec::new(),
            child_private_volume: VolumeRef::new(
                filesystem.clone(),
                "scratch",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
            )?,
            child_private_generation: GenerationRef::new(filesystem.clone(), [5; 32], None)?,
            inherited_context: Vec::new(),
            shared_grants: Vec::new(),
            reference_grants: Vec::new(),
            attachment_manifests: Vec::new(),
            inherited_through_sequence: 0,
            boundary: None,
        };
        seed.validate()?;
        let filesystem_only = CompositeForkVerifier::new(vec![std::sync::Arc::new(
            AcceptingVerifier(filesystem.clone()),
        )])?;
        assert!(matches!(
            futures::executor::block_on(filesystem_only.verify(&seed)),
            Err(Error::Unsupported(_))
        ));
        let complete = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
        ])?;
        futures::executor::block_on(complete.verify(&seed))?;
        let project_resource = seed
            .resources
            .get(1)
            .ok_or_else(|| Error::Invalid("fixture has no project resource".into()))?;
        let source_volume = match &project_resource.source {
            ResourceRevision::Project { volume, .. } => volume.clone(),
            _ => unreachable!("the test selects one project"),
        };
        let mut missing_seed = seed.clone();
        missing_seed.reference_grants.push(ReferenceGrant {
            file: FileRef::new(
                source_volume,
                "files/missing.txt",
                "one",
                FileDescriptor::from_bytes(b"missing", "text/plain")?,
                "missing.txt",
            )?,
            reader: seed.child_agent,
            attachment_manifest: None,
        });
        missing_seed.validate()?;
        let missing = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(MissingFileVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
        ])?;
        assert!(matches!(
            futures::executor::block_on(missing.verify(&missing_seed)),
            Err(Error::NotFound(_))
        ));
        let objects = ProviderRef::new("test", "objects", "2")?;
        let object_volume = VolumeRef::new(
            objects.clone(),
            "attachments",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
        )?;
        let member_volume = match &project_resource.source {
            ResourceRevision::Project { volume, .. } => volume.clone(),
            _ => unreachable!("the test selects one project"),
        };
        let member = FileRef::new(
            member_volume.clone(),
            "files/actual.txt",
            "one",
            FileDescriptor::from_bytes(b"actual", "text/plain")?,
            "actual.txt",
        )?;
        let forged = FileRef::new(
            member_volume,
            "files/forged.txt",
            "two",
            FileDescriptor::from_bytes(b"forged", "text/plain")?,
            "forged.txt",
        )?;
        let bytes = serde_json::to_vec(&vec![crate::conversation::Attachment {
            file: member.clone(),
            label: None,
        }])
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let manifest = FileRef::new(
            object_volume,
            "lists/one.json",
            "one",
            FileDescriptor::from_bytes(&bytes, "application/vnd.acyclic.harness.attachments+json")?,
            "one.json",
        )?;
        let mut manifest_seed = seed.clone();
        manifest_seed.attachment_manifests.push(manifest.clone());
        manifest_seed.reference_grants.push(ReferenceGrant {
            file: forged,
            reader: seed.child_agent,
            attachment_manifest: Some(manifest.clone()),
        });
        assert!(matches!(manifest_seed.validate(), Err(Error::Invalid(_))));
        manifest_seed.reference_grants.push(ReferenceGrant {
            file: manifest.clone(),
            reader: seed.child_agent,
            attachment_manifest: None,
        });
        manifest_seed.validate()?;
        let attached = AgentId::from_bytes([9; 16]);
        let mut attached_manifest_only = manifest_seed.clone();
        attached_manifest_only.attached_agents.push(attached);
        attached_manifest_only
            .reference_grants
            .push(ReferenceGrant {
                file: manifest.clone(),
                reader: attached,
                attachment_manifest: Some(manifest.clone()),
            });
        let first_grant = attached_manifest_only
            .reference_grants
            .first_mut()
            .ok_or_else(|| Error::Invalid("fixture has no reference grant".into()))?;
        first_grant.file = member.clone();
        attached_manifest_only.validate()?;
        let verified_members = std::sync::Arc::new(AtomicUsize::new(0));
        // Use a cached canonical carrier rather than allocating a new vector for every provider read; admission must decode this original backing.
        let cached_manifest: acyclic_stream::OwnedBytes = bytes.into();
        let cached_pointer = cached_manifest.as_ptr();
        let manifest_reads = std::sync::Arc::new(AtomicUsize::new(0));
        let verifier = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(CountingFileVerifier {
                provider: filesystem.clone(),
                count: verified_members.clone(),
            }),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(ManifestVerifier {
                provider: objects,
                bytes: cached_manifest.clone(),
                reads: manifest_reads.clone(),
            }),
        ])?;
        futures::executor::block_on(verifier.verify(&attached_manifest_only))?;
        assert_eq!(manifest_reads.load(Ordering::Relaxed), 1);
        assert_eq!(cached_manifest.as_ptr(), cached_pointer);
        let missing_manifest = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(MissingFileVerifier(manifest.volume().provider().clone())),
        ])?;
        assert!(matches!(
            futures::executor::block_on(missing_manifest.verify(&manifest_seed)),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            futures::executor::block_on(verifier.verify(&manifest_seed)),
            Err(Error::Invalid(_))
        ));
        let first_grant = manifest_seed
            .reference_grants
            .first_mut()
            .ok_or_else(|| Error::Invalid("fixture has no reference grant".into()))?;
        first_grant.file = member;
        futures::executor::block_on(verifier.verify(&manifest_seed))?;
        assert_eq!(verified_members.load(Ordering::Relaxed), 3);
        assert_eq!(manifest_reads.load(Ordering::Relaxed), 3);
        assert_eq!(cached_manifest.as_ptr(), cached_pointer);
        let optional = ResourceRevision::SharedVolume(VolumeRef::new(
            filesystem,
            "shared",
            VolumeClass::SessionShared,
            VolumeOwner::Session("session".into()),
        )?);
        let report = ForkReport {
            child_result_volume: seed.child_result_volume.clone(),
            child_result_generation: seed.child_result_generation.clone(),
            summary: None,
            history_witness: seed.history_witness,
            request: ForkRequest {
                authorized_source: seed.authorized_source.clone(),
                operation_id: seed.operation_id,
                parent: seed.parent.clone(),
                parent_revision: seed.parent_revision,
                child: seed.child.clone(),
                child_agent: seed.child_agent,
                attached_agents: Vec::new(),
                preparation: ForkPreparation {
                    child_result_volume: seed.child_result_volume.clone(),
                    summary: None,
                    child_project_volume: match &project_resource.revision {
                        ResourceRevision::Project { volume, .. } => volume.clone(),
                        _ => unreachable!("fixture includes the child project"),
                    },
                    child_private_volume: seed.child_private_volume.clone(),
                    inherited_through_sequence: 0,
                    maximum_inherited_messages: MAX_FORK_INHERITED_MESSAGES,
                    maximum_inherited_bytes: MAX_FORK_INHERITED_BYTES,
                    maximum_inherited_references: u32::try_from(MAX_FORK_REFERENCES).map_err(
                        |_| Error::Invalid("fixture reference limit overflows u32".into()),
                    )?,
                },
                selections: seed
                    .resources
                    .iter()
                    .map(|resource| ForkSelection {
                        required: true,
                        revision: resource.source.clone(),
                    })
                    .chain([ForkSelection {
                        required: false,
                        revision: optional.clone(),
                    }])
                    .collect(),
                boundary: None,
            },
            captures: seed
                .resources
                .iter()
                .cloned()
                .map(Capture::Captured)
                .chain([Capture::Indeterminate(OperationId::from_bytes([9; 16]))])
                .collect(),
            child_private_volume: seed.child_private_volume.clone(),
            child_private_generation: seed.child_private_generation.clone(),
            inherited_context: Vec::new(),
            shared_grants: Vec::new(),
            reference_grants: Vec::new(),
            attachment_manifests: Vec::new(),
            inherited_through_sequence: 0,
        };
        report.validate()?;
        let mut duplicate_request = report.request.clone();
        let first_selection = duplicate_request
            .selections
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture has no fork selection".into()))?;
        duplicate_request.selections.push(first_selection);
        assert!(duplicate_request.validate().is_err());
        let mut optional_project = report.request.clone();
        optional_project
            .selections
            .get_mut(1)
            .ok_or_else(|| Error::Invalid("fixture has no project selection".into()))?
            .required = false;
        assert!(optional_project.validate().is_err());
        let mut missing_history = report.request.clone();
        missing_history.selections.remove(0);
        assert!(missing_history.validate().is_err());
        let mut stale_history = report.request.clone();
        stale_history.parent_revision = stale_history.parent_revision.saturating_add(1);
        assert!(stale_history.validate().is_err());
        let mut changed_private = report.clone();
        changed_private.child_private_volume = VolumeRef::new(
            changed_private.child_private_volume.provider().clone(),
            "different-child-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(changed_private.request.child_agent),
        )?;
        assert!(changed_private.validate().is_err());
        let mut changed_project = report.clone();
        if let Some(Capture::Captured(project)) = changed_project.captures.get_mut(1)
            && let ResourceRevision::Project { volume, .. } = &mut project.revision
        {
            *volume = VolumeRef::new(
                volume.provider().clone(),
                "different-child-project",
                VolumeClass::Project,
                volume.owner().clone(),
            )?;
        }
        assert!(changed_project.validate().is_err());
        let mut changed_prefix = report.clone();
        changed_prefix.inherited_through_sequence = 1;
        assert!(changed_prefix.validate().is_err());
        let mut widened_request = report.request.clone();
        widened_request.preparation.maximum_inherited_messages = 16_385;
        assert!(widened_request.validate().is_ok());
        let published = report.into_seed()?;
        assert_eq!(published.omissions.len(), 1);
        let omission = published
            .omissions
            .first()
            .ok_or_else(|| Error::Invalid("fixture has no omission".into()))?;
        assert_eq!(omission.selection.revision, optional);
        assert!(matches!(omission.outcome, Capture::Indeterminate(_)));
        let mut shared_seed = seed.clone();
        shared_seed.resources.push(CapturedResource {
            source: optional.clone(),
            revision: optional.clone(),
        });
        assert!(shared_seed.validate().is_err());
        let ResourceRevision::SharedVolume(shared_volume) = optional else {
            return Err(Error::Invalid("fixture is not a shared volume".into()));
        };
        shared_seed.shared_grants.push(SharedGrant {
            volume: shared_volume.clone(),
            child_agent: seed.child_agent,
            operations: BTreeSet::from([VolumeOperation::Read]),
        });
        shared_seed.validate()?;
        assert!(
            shared_seed
                .shared_capabilities()?
                .contains(&shared_volume.capability(VolumeOperation::Read)?)
        );
        shared_seed
            .shared_grants
            .first_mut()
            .ok_or_else(|| Error::Invalid("fixture has no shared grant".into()))?
            .child_agent = AgentId::from_bytes([3; 16]);
        assert!(shared_seed.validate().is_err());
        let inherited = FileRef::new(
            seed.child_private_volume.clone(),
            ".system/inherited-conversation/prefix.json",
            "one",
            FileDescriptor::from_bytes(
                b"one",
                "application/vnd.acyclic.harness.inherited-conversation+json",
            )?,
            "prefix.json",
        )?;
        seed.inherited_through_sequence = 1;
        seed.inherited_context = vec![inherited.clone(), inherited.clone()];
        assert!(seed.validate().is_err());
        seed.inherited_context = vec![inherited.clone()];
        let attached = AgentId::from_bytes([5; 16]);
        seed.attached_agents.push(attached);
        seed.validate()?;
        let access = seed.attached_read_capabilities(attached)?;
        assert!(access.contains(&inherited.read_capability()?));
        assert!(
            !access.contains(
                &seed
                    .child_private_volume
                    .capability(VolumeOperation::Write)?
            )
        );
        assert!(
            seed.reference_capabilities(AgentId::from_bytes([7; 16]))
                .is_err()
        );
        seed.inherited_context.clear();
        seed.parent_revision += 1;
        assert!(seed.validate().is_err());
        Ok(())
    }
}
