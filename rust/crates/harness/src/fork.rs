//! Typed capture and publication of a child environment.

use crate::conversation::{
    ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    decode_attachment_manifest,
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
    /// Binds one contiguous parent prefix without retaining its message list.
    pub fn select(
        parent: Authority,
        parent_revision: u64,
        through_sequence: u64,
        attached_agents: &[AgentId],
        conversation: &crate::conversation::ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut owned = CodecQuota::observed(work)?;
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
        let message_digest = conversation.history_prefix_digest(through_sequence, &mut None)?;
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
    pub(crate) fn verify(
        &self, verifier: &crate::core::AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify_audience(&self.parent, work)?;
        let digest = self.conversation.history_prefix_digest(self.through_sequence(), work)?;
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

use crate::store::history::{HistoryIo, HistoryContentFuture};

/// One provider's admission barrier for prepared fork state.
pub trait ForkSeedVerifier: acyclic_stream::ProviderPlatform {
    /// Identifies the provider proved by this verifier.
    fn provider(&self) -> &ProviderRef;
    /// Bounds all descriptors verified during publication.
    fn maximum_reference_bytes(&self) -> u64 { MAX_FORK_REFERENCE_BYTES }
    /// Checks every selected revision owned by this provider.
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>>;
    /// Performs the provider's acquire private fence operation.
    fn acquire_private_fence<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ObservedForkPublicationGuard>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork private publication fence is not bound"))
        })
    }
    /// Performs the provider's read manifest operation.
    fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, _manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork attachment manifest reader is not bound"))
        })
    }
    /// Performs the provider's verify file operation.
    fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, _file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork file verifier is not bound"))
        })
    }
    /// Performs the provider's verify boundary operation.
    fn verify_boundary<'a, 'w: 'a>(&'a self, _boundary: &'a AttestedBoundary,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "provider does not attest fork boundaries"))
        })
    }
}

/// A provider-owned fence retained through publication or reconciliation.
pub trait ForkPublicationGuard: acyclic_stream::ProviderPlatform {
    /// Idempotently releases the exact fence after publication.
    fn release<'a, 'w: 'a>(&'a self, work: &'a mut Option<&'w mut dyn HistoryIo>)
        -> Result<HistoryContentFuture<'a, ()>>;
}

pub(crate) fn admit_fork_future<'a, 'w: 'a, T: 'a, F, B>(
    work: &'a mut Option<&'w mut dyn HistoryIo>, build: B,
) -> Result<HistoryContentFuture<'a, T>>
where F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
    B: FnOnce(&'a mut Option<&'w mut dyn HistoryIo>) -> F,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_ref().and_then(|io| io.original_native_work()).cloned() {
        return crate::NativeFutureBundle::new(&native, || build(work))
            .map(HistoryContentFuture::Native);
    }
    let frame_bytes = if let Some(io) = work.as_deref_mut() {
        let bytes = std::mem::size_of::<F>() as u64;
        io.check_cancellation()?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        bytes
    } else { 0 };
    Ok(HistoryContentFuture::Ordinary { future: Box::pin(build(work)), frame_bytes })
}

pub(super) fn boundary_error(
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

fn frame_bytes<T>(future: &HistoryContentFuture<'_, T>) -> u64 {
    match future {
        HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        HistoryContentFuture::Native(_) => 0,
    }
}

fn release_frame<T>(result: Result<T>, bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>) -> Result<T> {
    let released = if bytes == 0 { Ok(()) }
        else { work.as_deref_mut().expect("funded fork frame").release_owned(bytes) };
    match result { Err(error) => Err(error), Ok(value) => released.map(|()| value) }
}

pub(crate) async fn release_fork_guard_with_work(
    guard: &dyn ForkPublicationGuard, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let future = guard.release(work)?;
    let bytes = frame_bytes(&future);
    let result = future.await;
    release_frame(result, bytes, work)
}

macro_rules! fork_provider_dispatcher {
    ($name:ident, $operation:ident, $input:ident : $input_type:ty, $output:ty $(, $context:ident : $context_type:ty)?) => {
        async fn $name(verifier: &dyn ForkSeedVerifier, $($context: &$context_type,)?
            $input: &$input_type, work: &mut Option<&mut dyn HistoryIo>) -> Result<$output> {
            let future = verifier.$operation($($context,)? $input, work)?;
            let bytes = frame_bytes(&future);
            let result = future.await;
            release_frame(result, bytes, work)
        }
    };
}

pub(crate) struct ObservedForkPublicationGuard {
    pub(crate) guard: Box<dyn ForkPublicationGuard>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) _allocation: Option<acyclic_fs::NativeAllocation>,
}

async fn acquire_fork_fence(verifier: &dyn ForkSeedVerifier, seed: &ForkSeed,
    work: &mut Option<&mut dyn HistoryIo>) -> Result<ObservedForkPublicationGuard> {
    let future = verifier.acquire_private_fence(seed, work)?;
    let bytes = frame_bytes(&future);
    let result = future.await;
    let guard = release_frame(result, bytes, work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_ref().and_then(|io| io.original_native_work()) {
        if !guard._allocation.as_ref().is_some_and(|allocation| allocation.work().shares_ledger(native)) {
            let ObservedForkPublicationGuard { guard, _allocation } = guard;
            drop(guard);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: _allocation,
            });
        }
    }
    Ok(guard)
}
fork_provider_dispatcher!(verify_fork_provider_with_work, verify, seed: ForkSeed, ());
fork_provider_dispatcher!(verify_fork_file_with_work, verify_file, file: FileRef, (), seed: ForkSeed);
fork_provider_dispatcher!(verify_fork_boundary_with_work, verify_boundary, boundary: AttestedBoundary, ());
async fn read_fork_manifest(verifier: &dyn ForkSeedVerifier, seed: &ForkSeed,
    manifest: &FileRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<(acyclic_stream::OwnedBytes, u64)> {
    let future = verifier.read_manifest(seed, manifest, work)?;
    let frame = frame_bytes(&future);
    let result = future.await;
    let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
    let result = release_frame(result, frame, work);
    if result.is_err() && retained != 0 {
        let _ = work.as_deref_mut().expect("retained manifest observer").release_owned(retained);
    }
    result
}

/// Exhaustive provider dispatcher.
pub struct CompositeForkVerifier {
    providers: BTreeMap<ProviderRef, std::sync::Arc<dyn ForkSeedVerifier>>,
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
    /// Registers exact provider contracts using the caller's optional ledger.
    pub fn new(
        verifiers: &[std::sync::Arc<dyn ForkSeedVerifier>],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut owned = CodecQuota::observed(work)?;
        let mut providers = BTreeMap::new();
        for verifier in verifiers {
            owned.checkpoint()?;
            verifier.provider().validate_with_work(owned.work())?;
            if providers.contains_key(verifier.provider()) {
                return Err(owned.invalid("fork provider verifier is registered twice"));
            }
            owned.slots::<(ProviderRef, std::sync::Arc<dyn ForkSeedVerifier>)>(1)?;
            let provider = crate::core::copy_provider(verifier.provider(), &mut owned)?;
            providers.insert(provider, std::sync::Arc::clone(verifier));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _native_metadata = {
            let bytes = owned.retained();
            owned.work().as_deref_mut().filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(bytes)).transpose()?
        };
        owned.transfer();
        Ok(Self {
            providers,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata,
        })
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
    pub(crate) async fn activate(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ObservedForkPublicationGuard> {
        seed.validate(work)?;
        let private = self.provider(seed.child_private_volume.provider(), &mut CodecQuota::observed(work)?)?;
        let guard = acquire_fork_fence(private.as_ref(), seed, work).await?;
        if let Err(error) = self.verify(seed, work).await {
            let released = release_fork_guard_with_work(guard.guard.as_ref(), work).await;
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

    /// Releases a recovered fence after Stream proves publication terminal.
    pub async fn release_private_fence(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate(work)?;
        let private = self.provider(seed.child_private_volume.provider(), &mut CodecQuota::observed(work)?)?;
        match acquire_fork_fence(private.as_ref(), seed, work).await {
            Ok(guard) => release_fork_guard_with_work(guard.guard.as_ref(), work).await,
            // A different operation can acquire the gate after this fork's terminal release.
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Verifies every source, child, private, inherited, and attested provider.
    pub async fn verify(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate(work)?;
        let mut owned = CodecQuota::observed(work)?;
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
            owned.checkpoint()?;
            if !required.contains(provider) {
                owned.slots::<&ProviderRef>(1)?;
                required.insert(provider);
            }
        }
        for provider in required {
            let verifier = self.provider(provider, &mut owned)?;
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
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<&'a std::sync::Arc<dyn ForkSeedVerifier>> {
        owned.checkpoint()?;
        self.providers.get(provider).ok_or_else(|| boundary_error(owned.work(),
            crate::wire::ErrorCode::Unsupported, "fork resource has no exact provider verifier"))
    }

    /// Verifies direct references and complete attachment manifests after provider-wide resource capture has been attested.
    async fn verify_references(
        &self,
        seed: &ForkSeed,
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        let mut verified_files = BTreeSet::new();
        let mut reference_bytes = BTreeMap::new();
        let (direct_manifest_reads, member_grants) = collect_manifest_grants(seed, owned)?;
        let volume_readers = ForkVolumeReaders::new(seed, owned)?;
        self.verify_reference_files(seed, &mut verified_files, &mut reference_bytes, owned)
            .await?;
        self.verify_attachment_manifests(
            seed,
            &mut verified_files,
            &mut reference_bytes,
            &volume_readers,
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
        owned: &mut CodecQuota<'_, '_>,
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
        volume_readers: &ForkVolumeReaders,
        direct_manifest_reads: &DirectManifestReads,
        member_grants: &ManifestMemberGrants,
        owned: &mut CodecQuota<'_, '_>,
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
        volume_readers: &ForkVolumeReaders,
        direct_manifest_reads: &DirectManifestReads,
        member_grants: &ManifestMemberGrants,
        owned: &mut CodecQuota<'_, '_>,
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
        let volume = manifest.volume();
        let volume_digest = crate::contract::canonical_json_digest_with_work(volume,
            crate::store::default_projection_read_limits().maximum_bytes, owned.work())?;
        for reader in grants.into_iter().flat_map(BTreeMap::values).flatten() {
            if !volume_readers.allows(volume, &volume_digest, reader)
                && !direct_manifest_reads.contains(&(manifest_capability, *reader)) {
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
    owned: &mut CodecQuota<'_, '_>,
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
}

impl ForkVolumeReaders {
    fn new(seed: &ForkSeed, owned: &mut CodecQuota<'_, '_>) -> Result<Self> {
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
                let key = crate::contract::canonical_json_digest_with_work(volume, crate::store::default_projection_read_limits().maximum_bytes, owned.work())?;
                if !selected_projects.contains(&key) {
                    owned.slots::<[u8; 32]>(1)?;
                    selected_projects.insert(key);
                }
            }
        }
        let mut shared = BTreeMap::<[u8; 32], BTreeSet<AgentId>>::new();
        for grant in &seed.shared_grants {
            if grant.operations.contains(&VolumeOperation::Read) {
                let key = crate::contract::canonical_json_digest_with_work(&grant.volume, crate::store::default_projection_read_limits().maximum_bytes, owned.work())?;
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
        })
    }

    fn allows(&self, volume: &VolumeRef, digest: &[u8; 32], reader: &AgentId) -> bool {
        match volume.class() {
            VolumeClass::AgentPrivate => volume.owner() == &VolumeOwner::Agent(*reader)
                && self.all.contains(reader),
            VolumeClass::Project => self.selected_projects.contains(digest)
                && self.all.contains(reader),
            VolumeClass::SessionShared => self.shared.get(digest)
                .is_some_and(|readers| readers.contains(reader)),
        }
    }
}

fn reserve_reference_bytes<'a>(
    totals: &mut BTreeMap<&'a ProviderRef, u64>,
    verifier: &'a dyn ForkSeedVerifier,
    file: &FileRef,
    owned: &mut CodecQuota<'_, '_>,
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

fn decode_fork_manifest(
    reference: &FileRef,
    bytes: &[u8],
    owned: &mut CodecQuota<'_, '_>,
) -> Result<Vec<crate::conversation::Attachment>> {
    if owned.work().is_none() {
        return decode_attachment_manifest(reference, bytes, None, None);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = owned.work().as_deref().and_then(|io| io.original_native_work()) {
        let io = crate::filesystem::PreparationIo::from_native_work(native)?;
        let items = crate::conversation::decode_attachment_manifest(
            reference, bytes, None, Some(&io))?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        owned.work().as_deref_mut().expect("original manifest observer").absorb_native_custody(custody)?;
        owned.adopt(retained)?;
        return Ok(items);
    }
    reference.validate(&mut None)?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(Error::Invalid("attachment manifest metadata is invalid".into()));
    }
    if bytes.len() as u64 == reference.descriptor().byte_length()
        && let Some(observer) = owned.work().as_deref_mut()
    {
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    reference.descriptor().verify(bytes, &mut None)?;
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
    let decoded: Result<Vec<crate::conversation::Attachment>> =
        crate::contract::json_from_slice_bounded(std::marker::PhantomData, bytes, bounds, schema_bytes, None, None, &mut charge);
    drop(charge);
    let items = decoded.map_err(|error| {
        if observed_failure { error }
        else { Error::Invalid(format!("attachment manifest is invalid: {error}")) }
    })?;
    owned.release(diagnostic_bytes)?;
    if items.len() as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in &items {
        item.validate(&mut None)?;
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
}

impl ForkSeedVerifier for ContentForkVerifier {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            seed.validate(work)?;
            if seed.child_private_volume.provider() == &self.provider
                || seed.resources.iter().any(|capture| {
                    capture.source.provider() == &self.provider
                        || capture.revision.provider() == &self.provider
                })
            {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unsupported,
                    "content-only provider cannot attest mutable fork resources"));
            }
            let mut owned = CodecQuota::observed(work)?;
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
                match owned.work().as_deref_mut() {
                    Some(observer) => crate::store::history::verify_content_with_work(
                        observer, self.content.as_ref(), file).await?,
                    None => self.content.verify(file, file.descriptor().byte_length(), None)?.await?,
                }
            }
            Ok(())
        })
    }
    fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        admit_fork_future(work, |work| async move {
            if manifest.volume().provider() != &self.provider {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unauthorized,
                    "fork manifest belongs to another provider"));
            }
            match work.as_deref_mut() {
                Some(observer) => crate::store::history::read_content_with_work(
                    observer, self.content.as_ref(), manifest).await,
                None => self.content.read(manifest, manifest.descriptor().byte_length(), None)?.await.map(|bytes| (bytes, 0)),
            }
        })
    }
    fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            if file.volume().provider() != &self.provider {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unauthorized,
                    "fork file belongs to another provider"));
            }
            match work.as_deref_mut() {
                Some(observer) => crate::store::history::verify_content_with_work(
                    observer, self.content.as_ref(), file).await,
                None => self.content.verify(file, file.descriptor().byte_length(), None)?.await,
            }
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
    /// Registers the exact Stream provider using the caller's optional ledger.
    pub fn new(provider: &ProviderRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        let mut owned = CodecQuota::observed(work)?;
        provider.validate_with_work(owned.work())?;
        if provider.family() != "stream" {
            return Err(boundary_error(owned.work(), crate::wire::ErrorCode::Invalid,
                "history verifier requires a Stream provider"));
        }
        let provider = crate::core::copy_provider(provider, &mut owned)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _native_metadata = {
            let bytes = owned.retained();
            owned.work().as_deref_mut().filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(bytes)).transpose()?
        };
        owned.transfer();
        Ok(Self { provider,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata })
    }

    /// Shares the same funded provider identity; Arc's private backing is opaque.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_native_shared(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        admit_native_verifier_share(self._native_metadata.as_ref(), work, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(self))
    }
}

impl ForkSeedVerifier for StreamHistoryForkVerifier {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            seed.validate(work)?;
            for capture in &seed.resources {
                for revision in [&capture.source, &capture.revision] {
                    if revision.provider() == &self.provider
                        && !matches!(revision, ResourceRevision::History(_))
                    {
                        return Err(boundary_error(work, crate::wire::ErrorCode::Unsupported,
                            "Stream history verifier cannot prove this resource"));
                    }
                }
            }
            Ok(())
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
    pub fn validate(
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
                volume.validate(work)?;
                generation.validate_with_work(work)?;
                if paths.len() as u64 > MAX_FORK_RESOURCES as u64 {
                    return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
                        "too many scratch paths"));
                }
                for path in paths {
                    let relative = path
                        .strip_prefix('/')
                        .ok_or_else(|| boundary_error(work, crate::wire::ErrorCode::Invalid,
                            "scratch path must be absolute"))?;
                    crate::conversation::validate_content_path(relative, work)?;
                    if relative.split('/').any(|segment| segment == ".system") {
                        return Err(boundary_error(work, crate::wire::ErrorCode::Unauthorized,
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
                volume.validate(work)?;
                generation.validate_with_work(work)
            }
            Self::Context(value) => value.validate_with_work(work),
            Self::Process(value) if value.as_resource().provider().family() == "machines" => {
                value.validate_with_work(work)
            }
            Self::Artifact(value) => value.validate_with_work(work),
            Self::SharedVolume(value) if value.class() == VolumeClass::SessionShared => {
                value.validate(work)
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
            _ => Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
            return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
                return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
                    "summary fork requires a nonempty logical parent cut"));
            }
        }
        self.child_project_volume.validate(work)?;
        self.child_private_volume.validate(work)?;
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
            return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
                "fork preparation is invalid"));
        }
        if let Some(result) = &self.child_result_volume {
            validate_child_result_volume(result, &self.child_private_volume, child_agent, work)?;
            if result == &self.child_project_volume {
                return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
    result.validate(work)?;
    if result.class() != VolumeClass::AgentPrivate
        || result.owner() != &VolumeOwner::Agent(child)
        || result.provider() != scratch.provider()
        || result == scratch
    {
        return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
                return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
                    "child result generation belongs to another provider"));
            }
            Ok(())
        }
        _ => Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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

pub(super) use crate::wire_codec::CodecQuota;

impl<'a, 'b> CodecQuota<'a, 'b> {
    pub(super) fn observed(work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        Ok(Self::from_retained(work, 0))
    }

    fn invalid(&mut self, reason: &'static str) -> Error {
        boundary_error(self.work(), crate::wire::ErrorCode::Invalid, reason)
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
        let retained = self.retained().checked_add(bytes)
            .ok_or_else(|| self.invalid("fork validation storage overflows"))?;
        self.replace(retained)
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() { return Ok(()); }
        self.retained().checked_sub(bytes)
            .ok_or_else(|| self.invalid("fork validation storage underflows"))?;
        self.relinquish(bytes)
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

    fn capability<T: Serialize + ?Sized>(&mut self, prefix: &str, value: &T) -> Result<String> {
        let digest = crate::contract::canonical_json_digest_with_work(value, crate::store::default_projection_read_limits().maximum_bytes, self.work())?;
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

    pub(super) fn file_digest(&mut self, file: &FileRef) -> Result<[u8; 32]> {
        file.validate(self.work())?;
        crate::contract::canonical_json_digest_with_work(file, crate::store::default_projection_read_limits().maximum_bytes, self.work())
    }

    pub(super) fn volume_capability(&mut self, volume: &VolumeRef) -> Result<String> {
        self.capability("volume:read:", volume)
    }

    fn stream_path(&mut self, authority: &Authority) -> Result<String> {
        let path = authority.stream_path(self.work())?;
        self.adopt(path.capacity() as u64)?;
        Ok(path)
    }
}

impl ForkRequest {
    /// Resolves the context policy against the exact parent before preparation.
    pub fn with_history_policy(
        mut self,
        parent: &crate::core::Reducer,
        policy: ForkHistoryPolicy,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let policy_error = |code, reason| boundary_error(work, code, reason);
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
        self.validate(work)?;
        Ok(self)
    }

    /// Checks identities, resource selections and optional attestation metadata.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut work = CodecQuota::observed(work)?;
        if self.attached_agents.len() as u64 > MAX_FORK_AGENTS as u64
            || self.selections.len() as u64 > MAX_FORK_RESOURCES as u64
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
            source.validate(work.work())?;
            if source.operation_id != self.operation_id
                || !self.selections.iter().any(|selection| matches!(
                    &selection.revision,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation))
                || (source.source_task != source.caller_task
                    && (self.preparation.inherited_through_sequence != 0
                        || self.preparation.summary.is_some()))
            {
                return Err(boundary_error(work.work(), crate::wire::ErrorCode::Unauthorized,
                    "authorized source must select its exact project and history-free cross-task fork"));
            }
        }
        work.slots::<AgentId>(self.attached_agents.len())?;
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(work.invalid("fork attached agents are not unique"));
        }
        work.slots::<Vec<u8>>(self.selections.len())?;
        let mut selected = BTreeSet::new();
        let mut histories = 0_usize;
        let mut projects = 0_usize;
        let mut private = 0_usize;
        let mut contexts = 0_usize;
        let mut processes = 0_usize;
        for selection in &self.selections {
            work.checkpoint()?;
            selection.revision.validate(work.work())?;
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
#[allow(clippy::large_enum_variant, reason = "capture variants preserve the published v2 serde shape")]
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
        self.file.validate(work)?;
        if let Some(manifest) = &self.attachment_manifest {
            manifest.validate(work)?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
            {
                return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
        self.volume.validate(work)?;
        if self.volume.class() != VolumeClass::SessionShared || self.operations.is_empty() {
            return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
        self.selection.revision.validate(work)?;
        if self.selection.required || matches!(self.outcome, Capture::Captured(_)) {
            return Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
                "fork omission must be an uncaptured optional selection"));
        }
        Ok(())
    }
}

impl CapturedResource {
    /// Only project capture may create a new revision and volume identity.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.source.validate(work)?;
        self.revision.validate(work)?;
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
            | (_, ResourceRevision::PrivateVolume { .. }) => Err(boundary_error(
                work, crate::wire::ErrorCode::Invalid, "scratch capture changed its selection")),
            (ResourceRevision::Project { .. }, _) | (_, ResourceRevision::Project { .. }) => Err(
                boundary_error(work, crate::wire::ErrorCode::Invalid,
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
            _ => Err(boundary_error(work, crate::wire::ErrorCode::Invalid,
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
    /// Validates a prepared report, including unavailable captures.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.request.validate(work)?;
        let mut work = CodecQuota::observed(work)?;
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
                    resource.validate(work.work())?;
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
        self.child_private_volume.validate(work.work())?;
        self.child_private_generation.as_resource().validate_with_work(work.work())?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.request.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(work.invalid("fork report private volume is not child owned"));
        }
        for file in &self.inherited_context {
            work.checkpoint()?;
            file.validate(work.work())?;
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
            manifest.validate(work.work())?;
        }
        Ok(())
    }

    /// Produces a publishable seed only when every required capture succeeded.
    pub fn into_seed(
        self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ForkSeed> {
        let mut owned = CodecQuota::observed(work)?;
        self.validate(owned.work())?;
        let (mut resources, mut omissions) = if owned.work().is_some() {
            let resource_count = self.captures.iter()
                .filter(|capture| matches!(capture, Capture::Captured(_))).count();
            let omission_count = self.captures.len() - resource_count;
            let resources = crate::core::reserve_copy_vector::<CapturedResource>(
                resource_count, &mut owned)?;
            let omissions = crate::core::reserve_copy_vector::<ForkOmission>(
                omission_count, &mut owned)?;
            (resources, omissions)
        } else {
            (Vec::new(), Vec::new())
        };
        for (selection, capture) in self.request.selections.into_iter().zip(self.captures) {
            owned.checkpoint()?;
            match capture {
                Capture::Captured(resource) if resource.source == selection.revision => {
                    resource.validate(owned.work())?;
                    resources.push(resource);
                }
                Capture::Captured(_) => {
                    return Err(owned.invalid("fork capture source does not match selection"));
                }
                Capture::Unsupported(_) | Capture::InFlight(_) | Capture::Indeterminate(_)
                    if selection.required =>
                {
                    return Err(boundary_error(owned.work(), crate::wire::ErrorCode::Conflict,
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
        seed.validate(owned.work())?;
        // Drop failed vectors before their claims; transfer successful claims with the seed.
        owned.transfer();
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
    #[allow(clippy::too_many_lines, reason = "validates the complete fork seed contract")]
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut work = CodecQuota::observed(work)?;
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
        self.child_private_volume.validate(work.work())?;
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
            source.validate(work.work())?;
            if source.operation_id != self.operation_id
                || !self.resources.iter().any(|capture| matches!(
                    &capture.source,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation))
                || (source.source_task != source.caller_task
                    && (self.inherited_through_sequence != 0 || self.summary.is_some()))
            {
                return Err(boundary_error(work.work(), crate::wire::ErrorCode::Unauthorized,
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
            resource.validate(work.work())?;
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
            manifest.validate(work.work())?;
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
        let volume_readers = ForkVolumeReaders::new(self, &mut work)?;
        for manifest in &self.attachment_manifests {
            work.checkpoint()?;
            let capability = work.file_digest(manifest)?;
            let volume = manifest.volume();
            let volume_digest = crate::contract::canonical_json_digest_with_work(volume,
                crate::store::default_projection_read_limits().maximum_bytes, work.work())?;
            if let Some(grants) = member_grants.get(&capability) {
                for reader in grants.values().flat_map(|readers| readers.iter()) {
                    work.checkpoint()?;
                    if !volume_readers.allows(volume, &volume_digest, reader)
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
        work: &mut CodecQuota<'_, '_>,
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
        work: &mut CodecQuota<'_, '_>,
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

    fn validate_inherited_context(&self, work: &mut CodecQuota<'_, '_>) -> Result<()> {
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
            file.validate(work.work())?;
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

    /// Exact shared operations issuable to an attached reader or owning child.
    pub fn shared_capabilities(&self, reader: AgentId) -> Result<Capabilities> {
        self.validate(&mut None)?;
        if reader != self.child_agent && !self.attached_agents.contains(&reader) {
            return Err(Error::Unauthorized("agent is not attached to fork".into()));
        }
        let mut values = Vec::new();
        for grant in &self.shared_grants {
            if grant.child_agent == reader {
                for operation in &grant.operations {
                    values.push(grant.volume.capability(*operation, &mut None)?);
                }
            }
        }
        Ok(Capabilities::new(values))
    }

    /// Exact read capabilities issuable to one attached agent.
    pub fn reference_capabilities(&self, reader: AgentId) -> Result<Capabilities> {
        self.validate(&mut None)?;
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
                values.push(grant.volume.capability(VolumeOperation::Read, &mut None)?);
            }
        }
        for resource in &self.resources {
            if let ResourceRevision::Project { volume, .. } = &resource.revision {
                values.push(volume.capability(VolumeOperation::Read, &mut None)?);
            }
        }
        Ok(Capabilities::new(values))
    }
}

mod physical_clones {
    use super::*;
    use crate::store::history::HistoryIo;
    use crate::wire_codec::CodecQuota;

    use crate::core::{copy_text as text, copy_authority as authority, copy_provider as provider,
        copy_file as file, reserve_copy_vector as slots, copy_copy_vector as bytes};

    fn vector<T>(source: &[T], quota: &mut CodecQuota<'_, '_>,
        mut copy: impl FnMut(&T, &mut CodecQuota<'_, '_>) -> Result<T>) -> Result<Vec<T>> {
        crate::core::copy_vector(source, quota, |value, quota| { quota.checkpoint()?; copy(value, quota) })
    }

    fn agents(source: &[AgentId], quota: &mut CodecQuota<'_, '_>) -> Result<Vec<AgentId>> {
        for _ in source { quota.checkpoint()?; }
        bytes(source, quota)
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

    fn revision(source: &ResourceRevision, quota: &mut CodecQuota<'_, '_>) -> Result<ResourceRevision> {
        Ok(match source {
            ResourceRevision::History(value) => ResourceRevision::History(
                StreamRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Project { volume: source_volume, generation } => ResourceRevision::Project {
                volume: source_volume.clone_in_quota(quota)?,
                generation: GenerationRef::from_resource_with_work(resource(generation.as_resource(), quota)?, quota.work())?,
            },
            ResourceRevision::PrivateVolume { volume: source_volume, generation, paths } => {
                let volume = source_volume.clone_in_quota(quota)?;
                let generation = GenerationRef::from_resource_with_work(
                    resource(generation.as_resource(), quota)?, quota.work())?;
                let copied_paths = vector(paths, quota, |path, quota| text(path, quota))?;
                ResourceRevision::PrivateVolume { volume, generation, paths: copied_paths }
            },
            ResourceRevision::Context(value) => ResourceRevision::Context(
                ContextRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Process(value) => ResourceRevision::Process(
                CheckpointRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::Artifact(value) => ResourceRevision::Artifact(
                ArtifactRef::from_resource_with_work(resource(value.as_resource(), quota)?, quota.work())?),
            ResourceRevision::SharedVolume(value) => ResourceRevision::SharedVolume(value.clone_in_quota(quota)?),
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
            payloads: vector(&source.payloads, quota, file)?,
            references: vector(&source.references, quota, file)?,
        })
    }

    fn preparation(source: &ForkPreparation, quota: &mut CodecQuota<'_, '_>) -> Result<ForkPreparation> {
        Ok(ForkPreparation {
            child_project_volume: source.child_project_volume.clone_in_quota(quota)?,
            child_private_volume: source.child_private_volume.clone_in_quota(quota)?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| value.clone_in_quota(quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            maximum_inherited_messages: source.maximum_inherited_messages,
            maximum_inherited_bytes: source.maximum_inherited_bytes,
            maximum_inherited_references: source.maximum_inherited_references,
            summary: source.summary.as_ref().map(|value| summary_selection(value, quota)).transpose()?,
        })
    }

    fn shared(grant: &SharedGrant, quota: &mut CodecQuota<'_, '_>) -> Result<SharedGrant> {
        let volume = grant.volume.clone_in_quota(quota)?;
        let mut operations = BTreeSet::new();
        // Explicit entry payload; std node/control allocation is opaque.
        quota.slots::<VolumeOperation>(grant.operations.len())?;
        for operation in &grant.operations {
            quota.checkpoint()?;
            operations.insert(*operation);
        }
        Ok(SharedGrant { volume, child_agent: grant.child_agent, operations })
    }

    fn reference(grant: &ReferenceGrant, quota: &mut CodecQuota<'_, '_>) -> Result<ReferenceGrant> {
        Ok(ReferenceGrant {
            file: file(&grant.file, quota)?, reader: grant.reader,
            attachment_manifest: grant.attachment_manifest.as_ref().map(|entry| file(entry, quota)).transpose()?,
        })
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
        let selections = vector(&source.selections, quota, selection)?;
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
        let captures = vector(&source.captures, quota, capture)?;
        Ok(ForkReport {
            request,
            captures,
            child_private_volume: source.child_private_volume.clone_in_quota(quota)?,
            child_private_generation: GenerationRef::from_resource_with_work(
                resource(source.child_private_generation.as_resource(), quota)?, quota.work())?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| value.clone_in_quota(quota)).transpose()?,
            child_result_generation: source.child_result_generation.as_ref()
                .map(|value| GenerationRef::from_resource_with_work(
                    resource(value.as_resource(), quota)?, quota.work())).transpose()?,
            inherited_context: vector(&source.inherited_context, quota, file)?,
            summary: source.summary.as_ref().map(|value| summary_capture(value, quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            shared_grants: vector(&source.shared_grants, quota, shared)?,
            reference_grants: vector(&source.reference_grants, quota, reference)?,
            history_witness: source.history_witness,
            attachment_manifests: vector(&source.attachment_manifests, quota, file)?,
        })
    }

    fn seed(source: &ForkSeed, quota: &mut CodecQuota<'_, '_>) -> Result<ForkSeed> {
        let parent = authority(&source.parent, quota)?;
        let child = authority(&source.child, quota)?;
        let attached_agents = agents(&source.attached_agents, quota)?;
        let resources = vector(&source.resources, quota, captured)?;
        let mut omissions = slots::<ForkOmission>(source.omissions.len(), quota)?;
        for entry in &source.omissions {
            quota.checkpoint()?;
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
            child_private_volume: source.child_private_volume.clone_in_quota(quota)?,
            child_private_generation: GenerationRef::from_resource_with_work(
                resource(source.child_private_generation.as_resource(), quota)?, quota.work())?,
            child_result_volume: source.child_result_volume.as_ref()
                .map(|value| value.clone_in_quota(quota)).transpose()?,
            child_result_generation: source.child_result_generation.as_ref()
                .map(|value| GenerationRef::from_resource_with_work(
                    resource(value.as_resource(), quota)?, quota.work())).transpose()?,
            inherited_context: vector(&source.inherited_context, quota, file)?,
            summary: source.summary.as_ref().map(|value| summary_capture(value, quota)).transpose()?,
            inherited_through_sequence: source.inherited_through_sequence,
            shared_grants: vector(&source.shared_grants, quota, shared)?,
            reference_grants: vector(&source.reference_grants, quota, reference)?,
            history_witness: source.history_witness,
            attachment_manifests: vector(&source.attachment_manifests, quota, file)?,
            boundary: source.boundary.as_ref().map(|value| boundary(value, quota)).transpose()?,
        })
    }

    impl ResourceRevision {
        crate::core::physical_clone_factory!(copy, ResourceRevision, revision, self);
    }
    impl ForkReport {
        /// Copies report fields with bounded work and returns their retained bytes.
        pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
            if work.is_none() { return Ok((self.clone(), 0)); }
            let mut quota = CodecQuota::new(work, 0)?;
            let value = report(self, &mut quota)?;
            let held = quota.retained();
            quota.transfer();
            Ok((value, held))
        }
    }
    impl ForkRequest {
        crate::core::physical_clone_factory!(copy, ForkRequest, request, self);
    }
    impl ForkSeed {
        crate::core::physical_clone_factory!(copy, ForkSeed, seed, self);
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{conversation::FileDescriptor, core::AggregateKind};

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
                &mut None,
            )
            .is_ok()
        );
        Ok(())
    }

    struct AcceptingVerifier(ProviderRef);

    struct MissingFileVerifier(ProviderRef);

    struct ManifestVerifier {
        provider: ProviderRef,
        bytes: acyclic_stream::OwnedBytes,
    }

    impl ForkSeedVerifier for ManifestVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.provider
        }

        fn verify<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async { Ok(()) })
        }

        fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, file: &'a FileRef, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async move {
                if file.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                file.descriptor().verify(&self.bytes, &mut None)
            })
        }

        fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, manifest: &'a FileRef, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
            admit_fork_future(work, |_work| async move {
                if manifest.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                Ok((self.bytes.clone(), 0))
            })
        }
    }

    #[test]
    fn v2_fork_seed_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/fork-seed.json").trim();
        let seed: ForkSeed =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        seed.validate(&mut None)?;
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
        seed.validate(&mut None)?;
        let mut mutation = seed.clone();
        mutation.child_result_generation = None;
        assert_eq!(mutation.validate(&mut None).unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        mutation = seed.clone();
        mutation.child_result_volume = Some(seed.child_private_volume.clone());
        assert_eq!(mutation.validate(&mut None).unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        mutation = seed;
        mutation.child_result_generation = Some(GenerationRef::new(
            ProviderRef::new(provider.namespace(), provider.family(), "foreign-result-provider")?, [73; 32], None)?);
        assert_eq!(mutation.validate(&mut None).unwrap_err().code(), crate::wire::ErrorCode::Invalid);
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
        assert!(matches!(request.copy(&mut Some(&mut io)),
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
        assert!(request.copy(&mut Some(&mut cancelled_io)).is_err());
        assert_eq!(cancelled.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(cancelled.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(cancelled_io.into_custody().bytes(), 0);
        Ok(())
    }

    impl ForkSeedVerifier for AcceptingVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.0
        }

        fn verify<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async { Ok(()) })
        }

        fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, file: &'a FileRef, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async move {
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

        fn verify<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async { Ok(()) })
        }

        fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, _file: &'a FileRef, work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
            admit_fork_future(work, |_work| async { Err(Error::NotFound("pinned file".into())) })
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
        .validate(&mut None)?;
        assert!(
            CapturedResource {
                source: source.clone(),
                revision: source.clone()
            }
            .validate(&mut None)
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
            .validate(&mut None)
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn fork_history_must_be_exact_parent_prefix() -> Result<()> {
        let mut seed: ForkSeed = serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let filesystem = seed.child_private_volume.provider().clone();
        seed.validate(&mut None)?;
        let filesystem_only = CompositeForkVerifier::new(&[std::sync::Arc::new(
            AcceptingVerifier(filesystem.clone()),
        )], &mut None)?;
        assert!(matches!(
            futures::executor::block_on(filesystem_only.verify(&seed, &mut None)),
            Err(Error::Unsupported(_))
        ));
        let complete = CompositeForkVerifier::new(&[
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
        ], &mut None)?;
        futures::executor::block_on(complete.verify(&seed, &mut None))?;
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
                FileDescriptor::from_bytes(b"missing", "text/plain", &mut None)?,
                "missing.txt",
            )?,
            reader: seed.child_agent,
            attachment_manifest: None,
        });
        missing_seed.validate(&mut None)?;
        let missing = CompositeForkVerifier::new(&[
            std::sync::Arc::new(MissingFileVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
        ], &mut None)?;
        assert!(matches!(
            futures::executor::block_on(missing.verify(&missing_seed, &mut None)),
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
            FileDescriptor::from_bytes(b"actual", "text/plain", &mut None)?,
            "actual.txt",
        )?;
        let forged = FileRef::new(
            member_volume,
            "files/forged.txt",
            "two",
            FileDescriptor::from_bytes(b"forged", "text/plain", &mut None)?,
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
            FileDescriptor::from_bytes(&bytes, "application/vnd.acyclic.harness.attachments+json", &mut None)?,
            "one.json",
        )?;
        let mut manifest_seed = seed.clone();
        manifest_seed.attachment_manifests.push(manifest.clone());
        manifest_seed.reference_grants.push(ReferenceGrant {
            file: forged,
            reader: seed.child_agent,
            attachment_manifest: Some(manifest.clone()),
        });
        assert!(matches!(manifest_seed.validate(&mut None), Err(Error::Invalid(_))));
        manifest_seed.reference_grants.push(ReferenceGrant {
            file: manifest.clone(),
            reader: seed.child_agent,
            attachment_manifest: None,
        });
        manifest_seed.validate(&mut None)?;
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
        attached_manifest_only.validate(&mut None)?;
        let cached_manifest: acyclic_stream::OwnedBytes = bytes.into();
        let verifier = CompositeForkVerifier::new(&[
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(ManifestVerifier {
                provider: objects,
                bytes: cached_manifest.clone(),
            }),
        ], &mut None)?;
        futures::executor::block_on(verifier.verify(&attached_manifest_only, &mut None))?;
        let missing_manifest = CompositeForkVerifier::new(&[
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(MissingFileVerifier(manifest.volume().provider().clone())),
        ], &mut None)?;
        assert!(matches!(
            futures::executor::block_on(missing_manifest.verify(&manifest_seed, &mut None)),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            futures::executor::block_on(verifier.verify(&manifest_seed, &mut None)),
            Err(Error::Invalid(_))
        ));
        let first_grant = manifest_seed
            .reference_grants
            .first_mut()
            .ok_or_else(|| Error::Invalid("fixture has no reference grant".into()))?;
        first_grant.file = member;
        futures::executor::block_on(verifier.verify(&manifest_seed, &mut None))?;
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
        report.validate(&mut None)?;
        let mut duplicate_request = report.request.clone();
        let first_selection = duplicate_request
            .selections
            .first()
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture has no fork selection".into()))?;
        duplicate_request.selections.push(first_selection);
        assert!(duplicate_request.validate(&mut None).is_err());
        let mut optional_project = report.request.clone();
        optional_project
            .selections
            .get_mut(1)
            .ok_or_else(|| Error::Invalid("fixture has no project selection".into()))?
            .required = false;
        assert!(optional_project.validate(&mut None).is_err());
        let mut missing_history = report.request.clone();
        missing_history.selections.remove(0);
        assert!(missing_history.validate(&mut None).is_err());
        let mut stale_history = report.request.clone();
        stale_history.parent_revision = stale_history.parent_revision.saturating_add(1);
        assert!(stale_history.validate(&mut None).is_err());
        let mut changed_private = report.clone();
        changed_private.child_private_volume = VolumeRef::new(
            changed_private.child_private_volume.provider().clone(),
            "different-child-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(changed_private.request.child_agent),
        )?;
        assert!(changed_private.validate(&mut None).is_err());
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
        assert!(changed_project.validate(&mut None).is_err());
        let mut changed_prefix = report.clone();
        changed_prefix.inherited_through_sequence = 1;
        assert!(changed_prefix.validate(&mut None).is_err());
        let mut widened_request = report.request.clone();
        widened_request.preparation.maximum_inherited_messages = 16_385;
        assert!(widened_request.validate(&mut None).is_ok());
        let published = report.into_seed(&mut None)?;
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
        assert!(shared_seed.validate(&mut None).is_err());
        let ResourceRevision::SharedVolume(shared_volume) = optional else {
            return Err(Error::Invalid("fixture is not a shared volume".into()));
        };
        shared_seed.shared_grants.push(SharedGrant {
            volume: shared_volume.clone(),
            child_agent: seed.child_agent,
            operations: BTreeSet::from([VolumeOperation::Read]),
        });
        shared_seed.validate(&mut None)?;
        assert!(
            shared_seed
                .shared_capabilities(shared_seed.child_agent)?
                .contains(&shared_volume.capability(VolumeOperation::Read, &mut None)?)
        );
        shared_seed
            .shared_grants
            .first_mut()
            .ok_or_else(|| Error::Invalid("fixture has no shared grant".into()))?
            .child_agent = AgentId::from_bytes([3; 16]);
        assert!(shared_seed.validate(&mut None).is_err());
        let inherited = FileRef::new(
            seed.child_private_volume.clone(),
            ".system/inherited-conversation/prefix.json",
            "one",
            FileDescriptor::from_bytes(
                b"one",
                "application/vnd.acyclic.harness.inherited-conversation+json", &mut None,
            )?,
            "prefix.json",
        )?;
        seed.inherited_through_sequence = 1;
        seed.inherited_context = vec![inherited.clone(), inherited.clone()];
        assert!(seed.validate(&mut None).is_err());
        seed.inherited_context = vec![inherited.clone()];
        let attached = AgentId::from_bytes([5; 16]);
        seed.attached_agents.push(attached);
        seed.validate(&mut None)?;
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
        assert!(seed.validate(&mut None).is_err());
        Ok(())
    }
}
