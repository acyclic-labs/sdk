//! Typed capture and publication of a child environment.
//!
//! Resource providers prepare revisions independently. Only an explicit
//! attested boundary claims cross-provider consistency. Publication makes the
//! captured refs visible; project merge remains a separate Filesystem action.

use crate::conversation::{
    ContentResidencyVerifier, ConversationMessage, FileRef, VolumeClass, VolumeOperation,
    VolumeOwner, VolumeRef, decode_complete_attachment_manifest,
};
use crate::core::Authority;
use crate::resources::{
    ArtifactRef, CheckpointRef, ContextRef, GenerationRef, ProviderRef, ResourceRef, StreamRef,
};
use crate::{AgentId, Capabilities, Error, OperationId, Result};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::{future::Future, pin::Pin};

/// Hard protocol ceiling applied before allocating attached-reader state.
pub const MAX_FORK_AGENTS: usize = 1_024;
const MAX_FORK_RESOURCES: usize = 4_096;
const MAX_FORK_REFERENCES: usize = 65_536;
const MAX_FORK_ATTACHMENT_MANIFEST_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_FORK_INHERITED_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_FORK_REFERENCE_BYTES: u64 = 64 * 1_024 * 1_024 * 1_024;
/// Hard ceiling independent of a deployment's lower configured prefix limit.
pub const MAX_FORK_INHERITED_MESSAGES: u64 = 16_384;

type DirectManifestReads = BTreeSet<(String, AgentId)>;
type ManifestMemberGrants = BTreeMap<String, BTreeMap<String, BTreeSet<AgentId>>>;

/// Child-owned, ref-only exact prefix of the authoritative parent conversation.
/// Its canonical bytes are bound to the parent reducer before fork publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InheritedConversationPrefix {
    /// Parent conversation from which this prefix was selected.
    pub parent: Authority,
    /// Exact parent revision observed during selection.
    pub parent_revision: u64,
    /// Agent bound to the parent conversation.
    pub parent_agent: AgentId,
    /// Inclusive final sequence selected for inheritance.
    pub through_sequence: u64,
    /// Additional agents explicitly granted child reference access.
    pub attached_agents: Vec<AgentId>,
    /// Ordered ref-only messages in the selected prefix.
    pub messages: Vec<ConversationMessage>,
}

impl InheritedConversationPrefix {
    /// Selects one contiguous parent prefix without copying any attachment bodies.
    pub fn select(
        parent: Authority,
        parent_revision: u64,
        parent_agent: AgentId,
        through_sequence: u64,
        attached_agents: &[AgentId],
        messages: &[ConversationMessage],
    ) -> Result<Self> {
        let count = usize::try_from(through_sequence)
            .map_err(|_| Error::Invalid("inherited prefix is too large".into()))?;
        if count > messages.len() {
            return Err(Error::Invalid(
                "inherited prefix exceeds parent history".into(),
            ));
        }
        if through_sequence > MAX_FORK_INHERITED_MESSAGES {
            return Err(Error::Invalid(
                "inherited prefix exceeds protocol message limit".into(),
            ));
        }
        if attached_agents.len() > MAX_FORK_AGENTS {
            return Err(Error::Invalid(
                "too many attached agents for inherited context".into(),
            ));
        }
        let readers = attached_agents.iter().copied().collect::<BTreeSet<_>>();
        if readers.len() != attached_agents.len() {
            return Err(Error::Invalid(
                "inherited attached agents are invalid".into(),
            ));
        }
        Ok(Self {
            parent,
            parent_revision,
            parent_agent,
            through_sequence,
            attached_agents: attached_agents.to_vec(),
            messages: messages
                .get(..count)
                .ok_or_else(|| Error::Invalid("inherited prefix exceeds parent history".into()))?
                .to_vec(),
        })
    }

    /// Stable JSON descriptor input shared by staging and reducer admission.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(self)
    }
}

/// Provider-owned asynchronous fence guarding one fork publication.
pub type ForkFenceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn ForkPublicationGuard>>> + Send + 'a>>;

/// One exact provider's admission barrier for state prepared before publication.
pub trait ForkSeedVerifier: Send + Sync {
    /// Identity of the provider whose resources this verifier can prove.
    fn provider(&self) -> &ProviderRef;
    /// Maximum aggregate descriptor bytes verified for this provider during
    /// one publication, including members discovered through manifests.
    fn maximum_reference_bytes(&self) -> u64 {
        MAX_FORK_REFERENCE_BYTES
    }
    /// Checks every selected revision owned by this provider.
    fn verify<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    /// Acquires the child-private provider's durable write fence before any
    /// fork verification reads its mutable head. Only that provider implements
    /// this hook; all other provider verifiers remain read-only.
    fn acquire_private_fence<'a>(&'a self, _seed: &'a ForkSeed) -> ForkFenceFuture<'a> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fork private publication fence is not bound".into(),
            ))
        })
    }
    /// Reads a complete, pinned attachment manifest from this provider. The
    /// composite admission barrier checks membership across provider borders.
    fn read_manifest<'a>(
        &'a self,
        _manifest: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fork attachment manifest reader is not bound".into(),
            ))
        })
    }
    /// Proves one immutable member is resident at its owning provider.
    fn verify_file<'a>(
        &'a self,
        _file: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Err(Error::Unsupported("fork file verifier is not bound".into())) })
    }
    /// Checks a cross-provider boundary claim, if this provider issues one.
    fn verify_boundary<'a>(
        &'a self,
        _boundary: &'a AttestedBoundary,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "provider does not attest fork boundaries".into(),
            ))
        })
    }
}

/// An exact provider-owned write fence retained through Stream publication or
/// reconciliation. Dropping this handle intentionally does not release the
/// durable gate after an indeterminate append.
pub trait ForkPublicationGuard: Send + Sync {
    /// Idempotently releases the exact fence after a terminal publication result.
    fn release<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Exhaustive provider dispatcher. A seed cannot be admitted when any exact
/// provider identity is unbound, even when its envelope is syntactically valid.
pub struct CompositeForkVerifier {
    providers: BTreeMap<String, std::sync::Arc<dyn ForkSeedVerifier>>,
}

impl CompositeForkVerifier {
    /// Rejects duplicate registrations for one exact provider contract.
    pub fn new(verifiers: Vec<std::sync::Arc<dyn ForkSeedVerifier>>) -> Result<Self> {
        let mut providers = BTreeMap::new();
        for verifier in verifiers {
            verifier.provider().validate()?;
            let key = provider_key(verifier.provider());
            if providers.insert(key, verifier).is_some() {
                return Err(Error::Invalid(
                    "fork provider verifier is registered twice".into(),
                ));
            }
        }
        Ok(Self { providers })
    }

    /// Fences the child-private writer, then verifies all selected providers.
    /// The caller must keep the returned guard until Stream reaches a known
    /// committed or rejected result; uncertainty leaves the durable gate held.
    pub async fn activate(&self, seed: &ForkSeed) -> Result<Box<dyn ForkPublicationGuard>> {
        seed.validate()?;
        let private = self
            .providers
            .get(&provider_key(seed.child_private_volume.provider()))
            .ok_or_else(|| Error::Unsupported("fork private provider is not bound".into()))?;
        let guard = private.acquire_private_fence(seed).await?;
        if let Err(error) = self.verify(seed).await {
            guard
                .release()
                .await
                .map_err(|_| Error::Indeterminate(seed.operation_id))?;
            return Err(error);
        }
        Ok(guard)
    }

    /// Recovers and releases a previously held private fence only after an
    /// exact Stream idempotency observation proves publication terminal.
    pub async fn release_private_fence(&self, seed: &ForkSeed) -> Result<()> {
        seed.validate()?;
        let private = self
            .providers
            .get(&provider_key(seed.child_private_volume.provider()))
            .ok_or_else(|| Error::Unsupported("fork private provider is not bound".into()))?;
        match private.acquire_private_fence(seed).await {
            Ok(guard) => guard.release().await,
            // A different operation can acquire the gate after this fork's
            // terminal release. In that case this operation cannot still own
            // the fence, and a positive Stream observation is sufficient.
            Err(Error::Conflict(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Verifies every source, child, private, inherited, and attested provider.
    pub async fn verify(&self, seed: &ForkSeed) -> Result<()> {
        seed.validate()?;
        let mut required = BTreeSet::new();
        required.insert(provider_key(seed.child_private_volume.provider()));
        for file in &seed.inherited_context {
            required.insert(provider_key(file.volume().provider()));
        }
        for grant in &seed.reference_grants {
            required.insert(provider_key(grant.file.volume().provider()));
        }
        for manifest in &seed.attachment_manifests {
            required.insert(provider_key(manifest.volume().provider()));
        }
        for capture in &seed.resources {
            required.insert(provider_key(capture.source.provider()));
            required.insert(provider_key(capture.revision.provider()));
        }
        if let Some(boundary) = &seed.boundary {
            required.insert(provider_key(&boundary.provider));
        }
        for key in required {
            let verifier = self.providers.get(&key).ok_or_else(|| {
                Error::Unsupported("fork resource has no exact provider verifier".into())
            })?;
            verifier.verify(seed).await?;
            if let Some(boundary) = &seed.boundary
                && provider_key(&boundary.provider) == key
            {
                verifier.verify_boundary(boundary).await?;
            }
        }
        self.verify_references(seed).await
    }

    /// Verifies direct references and complete attachment manifests after
    /// provider-wide resource capture has been attested.
    async fn verify_references(&self, seed: &ForkSeed) -> Result<()> {
        let mut verified_files = BTreeSet::new();
        let mut reference_bytes = BTreeMap::new();
        let (direct_manifest_reads, member_grants) = collect_manifest_grants(seed)?;
        let mut volume_readers = ForkVolumeReaders::new(seed)?;
        self.verify_reference_files(seed, &mut verified_files, &mut reference_bytes)
            .await?;
        self.verify_attachment_manifests(
            seed,
            &mut verified_files,
            &mut reference_bytes,
            &mut volume_readers,
            &direct_manifest_reads,
            &member_grants,
        )
        .await?;
        Ok(())
    }

    async fn verify_reference_files(
        &self,
        seed: &ForkSeed,
        verified_files: &mut BTreeSet<String>,
        reference_bytes: &mut BTreeMap<String, u64>,
    ) -> Result<()> {
        for file in seed
            .inherited_context
            .iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
        {
            if !verified_files.insert(file.read_capability()?) {
                continue;
            }
            let verifier = self
                .providers
                .get(&provider_key(file.volume().provider()))
                .ok_or_else(|| Error::Unsupported("fork file provider is not bound".into()))?;
            reserve_reference_bytes(reference_bytes, verifier.as_ref(), file)?;
            verifier.verify_file(file).await?;
        }
        Ok(())
    }

    async fn verify_attachment_manifests(
        &self,
        seed: &ForkSeed,
        verified_files: &mut BTreeSet<String>,
        reference_bytes: &mut BTreeMap<String, u64>,
        volume_readers: &mut ForkVolumeReaders,
        direct_manifest_reads: &DirectManifestReads,
        member_grants: &ManifestMemberGrants,
    ) -> Result<()> {
        let mut manifest_members = 0_usize;
        for manifest in &seed.attachment_manifests {
            let manifest_capability = manifest.read_capability()?;
            let verifier = self
                .providers
                .get(&provider_key(manifest.volume().provider()))
                .ok_or_else(|| Error::Unsupported("fork manifest provider is not bound".into()))?;
            if verified_files.insert(manifest_capability.clone()) {
                reserve_reference_bytes(reference_bytes, verifier.as_ref(), manifest)?;
                verifier.verify_file(manifest).await?;
            }
            let bytes = verifier.read_manifest(manifest).await?;
            let items = decode_complete_attachment_manifest(manifest, &bytes)?;
            manifest_members = manifest_members
                .checked_add(items.len())
                .ok_or_else(|| Error::Invalid("fork manifest membership count overflow".into()))?;
            if manifest_members > MAX_FORK_REFERENCES {
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
            )?;
            for item in items {
                let item_capability = item.file.read_capability()?;
                if verified_files.insert(item_capability) {
                    let member_verifier = self
                        .providers
                        .get(&provider_key(item.file.volume().provider()))
                        .ok_or_else(|| {
                            Error::Unsupported("fork member provider is not bound".into())
                        })?;
                    reserve_reference_bytes(reference_bytes, member_verifier.as_ref(), &item.file)?;
                    member_verifier.verify_file(&item.file).await?;
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
    ) -> Result<()> {
        let manifest_capability = manifest.read_capability()?;
        let members = items
            .iter()
            .map(|item| item.file.read_capability())
            .collect::<Result<BTreeSet<_>>>()?;
        let grants = member_grants.get(&manifest_capability);
        if grants.is_some_and(|grants| grants.keys().any(|file| !members.contains(file))) {
            return Err(Error::Invalid(
                "fork reference is not in its published manifest".into(),
            ));
        }
        let implicit_readers = volume_readers.implicit_readers(manifest.volume())?.clone();
        let mut manifest_readers = BTreeSet::new();
        if let Some(grants) = grants {
            for readers in grants.values() {
                manifest_readers.extend(readers);
            }
        }
        for reader in manifest_readers {
            if !implicit_readers.contains(&reader)
                && !direct_manifest_reads.contains(&(manifest_capability.clone(), reader))
            {
                return Err(Error::Invalid(
                    "fork omits a manifest attachment read grant".into(),
                ));
            }
        }
        Ok(())
    }
}

fn collect_manifest_grants(seed: &ForkSeed) -> Result<(DirectManifestReads, ManifestMemberGrants)> {
    let mut direct_manifest_reads = BTreeSet::new();
    let mut member_grants = ManifestMemberGrants::new();
    for grant in &seed.reference_grants {
        if let Some(manifest) = &grant.attachment_manifest {
            let manifest_capability = manifest.read_capability()?;
            let file_capability = grant.file.read_capability()?;
            if manifest_capability == file_capability {
                direct_manifest_reads.insert((file_capability, grant.reader));
                continue;
            }
            member_grants
                .entry(manifest_capability)
                .or_default()
                .entry(file_capability)
                .or_default()
                .insert(grant.reader);
        } else {
            direct_manifest_reads.insert((grant.file.read_capability()?, grant.reader));
        }
    }
    Ok((direct_manifest_reads, member_grants))
}

/// Precomputed reader authority for a fork. Valid project/shared volume grants
/// are paid once per distinct volume, not once per attachment per agent.
struct ForkVolumeReaders {
    all: BTreeSet<AgentId>,
    selected_projects: BTreeSet<String>,
    shared: BTreeMap<String, BTreeSet<AgentId>>,
    implicit: BTreeMap<String, BTreeSet<AgentId>>,
}

impl ForkVolumeReaders {
    fn new(seed: &ForkSeed) -> Result<Self> {
        let all = std::iter::once(seed.child_agent)
            .chain(seed.attached_agents.iter().copied())
            .collect();
        let mut selected_projects = BTreeSet::new();
        for resource in &seed.resources {
            if let ResourceRevision::Project { volume, .. } = &resource.revision {
                selected_projects.insert(volume.capability(VolumeOperation::Read)?);
            }
        }
        let mut shared = BTreeMap::<String, BTreeSet<AgentId>>::new();
        for grant in &seed.shared_grants {
            if grant.operations.contains(&VolumeOperation::Read) {
                shared
                    .entry(grant.volume.capability(VolumeOperation::Read)?)
                    .or_default()
                    .insert(grant.child_agent);
            }
        }
        Ok(Self {
            all,
            selected_projects,
            shared,
            implicit: BTreeMap::new(),
        })
    }

    /// Readers that can resolve a file through a whole-volume grant rather
    /// than through a direct exact-file reference. Private volumes are only
    /// implicitly readable by their owner; attached readers still need the
    /// exact manifest/member grants selected by the parent.
    fn implicit_readers(&mut self, volume: &VolumeRef) -> Result<&BTreeSet<AgentId>> {
        let key = volume.capability(VolumeOperation::Read)?;
        let mut implicit_readers = BTreeSet::new();
        match volume.class() {
            VolumeClass::AgentPrivate => {
                if let VolumeOwner::Agent(owner) = volume.owner()
                    && self.all.contains(owner)
                {
                    implicit_readers.insert(*owner);
                }
            }
            VolumeClass::Project if self.selected_projects.contains(&key) => {
                implicit_readers = self.all.clone();
            }
            VolumeClass::SessionShared => {
                if let Some(granted) = self.shared.get(&key) {
                    implicit_readers.extend(granted.iter().copied());
                }
            }
            VolumeClass::Project => {}
        }
        Ok(self.implicit.entry(key).or_insert(implicit_readers))
    }
}

fn reserve_reference_bytes(
    totals: &mut BTreeMap<String, u64>,
    verifier: &dyn ForkSeedVerifier,
    file: &FileRef,
) -> Result<()> {
    let total = totals.entry(provider_key(verifier.provider())).or_default();
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

fn provider_key(provider: &ProviderRef) -> String {
    format!(
        "{}\0{}\0{}",
        provider.namespace(),
        provider.family(),
        provider.version()
    )
}

/// Fork barrier for immutable file-only providers such as Objects. Mutable
/// resources still require their own provider-specific revision verifier.
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
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn verify<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            seed.validate()?;
            if seed.child_private_volume.provider() == &self.provider
                || seed.resources.iter().any(|capture| {
                    capture.source.provider() == &self.provider
                        || capture.revision.provider() == &self.provider
                })
            {
                return Err(Error::Unsupported(
                    "content-only provider cannot attest mutable fork resources".into(),
                ));
            }
            let mut checked = BTreeSet::new();
            for file in seed
                .inherited_context
                .iter()
                .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                .chain(seed.attachment_manifests.iter())
            {
                if file.volume().provider() == &self.provider
                    && checked.insert(file.read_capability()?)
                {
                    self.content.verify(file).await?;
                }
            }
            Ok(())
        })
    }

    fn read_manifest<'a>(
        &'a self,
        manifest: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            if manifest.volume().provider() != &self.provider {
                return Err(Error::Unauthorized(
                    "fork manifest belongs to another provider".into(),
                ));
            }
            self.content.read(manifest).await
        })
    }

    fn verify_file<'a>(
        &'a self,
        file: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if file.volume().provider() != &self.provider {
                return Err(Error::Unauthorized(
                    "fork file belongs to another provider".into(),
                ));
            }
            self.content.verify(file).await
        })
    }
}

/// Stream history proof scoped to the parent aggregate already opened by the
/// admission path. `ForkSeed::validate` checks its exact path and revision.
pub struct StreamHistoryForkVerifier {
    provider: ProviderRef,
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
        Ok(Self { provider })
    }
}

impl ForkSeedVerifier for StreamHistoryForkVerifier {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn verify<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            seed.validate()?;
            for capture in &seed.resources {
                for revision in [&capture.source, &capture.revision] {
                    if revision.provider() == &self.provider
                        && !matches!(revision, ResourceRevision::History(_))
                    {
                        return Err(Error::Unsupported(
                            "Stream history verifier cannot prove this resource".into(),
                        ));
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
pub enum ResourceRevision {
    /// Immutable Stream history prefix.
    History(StreamRef),
    /// Immutable project workspace generation.
    Project {
        /// Project volume selected for the child.
        volume: VolumeRef,
        /// Exact immutable generation in that volume.
        generation: GenerationRef,
    },
    /// Selected model context revision.
    Context(ContextRef),
    /// Qualified process checkpoint.
    Process(CheckpointRef),
    /// Immutable retained artifact.
    Artifact(ArtifactRef),
    /// Session-shared volume reference.
    SharedVolume(VolumeRef),
    /// Namespaced extension state with pinned implementation version.
    Extension {
        /// Namespaced state identity.
        name: String,
        /// Immutable extension implementation version.
        version: u32,
        /// Digest of the exact extension implementation selected by the parent.
        implementation_digest: [u8; 32],
        /// Provider-owned retained state reference.
        reference: ResourceRef,
    },
}

impl ResourceRevision {
    /// Exact provider contract owning this revision.
    #[must_use]
    pub fn provider(&self) -> &ProviderRef {
        match self {
            Self::History(value) => value.as_resource().provider(),
            Self::Project { volume, .. } | Self::SharedVolume(volume) => volume.provider(),
            Self::Context(value) => value.as_resource().provider(),
            Self::Process(value) => value.as_resource().provider(),
            Self::Artifact(value) => value.as_resource().provider(),
            Self::Extension { reference, .. } => reference.provider(),
        }
    }

    /// Checks provider families and the semantic resource class.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::History(value) if value.as_resource().provider().family() == "stream" => {
                value.validate()
            }
            Self::Project { volume, generation }
                if volume.class() == VolumeClass::Project
                    && generation.as_resource().provider() == volume.provider()
                    && volume.provider().family() == "filesystem" =>
            {
                volume.validate()?;
                generation.validate()
            }
            Self::Context(value) => value.validate(),
            Self::Process(value) if value.as_resource().provider().family() == "machines" => {
                value.validate()
            }
            Self::Artifact(value) => value.validate(),
            Self::SharedVolume(value) if value.class() == VolumeClass::SessionShared => {
                value.validate()
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
                reference.validate()
            }
            _ => Err(Error::Invalid(
                "fork resource has an invalid kind or provider".into(),
            )),
        }
    }
}

/// Required or optional exact resource selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSelection {
    /// Required selections must capture successfully before publication.
    pub required: bool,
    /// Exact source revision.
    pub revision: ResourceRevision,
}

/// Provider evidence of a consistent boundary across selected resources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttestedBoundary {
    /// Provider qualified to attest this boundary.
    pub provider: ProviderRef,
    /// Opaque bounded proof, interpreted only by that provider.
    pub evidence: Vec<u8>,
}

impl AttestedBoundary {
    /// Validates an attestation envelope without claiming to verify its proof.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        if self.evidence.is_empty() || self.evidence.len() > 4_096 {
            return Err(Error::Invalid("fork boundary evidence is invalid".into()));
        }
        Ok(())
    }
}

/// Idempotent request to capture selected resources for a new child.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkRequest {
    /// Stable identity used for preparation and publication reconciliation.
    pub operation_id: OperationId,
    /// Publishing parent aggregate.
    pub parent: Authority,
    /// Exact parent revision before publication.
    pub parent_revision: u64,
    /// Fresh child aggregate.
    pub child: Authority,
    /// Fresh child agent identity.
    pub child_agent: AgentId,
    /// Additional agents allowed to attach to the child environment as readers.
    pub attached_agents: Vec<AgentId>,
    /// Exact child allocation and bounded inherited prefix chosen before preparation.
    pub preparation: ForkPreparation,
    /// Ordered required and optional resource selections.
    pub selections: Vec<ForkSelection>,
    /// Present only when one provider attests a common capture boundary.
    pub boundary: Option<AttestedBoundary>,
}

/// Immutable provider allocation and context bounds for one fork operation.
/// Retrying the request must never choose different child volumes or a wider
/// inherited prefix under the same operation identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkPreparation {
    /// Fresh project workspace derived from the selected project generation.
    pub child_project_volume: VolumeRef,
    /// Fresh private workspace owned by the child agent.
    pub child_private_volume: VolumeRef,
    /// Inclusive final parent conversation sequence; zero selects none.
    pub inherited_through_sequence: u64,
    /// Deployment limit, no greater than the protocol ceiling.
    pub maximum_inherited_messages: u64,
    /// Maximum bytes in the child-owned inherited-context file.
    pub maximum_inherited_bytes: u64,
    /// Maximum retained references in the inherited prefix.
    pub maximum_inherited_references: u32,
}

/// Provider-neutral owner boundary for an idempotent multi-resource fork
/// preparation. Each provider reports its own capture result; this trait does
/// not imply a cross-provider atomic snapshot.
pub trait ForkPreparer: Send + Sync {
    /// Exact immutable parent projection this preparer was constructed from.
    /// A later parent revision requires a newly bound preparer.
    fn parent_snapshot(&self) -> (&Authority, u64);

    /// Captures or returns the existing report for the exact request identity.
    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>>;

    /// Observes an uncertain preparation for the exact original request.
    /// The operation ID alone cannot authorize a report with different
    /// resources, child identities, or capture parameters.
    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>>;
}

/// Provider-owned capture of one selected revision during parent-controlled
/// fork preparation. Implementations must reconcile retries by the request's
/// stable operation ID and never substitute a different source revision.
pub trait ForkCaptureProvider: Send + Sync {
    /// Exact provider identity whose resource revisions this adapter owns.
    fn provider(&self) -> &ProviderRef;

    /// Captures one immutable child-visible revision or reports an explicit
    /// unavailable state. An uncertain provider effect returns an error so
    /// the top-level preparation is retried, not journaled as complete.
    fn capture<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>>;

    /// Observes a capture whose start was durably recorded but whose result
    /// may have been lost. `None` means unresolved, not permission to repeat
    /// the side effect. Read-only providers may safely observe by re-reading.
    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>>;
}

impl ForkRequest {
    /// Checks identities, resource selections and optional attestation metadata.
    pub fn validate(&self) -> Result<()> {
        if self.attached_agents.len() > MAX_FORK_AGENTS
            || self.selections.len() > MAX_FORK_RESOURCES
        {
            return Err(Error::Invalid(
                "fork request exceeds protocol limits".into(),
            ));
        }
        if self.parent == self.child {
            return Err(Error::Invalid("fork child cannot equal its parent".into()));
        }
        if self.parent.kind != crate::core::AggregateKind::Conversation
            || self.child.kind != crate::core::AggregateKind::Conversation
        {
            return Err(Error::Invalid(
                "fork must link parent and child conversations".into(),
            ));
        }
        self.parent.stream_path()?;
        self.child.stream_path()?;
        self.preparation.child_project_volume.validate()?;
        self.preparation.child_private_volume.validate()?;
        if self.preparation.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.preparation.child_private_volume.owner()
                != &VolumeOwner::Agent(self.child_agent)
            || self.preparation.child_project_volume.class() != VolumeClass::Project
            || self.preparation.child_private_volume.provider()
                != self.preparation.child_project_volume.provider()
            || self.preparation.child_private_volume.provider().family() != "filesystem"
            || self.preparation.maximum_inherited_messages == 0
            || self.preparation.maximum_inherited_messages > MAX_FORK_INHERITED_MESSAGES
            || self.preparation.inherited_through_sequence
                > self.preparation.maximum_inherited_messages
            || self.preparation.maximum_inherited_bytes == 0
            || self.preparation.maximum_inherited_bytes > MAX_FORK_INHERITED_BYTES
            || self.preparation.maximum_inherited_references == 0
            || self.preparation.maximum_inherited_references as usize > MAX_FORK_REFERENCES
        {
            return Err(Error::Invalid("fork preparation is invalid".into()));
        }
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(Error::Invalid("fork attached agents are not unique".into()));
        }
        let mut selected = BTreeSet::new();
        let mut histories = 0_usize;
        let mut projects = 0_usize;
        let mut contexts = 0_usize;
        let mut processes = 0_usize;
        for selection in &self.selections {
            selection.revision.validate()?;
            let identity = crate::contract::canonical_json_bytes(&selection.revision)?;
            if !selected.insert(identity) {
                return Err(Error::Invalid(
                    "fork resource is selected more than once".into(),
                ));
            }
            match &selection.revision {
                ResourceRevision::History(reference) => {
                    histories += 1;
                    if !selection.required
                        || reference.as_resource().key() != self.parent.stream_path()?.as_bytes()
                        || reference
                            .as_resource()
                            .version()
                            .and_then(|version| version.parse::<u64>().ok())
                            != Some(self.parent_revision)
                    {
                        return Err(Error::Invalid(
                            "fork history is not the required exact parent prefix".into(),
                        ));
                    }
                }
                ResourceRevision::Project { volume, .. } => {
                    projects += 1;
                    if !selection.required
                        || volume == &self.preparation.child_project_volume
                        || volume.provider() != self.preparation.child_project_volume.provider()
                        || volume.owner() != self.preparation.child_project_volume.owner()
                    {
                        return Err(Error::Invalid(
                            "fork project selection does not match child allocation".into(),
                        ));
                    }
                }
                ResourceRevision::Context(_) => contexts += 1,
                ResourceRevision::Process(_) => processes += 1,
                _ => {}
            }
        }
        if histories != 1 || projects != 1 || contexts > 1 || processes > 1 {
            return Err(Error::Invalid(
                "fork requires one history and project selection".into(),
            ));
        }
        if let Some(boundary) = &self.boundary {
            boundary.validate()?;
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

/// An optional resource that was not captured, with its exact reconciliation
/// identity retained in the published child seed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkOmission {
    /// Exact optional selection requested by the caller.
    pub selection: ForkSelection,
    /// Unsupported, in-flight, or indeterminate provider outcome.
    pub outcome: Capture,
}

/// Explicit child-bound operations for one selected session-shared volume.
/// The child issuer still must sign a scope carrying these capabilities;
/// possession of this record or the volume ref alone grants no byte access.
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
/// The owning provider must still resolve and authorize the bytes; this is not
/// a transferable write grant or a capability embedded in the reference.
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
    /// Validates the standalone owner-mediated reference envelope. Seed
    /// admission additionally checks the attached reader and published list.
    pub fn validate(&self) -> Result<()> {
        self.file.validate()?;
        if let Some(manifest) = &self.attachment_manifest {
            manifest.validate()?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
            {
                return Err(Error::Invalid(
                    "reference grant manifest has an invalid media type".into(),
                ));
            }
        }
        Ok(())
    }

    /// Exact, per-file capability the owner may issue to this reader.
    pub fn capability(&self) -> Result<String> {
        self.validate()?;
        self.file.read_capability()
    }
}

impl SharedGrant {
    /// Validates the grant envelope; the seed checks selection and child binding.
    pub fn validate(&self) -> Result<()> {
        self.volume.validate()?;
        if self.volume.class() != VolumeClass::SessionShared || self.operations.is_empty() {
            return Err(Error::Invalid("invalid fork shared-volume grant".into()));
        }
        Ok(())
    }
}

impl ForkOmission {
    /// Required captures and successful captures cannot become omissions.
    pub fn validate(&self) -> Result<()> {
        self.selection.revision.validate()?;
        if self.selection.required || matches!(self.outcome, Capture::Captured(_)) {
            return Err(Error::Invalid(
                "fork omission must be an uncaptured optional selection".into(),
            ));
        }
        Ok(())
    }
}

impl CapturedResource {
    /// Only project capture may create a new revision and volume identity.
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.revision.validate()?;
        match (&self.source, &self.revision) {
            (
                ResourceRevision::Project { volume: source, .. },
                ResourceRevision::Project { volume: child, .. },
            ) if source.provider() == child.provider()
                && source.owner() == child.owner()
                && source != child =>
            {
                Ok(())
            }
            (ResourceRevision::Project { .. }, _) | (_, ResourceRevision::Project { .. }) => Err(
                Error::Invalid("project capture has an invalid child volume".into()),
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
            _ => Err(Error::Invalid(
                "capture changed an immutable selected revision".into(),
            )),
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
    /// Bounded parent context materialized as child-owned files.
    pub inherited_context: Vec<FileRef>,
    /// Inclusive conversation message sequence inherited by the child; zero selects none.
    pub inherited_through_sequence: u64,
    /// Explicit child-bound access to selected session-shared volumes.
    pub shared_grants: Vec<SharedGrant>,
    /// Exact parent-owned references retained for child and attached readers.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Published manifests whose complete private members must remain readable.
    pub attachment_manifests: Vec<FileRef>,
}

impl ForkReport {
    /// Validates a prepared report even when a required capture is still
    /// unavailable; only `into_seed` demands all required captures succeed.
    pub fn validate(&self) -> Result<()> {
        self.request.validate()?;
        if self.inherited_through_sequence > MAX_FORK_INHERITED_MESSAGES
            || self.inherited_context.len() > MAX_FORK_RESOURCES
            || self.shared_grants.len() > MAX_FORK_REFERENCES
            || self.reference_grants.len() > MAX_FORK_REFERENCES
            || self.attachment_manifests.len() > MAX_FORK_RESOURCES
        {
            return Err(Error::Invalid("fork report exceeds protocol limits".into()));
        }
        if self.inherited_through_sequence != self.request.preparation.inherited_through_sequence
            || self.child_private_volume != self.request.preparation.child_private_volume
            || self.reference_grants.len()
                > self.request.preparation.maximum_inherited_references as usize
        {
            return Err(Error::Invalid(
                "fork report changed the requested child allocation or prefix".into(),
            ));
        }
        let inherited_bytes = self
            .inherited_context
            .iter()
            .try_fold(0_u64, |total, file| {
                total
                    .checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| {
                        Error::Invalid("fork inherited context byte count overflow".into())
                    })
            })?;
        if inherited_bytes > self.request.preparation.maximum_inherited_bytes {
            return Err(Error::Invalid(
                "fork inherited context exceeds requested byte limit".into(),
            ));
        }
        if self.captures.len() != self.request.selections.len() {
            return Err(Error::Invalid(
                "fork capture count does not match selections".into(),
            ));
        }
        for (selection, capture) in self.request.selections.iter().zip(&self.captures) {
            match capture {
                Capture::Captured(resource) if resource.source == selection.revision => {
                    resource.validate()?;
                    if let ResourceRevision::Project { volume, .. } = &resource.revision
                        && volume != &self.request.preparation.child_project_volume
                    {
                        return Err(Error::Invalid(
                            "fork report changed the requested project volume".into(),
                        ));
                    }
                }
                Capture::Captured(_) => {
                    return Err(Error::Invalid(
                        "fork capture source does not match selection".into(),
                    ));
                }
                Capture::Unsupported(reason) if reason.is_empty() || reason.len() > 4_096 => {
                    return Err(Error::Invalid("fork capture reason is invalid".into()));
                }
                _ => {}
            }
        }
        self.child_private_volume.validate()?;
        self.child_private_generation.validate()?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.request.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(Error::Invalid(
                "fork report private volume is not child owned".into(),
            ));
        }
        for file in &self.inherited_context {
            file.validate()?;
        }
        for grant in &self.shared_grants {
            grant.validate()?;
        }
        for grant in &self.reference_grants {
            grant.validate()?;
        }
        for manifest in &self.attachment_manifests {
            manifest.validate()?;
        }
        Ok(())
    }

    /// Produces a publishable seed only when every required capture succeeded.
    pub fn into_seed(self) -> Result<ForkSeed> {
        self.validate()?;
        let mut resources = Vec::new();
        let mut omissions = Vec::new();
        for (selection, capture) in self.request.selections.iter().zip(self.captures) {
            match capture {
                Capture::Captured(resource) if resource.source == selection.revision => {
                    resource.validate()?;
                    resources.push(resource);
                }
                Capture::Captured(_) => {
                    return Err(Error::Invalid(
                        "fork capture source does not match selection".into(),
                    ));
                }
                Capture::Unsupported(_) | Capture::InFlight(_) | Capture::Indeterminate(_)
                    if selection.required =>
                {
                    return Err(Error::Conflict(
                        "required fork capture is unavailable".into(),
                    ));
                }
                outcome => omissions.push(ForkOmission {
                    selection: selection.clone(),
                    outcome,
                }),
            }
        }
        let seed = ForkSeed {
            operation_id: self.request.operation_id,
            parent: self.request.parent,
            parent_revision: self.request.parent_revision,
            child: self.request.child,
            child_agent: self.request.child_agent,
            attached_agents: self.request.attached_agents,
            resources,
            omissions,
            child_private_volume: self.child_private_volume,
            child_private_generation: self.child_private_generation,
            inherited_context: self.inherited_context,
            inherited_through_sequence: self.inherited_through_sequence,
            shared_grants: self.shared_grants,
            reference_grants: self.reference_grants,
            attachment_manifests: self.attachment_manifests,
            boundary: self.request.boundary,
        };
        seed.validate()?;
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
    /// Child-owned inherited conversation files.
    pub inherited_context: Vec<FileRef>,
    /// Inclusive bounded parent conversation prefix; zero selects no messages.
    pub inherited_through_sequence: u64,
    /// Explicit child-bound access to selected session-shared volumes.
    pub shared_grants: Vec<SharedGrant>,
    /// Parent-approved read-only access to pinned references.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Exact published attachment lists. Each reader needs owner, selected
    /// volume, or direct exact-file authority for the manifest itself.
    pub attachment_manifests: Vec<FileRef>,
    /// Optional provider-attested common boundary.
    pub boundary: Option<AttestedBoundary>,
}

impl ForkSeed {
    /// Prevents private-volume inheritance, duplicate singletons, and malformed refs.
    #[allow(
        clippy::too_many_lines,
        reason = "validates the complete fork seed contract"
    )]
    pub fn validate(&self) -> Result<()> {
        if self.attached_agents.len() > MAX_FORK_AGENTS
            || self.resources.len().saturating_add(self.omissions.len()) > MAX_FORK_RESOURCES
            || self.reference_grants.len() > MAX_FORK_REFERENCES
            || self.shared_grants.len() > MAX_FORK_REFERENCES
            || self.inherited_context.len() > MAX_FORK_RESOURCES
            || self.attachment_manifests.len() > MAX_FORK_RESOURCES
            || self.inherited_through_sequence > MAX_FORK_INHERITED_MESSAGES
        {
            return Err(Error::Invalid("fork seed exceeds protocol limits".into()));
        }
        if self.parent == self.child {
            return Err(Error::Invalid("fork child cannot equal its parent".into()));
        }
        if self.parent.kind != crate::core::AggregateKind::Conversation
            || self.child.kind != crate::core::AggregateKind::Conversation
        {
            return Err(Error::Invalid(
                "fork must link parent and child conversations".into(),
            ));
        }
        self.parent.stream_path()?;
        self.child.stream_path()?;
        let attached: BTreeSet<_> = self.attached_agents.iter().copied().collect();
        if attached.len() != self.attached_agents.len() || attached.contains(&self.child_agent) {
            return Err(Error::Invalid("fork attached agents are not unique".into()));
        }
        self.child_private_volume.validate()?;
        self.child_private_generation.validate()?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(Error::Invalid(
                "fork private volume is not child owned".into(),
            ));
        }
        let mut history = 0;
        let mut project = 0;
        let mut context = 0;
        let mut process = 0;
        let mut unique = BTreeSet::new();
        let mut selected = BTreeSet::new();
        let mut selected_shared = BTreeSet::new();
        for resource in &self.resources {
            resource.validate()?;
            let source = crate::contract::canonical_json_bytes(&resource.source)?;
            if !selected.insert(source) {
                return Err(Error::Invalid("fork selection appears twice".into()));
            }
            match &resource.revision {
                ResourceRevision::History(reference) => {
                    history += 1;
                    if reference.as_resource().key() != self.parent.stream_path()?.as_bytes()
                        || reference
                            .as_resource()
                            .version()
                            .and_then(|version| version.parse::<u64>().ok())
                            != Some(self.parent_revision)
                    {
                        return Err(Error::Invalid(
                            "fork history is not the exact parent prefix".into(),
                        ));
                    }
                }
                ResourceRevision::Project { .. } => project += 1,
                ResourceRevision::Context(_) => context += 1,
                ResourceRevision::Process(_) => process += 1,
                ResourceRevision::SharedVolume(volume) => {
                    selected_shared.insert(crate::contract::canonical_json_bytes(volume)?);
                }
                _ => {}
            }
            let encoded = crate::contract::canonical_json_bytes(&resource.revision)?;
            if !unique.insert(encoded) {
                return Err(Error::Invalid("fork resource appears twice".into()));
            }
        }
        for omission in &self.omissions {
            omission.validate()?;
            let encoded = crate::contract::canonical_json_bytes(&omission.selection.revision)?;
            if !selected.insert(encoded) {
                return Err(Error::Invalid("fork selection appears twice".into()));
            }
        }
        self.validate_shared_grants(&selected_shared, &attached)?;
        self.validate_reference_grants(&attached)?;
        let mut manifest_ids = BTreeSet::new();
        let mut manifest_bytes = 0_u64;
        for manifest in &self.attachment_manifests {
            manifest.validate()?;
            manifest_bytes = manifest_bytes
                .checked_add(manifest.descriptor().byte_length())
                .ok_or_else(|| Error::Invalid("fork attachment manifest bytes overflow".into()))?;
            if manifest_bytes > MAX_FORK_ATTACHMENT_MANIFEST_BYTES {
                return Err(Error::Invalid(
                    "fork attachment manifests exceed aggregate safety limit".into(),
                ));
            }
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
                || !manifest_ids.insert(manifest.read_capability()?)
            {
                return Err(Error::Invalid(
                    "fork attachment manifest is invalid or duplicated".into(),
                ));
            }
        }
        let mut direct_manifest_reads = BTreeSet::new();
        let mut member_grants = BTreeMap::<String, BTreeMap<String, BTreeSet<AgentId>>>::new();
        for grant in &self.reference_grants {
            if let Some(manifest) = &grant.attachment_manifest
                && !manifest_ids.contains(&manifest.read_capability()?)
            {
                return Err(Error::Invalid(
                    "reference grant names an unselected attachment manifest".into(),
                ));
            }
            let file_capability = grant.file.read_capability()?;
            if let Some(manifest) = &grant.attachment_manifest {
                let manifest_capability = manifest.read_capability()?;
                if manifest_capability == file_capability {
                    direct_manifest_reads.insert((file_capability, grant.reader));
                } else {
                    member_grants
                        .entry(manifest_capability)
                        .or_default()
                        .entry(file_capability)
                        .or_default()
                        .insert(grant.reader);
                }
            } else {
                direct_manifest_reads.insert((file_capability, grant.reader));
            }
        }
        let mut volume_readers = ForkVolumeReaders::new(self)?;
        for manifest in &self.attachment_manifests {
            let capability = manifest.read_capability()?;
            let implicit_readers = volume_readers.implicit_readers(manifest.volume())?.clone();
            let mut manifest_readers = BTreeSet::new();
            if let Some(grants) = member_grants.get(&capability) {
                for readers in grants.values() {
                    manifest_readers.extend(readers);
                }
            }
            for reader in manifest_readers {
                if !implicit_readers.contains(&reader)
                    && !direct_manifest_reads.contains(&(capability.clone(), reader))
                {
                    return Err(Error::Invalid(
                        "fork omits a manifest file read grant".into(),
                    ));
                }
            }
        }
        if history != 1 || project != 1 || context > 1 || process > 1 {
            return Err(Error::Invalid(
                "fork needs one history and project revision".into(),
            ));
        }
        self.validate_inherited_context()?;
        if let Some(boundary) = &self.boundary {
            boundary.validate()?;
        }
        Ok(())
    }

    fn validate_shared_grants(
        &self,
        selected_shared: &BTreeSet<Vec<u8>>,
        attached: &BTreeSet<AgentId>,
    ) -> Result<()> {
        let mut granted_shared = BTreeSet::new();
        let mut granted_pairs = BTreeSet::new();
        for grant in &self.shared_grants {
            grant.validate()?;
            if grant.child_agent != self.child_agent && !attached.contains(&grant.child_agent) {
                return Err(Error::Invalid(
                    "shared grant belongs to an unattached agent".into(),
                ));
            }
            let volume = crate::contract::canonical_json_bytes(&grant.volume)?;
            if !selected_shared.contains(&volume) {
                return Err(Error::Invalid("shared grant was not selected".into()));
            }
            if !granted_pairs.insert((volume.clone(), grant.child_agent)) {
                return Err(Error::Invalid(
                    "shared volume is granted twice to one agent".into(),
                ));
            }
            if grant.child_agent == self.child_agent {
                granted_shared.insert(volume);
            }
        }
        if selected_shared != &granted_shared {
            return Err(Error::Invalid(
                "shared-volume selections need exact child grants".into(),
            ));
        }
        Ok(())
    }

    fn validate_reference_grants(&self, attached: &BTreeSet<AgentId>) -> Result<()> {
        let mut unique = BTreeSet::new();
        for grant in &self.reference_grants {
            grant.validate()?;
            if grant.reader != self.child_agent && !attached.contains(&grant.reader) {
                return Err(Error::Invalid(
                    "reference reader is not attached to fork".into(),
                ));
            }
            if grant.file.volume().class() == VolumeClass::AgentPrivate
                && grant.file.volume().owner() == &VolumeOwner::Agent(grant.reader)
            {
                return Err(Error::Invalid(
                    "owner needs no inherited reference grant".into(),
                ));
            }
            let identity = (
                grant.capability()?,
                grant.reader,
                grant
                    .attachment_manifest
                    .as_ref()
                    .map(FileRef::read_capability)
                    .transpose()?,
            );
            if !unique.insert(identity) {
                return Err(Error::Invalid("reference grant appears twice".into()));
            }
        }
        Ok(())
    }

    fn validate_inherited_context(&self) -> Result<()> {
        if (self.inherited_through_sequence == 0 && !self.inherited_context.is_empty())
            || (self.inherited_through_sequence > 0
                && (self.inherited_context.len() != 1
                    || self.inherited_context.first().is_none_or(|file| {
                        file.path() != ".system/inherited-conversation/prefix.json"
                            || file.descriptor().media_type()
                                != "application/vnd.acyclic.harness.inherited-conversation+json"
                    })))
        {
            return Err(Error::Invalid(
                "inherited context does not match its selected prefix".into(),
            ));
        }
        let mut inherited_paths = BTreeSet::new();
        for file in &self.inherited_context {
            file.validate()?;
            if file.volume() != &self.child_private_volume
                || !file.path().starts_with(".system/inherited-conversation/")
            {
                return Err(Error::Invalid(
                    "inherited context is not child owned".into(),
                ));
            }
            if !inherited_paths.insert(file.path()) {
                return Err(Error::Invalid(
                    "inherited context path appears twice".into(),
                ));
            }
        }
        Ok(())
    }

    /// Capabilities the child issuer may sign after admitting this exact seed.
    /// This does not itself authorize a read or write.
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

    /// Exact read capabilities issuable to one attached agent. Possession of a
    /// `FileRef` alone never authorizes resolution.
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
        // Inherited files are pinned child-owned content. Both the owner and
        // attached readers receive exact-file read authority; no write grant
        // is implied by these capabilities.
        for file in &self.inherited_context {
            values.push(file.read_capability()?);
        }
        Ok(Capabilities::new(values))
    }

    /// Read-only environment capabilities for one attached agent. Project
    /// writes remain an independent grant controlled by the parent runtime.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{conversation::FileDescriptor, core::AggregateKind};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn inherited_prefix_bounds_readers_before_copying_history() -> Result<()> {
        let readers = (0..=MAX_FORK_AGENTS)
            .map(|index| {
                let mut bytes = [0_u8; 16];
                bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
                AgentId::from_bytes(bytes)
            })
            .collect::<Vec<_>>();
        assert!(
            InheritedConversationPrefix::select(
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "parent".into()
                },
                0,
                AgentId::from_bytes([255; 16]),
                0,
                &readers,
                &[],
            )
            .is_err()
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
        bytes: Vec<u8>,
    }

    impl ForkSeedVerifier for ManifestVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.provider
        }

        fn verify<'a>(
            &'a self,
            _seed: &'a ForkSeed,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(
            &'a self,
            file: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                if file.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                file.descriptor().verify(&self.bytes)
            })
        }

        fn read_manifest<'a>(
            &'a self,
            manifest: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
            Box::pin(async move {
                if manifest.volume().provider() != &self.provider {
                    return Err(Error::Unauthorized("wrong manifest provider".into()));
                }
                Ok(self.bytes.clone())
            })
        }
    }

    #[test]
    fn v2_resource_revision_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/resource-revision.json").trim();
        let revision: ResourceRevision =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        revision.validate()?;
        let encoded =
            serde_json::to_string(&revision).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn v2_fork_request_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/fork-request.json").trim();
        let request: ForkRequest =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        request.validate()?;
        let encoded =
            serde_json::to_string(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
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
        let mut readers = ForkVolumeReaders::new(&seed)?;
        assert_eq!(
            readers.implicit_readers(&child_project)?,
            &BTreeSet::from([seed.child_agent, attached])
        );
        assert_eq!(
            readers.implicit_readers(&seed.child_private_volume)?,
            &BTreeSet::from([seed.child_agent])
        );
        assert_eq!(readers.implicit_readers(&parent_project)?, &BTreeSet::new());
        Ok(())
    }

    #[test]
    fn v2_reference_grant_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/reference-grant.json").trim();
        let grant: ReferenceGrant =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        grant.file.validate()?;
        let encoded =
            serde_json::to_string(&grant).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    impl ForkSeedVerifier for AcceptingVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.0
        }

        fn verify<'a>(
            &'a self,
            _seed: &'a ForkSeed,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(
            &'a self,
            file: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
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

        fn verify<'a>(
            &'a self,
            _seed: &'a ForkSeed,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(
            &'a self,
            _file: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Err(Error::NotFound("pinned file".into())) })
        }
    }

    impl ForkSeedVerifier for CountingFileVerifier {
        fn provider(&self) -> &ProviderRef {
            &self.provider
        }

        fn verify<'a>(
            &'a self,
            _seed: &'a ForkSeed,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn verify_file<'a>(
            &'a self,
            _file: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    #[test]
    fn project_capture_requires_new_same_owner_volume() -> Result<()> {
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
            .is_err()
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
        // An attached reader may receive a direct manifest reference without
        // the child needing a duplicate direct grant annotation. The exact
        // manifest identity is still a direct read grant even when the
        // producer carries the manifest annotation on that grant.
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
        let verifier = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(CountingFileVerifier {
                provider: filesystem.clone(),
                count: verified_members.clone(),
            }),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(ManifestVerifier {
                provider: objects,
                bytes,
            }),
        ])?;
        futures::executor::block_on(verifier.verify(&attached_manifest_only))?;
        let missing_manifest = CompositeForkVerifier::new(vec![
            std::sync::Arc::new(AcceptingVerifier(filesystem.clone())),
            std::sync::Arc::new(AcceptingVerifier(ProviderRef::new("test", "stream", "2")?)),
            std::sync::Arc::new(MissingFileVerifier(manifest.volume().provider().clone())),
        ])?;
        assert!(matches!(
            futures::executor::block_on(missing_manifest.verify(&manifest_seed)),
            Err(Error::NotFound(_))
        ));
        let mut oversized = seed.clone();
        for index in 0_u8..2 {
            oversized.attachment_manifests.push(FileRef::new(
                manifest.volume().clone(),
                format!("lists/oversized-{index}.json"),
                format!("oversized-{index}"),
                FileDescriptor::new(
                    [index + 1; 32],
                    MAX_FORK_ATTACHMENT_MANIFEST_BYTES / 2 + 1,
                    "application/vnd.acyclic.harness.attachments+json",
                )?,
                format!("oversized-{index}.json"),
            )?);
        }
        assert!(matches!(oversized.validate(), Err(Error::Invalid(_))));
        assert!(matches!(
            futures::executor::block_on(verifier.verify(&oversized)),
            Err(Error::Invalid(_))
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
        let optional = ResourceRevision::SharedVolume(VolumeRef::new(
            filesystem,
            "shared",
            VolumeClass::SessionShared,
            VolumeOwner::Session("session".into()),
        )?);
        let report = ForkReport {
            request: ForkRequest {
                operation_id: seed.operation_id,
                parent: seed.parent.clone(),
                parent_revision: seed.parent_revision,
                child: seed.child.clone(),
                child_agent: seed.child_agent,
                attached_agents: Vec::new(),
                preparation: ForkPreparation {
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
        widened_request.preparation.maximum_inherited_messages = MAX_FORK_INHERITED_MESSAGES + 1;
        assert!(widened_request.validate().is_err());
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
