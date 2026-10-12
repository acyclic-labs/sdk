//! Selected provider revisions and fork preparation.
use super::*;
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
    /// Direct parent's pinned scratch. Empty paths select an empty initial view;
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
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
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
                generation.validate(work)?;
                if paths.len() as u64 > MAX_FORK_RESOURCES as u64 {
                    return Err(boundary_error(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "too many scratch paths",
                    ));
                }
                for path in paths {
                    let relative = path.strip_prefix('/').ok_or_else(|| {
                        boundary_error(
                            work,
                            crate::wire::ErrorCode::Invalid,
                            "scratch path must be absolute",
                        )
                    })?;
                    crate::conversation::validate_content_path(relative, work)?;
                    if relative.split('/').any(|segment| segment == ".system") {
                        return Err(boundary_error(
                            work,
                            crate::wire::ErrorCode::Unauthorized,
                            "scratch selection includes reserved runtime state",
                        ));
                    }
                }
                Ok(())
            }
            Self::History(value) if value.as_resource().provider().family() == "stream" => {
                value.validate(work)
            }
            Self::Project { volume, generation }
                if generation.as_resource().provider() == volume.provider()
                    && volume.provider().family() == "filesystem" =>
            {
                volume.validate(work)?;
                generation.validate(work)
            }
            Self::Context(value) => value.validate(work),
            Self::Process(value) if value.as_resource().provider().family() == "machines" => {
                value.validate(work)
            }
            Self::Artifact(value) => value.validate(work),
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
                reference.validate(work)
            }
            _ => Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork resource has an invalid kind or provider",
            )),
        }
    }

    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        CodecQuota::produce(work, |quota| {
            crate::core::copy_owned(self, quota, |source, work| {
                source.validate(work)?;
                crate::core::owned_bytes(source)
            })
        })
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
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.provider.validate(work)?;
        if self.evidence.is_empty() {
            return Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork boundary evidence is invalid",
            ));
        }
        Ok(())
    }
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
    /// Optional child-owned result namespace, distinct from scratch.
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
    pub(in crate::fork) fn validate(
        &self,
        child_agent: AgentId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(selection) = &self.summary {
            selection.validate_with_work(work)?;
            if self.inherited_through_sequence == 0 {
                return Err(boundary_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "summary fork requires a nonempty logical parent cut",
                ));
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
            return Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork preparation is invalid",
            ));
        }
        if let Some(result) = &self.child_result_volume {
            validate_child_result_volume(result, &self.child_private_volume, child_agent, work)?;
            if result == &self.child_project_volume {
                return Err(boundary_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "child result volume cannot alias the project volume",
                ));
            }
        }
        Ok(())
    }
}

pub(in crate::fork) fn validate_child_result_volume(
    result: &VolumeRef,
    scratch: &VolumeRef,
    child: AgentId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    result.validate(work)?;
    if result.class() != VolumeClass::AgentPrivate
        || result.owner() != &VolumeOwner::Agent(child)
        || result.provider() != scratch.provider()
        || result == scratch
    {
        return Err(boundary_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "child result volume must be child-owned and distinct from scratch",
        ));
    }
    Ok(())
}

pub(in crate::fork) fn validate_child_result_generation(
    result: Option<&VolumeRef>,
    generation: Option<&GenerationRef>,
    scratch: &VolumeRef,
    child: AgentId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    match (result, generation) {
        (None, None) => Ok(()),
        (Some(result), Some(generation)) => {
            validate_child_result_volume(result, scratch, child, work)?;
            generation.validate(work)?;
            if generation.as_resource().provider() != result.provider() {
                return Err(boundary_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "child result generation belongs to another provider",
                ));
            }
            Ok(())
        }
        _ => Err(boundary_error(
            work,
            crate::wire::ErrorCode::Invalid,
            "child result volume and generation must be captured together",
        )),
    }
}

/// Provider-neutral owner boundary for an idempotent multi-resource fork
pub trait ForkPreparer: acyclic_stream::ProviderPlatform {
    /// Exact immutable parent projection this preparer was constructed from.
    fn parent_snapshot(&self) -> (&Authority, u64);

    /// Captures or returns the existing report for the exact request identity.
    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>>;

    /// Observes an uncertain preparation for the exact original request.
    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>>;
}

/// Provider-owned capture of one selected revision during parent-controlled
pub trait ForkCaptureProvider: acyclic_stream::ProviderPlatform {
    /// Exact provider identity whose resource revisions this adapter owns.
    fn provider(&self) -> &ProviderRef;

    /// Captures one immutable child-visible revision or reports an explicit
    fn capture<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>>;

    /// Observes a capture whose start was durably recorded but whose result
    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>>;
}
