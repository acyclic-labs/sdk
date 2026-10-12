//! capture fork protocol.
use super::*;
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
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.file.validate(work)?;
        if let Some(manifest) = &self.attachment_manifest {
            manifest.validate(work)?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
            {
                return Err(boundary_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "reference grant manifest has an invalid media type",
                ));
            }
        }
        Ok(())
    }

    /// Exact, per-file capability the owner may issue to this reader.
    pub fn capability(&self) -> Result<String> {
        self.validate(&mut None)?;
        self.file.read_capability(&mut None)
    }
}

impl SharedGrant {
    /// Validates the grant envelope; the seed checks selection and child binding.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate(work)?;
        if self.volume.class() != VolumeClass::SessionShared || self.operations.is_empty() {
            return Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "invalid fork shared-volume grant",
            ));
        }
        Ok(())
    }
}

impl ForkOmission {
    /// Required captures and successful captures cannot become omissions.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.selection.revision.validate(work)?;
        if self.selection.required || matches!(self.outcome, Capture::Captured(_)) {
            return Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork omission must be an uncaptured optional selection",
            ));
        }
        Ok(())
    }
}

impl CapturedResource {
    /// Only project capture may create a new revision and volume identity.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
                work,
                crate::wire::ErrorCode::Invalid,
                "scratch capture changed its selection",
            )),
            (ResourceRevision::Project { .. }, _) | (_, ResourceRevision::Project { .. }) => {
                Err(boundary_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "project capture has an invalid child volume",
                ))
            }
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
            _ => Err(boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "capture changed an immutable selected revision",
            )),
        }
    }
}
