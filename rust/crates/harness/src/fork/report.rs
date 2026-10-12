//! report fork protocol.
use super::*;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
            self.child_result_volume.as_ref(),
            self.child_result_generation.as_ref(),
            &self.child_private_volume,
            self.request.child_agent,
            work.work(),
        )?;
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
            return Err(
                work.invalid("fork report changed the requested child allocation or prefix")
            );
        }
        let inherited_bytes = self
            .inherited_context
            .iter()
            .try_fold(0_u64, |total, file| {
                work.checkpoint()?;
                total
                    .checked_add(file.descriptor().byte_length())
                    .ok_or_else(|| work.invalid("fork inherited context byte count overflow"))
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
                        return Err(
                            work.invalid("fork report changed the requested project volume")
                        );
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
        self.child_private_generation
            .as_resource()
            .validate(work.work())?;
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
        let resource_count = self
            .captures
            .iter()
            .filter(|capture| matches!(capture, Capture::Captured(_)))
            .count();
        let mut resources =
            crate::core::reserve_copy_vector::<CapturedResource>(resource_count, &mut owned)?;
        let mut omissions = crate::core::reserve_copy_vector::<ForkOmission>(
            self.captures.len() - resource_count,
            &mut owned,
        )?;
        for (selection, capture) in self.request.selections.into_iter().zip(self.captures) {
            owned.checkpoint()?;
            match capture {
                Capture::Captured(resource) => resources.push(resource),
                Capture::Unsupported(_) | Capture::InFlight(_) | Capture::Indeterminate(_)
                    if selection.required =>
                {
                    return Err(boundary_error(
                        owned.work(),
                        crate::wire::ErrorCode::Conflict,
                        "required fork capture is unavailable",
                    ));
                }
                outcome => omissions.push(ForkOmission { selection, outcome }),
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

    /// Copies report fields under the optional work ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        CodecQuota::produce(work, |quota| {
            crate::core::copy_owned(self, quota, |source, work| {
                use crate::core::{owned_buffer, owned_capture, owned_sum, owned_vector};
                source.validate(work)?;
                owned_sum([
                    source.request.logical_owned_bytes()?,
                    owned_vector(&source.captures, owned_capture)?,
                    source.child_private_volume.logical_owned_bytes()?,
                    source
                        .child_private_generation
                        .as_resource()
                        .logical_owned_bytes()?,
                    source
                        .child_result_volume
                        .as_ref()
                        .map(VolumeRef::logical_owned_bytes)
                        .transpose()?
                        .unwrap_or(0),
                    source
                        .child_result_generation
                        .as_ref()
                        .map(|reference| reference.as_resource().logical_owned_bytes())
                        .transpose()?
                        .unwrap_or(0),
                    owned_vector(&source.inherited_context, FileRef::logical_owned_bytes)?,
                    owned_vector(&source.attachment_manifests, FileRef::logical_owned_bytes)?,
                    owned_vector(&source.shared_grants, |grant| {
                        owned_sum([
                            grant.volume.logical_owned_bytes()?,
                            owned_buffer::<VolumeOperation>(grant.operations.len())?,
                        ])
                    })?,
                    owned_vector(&source.reference_grants, |grant| {
                        owned_sum([
                            grant.file.logical_owned_bytes()?,
                            grant
                                .attachment_manifest
                                .as_ref()
                                .map(FileRef::logical_owned_bytes)
                                .transpose()?
                                .unwrap_or(0),
                        ])
                    })?,
                    source
                        .summary
                        .as_ref()
                        .map(|summary| {
                            owned_sum([
                                summary.selection.checkpoint.logical_owned_bytes()?,
                                summary.context.logical_owned_bytes()?,
                                owned_vector(&summary.payloads, FileRef::logical_owned_bytes)?,
                                owned_vector(&summary.references, FileRef::logical_owned_bytes)?,
                            ])
                        })
                        .transpose()?
                        .unwrap_or(0),
                ])
            })
        })
    }
}
