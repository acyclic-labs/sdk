//! seed fork protocol.
use super::*;
pub(crate) fn copy_seed(source: &ForkSeed, quota: &mut CodecQuota<'_, '_>) -> Result<ForkSeed> {
    crate::core::copy_owned(source, quota, |source, work| {
        source.validate(work)?;
        crate::core::owned_bytes(source)
    })
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_witness: Option<[u8; 32]>,
    /// Exact published attachment lists.
    pub attachment_manifests: Vec<FileRef>,
    /// Optional provider-attested common boundary.
    pub boundary: Option<AttestedBoundary>,
}

impl ForkSeed {
    pub(in crate::fork) fn allows_manifest(
        &self,
        manifest: &FileRef,
        reader: AgentId,
        work: &mut CodecQuota<'_, '_>,
    ) -> Result<bool> {
        let volume = manifest.volume();
        if volume.class() == VolumeClass::AgentPrivate
            && volume.owner() == &VolumeOwner::Agent(reader)
        {
            return Ok(true);
        }
        if volume.class() == VolumeClass::Project {
            for resource in &self.resources {
                work.checkpoint()?;
                if matches!(&resource.revision, ResourceRevision::Project { volume: selected, .. } if selected == volume)
                {
                    return Ok(true);
                }
            }
        }
        if volume.class() == VolumeClass::SessionShared {
            for grant in &self.shared_grants {
                work.checkpoint()?;
                if grant.volume == *volume
                    && grant.child_agent == reader
                    && grant.operations.contains(&VolumeOperation::Read)
                {
                    return Ok(true);
                }
            }
        }
        for grant in &self.reference_grants {
            work.checkpoint()?;
            if grant.reader == reader
                && grant.file == *manifest
                && grant
                    .attachment_manifest
                    .as_ref()
                    .is_none_or(|from| from == manifest)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Validates pinned scratch lineage, singleton selections, and resource references.
    #[allow(
        clippy::too_many_lines,
        reason = "validates the complete fork seed contract"
    )]
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
        self.child_private_generation
            .as_resource()
            .validate(work.work())?;
        if self.child_private_volume.class() != VolumeClass::AgentPrivate
            || self.child_private_volume.provider().family() != "filesystem"
            || self.child_private_volume.owner() != &VolumeOwner::Agent(self.child_agent)
            || self.child_private_generation.as_resource().provider()
                != self.child_private_volume.provider()
        {
            return Err(work.invalid("fork private volume is not child owned"));
        }
        validate_child_result_generation(
            self.child_result_volume.as_ref(),
            self.child_result_generation.as_ref(),
            &self.child_private_volume,
            self.child_agent,
            work.work(),
        )?;
        if let Some(source) = &self.authorized_source {
            source.validate(work.work())?;
            if source.operation_id != self.operation_id
                || !self.resources.iter().any(|capture| {
                    matches!(
                    &capture.source,
                    ResourceRevision::Project { volume, generation }
                        if volume == &source.project && generation == &source.generation)
                })
                || (source.source_task != source.caller_task
                    && (self.inherited_through_sequence != 0 || self.summary.is_some()))
            {
                return Err(boundary_error(
                    work.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "authorized source must select its exact project and history-free cross-task fork",
                ));
            }
        }
        let mut history = 0;
        let mut project = 0;
        let mut private = 0;
        let mut context = 0;
        let mut process = 0;
        for (index, resource) in self.resources.iter().enumerate() {
            work.checkpoint()?;
            resource.validate(work.work())?;
            for revision in [&resource.source, &resource.revision] {
                crate::executor::json::json_byte_length(
                    revision,
                    crate::store::default_projection_read_limits().maximum_bytes,
                    work.work(),
                )
                .map(|_| ())?;
            }
            for previous in &self.resources[..index] {
                work.checkpoint()?;
                if previous.source == resource.source {
                    return Err(work.invalid("fork selection appears twice"));
                }
                if previous.revision == resource.revision {
                    return Err(work.invalid("fork resource appears twice"));
                }
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
                _ => {}
            }
        }
        for (index, omission) in self.omissions.iter().enumerate() {
            work.checkpoint()?;
            omission.validate(work.work())?;
            crate::executor::json::json_byte_length(
                &omission.selection.revision,
                crate::store::default_projection_read_limits().maximum_bytes,
                work.work(),
            )
            .map(|_| ())?;
            for previous in self.resources.iter().map(|capture| &capture.source).chain(
                self.omissions[..index]
                    .iter()
                    .map(|omission| &omission.selection.revision),
            ) {
                work.checkpoint()?;
                if *previous == omission.selection.revision {
                    return Err(work.invalid("fork selection appears twice"));
                }
            }
        }
        self.validate_shared_grants(&attached, &mut work)?;
        self.validate_reference_grants(&attached, &mut work)?;
        let mut manifest_bytes = 0_u64;
        for (index, manifest) in self.attachment_manifests.iter().enumerate() {
            work.checkpoint()?;
            manifest.validate(work.work())?;
            crate::executor::json::json_byte_length(
                manifest,
                crate::store::default_projection_read_limits().maximum_bytes,
                work.work(),
            )
            .map(|_| ())?;
            manifest_bytes = manifest_bytes
                .checked_add(manifest.descriptor().byte_length())
                .ok_or_else(|| work.invalid("fork attachment manifest bytes overflow"))?;
            if manifest.descriptor().media_type()
                != "application/vnd.acyclic.harness.attachments+json"
            {
                return Err(work.invalid("fork attachment manifest is invalid or duplicated"));
            }
            for previous in &self.attachment_manifests[..index] {
                work.checkpoint()?;
                if previous == manifest {
                    return Err(work.invalid("fork attachment manifest is invalid or duplicated"));
                }
            }
        }
        for grant in &self.reference_grants {
            work.checkpoint()?;
            if let Some(manifest) = &grant.attachment_manifest {
                let mut selected = false;
                for candidate in &self.attachment_manifests {
                    work.checkpoint()?;
                    selected |= candidate == manifest;
                }
                if !selected {
                    return Err(
                        work.invalid("reference grant names an unselected attachment manifest")
                    );
                }
                if grant.file != *manifest
                    && !self.allows_manifest(manifest, grant.reader, &mut work)?
                {
                    return Err(work.invalid("fork omits a manifest file read grant"));
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

    pub(in crate::fork) fn validate_shared_grants(
        &self,
        attached: &BTreeSet<AgentId>,
        work: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        for (index, grant) in self.shared_grants.iter().enumerate() {
            work.checkpoint()?;
            grant.validate(work.work())?;
            if grant.child_agent != self.child_agent && !attached.contains(&grant.child_agent) {
                return Err(work.invalid("shared grant belongs to an unattached agent"));
            }
            let mut selected = false;
            for resource in &self.resources {
                work.checkpoint()?;
                selected |= matches!(&resource.revision, ResourceRevision::SharedVolume(volume) if volume == &grant.volume);
            }
            if !selected {
                return Err(work.invalid("shared grant was not selected"));
            }
            for previous in &self.shared_grants[..index] {
                work.checkpoint()?;
                if previous.volume == grant.volume && previous.child_agent == grant.child_agent {
                    return Err(work.invalid("shared volume is granted twice to one agent"));
                }
            }
        }
        for resource in &self.resources {
            work.checkpoint()?;
            if let ResourceRevision::SharedVolume(volume) = &resource.revision {
                let mut granted = false;
                for grant in &self.shared_grants {
                    work.checkpoint()?;
                    granted |= grant.volume == *volume && grant.child_agent == self.child_agent;
                }
                if !granted {
                    return Err(work.invalid("shared-volume selections need exact child grants"));
                }
            }
        }
        Ok(())
    }

    pub(in crate::fork) fn validate_reference_grants(
        &self,
        attached: &BTreeSet<AgentId>,
        work: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        for (index, grant) in self.reference_grants.iter().enumerate() {
            work.checkpoint()?;
            grant.validate(work.work())?;
            for file in std::iter::once(&grant.file).chain(grant.attachment_manifest.iter()) {
                crate::executor::json::json_byte_length(
                    file,
                    crate::store::default_projection_read_limits().maximum_bytes,
                    work.work(),
                )
                .map(|_| ())?;
            }
            if grant.reader != self.child_agent && !attached.contains(&grant.reader) {
                return Err(work.invalid("reference reader is not attached to fork"));
            }
            if grant.file.volume().class() == VolumeClass::AgentPrivate
                && grant.file.volume().owner() == &VolumeOwner::Agent(grant.reader)
            {
                return Err(work.invalid("owner needs no inherited reference grant"));
            }
            for previous in &self.reference_grants[..index] {
                work.checkpoint()?;
                if previous == grant {
                    return Err(work.invalid("reference grant appears twice"));
                }
            }
        }
        Ok(())
    }

    pub(in crate::fork) fn validate_inherited_context(
        &self,
        work: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        if let Some(summary) = &self.summary {
            summary.validate_with_work(&self.child_private_volume, work.work())?;
            if self.inherited_through_sequence == 0 {
                return Err(work.invalid("summary fork has no inherited logical history"));
            }
        }
        let prefix = if self.inherited_through_sequence > 0 {
            Some(
                self.inherited_context
                    .first()
                    .filter(|file| {
                        file.path() == ".system/inherited-conversation/prefix.json"
                            && file.descriptor().media_type()
                                == "application/vnd.acyclic.harness.inherited-conversation+json"
                    })
                    .ok_or_else(|| work.invalid("inherited context omitted its selected prefix"))?,
            )
        } else {
            None
        };
        let expected = prefix.into_iter().chain(
            self.summary
                .iter()
                .flat_map(SummaryForkCapture::inherited_files),
        );
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
            values.push(file.read_capability(&mut None)?);
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

    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        crate::wire_codec::CodecQuota::produce(work, |quota| copy_seed(self, quota))
    }
}
