use super::*;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationConfig {
    pub operation_id: OperationId,
    pub caller: TaskId,
    pub caller_lease: WorkLease,
    pub owner: AuthorityBinding,
    pub private_volume: VolumeRef,
    pub root: PathBuf,
    pub operation_directory: PathBuf,
    pub state_root: PathBuf,
    pub source_root_identity: [u8; 16],
    pub from_project: VolumeRef,
    pub to_project: VolumeRef,
    pub from: GenerationId,
    pub to: GenerationId,
    pub excluded_names: Vec<String>,
    pub budget: WorkBudget,
    pub maximum_directory_entries: u32,
    pub maximum_extent_spans: u32,
    pub transfer_bytes: u64,
}
impl NativePublicationConfig {
    pub fn validate(&self) -> Result<()> {
        for path in [&self.root, &self.operation_directory, &self.state_root] {
            if !path.is_absolute()
                || path
                    .components()
                    .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(Error::Invalid(
                    "native publication paths must be explicit absolute locations".into(),
                ));
            }
        }
        if self.operation_id.into_bytes() == [0; 16]
            || self.source_root_identity == [0; 16]
            || !self.budget.has_finite_allowances()
            || self.budget.peak_allocation_bytes == 0
            || self.maximum_directory_entries == 0
            || self.maximum_extent_spans == 0
            || self.transfer_bytes == 0
            || self.transfer_bytes > MAX_FRAME_BYTES as u64
            || self.excluded_names.len() > 64
        {
            return Err(Error::Invalid(
                "native publication requires original identities and finite allowances".into(),
            ));
        }
        for name in &self.excluded_names {
            if name.is_empty()
                || name.len() > 255
                || matches!(name.as_str(), "." | "..")
                || name.contains(['/', '\\', '\0'])
            {
                return Err(Error::Invalid(
                    "publication exclusion must be one root-level name".into(),
                ));
            }
        }
        self.private_volume.validate(&mut None)?;
        self.from_project.validate(&mut None)?;
        self.to_project.validate(&mut None)?;
        if self.private_volume.class() != VolumeClass::AgentPrivate
            || self.from_project.class() != VolumeClass::Project
            || self.to_project.class() != VolumeClass::Project
            || self.from_project.provider() != self.to_project.provider()
            || self.private_volume.provider() != self.to_project.provider()
        {
            return Err(Error::Invalid(
                "publication source selections must belong to the original filesystem provider"
                    .into(),
            ));
        }
        let verifier = self.owner.verifier()?;
        for (volume, operation) in [
            (&self.from_project, VolumeOperation::Read),
            (&self.to_project, VolumeOperation::Read),
            (&self.to_project, VolumeOperation::Write),
            (&self.private_volume, VolumeOperation::Write),
        ] {
            ContentGrant::verify(
                &verifier,
                &self.owner.scope,
                volume,
                operation,
                u64::MAX,
                &mut None,
            )?;
        }
        Ok(())
    }
}
