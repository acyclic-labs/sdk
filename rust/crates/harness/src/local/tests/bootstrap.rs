//! Share immutable empty-volume bytes, never live providers or effect ledgers.
use super::super::admission::snapshot::Snapshot;
use super::*;

pub(super) struct Bootstrap {
    source: tempfile::TempDir,
    filesystem: Snapshot,
    state: Snapshot,
    project: VolumeRef,
    pub agent: Option<crate::AgentId>,
}

impl Bootstrap {
    pub async fn new() -> Result<Self> {
        let source = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(source.path()).await?;
        let harness = LocalHarness::open(
            volume_config(source.path(), project.clone(), scripted([]))?.without_default_tools(),
        )
        .await?;
        let agent = harness.signed.agent();
        drop(harness);
        Ok(Self {
            filesystem: Snapshot::capture(&source.path().join("filesystem"))?,
            state: Snapshot::capture(
                &source
                    .path()
                    .join("state")
                    .join(project.storage_name(&mut None)?),
            )?,
            source,
            project,
            agent,
        })
    }

    pub fn restore(&self) -> Result<tempfile::TempDir> {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        assert_ne!(directory.path(), self.source.path());
        self.filesystem
            .restore(&directory.path().join("filesystem"))?;
        let parent = directory.path().join("state");
        std::fs::create_dir(&parent).map_err(super::super::storage)?;
        self.state
            .restore(&parent.join(self.project.storage_name(&mut None)?))?;
        Ok(directory)
    }

    pub fn config(
        &self,
        directory: &Path,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<LocalHarnessConfig> {
        Ok(volume_config(directory, self.project.clone(), provider)?.without_default_tools())
    }
}
