use super::super::snapshot::Snapshot;
use super::*;

pub(super) struct Bootstrap {
    source: Root,
    provider: Snapshot,
    state: Snapshot,
    volume: VolumeRef,
    pub agent: Option<AgentId>,
}

pub(super) struct Root {
    pub directory: tempfile::TempDir,
    state: PathBuf,
}

impl Bootstrap {
    pub async fn new() -> Result<Self> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let volume = super::super::super::tests::volume(directory.path()).await?;
        let provider = Arc::new(ScriptedModelProvider::new([]));
        let config =
            super::super::super::tests::volume_config(directory.path(), volume.clone(), provider)?
                .compaction(CompactionPolicy::Disabled);
        let state = config
            .state_directory
            .clone()
            .expect("explicit fixture state");
        assert!(state.starts_with(directory.path()));
        // Open resolves exactly the injected directory; under test the per-user
        // default is unreachable, so it cannot be the snapshot source.
        assert_eq!(config.resolved_state_directory()?, state);
        assert!(super::super::super::owner::user_state_root().is_err());
        let source = Root { directory, state };
        let harness = LocalHarness::open(config).await?;
        let agent = harness.signed.agent();
        assert!(harness.current_turn().await?.0.is_none());
        drop(harness);
        Ok(Self {
            provider: Snapshot::capture(&source.directory.path().join("filesystem"))?,
            state: Snapshot::capture(&source.state)?,
            source,
            volume,
            agent,
        })
    }

    pub fn restore(&self) -> Result<(Root, Arc<ScriptedModelProvider>)> {
        let parent = tempfile::tempdir().map_err(storage)?;
        let directory = parent.path().join("root");
        std::fs::create_dir(&directory).map_err(storage)?;
        self.provider.restore(&directory.join("filesystem"))?;
        let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
            "done".into(),
        )]));
        let config = self.config(&directory, model.clone())?;
        let state = config
            .state_directory
            .clone()
            .expect("explicit fixture state");
        assert_ne!(state, self.source.state);
        assert!(state.starts_with(&directory));
        std::fs::create_dir_all(state.parent().expect("state parent")).map_err(storage)?;
        self.state.restore(&state)?;
        Ok((
            Root {
                directory: parent,
                state,
            },
            model,
        ))
    }

    pub fn config(
        &self,
        root: &Path,
        model: Arc<ScriptedModelProvider>,
    ) -> Result<LocalHarnessConfig> {
        let mut config =
            super::super::super::tests::volume_config(root, self.volume.clone(), model)?
                .compaction(CompactionPolicy::Disabled);
        config.goal_continuations = None;
        Ok(config)
    }
}
