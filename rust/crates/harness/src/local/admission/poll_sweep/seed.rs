//! Restore independently owned bytes from one initialized scenario.
use super::*;

pub(super) struct Seed {
    scenario: Scenario,
    pub parent: PathBuf,
    files: acyclic_native_runtime::testing::snapshot::Snapshot,
    stream: acyclic_native_runtime::testing::snapshot::Snapshot,
    pub history: Option<crate::projection::SelectedModelContext>,
}

impl Seed {
    pub async fn new(scenario: Scenario) -> Result<Self> {
        let fixture = Fixture::new(scenario, None).await?;
        let harness = fixture.open().await?;
        fixture.initialize(&harness).await?;
        drop(harness);
        let root = fixture.directory.path();
        Ok(Self {
            scenario,
            parent: root.parent().expect("fixture parent").to_owned(),
            files: acyclic_native_runtime::testing::snapshot::Snapshot::capture(
                &root.join("filesystem"),
            )
            .map_err(storage)?,
            stream: acyclic_native_runtime::testing::snapshot::Snapshot::capture(
                &root.join("tasks"),
            )
            .map_err(storage)?,
            history: fixture.history.into_inner(),
        })
    }

    pub fn restore(&self, scenario: Scenario, root: &Path) -> Result<()> {
        assert_eq!(self.scenario, scenario);
        self.files
            .restore(&root.join("filesystem"))
            .map_err(storage)?;
        self.stream.restore(&root.join("tasks")).map_err(storage)
    }
}
