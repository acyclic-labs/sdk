//! Restore independently owned bytes from one initialized scenario.
use super::*;

pub(super) struct Seed {
    scenario: Scenario,
    pub parent: PathBuf,
    files: super::super::snapshot::Snapshot,
    stream: super::super::snapshot::Snapshot,
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
            files: super::super::snapshot::Snapshot::capture(&root.join("filesystem"))?,
            stream: super::super::snapshot::Snapshot::capture(&root.join("tasks"))?,
            history: fixture.history.into_inner(),
        })
    }

    pub fn restore(&self, scenario: Scenario, root: &Path) -> Result<()> {
        assert_eq!(self.scenario, scenario);
        self.files.restore(&root.join("filesystem"))?;
        self.stream.restore(&root.join("tasks"))
    }
}
