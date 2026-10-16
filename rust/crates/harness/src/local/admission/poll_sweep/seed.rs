//! Share only the immutable, durably created empty-volume fixture.
use super::*;

pub(super) struct Seed(super::super::snapshot::Snapshot);

impl Seed {
    pub async fn new() -> Result<Self> {
        let fixture = Fixture::new(Scenario::Text, None).await?;
        let root = fixture.directory.path().join("filesystem");
        Ok(Self(super::super::snapshot::Snapshot::capture(&root)?))
    }

    pub fn restore(&self, root: &Path) -> Result<()> {
        self.0.restore(root)
    }
}
