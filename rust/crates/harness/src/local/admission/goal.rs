use super::*;

impl LocalHarness {
    pub(super) fn goal_task_path(&self) -> Option<&'static str> {
        // The current publisher resolves paths inside its one output volume.
        // Composed mount routing replaces this lookup when #503 lands.
        (self.runtime.harness().output_volume() == Some(&self.scratch)).then_some("GOAL.md")
    }

    pub(super) async fn block_goal(
        &self,
        goal: &GoalView,
        settled: OperationId,
        contents: &str,
    ) -> Result<()> {
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.signed,
            &self.scratch,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        self.files
            .put_content(
                &self.scratch,
                &grant,
                "GOAL.md",
                contents.as_bytes(),
                "text/markdown",
                "GOAL.md",
                self.limits.file_bytes,
                &crate::IdempotencyKey::new(format!(
                    "goal-block:{settled}:{}",
                    goal.source.version()
                ))?,
                Some(&self.files.file_generation(&goal.source, None)?),
                None,
            )
            .await?;
        Ok(())
    }
}
