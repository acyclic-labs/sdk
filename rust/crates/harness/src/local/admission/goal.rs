use super::*;

impl LocalHarness {
    pub(super) fn goal_task_path(&self) -> Option<&'static str> {
        ["GOAL.md", ".volumes/scratch/GOAL.md"]
            .into_iter()
            .find(|path| {
                self.runtime
                    .harness()
                    .output_destination(path)
                    .is_some_and(|(volume, relative)| {
                        volume == &self.scratch && relative == "GOAL.md"
                    })
            })
    }

    pub(super) async fn block_goal(
        &self,
        goal: &GoalView,
        settled: OperationId,
        contents: &str,
    ) -> Result<(crate::conversation::FileRef, OperationId)> {
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.signed,
            &self.scratch,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let key =
            crate::IdempotencyKey::new(format!("goal-block:{settled}:{}", goal.source.version()))?;
        let file = self
            .files
            .put_content(
                &self.scratch,
                &grant,
                "GOAL.md",
                contents.as_bytes(),
                "text/markdown",
                "GOAL.md",
                self.limits.file_bytes,
                &key,
                Some(&self.files.file_generation(&goal.source, None)?),
                None,
            )
            .await?;
        Ok((
            file,
            OperationId::from_bytes(crate::filesystem::filesystem_key(&key).into_bytes()),
        ))
    }

    pub(super) async fn record_goal_revision(
        &self,
        current: &Turn,
        generation: &GenerationRef,
        goal: &crate::conversation::FileRef,
        publication: Option<OperationId>,
    ) -> Result<()> {
        if current.goal_revision == publication {
            return Ok(());
        }
        let mut next = current.clone();
        next.goal_revision = publication;
        self.write_turn(
            &next,
            generation,
            &crate::IdempotencyKey::new(format!(
                "goal-observed:{}:{}",
                current.operation,
                goal.version()
            ))?,
        )
        .await
    }
}
