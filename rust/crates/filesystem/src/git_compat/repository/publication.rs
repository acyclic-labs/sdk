use super::*;

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Records a durable authored snapshot as an explicit compatibility commit.
    pub async fn record_commit(
        &self,
        expected_head: Option<GitCommitId>,
        generation: GenerationId,
        tracked_paths: BTreeSet<String>,
        message: impl Into<String>,
        author: impl Into<String>,
        authored_at_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let tree = GitTreeRef::exact(self.workspace_id, generation);
        let commit = GitCommit::new(
            tree,
            tree,
            None,
            tracked_paths,
            expected_head.into_iter().collect(),
            author,
            authored_at_seconds,
            message,
        );
        self.update(|state| record_captured_commit_state(state, commit.clone()))
            .await
    }

    /// Records an idempotent distributed workspace publication.
    pub async fn record_publication(
        &self,
        publication: GitPublicationRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let current = state.current().map_err(|_| GitCompatError::InvalidState)?;
            let head = current.head;
            if publication.workspace_tree.workspace_id() != current.workspace_id {
                return Err(GitCompatError::WorkspaceMismatch);
            }
            let previous = head.and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
            if previous == Some(publication.tree) {
                return Ok(GitCommandOutput::NoOp);
            }
            if !capture_proof_valid(
                publication.tree,
                publication.workspace_tree,
                publication.capture_proof.as_ref(),
                previous,
            ) {
                return Err(GitCompatError::WorkspaceMismatch);
            }
            let mut parents: Vec<_> = head.into_iter().collect();
            if let Some(source) = publication.source_head
                && !parents.contains(&source)
            {
                if !state.commits.contains_key(&source) {
                    state.external_parents.insert(source);
                }
                parents.push(source);
            }
            let commit = GitCommit::new(
                publication.tree,
                publication.workspace_tree,
                publication.capture_proof.clone(),
                publication.tracked_paths.clone(),
                parents,
                publication.author.clone(),
                publication.authored_at_seconds,
                publication.message.clone(),
            );
            let output = record_commit_state::<S::Error>(&mut state, commit, head)?;
            if self.publish(state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }
}
