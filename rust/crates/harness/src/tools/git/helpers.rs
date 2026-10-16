use super::*;

pub(super) fn existing_tracked_paths<'a>(
    generation: &'a LocalGeneration,
    candidates: &'a BTreeSet<String>,
) -> impl std::future::Future<Output = Result<BTreeSet<String>, Error>> + Send + 'a {
    async move {
        let mut tracked = BTreeSet::new();
        for path in candidates {
            match generation
                .stat(
                    &format!("/{path}"),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
            {
                Ok(stat) if stat.kind != FileKind::Directory => {
                    tracked.insert(path.clone());
                }
                Ok(_) | Err(WorkspaceError::NotFound) => {}
                Err(error) => return Err(Error::from(error)),
            }
        }
        Ok(tracked)
    }
}
