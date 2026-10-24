use super::*;

pub(super) fn existing_tracked_paths<'a>(
    generation: &'a LocalGeneration,
    candidates: &'a BTreeSet<String>,
) -> impl std::future::Future<Output = Result<BTreeSet<String>, Error>> + Send + 'a {
    async move {
        let mut tracked = BTreeSet::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        for path in candidates {
            let absolute = format!("/{path}");
            // Each stat runs in a heap frame of its own.
            match acyclic_fs::in_heap(|| {
                generation.stat(&absolute, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            })
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
