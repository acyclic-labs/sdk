use super::*;

pub(super) fn existing_tracked_paths<'a, A: acyclic_fs::AsyncAuthorityStore + 'static, O: acyclic_fs::AsyncObjectStore + 'static>(
    generation: &'a Generation<A, O>,
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

// Preparation can await storage and merge drivers. Revalidate after it,
// before handing any prepared result to a publication using the lease permit.
pub(super) async fn prepared<T, P, F, Fut>(preparation: P, before_effect: &F) -> Result<T, Error>
where
    P: Future<Output = Result<T, Error>>,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let value = preparation.await?;
    before_effect().await?;
    Ok(value)
}
