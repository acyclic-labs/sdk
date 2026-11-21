use super::*;

pub(super) async fn existing_tracked_paths<
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
>(
    generation: &Generation<A, O>,
    candidates: &BTreeSet<String>,
) -> Result<BTreeSet<String>, Error> {
    let mut tracked = BTreeSet::new();
    for path in candidates {
        match generation
            .stat(
                &format!("/{path}"),
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
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

// Preparation can await storage and merge drivers. Revalidate after it,
// before handing any prepared result to a publication using the lease permit.
pub(super) async fn prepared<T, P, F, Fut>(preparation: P, before_effect: &F) -> Result<T, Error>
where
    P: Future<Output = Result<T, Error>>,
    F: Fn() -> Fut + ?Sized,
    Fut: Future<Output = Result<(), Error>>,
{
    let value = preparation.await?;
    before_effect().await?;
    Ok(value)
}
