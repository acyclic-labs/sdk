use super::*;

/// Authenticated immutable files paired with their original follow boundary.
/// Later appends are followed from `publication.next`, with its preceding
/// commit fence; no second head observation or history traversal is needed.
pub struct WorkspaceSnapshot<A: 'static, O: 'static> {
    /// The selected immutable state under the workspace's existing access view.
    pub generation: Generation<A, O>,
    /// Original change-stream cut that produced this generation.
    pub publication: crate::PublicationSnapshot,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Workspace<A, O> {
    /// Selects and authenticates an immutable generation with its change cursor.
    /// The source tail freezes the cut before its exact last record is read.
    /// A missing source or record fails; concurrent append requires no retry.
    pub async fn snapshot<W: crate::facade::FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceSnapshot<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let publication = self
                .volume
                .publication_snapshot(&mut accounting, cancellation)
                .await?;
            let checkout = self
                .checkout_accounted(
                    GenerationSelector::Exact(publication.context.generation),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(WorkspaceSnapshot {
                generation: Generation {
                    workspace: self.clone(),
                    id: checkout.generation_id(),
                    publication_closure: None,
                },
                publication,
            })
        }
        .await;
        work.finish(result, &accounting)
    }
}
