use super::*;

pub(super) async fn apply_git_capture<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    parent: &Generation<A, O>,
    base: Option<&Generation<A, O>>,
    source: Option<&Generation<A, O>>,
    paths: &[String],
    tracked_paths: BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError> {
    let bytes = operation_id.into_bytes();
    let workspace_hash = hex::encode(&parent.workspace_id().into_bytes()[..6]);
    let capture = parent
        .workspace()
        .fork::<_, _, crate::WorkspaceError>(
            format!("git-capture-{workspace_hash}-{}", hex::encode(bytes)),
            crate::ForkOptions::from_generation(parent.clone(), IdempotencyKey::from_bytes(bytes)),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<crate::FsError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let mut apply_bytes = bytes;
    apply_bytes[0] ^= 0xa5;
    let apply_key = IdempotencyKey::from_bytes(apply_bytes);
    let replay = capture
        .operation_generation(apply_key, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let (generation, initial_generation) = if let Some(generation) = replay {
        let parents = generation
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let [initial_generation] = parents.as_slice() else {
            return Err(GitCaptureError::Conflict);
        };
        (generation, *initial_generation)
    } else {
        let head = capture
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        match capture
            .apply_paths_from(
                base,
                source,
                paths,
                head.id(),
                apply_key,
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        {
            crate::WorkspacePathApply::Applied(generation) => (generation, head.id()),
            crate::WorkspacePathApply::AlreadyApplied(generation) => {
                let parents = generation
                    .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?;
                let [initial_generation] = parents.as_slice() else {
                    return Err(GitCaptureError::Conflict);
                };
                (generation, *initial_generation)
            }
            crate::WorkspacePathApply::NoChanges(generation) => (generation, head.id()),
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => {
                return Err(GitCaptureError::Conflict);
            }
        }
    };
    generation
        .pin(
            format!("capture-{}", hex::encode(bytes)),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    Ok(GitCapturedGeneration {
        generation,
        tracked_paths,
        origin: Some((parent.clone(), initial_generation)),
    })
}
