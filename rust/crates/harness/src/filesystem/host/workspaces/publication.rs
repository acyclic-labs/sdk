//! Retry only a definite, unpublished conflict for an unconditional update.
use super::*;
use crate::conversation::BoundedContentFuture;

pub(super) struct Update<'a, A, O> {
    pub host: &'a FilesystemHost<A, O>,
    pub workspace: &'a Workspace<A, O>,
    pub expected: Option<&'a GenerationRef>,
    pub mutations: &'a [WorkspaceMutation],
    pub operation: FilesystemKey,
    pub io: &'a PreparationIo,
    pub preparation: Option<&'a Arc<crate::resource_preparation::TaskResourcePreparation>>,
    pub unmeasured: bool,
}

pub(super) async fn finish<S: acyclic_stream::ProviderTask, T: acyclic_stream::ProviderTask>(
    state: &mut S,
    mut attempt: impl for<'a> FnMut(&'a mut S) -> Result<BoundedContentFuture<'a, Option<T>>>
    + acyclic_stream::ProviderTask,
) -> Result<T> {
    loop {
        if let Some(value) = attempt(state)?.await? {
            return Ok(value);
        }
        #[cfg(acyclic_harness_loom)]
        if model::skip_retry() {
            return Err(Error::Conflict("negative control omitted the retry".into()));
        }
    }
}

pub(super) fn attempt<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    update: &'a mut Update<'_, A, O>,
) -> Result<BoundedContentFuture<'a, Option<Generation<A, O>>>> {
    BoundedContentFuture::new(Some(update.io), || async move {
        let host = update.host;
        let workspace = update.workspace;
        let expected = update.expected;
        let mutations = update.mutations;
        let operation = update.operation;
        let io = update.io;
        let preparation = update.preparation;
        let unmeasured = update.unmeasured;
        #[cfg(not(target_arch = "wasm32"))]
        let mut native_before = {
            let preparation = preparation.cloned();
            move |work: &acyclic_fs::NativeMaterializationWork| {
                let preparation = preparation.clone();
                let work = work.clone();
                async move {
                    if let Some(task) = preparation {
                        let io = PreparationIo::from_native_work(&work)?;
                        task.verify(false, Some(&io))?.await?;
                    }
                    Ok::<(), Error>(())
                }
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            native_before(work).await?;
        }

        let pinned = match expected {
            Some(reference) => {
                crate::conversation::BoundedContentFuture::new(Some(io), || {
                    host.generation(&workspace, reference, Some(io))
                })?
                .await?
            }
            None => {
                crate::conversation::BoundedContentFuture::new(Some(io), || {
                    workspace
                        .head(io, io.cancellation())
                        .map(|result| io.sdk(result))
                })?
                .await?
            }
        };
        let mut transaction = crate::conversation::BoundedContentFuture::new(Some(io), || {
            workspace
                .begin_pinned_transaction(&pinned, operation, io, io.cancellation())
                .map(|result| io.sdk(result))
        })?
        .await?;
        crate::conversation::BoundedContentFuture::new(Some(io), || async {
            for mutation in mutations {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    native_before(work).await?;
                }
                match mutation {
                    WorkspaceMutation::CreateDirectory { path } => {
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            transaction
                                .create_dir_all(path, io, io.cancellation())
                                .map(|result| io.sdk(result))
                        })?
                        .await?
                    }
                    WorkspaceMutation::PutFile { path, bytes } => {
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            transaction
                                .write(path, bytes.clone(), io, io.cancellation())
                                .map(|result| io.sdk(result))
                        })?
                        .await?
                    }
                    WorkspaceMutation::Remove { path } if unmeasured => {
                        crate::conversation::BoundedContentFuture::new(Some(io), || {
                            transaction
                                .remove(path, io, io.cancellation())
                                .map(|result| io.sdk(result))
                        })?
                        .await?
                    }
                    WorkspaceMutation::Remove { .. } => {
                        return Err(stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "bounded publication does not remove content",
                        ));
                    }
                }
            }
            Ok(())
        })?
        .await?;
        let commit = crate::conversation::BoundedContentFuture::new(Some(io), || async {
            let commit = 'commit: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    break 'commit transaction
                        .commit_with_native_work(
                            None,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            work,
                            io.cancellation(),
                            &mut native_before,
                        )
                        .await?;
                }
                io.receive(
                    transaction
                        .commit(
                            None,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            io.remaining()?,
                            io.cancellation(),
                            &mut |_| async move {
                                if let Some(task) = preparation {
                                    task.verify(false, Some(io))
                                        .map_err(acyclic_fs::OperationFailure::before_work)?
                                        .await
                                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                                }
                                Ok(acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                })
                            },
                        )
                        .await,
                )?
            };
            Ok(commit)
        })?
        .await?;
        match commit {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(Some(generation)),
            TransactionCommit::Conflict { .. } if expected.is_none() => Ok(None),
            TransactionCommit::Conflict { .. } => Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "workspace publication observed changed dependencies",
            )),
            TransactionCommit::Fenced => Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "workspace publication was fenced",
            )),
            TransactionCommit::IdempotencyConflict => Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "workspace publication retry identity changed",
            )),
        }
    })
}

#[cfg(acyclic_harness_loom)]
mod model;
