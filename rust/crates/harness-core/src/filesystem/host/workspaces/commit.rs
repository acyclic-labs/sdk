use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) fn apply_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        expected: Option<&'a GenerationRef>,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        self.publish_generation(
            reference,
            mutations,
            key,
            io,
            FilePublication::Replace(expected),
        )
    }

    pub(in crate::filesystem) fn apply_authorized_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        authorization: (acyclic_fs::access::PolicyFence, &'a ContentGrant),
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        self.publish_generation(
            reference,
            mutations,
            key,
            io,
            FilePublication::Authorized(authorization.0, authorization.1),
        )
    }

    fn publish_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
        publication: FilePublication<'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        let expected = match publication {
            FilePublication::Replace(expected) => expected,
            FilePublication::Authorized(..) => None,
        };
        let Some(io) = io else {
            return crate::conversation::BoundedContentFuture::new(None, || async move {
                self.apply_generation_unmeasured(
                    reference,
                    expected,
                    mutations,
                    &key,
                    false,
                    publication,
                )?
                .await
            });
        };
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            let workspace = self.open(reference, Some(io)).await?;
            let operation = bounded_io::operation_key(&key, Some(io))?;
            let mut transaction = if matches!(publication, FilePublication::Replace(_)) {
                let pinned = match expected {
                    Some(reference) => self.generation(&workspace, reference, Some(io)).await?,
                    None => io.sdk(workspace.head(io, io.cancellation()).await)?,
                };
                io.sdk(
                    workspace
                        .begin_transaction_at(&pinned, operation, io, io.cancellation())
                        .await,
                )?
            } else {
                io.sdk(
                    workspace
                        .begin_transaction(operation, io, io.cancellation())
                        .await,
                )?
            };
            if let Some(fence) = publication.fence() {
                io.sdk(
                    transaction
                        .observe_policy_fence(fence, io, io.cancellation())
                        .await,
                )?;
            }
            #[cfg(test)]
            if let Some(pause) = &self.publication_pause {
                pause().await;
            }
            Self::publish_mutations(&mut transaction, mutations, io).await?;
            Self::commit_publication(transaction, io, publication).await
        })
    }
    async fn publish_mutations(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        mutations: &[WorkspaceMutation],
        io: &PreparationIo,
    ) -> Result<()> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async {
            #[cfg(not(target_arch = "wasm32"))]
            {
                if Self::stage_publication_batch(
                    transaction,
                    mutations,
                    Some(io),
                    io.cancellation(),
                )
                .await?
                {
                    return Ok(());
                }
            }
            for mutation in mutations {
                match mutation {
                    WorkspaceMutation::CreateDirectory { path } => io.sdk(
                        transaction
                            .create_dir_all(path, io, io.cancellation())
                            .await,
                    )?,
                    WorkspaceMutation::PutFile { path, bytes } => io.sdk(
                        transaction
                            .write(path, bytes.clone(), io, io.cancellation())
                            .await,
                    )?,
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
        .await
    }

    pub(in crate::filesystem) async fn commit_publication(
        transaction: acyclic_fs::Transaction<A, O>,
        io: &PreparationIo,
        publication: FilePublication<'_>,
    ) -> Result<Generation<A, O>> {
        Self::commit_contextual_publication(transaction, io, publication, None).await
    }

    pub(in crate::filesystem::host) async fn commit_contextual_publication(
        mut transaction: acyclic_fs::Transaction<A, O>,
        io: &PreparationIo,
        publication: FilePublication<'_>,
        context: Option<(
            acyclic_fs::Digest,
            acyclic_stream::OwnedBytes,
            acyclic_fs::PublicationPermit,
        )>,
    ) -> Result<Generation<A, O>> {
        let commit = crate::conversation::BoundedContentFuture::new(Some(io), || async {
            let commit = 'commit: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    let fence = publication.fence();
                    let authority = if let FilePublication::Authorized(_, grant) = publication {
                        let before = io.owned_bytes()?;
                        let verifier = grant.copy_verifier(&mut Some(&mut io.history_io()))?;
                        let scope = grant.policy_scope().copy(&mut Some(&mut io.history_io()))?;
                        let metadata = (std::mem::size_of::<(
                            AuthorityVerifier,
                            Scope,
                            acyclic_fs::NativeAllocation,
                        )>() + 2 * std::mem::size_of::<usize>())
                            as u64;
                        io.retain_owned(metadata)?;
                        io.allocation_attempt(metadata)?;
                        let custody = io.take_native_custody(io.owned_since(before)?)?;
                        Some(Arc::new((verifier, scope, custody)))
                    } else {
                        None
                    };
                    let mut native_before = move |work: &acyclic_fs::NativeMaterializationWork| {
                        let work = work.clone();
                        let authority = authority.clone();
                        async move {
                            let check = || -> Result<()> {
                                require_publication_fence(fence)?;
                                if let Some(authority) = &authority {
                                    let io = PreparationIo::from_native_work(&work)?;
                                    authority
                                        .0
                                        .verify(&authority.1, &mut Some(&mut io.history_io()))?;
                                }
                                Ok(())
                            };
                            check()
                        }
                    };
                    break 'commit transaction
                        .commit_with_native_work(
                            context,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            work,
                            io.cancellation(),
                            &mut native_before,
                        )
                        .await?;
                }
                break 'commit io.receive(
                    transaction
                        .commit(
                            context,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            io.remaining()?,
                            io.cancellation(),
                            &mut |_| async move {
                                require_publication_time(publication, Some(io))
                                    .map_err(acyclic_fs::OperationFailure::before_work)?;
                                Ok(acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                })
                            },
                        )
                        .await,
                )?;
            };
            Ok(commit)
        })?
        .await?;
        match commit {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "workspace publication conflicted or was fenced",
            )),
        }
    }
}
