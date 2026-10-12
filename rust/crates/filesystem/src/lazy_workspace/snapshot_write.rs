use super::*;
async fn seal<G: crate::AuthorityGuard, E>(
    guard: &mut G,
    ctx: &mut KernelAccounting<'_>,
) -> Result<(), E>
where
    G::Error: Into<E>,
    E: From<crate::FsError>,
{
    let native = ctx.native_work().is_some();
    let mut work = match ctx.native_work() {
        Some(original) => crate::AuthorityWork::native(original, cancellation(ctx)),
        None => crate::AuthorityWork::new(ctx.remaining()?, cancellation(ctx)),
    };
    let result = guard.seal(&mut work).await;
    let spent = work.spent();
    drop(work);
    if !native {
        ctx.record_completed(spent)?;
    }
    result.map_err(|error| match error {
        crate::AuthorityError::Authority(error) => crate::FsError::from(error).into(),
        crate::AuthorityError::Owner(error) => error.into(),
    })
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Publishes a frozen file/directory batch and preserves the original ACK on replay.
    pub async fn write_at_snapshot<'path, I, G, E>(
        &self,
        basis: LazySnapshotRef,
        mutations: I,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
        guard: &mut G,
    ) -> Result<(TransactionCommit<A, O>, Result<LazySnapshotRef, E>), E>
    where
        I: ExactSizeIterator<Item = (&'path str, Option<acyclic_stream::OwnedBytes>)>
            + Clone
            + Send,
        G: crate::AuthorityGuard + Clone + Send + 'static,
        G::Error: Into<E>,
        E: From<LazyWorkspaceError> + From<WorkspaceError> + From<crate::FsError> + Send + 'static,
    {
        seal::<_, E>(guard, ctx).await?;
        self.validate_snapshot(basis, ctx)?;
        let domain = b"acyclic-fs-lazy-write-input-v1\0";
        let mut hash = blake3::Hasher::new();
        ctx.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 40,
            ..WorkCounters::UNCHARGED
        })?;
        hash.update(domain);
        hash.update(&basis.id.into_bytes());
        for (path, bytes) in mutations.clone() {
            let path = self.canonical_path(path, ctx)?;
            let result = (|| {
                let length = bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
                let hashed = (path.value.len() as u64)
                    .checked_add(length)
                    .and_then(|n| n.checked_add(17))
                    .ok_or(crate::WorkError::Overflow)
                    .map_err(LazyWorkspaceError::from)?;
                ctx.charge(WorkCounters {
                    bytes_hashed: hashed,
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                hash.update(&[u8::from(bytes.is_some())]);
                hash.update(&(path.value.len() as u64).to_be_bytes());
                hash.update(path.value.as_bytes());
                hash.update(&length.to_be_bytes());
                if let Some(bytes) = bytes {
                    hash.update(&bytes);
                }
                Ok::<_, E>(())
            })();
            path.retire(ctx)?;
            result?;
        }
        hash.update(&(mutations.len() as u64).to_be_bytes());
        let input = crate::Digest::from_bytes(*hash.finalize().as_bytes());
        if let Some(id) = self.operation_generation(key, ctx).await? {
            let original = crate::Generation {
                workspace: self.workspace.clone(),
                id,
                publication_closure: None,
            };
            let proof = async {
                seal::<_, E>(guard, ctx).await?;
                let (snapshot, saved) = self.selected_snapshot(key.operation_id(), id, ctx).await?;
                if saved != input {
                    return Err(LazyWorkspaceError::StaleIdentity.into());
                }
                Ok(snapshot)
            }
            .await;
            return Ok((TransactionCommit::AlreadyCommitted(original), proof));
        }
        let state = self.state(ctx).await?;
        if state.source != basis.source
            || state.overlay != basis.overlay
            || state.shadows != basis.shadows
        {
            return Err(LazyWorkspaceError::StaleSource.into());
        }
        let head = self.head(ctx).await?;
        let checkout = self
            .workspace
            .checkout_accounted(
                crate::model::GenerationSelector::Exact(basis.authored_generation),
                crate::model::CheckoutMode::tracking_transaction(),
                ctx,
                cancellation(ctx),
            )
            .await?;
        let mut transaction = crate::Transaction::from_checkout(
            &self.workspace,
            checkout,
            key,
            head.id() != basis.authored_generation,
        );
        for (path, bytes) in mutations {
            if let Some(bytes) = bytes {
                let (lookup, _, funding) = self.lookup_at_snapshot(basis, path, ctx).await?;
                let result = self
                    .stage_write(&mut transaction, path, bytes, lookup.as_ref(), ctx)
                    .await;
                if let Some(lookup) = lookup {
                    self.retire_lookup((lookup, funding), ctx)?;
                } else {
                    crate::kernel::retire_allocation(funding, 0, ctx)?;
                }
                result?;
            } else {
                accounted!(ctx, |work, cancel| transaction
                    .create_dir_all(path, work, cancel))?;
            }
        }
        let context = NativeSnapshotContext::Selected {
            cut: qualification::LazyPublicationCut {
                workspace_id: basis.workspace_id,
                config: self.workspace.volume.config,
                source: basis.source,
                overlay: basis.overlay,
                shadows: basis.shadows,
            },
            input,
        };
        let (context, evidence) = publication_evidence(context, key.operation_id(), permit, ctx)?;
        let source = Arc::clone(&self.source);
        let before = guard.clone();
        let commit = 'sdk: {
            if let Some(work) = ctx.native_work() {
                let mut callback = move |work: &crate::NativeMaterializationWork| {
                    let source = Arc::clone(&source);
                    let mut before = before.clone();
                    let work = work.clone();
                    async move {
                        if source.reference() != basis.source {
                            return Err(E::from(LazyWorkspaceError::StaleSource));
                        }
                        let mut ctx = KernelAccounting::native(&work, work.cancellation());
                        seal::<_, E>(&mut before, &mut ctx).await
                    }
                };
                break 'sdk transaction
                    .commit_with_native_work(
                        Some((context, evidence, permit)),
                        permit,
                        work,
                        cancellation(ctx),
                        &mut callback,
                    )
                    .await?;
            }
            let cancel = cancellation(ctx).clone();
            let result = transaction
                .commit(
                    Some((context, evidence, permit)),
                    permit,
                    ctx.remaining()?,
                    &cancel.clone(),
                    move |budget| {
                        let source = Arc::clone(&source);
                        let mut before = before.clone();
                        let cancel = cancel.clone();
                        async move {
                            let mut ctx = KernelAccounting::standalone(budget, &cancel);
                            let result = if source.reference() != basis.source {
                                Err(E::from(LazyWorkspaceError::StaleSource))
                            } else {
                                seal::<_, E>(&mut before, &mut ctx).await
                            };
                            result
                                .map(|()| OperationReceipt {
                                    value: (),
                                    work: ctx.spent(),
                                })
                                .map_err(|error| crate::OperationFailure::new(error, ctx.spent()))
                        }
                    },
                )
                .await;
            let (result, spent) = match result {
                Ok(receipt) => (Ok(receipt.value), receipt.work),
                Err(failed) => (Err(failed.error), *failed.work),
            };
            let (ledger, work, budget, _) = ctx
                .standalone_parts()
                .ok_or(LazyWorkspaceError::Concurrent)?;
            *work = work
                .with_backend(spent, ledger.live_bytes())
                .map_err(LazyWorkspaceError::from)?;
            work.verify(budget).map_err(LazyWorkspaceError::from)?;
            result?
        };
        let snapshot = match &commit {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => {
                async {
                    seal::<_, E>(guard, ctx).await?;
                    self.selected_snapshot(key.operation_id(), generation.id(), ctx)
                        .await
                        .map(|(snapshot, _)| snapshot)
                        .map_err(E::from)
                }
                .await
            }
            _ => Err(LazyWorkspaceError::OriginalSnapshotUnavailable.into()),
        };
        Ok((commit, snapshot))
    }
}
