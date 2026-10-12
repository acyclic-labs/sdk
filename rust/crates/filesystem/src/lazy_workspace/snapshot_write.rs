use super::*;
use crate::kernel::KernelAccounting;

async fn seal<G: crate::AuthorityGuard, E>(
    guard: &mut G,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), E>
where
    G::Error: Into<E>,
    E: From<crate::FsError>,
{
    let native = accounting.native_work().is_some();
    let mut work = match accounting.native_work() {
        Some(original) => crate::AuthorityWork::native(original, cancellation(accounting)),
        None => crate::AuthorityWork::new(accounting.remaining()?, cancellation(accounting)),
    };
    let result = guard.seal(&mut work).await;
    let spent = work.spent();
    drop(work);
    if !native {
        accounting.record_completed(spent)?;
    }
    result.map_err(|error| match error {
        crate::AuthorityError::Authority(error) => crate::FsError::from(error).into(),
        crate::AuthorityError::Owner(error) => error.into(),
    })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Publishes one frozen directory/file batch and recovers its immutable original result.
    pub async fn write_at_snapshot<'path, I, G, E>(
        &self,
        basis: LazySnapshotRef,
        mutations: I,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
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
        seal::<_, E>(guard, accounting).await?;
        self.validate_snapshot(basis, accounting)?;
        let mut hasher = blake3::Hasher::new();
        let domain = b"acyclic-fs-lazy-write-input-v1\0";
        accounting.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 40,
            ..WorkCounters::UNCHARGED
        })?;
        hasher.update(domain);
        hasher.update(&basis.id.into_bytes());
        for (path, bytes) in mutations.clone() {
            let path = self.canonical_path(path, accounting)?;
            let length = bytes.as_ref().map_or(0, |bytes| bytes.len() as u64);
            let hashed = (path.value.len() as u64)
                .checked_add(length)
                .and_then(|bytes| bytes.checked_add(17))
                .ok_or(crate::WorkError::Overflow)
                .map_err(LazyWorkspaceError::from)?;
            accounting.charge(WorkCounters {
                bytes_hashed: hashed,
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            hasher.update(&[u8::from(bytes.is_some())]);
            hasher.update(&(path.value.len() as u64).to_be_bytes());
            hasher.update(path.value.as_bytes());
            hasher.update(&length.to_be_bytes());
            if let Some(bytes) = bytes {
                hasher.update(&bytes);
            }
            path.retire(accounting)?;
        }
        hasher.update(&(mutations.len() as u64).to_be_bytes());
        let input = crate::Digest::from_bytes(*hasher.finalize().as_bytes());
        let original = 'accounted: {
            if let Some(original) = accounting.native_work() {
                break 'accounted self
                    .workspace
                    .operation_generation(key, original, cancellation(accounting))
                    .await?;
            }
            accept_receipt(
                self.workspace
                    .operation_generation(key, accounting.remaining()?, cancellation(accounting))
                    .await,
                accounting,
            )?
        };
        if let Some(original) = original {
            let proof = async {
                seal::<_, E>(guard, accounting).await?;
                let (snapshot, saved) = self
                    .selected_snapshot(key.operation_id(), original.id(), accounting)
                    .await?;
                if saved != input {
                    return Err(LazyWorkspaceError::StaleIdentity.into());
                }
                Ok(snapshot)
            }
            .await;
            return Ok((TransactionCommit::AlreadyCommitted(original), proof));
        }
        let state = self.state(accounting).await?;
        if state.source != basis.source
            || state.overlay != basis.overlay
            || state.shadows != basis.shadows
            || state.pending_remove.is_some()
        {
            return Err(LazyWorkspaceError::StaleSource.into());
        }
        let head = self.head(accounting).await?;
        let checkout = self
            .workspace
            .checkout_accounted(
                crate::model::GenerationSelector::Exact(basis.authored_generation),
                crate::model::CheckoutMode::tracking_transaction(),
                accounting,
                cancellation(accounting),
            )
            .await?;
        let mut transaction = crate::Transaction::from_checkout(
            &self.workspace,
            checkout,
            key,
            head.id() != basis.authored_generation,
        );
        for (path, bytes) in mutations {
            match bytes {
                None => {
                    transaction
                        .create_dirs(path, accounting, cancellation(accounting))
                        .await?
                }
                Some(bytes) => {
                    let (lookup, _, allocation) =
                        self.lookup_at_snapshot(basis, path, accounting).await?;
                    let result = self
                        .stage_write(&mut transaction, path, bytes, lookup.as_ref(), accounting)
                        .await;
                    match lookup {
                        Some(lookup) => self.retire_lookup((lookup, allocation), accounting)?,
                        None => crate::kernel::retire_allocation(allocation, 0, accounting)?,
                    }
                    result?;
                }
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
        let (context, evidence) = publication_codec::publication_evidence(
            context,
            key.operation_id(),
            permit,
            accounting,
        )?;
        let source = Arc::clone(&self.source);
        let commit = 'accounted: {
            if let Some(original) = accounting.native_work() {
                let before = guard.clone();
                let mut callback = move |original: &crate::NativeMaterializationWork| {
                    let source = Arc::clone(&source);
                    let mut before = before.clone();
                    let original = original.clone();
                    async move {
                        if source.reference() != basis.source {
                            return Err(E::from(LazyWorkspaceError::StaleSource));
                        }
                        let mut work =
                            crate::AuthorityWork::native(&original, original.cancellation());
                        before.seal(&mut work).await.map_err(|error| match error {
                            crate::AuthorityError::Authority(error) => {
                                E::from(crate::FsError::from(error))
                            }
                            crate::AuthorityError::Owner(error) => error.into(),
                        })
                    }
                };
                break 'accounted transaction
                    .commit_with_native_work(
                        Some((context, evidence, permit)),
                        permit,
                        original,
                        cancellation(accounting),
                        &mut callback,
                    )
                    .await?;
            }
            let before = guard.clone();
            let cancellation = cancellation(accounting).clone();
            let receipt = transaction
                .commit(
                    Some((context, evidence, permit)),
                    permit,
                    accounting.remaining()?,
                    &cancellation.clone(),
                    move |budget| {
                        let source = Arc::clone(&source);
                        let mut before = before.clone();
                        let cancellation = cancellation.clone();
                        async move {
                            let mut work = crate::AuthorityWork::new(budget, &cancellation);
                            let result = if source.reference() != basis.source {
                                Err(E::from(LazyWorkspaceError::StaleSource))
                            } else {
                                before.seal(&mut work).await.map_err(|error| match error {
                                    crate::AuthorityError::Authority(error) => {
                                        E::from(crate::FsError::from(error))
                                    }
                                    crate::AuthorityError::Owner(error) => error.into(),
                                })
                            };
                            result
                                .map(|()| OperationReceipt {
                                    value: (),
                                    work: work.spent(),
                                })
                                .map_err(|error| crate::OperationFailure::new(error, work.spent()))
                        }
                    },
                )
                .await;
            let (result, spent) = match receipt {
                Ok(receipt) => (Ok(receipt.value), receipt.work),
                Err(failure) => (Err(failure.error), *failure.work),
            };
            let (allocations, work, budget, _) = accounting
                .standalone_parts()
                .ok_or(LazyWorkspaceError::Concurrent)?;
            *work = work
                .with_backend(spent, allocations.live_bytes())
                .map_err(LazyWorkspaceError::from)?;
            work.verify(budget).map_err(LazyWorkspaceError::from)?;
            result?
        };
        let snapshot = match &commit {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => {
                async {
                    seal::<_, E>(guard, accounting).await?;
                    self.selected_snapshot(key.operation_id(), generation.id(), accounting)
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
