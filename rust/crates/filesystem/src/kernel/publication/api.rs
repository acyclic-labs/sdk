use super::*;

/// Authenticates an immutable generation's complete closure, then atomically submits it to the volume authority.
pub async fn publish_generation<O: crate::AsyncObjectStore, A: crate::AsyncAuthorityStore>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_with_permit(
        objects,
        authority,
        request,
        PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
        closure_limits,
        budget,
        cancellation,
    )
    .await
}

pub(crate) async fn publish_generation_async_with_permit<
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_guarded(
        objects,
        authority,
        request,
        intent,
        closure_limits,
        budget,
        cancellation,
        &mut |_| async {
            Ok::<_, OperationFailure<crate::FsError>>(crate::OperationReceipt {
                value: (),
                work: WorkCounters::UNCHARGED,
            })
        },
        None,
        None,
    )
    .await
    .map_err(|failure| OperationFailure::new(publication_error(failure.error), *failure.work))
}

pub(crate) async fn publish_generation_async_guarded<O, A, E, F, Fut>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    before_publication: &mut F,
    resolved: Option<crate::foundation::DurableCommit>,
    mut prepare_roots: Option<PublicationRootPreparation<'_, E>>,
) -> Result<PublicationReceipt, OperationFailure<E>>
where
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
    E: From<crate::FsError> + Send,
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::MeasuredResult<crate::OperationReceipt<()>, E>> + ProviderTask,
{
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let mut seal = |allowance: Option<WorkBudget>| {
        let future = before_publication(
            allowance.expect("the ordinary engine supplies its remaining budget"),
        );
        async move {
            future
                .await
                .map(|receipt| Some(receipt.work))
                .map_err(|failure| PublicationStepFailure {
                    cause: failure.error,
                    work: Some(*failure.work),
                })
        }
    };
    let mut dispatch = |request: GuardedAppend, allowance: Option<WorkBudget>| {
        let future = async move {
            let __authority_args = (
                request,
                allowance.expect("the ordinary engine supplies its remaining budget"),
                cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
            let __authority_result = authority
                .compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>)
                .await
                .map_err(|error| error.into_error(std::convert::identity));
            __authority_work.receipt(__authority_result)
        };
        async move {
            future
                .await
                .map(|receipt| (receipt.value, Some(receipt.work)))
                .map_err(|failure| PublicationStepFailure {
                    cause: E::from(crate::FsError::Authority(failure.error)),
                    work: Some(*failure.work),
                })
        }
    };
    let mut roots = |root: &crate::kernel::GenerationRoot,
                     _: &mut ProofGuard,
                     accounting: &mut KernelAccounting<'_>| {
        if let Some(prepare) = prepare_roots.as_mut() {
            let result = prepare(root, accounting.remaining().map_err(E::from)?)
                .map(|receipt| (receipt.value, Some(receipt.work)))
                .map_err(|failure| PublicationStepFailure {
                    cause: failure.error,
                    work: Some(*failure.work),
                });
            let bytes = accept_step(accounting, result)?;
            retain(accounting, bytes).map_err(E::from)?;
        }
        Ok(())
    };
    let result = publish_generation_async_inner(
        objects,
        request,
        intent,
        closure_limits,
        cancellation,
        &mut accounting,
        &mut seal,
        &mut dispatch,
        resolved,
        &mut roots,
    )
    .await;
    result
        .map(|output| output.0)
        .map_err(|error| OperationFailure::new(error, accounting.spent()))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_publication_owner<E, F, Fut>(
    original: crate::NativeMaterializationWork,
    mut factory: F,
) -> impl FnMut(
    &crate::NativeMaterializationWork,
) -> futures::future::Either<
    std::future::Ready<Result<(), E>>,
    acyclic_stream::BoxProviderFuture<'static, Result<(), E>>,
> + Clone
+ Send
+ 'static
where
    E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    move |work| {
        let admitted = if original.shares_ledger(work) {
            original
                .admit(WorkCounters::UNCHARGED)
                .map_err(crate::FsError::from)
                .map_err(E::from)
        } else {
            Err(E::from(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )))
        };
        match admitted {
            Ok(()) => {
                let boxed = crate::heap_future::in_heap_admitted(
                    None,
                    |allocation, bytes| {
                        *allocation = Some(
                            original
                                .claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)
                                .map_err(crate::FsError::from)
                                .map_err(E::from)?,
                        );
                        Ok::<(), E>(())
                    },
                    |allocation| {
                        let future = factory(work);
                        async move {
                            let _allocation = allocation;
                            future.await
                        }
                    },
                );
                match boxed {
                    Ok((future, _)) => futures::future::Either::Right(future),
                    Err(error) => futures::future::Either::Left(std::future::ready(Err(error))),
                }
            }
            Err(error) => futures::future::Either::Left(std::future::ready(Err(error))),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn publish_generation_async_guarded_with_native_work<'a, O, A, E, F, Fut>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    original: &'a crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    before_publication: &mut F,
    resolved: Option<crate::foundation::DurableCommit>,
    mut prepare_roots: Option<NativePublicationRootPreparation<'_, E>>,
) -> Result<(PublicationReceipt, crate::NativeAllocation), E>
where
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
    E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    let mut accounting = KernelAccounting::native(original, cancellation);
    let owned_owner = native_publication_owner(original.clone(), before_publication.clone());
    let mut dispatch = move |request: GuardedAppend, _: Option<WorkBudget>| {
        let mut guard = crate::NativeAuthorityGuard(owned_owner.clone());
        async move {
            let mut work = crate::AuthorityWork::native(original, cancellation);
            let outcome = authority
                .compare_and_append(request, &mut work, Some(&mut guard))
                .await
                .map_err(|error| PublicationStepFailure {
                    cause: error.into_error(|error| E::from(native_authority_error(error))),
                    work: None,
                })?;
            Ok((outcome, None))
        }
    };
    let mut seal = |_| {
        let future = before_publication(original);
        async move {
            future
                .await
                .map(|()| None)
                .map_err(|cause| PublicationStepFailure { cause, work: None })
        }
    };
    let mut roots = |root: &crate::kernel::GenerationRoot,
                     guard: &mut ProofGuard,
                     _: &mut KernelAccounting<'_>| {
        if let Some(prepare) = prepare_roots.as_mut() {
            match guard {
                ProofGuard::Native(guard) => prepare(root, guard, original)?,
                ProofGuard::Standalone { .. } => {
                    unreachable!("native preparation owns a native proof")
                }
            }
        }
        Ok(())
    };
    let mut output = publish_generation_async_inner(
        objects,
        request,
        intent,
        closure_limits,
        cancellation,
        &mut accounting,
        &mut seal,
        &mut dispatch,
        resolved,
        &mut roots,
    )
    .await?;
    match &mut output.1 {
        ProofGuard::Native(guard) => {
            let allocation = guard
                .split_off(guard.bytes())
                .map_err(crate::FsError::from)
                .map_err(E::from)?;
            Ok((output.0, allocation))
        }
        ProofGuard::Standalone { .. } => {
            unreachable!("original proof retains its original guard")
        }
    }
}
