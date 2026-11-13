// Command data changes authority once, at its first mutation. Authorization
// continues to name the original volume; no new capability is minted for data.
struct CommandCheckout<A: 'static, O: 'static> {
    source: crate::Workspace<A, O>,
    writable: bool,
    access: Arc<crate::access::AccessView>,
    refresh: Option<Arc<dyn super::NativeAccessOwner>>,
    fork: StdMutex<Option<CommandFork<A, O>>>,
}

struct CommandFork<A: 'static, O: 'static> {
    origin: crate::GenerationId,
    policy: crate::kernel::MetadataField<ObjectId>,
    workspace: crate::Workspace<A, O>,
    custody: Option<crate::EphemeralFork>,
}

impl<A: 'static, O: 'static> CheckoutMountSource<A, O> {
    pub(in crate::native_mount) fn configure_command(
        source: &mut Arc<Self>,
        workspace: crate::Workspace<A, O>,
        writable: bool,
    ) -> Result<(), MountSourceError> {
        // Admission still owns both unique values. Configuration cannot change
        // the meaning of a source already used by a callback or reader.
        let source = Arc::get_mut(source).ok_or(MountSourceError::Stale)?;
        let shared = Arc::get_mut(&mut source.checkout).ok_or(MountSourceError::Stale)?;
        if shared.command.is_some()
            || workspace.id().volume_id() != shared.volume_id
            || shared.state.get_mut().publication != MountPublication::Manual
        {
            return Err(MountSourceError::AccessDenied);
        }
        shared.command = Some(CommandCheckout {
            source: workspace,
            writable,
            access: source
                .access_view
                .clone()
                .ok_or(MountSourceError::AccessDenied)?,
            refresh: source.access_refresh.clone(),
            fork: StdMutex::new(None),
        });
        Ok(())
    }
}

impl<A: 'static, O: 'static> SharedCheckout<A, O> {
    fn authorization_generation(
        &self,
        checkout: &Checkout<A, O>,
    ) -> Result<crate::GenerationId, MountSourceError> {
        if checkout.volume_id() == self.volume_id {
            return Ok(checkout.generation_id());
        }
        let command = self
            .command
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?;
        let fork = command.fork.lock().unwrap_or_else(PoisonError::into_inner);
        match fork.as_ref() {
            Some(fork) if fork.workspace.id().volume_id() == checkout.volume_id() => {
                Ok(fork.origin)
            }
            _ => Err(MountSourceError::AccessDenied),
        }
    }
}

fn command_error(error: crate::WorkspaceError) -> MountSourceError {
    match error {
        crate::WorkspaceError::Filesystem(error) => error.into(),
        crate::WorkspaceError::Work(error) => MountSourceError::Work(error),
        crate::WorkspaceError::Cancelled(_) => MountSourceError::Cancelled,
        crate::WorkspaceError::AccessDenied => MountSourceError::AccessDenied,
        error => engine_error(error),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn promote_command(
        &self,
        state: &mut SharedCheckoutState<A, O>,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError> {
        let Some(command) = &self.command else {
            return Ok(());
        };
        if !command.writable {
            return Err(MountSourceError::ReadOnly);
        }
        if command
            .fork
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return Ok(());
        }
        // The existing checkout gate serializes promotion, including the first
        // writes through different open handles. Nothing is installed until
        // the exact fork and its checkout are both complete.
        native_future(work, || async {
            let origin = state.checkout.generation_id();
            let policy = state
                .checkout
                .access_policy_revision(work, cancellation)
                .await?;
            let generation = {
                let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
                let observed = state
                    .checkout
                    .private_candidate(&mut accounting, cancellation)?;
                crate::Generation::observed(&command.source, observed, &mut accounting)
                    .map_err(command_error)?
            };
            let key = crate::IdempotencyKey::new();
            let mut name = [0_u8; 32];
            hex::encode_to_slice(key.into_bytes(), &mut name)
                .map_err(|_| MountSourceError::OriginalSnapshotUnavailable)?;
            let name = std::str::from_utf8(&name)
                .map_err(|_| MountSourceError::OriginalSnapshotUnavailable)?;
            let (workspace, custody) = command
                .source
                .fork_ephemeral(
                    name,
                    crate::ForkOptions::from_generation(generation, key),
                    work,
                    cancellation,
                )
                .await
                .map_err(command_error)?;
            let checkout = workspace
                .checkout(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    work,
                    cancellation,
                )
                .await
                .map_err(command_error)?;
            *command.fork.lock().unwrap_or_else(PoisonError::into_inner) = Some(CommandFork {
                origin,
                policy,
                workspace,
                custody: Some(custody),
            });
            state.checkout = checkout;
            Ok(())
        })
        .map_err(MountSourceError::from)?
        .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    pub(in crate::native_mount) async fn apply_command_join(
        self: &Arc<Self>,
        plan: &crate::JoinPlan<A, O>,
        options: crate::ApplyOptions,
        candidate: Option<&crate::UnpublishedMergeCandidate>,
    ) -> Result<
        (crate::JoinOutcome<A, O>, crate::NativeAllocation),
        super::super::MountLifecycleError,
    > {
        let command = self
            .checkout
            .command
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?;
        if !self.cancellation.is_cancelled()
            || plan.target().id() != command.source.id()
            || !plan
                .target()
                .volume
                .fs
                .same_deployment(&command.source.volume.fs)
        {
            return Err(MountSourceError::AccessDenied.into());
        }
        let (origin, policy) = {
            let fork = command.fork.lock().unwrap_or_else(PoisonError::into_inner);
            let fork = fork.as_ref().ok_or(MountSourceError::AccessDenied)?;
            if fork.custody.is_some() || plan.source().id() != fork.workspace.id() {
                return Err(MountSourceError::AccessDenied.into());
            }
            (fork.origin, fork.policy)
        };
        let work = self.operation_work()?;
        let source = Arc::clone(self);
        let mut seal = move |original: &crate::NativeMaterializationWork| {
            let source = Arc::clone(&source);
            let original = original.clone();
            async move {
                source
                    .validate_command_publication(origin, policy, &original)
                    .await
                    .map_err(super::super::MountLifecycleError::from)
            }
        };
        let (outcome, allocation) = match candidate {
            Some(candidate) => {
                plan.apply_candidate_with_native_work(
                    options,
                    candidate,
                    crate::PublicationPermit::Unrestricted,
                    &work,
                    work.cancellation(),
                    &mut seal,
                )
                .await?
            }
            None => {
                plan.apply_with_native_work(
                    options,
                    &[],
                    crate::PublicationPermit::Unrestricted,
                    &work,
                    work.cancellation(),
                    &mut seal,
                )
                .await?
            }
        };
        Ok((outcome, allocation))
    }

    pub(in crate::native_mount) fn is_private_command(&self) -> bool {
        self.checkout.command.is_some()
    }

    pub(in crate::native_mount) async fn finish_retired_command(
        &self,
    ) -> Result<Option<(crate::Workspace<A, O>, crate::EphemeralFork)>, MountSourceError> {
        let command = self
            .checkout
            .command
            .as_ref()
            .ok_or(MountSourceError::Invalid(
                "source is not a private command".into(),
            ))?;
        if !self.cancellation.is_cancelled() {
            return Err(MountSourceError::Stale);
        }
        let selected = command
            .fork
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|fork| (fork.origin, fork.policy));
        let Some((origin, policy)) = selected else {
            return Ok(None);
        };
        let work = self.operation_work()?;
        self.validate_command_publication(origin, policy, &work)
            .await?;
        // Retirement has drained every callback. The view's cancellation now
        // forbids new work, while this verified publication retains the
        // original work and callback guard without that retired lifetime.
        self.publish_async(crate::PublicationPermit::Unrestricted, None)
            .await?;
        let mut fork = command.fork.lock().unwrap_or_else(PoisonError::into_inner);
        let fork = fork.as_mut().ok_or(MountSourceError::Stale)?;
        let custody = fork.custody.take().ok_or(MountSourceError::Stale)?;
        Ok(Some((fork.workspace.clone(), custody)))
    }

    async fn validate_command_publication(
        &self,
        origin: crate::GenerationId,
        policy: crate::kernel::MetadataField<ObjectId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let command = self
            .checkout
            .command
            .as_ref()
            .ok_or(MountSourceError::AccessDenied)?;
        let view = access::policy_snapshot(
            &self.checkout,
            &command.access,
            command.refresh.as_ref(),
            &self.runtime.clock,
            work.cancellation(),
            work,
        )
        .await?;
        if view
            .data_generation()
            .is_some_and(|selected| selected != origin)
            || !view.matches_policy_revision(origin, policy)
        {
            return Err(MountSourceError::AccessDenied);
        }
        Ok(())
    }
}
