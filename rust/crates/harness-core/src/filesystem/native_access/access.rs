use super::*;
use acyclic_fs::{
    GenerationId, Workspace,
    access::{AccessView, PathTrie},
    native_mount::{NativeAccessOwner, NativeMountCallbackFuture, NativeMountError},
};

struct PolicyOwner<A: 'static, O: 'static> {
    workspace: Workspace<A, O>,
    scope: Scope,
    verifier: AuthorityVerifier,
    ceiling: Option<(crate::Capabilities, NativeAllocation)>,
    generation: GenerationId,
    writable: bool,
    view: Arc<AccessView>,
    _custody: NativeAllocation,
}

async fn compile<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<A, O>,
    scope: &Scope,
    ceiling: &crate::Capabilities,
    generation: GenerationId,
    writable: bool,
    io: &PreparationIo,
) -> Result<(PathTrie, NativeAllocation)> {
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let payload = match label {
        Some(_) => crate::filesystem::access::capability_payload(label, VolumeOperation::Read, io)?,
        None if writable => crate::filesystem::access::whole_volume_payload(
            acyclic_fs::access::ActionSet::MountWrite,
        )?,
        None => crate::filesystem::access::whole_volume_payload(
            acyclic_fs::access::ActionSet::MountRead,
        )?,
    };
    let (view, holds, _) = crate::filesystem::access::access_view(
        workspace,
        scope,
        if writable { None } else { Some(generation) },
        payload,
        Some(ceiling),
        io,
    )
    .await?;
    let mut custody = io
        .original_native_work()
        .ok_or_else(|| io.invalid("native access lost its original ledger"))?
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    for hold in holds.into_iter().flatten() {
        let (_, mut held) = crate::filesystem::native_decoded(((), hold))?;
        custody.absorb(&mut held)?;
    }
    Ok((view, custody))
}

/// Authenticated identity and caller ceiling for an owned native mount.
pub struct NativeAccessAuthority<'a> {
    /// Verifies the complete presented scope.
    pub verifier: &'a AuthorityVerifier,
    /// Signed, attenuable filesystem capability.
    pub scope: &'a Scope,
    /// Original caller's authority ceiling.
    pub ceiling: &'a crate::Capabilities,
}

/// Bind a host owner's exact-volume grant through the shared policy compiler.
pub async fn owner_mount_options<A, O>(
    workspace: &Workspace<A, O>,
    issuer: &crate::core::AuthorityRoot,
    agent: Option<crate::AgentId>,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let grants = mount_capabilities(workspace, writable)?;
    let io = PreparationIo::from_native_work(work)?;
    let scope = issuer.bootstrap_scope(
        "native-volume",
        &grants,
        agent,
        &mut Some(&mut io.history_io()),
    )?;
    let verifier = issuer.verifier(&mut Some(&mut io.history_io()))?;
    let generation = workspace.head(work, work.cancellation()).await?.id();
    authenticated_mount_options(
        workspace,
        generation,
        NativeAccessAuthority {
            verifier: &verifier,
            scope: &scope,
            ceiling: &grants,
        },
        writable,
        work,
    )
    .await
}

pub(super) fn mount_capabilities<A, O>(
    workspace: &Workspace<A, O>,
    writable: bool,
) -> Result<crate::Capabilities>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    use acyclic_fs::access::{ActionSet, Capability};
    let actions = if writable {
        ActionSet::MountWrite
    } else {
        ActionSet::MountRead
    };
    let capability = Capability::whole_volume(
        Some(uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string()),
        actions.actions(),
    );
    Ok(crate::Capabilities::new([capability
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]))
}

/// Compile persisted policy and retain the same authenticated refresh owner.
pub async fn authenticated_mount_options<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    authority: NativeAccessAuthority<'_>,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let io = PreparationIo::from_native_work(work)?;
    authority
        .verifier
        .verify(authority.scope, &mut Some(&mut io.history_io()))?;
    if crate::core::filesystem_scope_label(
        authority.scope.capabilities(),
        &mut Some(&mut io.history_io()),
    )?
    .is_none()
    {
        return Err(io.invalid("workspace mount requires an exact signed filesystem capability"));
    }
    bind_mount_options(
        workspace,
        generation,
        authority.scope,
        authority.verifier,
        authority.ceiling,
        writable,
        work,
    )
    .await
}

async fn bind_mount_options<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    admitted_scope: &Scope,
    verifier: &AuthorityVerifier,
    admitted_ceiling: &crate::Capabilities,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let io = PreparationIo::from_native_work(work)?;
    verifier.verify(admitted_scope, &mut Some(&mut io.history_io()))?;
    let (compiled, custody) = compile(
        workspace,
        admitted_scope,
        admitted_ceiling,
        generation,
        writable,
        &io,
    )
    .await?;
    verifier.verify(admitted_scope, &mut Some(&mut io.history_io()))?;
    let view = AccessView::new_native(compiled, custody)?;
    let scope = admitted_scope.copy(&mut Some(&mut io.history_io()))?;
    let verifier = verifier.copy(&mut Some(&mut io.history_io()))?;
    io.retain_owned(
        (std::mem::size_of::<PolicyOwner<A, O>>() + 2 * std::mem::size_of::<usize>()) as u64,
    )?;
    io.allocation_attempt(
        (std::mem::size_of::<PolicyOwner<A, O>>() + 2 * std::mem::size_of::<usize>()) as u64,
    )?;
    let ceiling = (admitted_ceiling != admitted_scope.capabilities())
        .then(|| crate::Capabilities::copy(admitted_ceiling.iter(), work))
        .transpose()?;
    let retained = io.owned_bytes()?;
    let refresh = Arc::new(PolicyOwner {
        workspace: workspace.clone(),
        scope,
        verifier,
        ceiling,
        generation,
        writable,
        view: Arc::clone(&view),
        _custody: io.into_native_custody(retained)?,
    });
    Ok((if writable {
        acyclic_fs::MountOptions::read_write()
    } else {
        acyclic_fs::MountOptions::read_only()
    })
    .access_view(view)
    .access_refresh(refresh.clone())
    .native_context(work, Some(refresh), Some(work)))
}

impl<A, O> acyclic_fs::native_mount::NativeMountCallbackGuard for PolicyOwner<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, move || async move {
            if !self._custody.work().shares_ledger(work) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native source guard changed its original ledger",
                });
            }
            let io = PreparationIo::from_native_work(work)?;
            self.verifier
                .verify(&self.scope, &mut Some(&mut io.history_io()))
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        if !self.writable {
            return Err(acyclic_fs::native_mount::MountSourceError::AccessDenied.into());
        }
        acyclic_fs::native_mount::NativeMountCallbackGuard::check(self, work)
    }
}

impl<A, O> NativeAccessOwner for PolicyOwner<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, move || async move {
            if !self._custody.work().shares_ledger(work) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native policy refresh changed its ledger",
                });
            }
            let io = PreparationIo::from_native_work(work)?;
            self.verifier
                .verify(&self.scope, &mut Some(&mut io.history_io()))?;
            let (view, custody) = compile(
                &self.workspace,
                &self.scope,
                self.ceiling
                    .as_ref()
                    .map_or(self.scope.capabilities(), |(grants, _)| grants),
                self.generation,
                self.writable,
                &io,
            )
            .await?;
            self.verifier
                .verify(&self.scope, &mut Some(&mut io.history_io()))?;
            self.view.publish_native(view, custody)?;
            Ok(())
        })
    }
    fn select<'a>(
        &'a self,
        generation: GenerationId,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<
        NativeMountCallbackFuture<'a, acyclic_fs::MountOptions>,
        NativeMountError,
    > {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, move || async move {
            if !self._custody.work().shares_ledger(work) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native selection changed its ledger",
                });
            }
            bind_mount_options(
                &self.workspace,
                generation,
                &self.scope,
                &self.verifier,
                self.ceiling
                    .as_ref()
                    .map_or(self.scope.capabilities(), |(grants, _)| grants),
                self.writable,
                work,
            )
            .await
        })
    }
}

pub use crate::filesystem::workspace_access::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod workload_tests {
    use super::*;

    #[tokio::test]
    async fn retained_mount_owner_rejects_revocation_on_refresh_and_selection()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let fs = acyclic_fs::Fs::memory();
        let workspace = fs
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "retained-mount",
                acyclic_fs::VolumeInitialization {
                    config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::new(),
                &work,
                &cancellation,
                None,
            )
            .await?
            .workspace();
        let generation = workspace.head(&work, &cancellation).await?.id();
        let issuer = crate::core::AuthorityRoot::new(
            "mount",
            [72; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "mount".into(),
            },
        );
        let scope = issuer.bootstrap_scope(
            "mount",
            &crate::Capabilities::new(["unused"]),
            None,
            &mut None,
        )?;
        let verifier = issuer.verifier(&mut None)?;
        let options = bind_mount_options(
            &workspace,
            generation,
            &scope,
            &verifier,
            scope.capabilities(),
            false,
            &work,
        )
        .await?;
        // Both callbacks use the retained public verifier, with no root signer.
        let refresh = PolicyOwner {
            workspace: workspace.clone(),
            scope: scope.clone(),
            verifier,
            ceiling: None,
            generation,
            writable: false,
            view: options.require_access_view()?.clone(),
            _custody: work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?,
        };
        refresh.check(&work)?.await?;
        issuer.revoke_workload(&scope)?;
        assert!(refresh.check(&work)?.await.is_err());
        assert!(refresh.select(generation, &work)?.await.is_err());
        Ok(())
    }
}
