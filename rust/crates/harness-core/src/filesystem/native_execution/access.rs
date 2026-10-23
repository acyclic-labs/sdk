use super::*;
use acyclic_fs::{
    GenerationId, Workspace,
    access::{AccessView, PathTrie},
    native_mount::{NativeAccessOwner, NativeMountCallbackFuture, NativeMountError},
};

struct PolicyOwner<A, O> {
    workspace: Workspace<A, O>,
    scope: Scope,
    verifier: AuthorityVerifier,
    ceiling: Option<(crate::Capabilities, NativeAllocation)>,
    generation: GenerationId,
    writable: bool,
    view: Arc<AccessView>,
    _custody: NativeAllocation,
}

async fn compile<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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
        None if writable => include_bytes!("../access/legacy-native.json"),
        None => crate::filesystem::access::capability_payload(None, VolumeOperation::Read, io)?,
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

impl<A, O> BorrowedNativeVolumeBinding<'_, A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Bind a native checkout to its verified scope and original caller ceiling.
    pub async fn mount_options(
        &self,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        context: &Arc<crate::runtime::TaskContext>,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::MountOptions> {
        let io = PreparationIo::from_native_work(work)?;
        for operation in [VolumeOperation::Read]
            .into_iter()
            .chain(self.writable.then_some(VolumeOperation::Write))
        {
            ContentGrant::authorize_volume(
                self.verifier,
                self.scope,
                self.volume,
                operation,
                context.scope().limits().file_bytes,
                &mut Some(&mut io.history_io()),
            )?;
            let admitted = match operation {
                VolumeOperation::Read => crate::core::directory_read_granted_with_work(
                    context.scope().grants(),
                    self.volume,
                    "",
                    &mut Some(&mut io.history_io()),
                )?,
                VolumeOperation::Write => crate::core::volume_write_granted_with_work(
                    context.scope().grants(),
                    self.volume,
                    &mut Some(&mut io.history_io()),
                )?,
            };
            if !admitted {
                return Err(io.invalid("native caller lacks volume authority"));
            }
        }
        if !self.host.filesystem.owns_workspace(workspace)
            || self.volume.storage_name(&mut Some(&mut io.history_io()))?
                != workspace.name().as_str()
        {
            return Err(io.invalid("native access differs from its authenticated volume"));
        }
        self.generation.validate(&mut Some(&mut io.history_io()))?;
        if self.generation.as_resource().provider() != self.volume.provider()
            || self.generation.as_resource().key() != generation.digest().as_bytes()
        {
            return Err(io.invalid("native access differs from its selected generation"));
        }
        bind_mount_options(
            workspace,
            generation,
            self.scope,
            self.verifier,
            context.scope().grants(),
            self.writable,
            work,
        )
        .await
    }
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
    issuer: &crate::core::AuthorityIssuer,
    agent: Option<crate::AgentId>,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: if writable {
                Action::ALL.into_iter().collect()
            } else {
                [Action::List, Action::Read, Action::ReadHistory]
                    .into_iter()
                    .collect()
            },
            resource: ResourcePattern {
                lineage_root: None,
                volume: Some(
                    uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string(),
                ),
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: None,
    };
    let grants = crate::Capabilities::new([capability
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]);
    let scope = issuer.issue("native-volume", &grants, agent, None, &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
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
    // This host owns the callback ledger; it has no borrowed native execution
    // whose physical input capture must be billed through an external lease.
    .map(|options| options.native_context(work, None, None))
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
    .access_refresh(refresh))
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

/// Check a verified scope and caller ceiling before exposing a raw workspace.
/// The caller must authenticate the scope before invoking this admission check.
pub async fn require_unrestricted_volume_access<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &acyclic_fs::Workspace<A, O>,
    volume: &VolumeRef,
    signed: &Scope,
    ceiling: &crate::Capabilities,
    surface: &'static str,
) -> Result<()> {
    use crate::filesystem::{PreparationIo, access};
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    for grants in [ceiling, signed.capabilities()] {
        if crate::core::filesystem_scope_label(grants, &mut None)?.is_none()
            && (!grants.contains(&volume.capability(VolumeOperation::Read, &mut None)?)
                || !grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?))
        {
            return Err(Error::FilesystemAccessConfiguration(surface));
        }
    }
    let label = crate::core::filesystem_scope_label(signed.capabilities(), &mut None)?;
    let payload = match label {
        Some(_) => access::capability_payload(label, VolumeOperation::Read, &io)?,
        None => include_bytes!("../access/legacy-checkout.json"),
    };
    let compiled =
        access::access_view(workspace, signed, None, payload, Some(ceiling), &io).await?;
    if compiled.0.generation_sensitive() || compiled.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted(&compiled.0, &acyclic_fs::access::Action::ALL, surface)
}

/// Verify a scope before exposing read-only raw checkout context.
pub async fn require_unrestricted_workspace_read<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &acyclic_fs::Workspace<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    io: &PreparationIo,
    surface: &'static str,
) -> Result<()> {
    use crate::filesystem::access;
    verifier.verify(scope, &mut Some(&mut io.history_io()))?;
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let view = access::access_view(
        workspace,
        scope,
        None,
        access::capability_payload(label, VolumeOperation::Read, io)?,
        None,
        io,
    )
    .await?;
    if view.0.generation_sensitive() || view.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted(
        &view.0,
        &[
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::List,
        ],
        surface,
    )
}

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
        let issuer = crate::core::AuthorityIssuer::new(
            "mount",
            [72; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "mount".into(),
            },
        );
        let scope = issuer.issue(
            "mount",
            &crate::Capabilities::new(["unused"]),
            None,
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
