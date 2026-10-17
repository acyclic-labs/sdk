use super::*;
use acyclic_fs::{
    GenerationId, Workspace,
    access::{AccessView, PathTrie},
    native_mount::{NativeAccessOwner, NativeMountCallbackFuture, NativeMountError},
};

struct PolicyOwner<A, O> {
    workspace: Workspace<A, O>,
    scope: Scope,
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
    admitted_ceiling: &crate::Capabilities,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let io = PreparationIo::from_native_work(work)?;
    let (compiled, custody) = compile(
        workspace,
        admitted_scope,
        admitted_ceiling,
        generation,
        writable,
        &io,
    )
    .await?;
    let view = AccessView::new_native(compiled, custody)?;
    let scope = admitted_scope.copy(&mut Some(&mut io.history_io()))?;
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
