use super::*;

pub(super) async fn authorize_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    grant: &ContentGrant,
    generation: GenerationId,
    io: &PreparationIo,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<()> {
    let access = compile(
        workspace,
        grant,
        Some(generation),
        VolumeOperation::Read,
        &[
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::List,
            acyclic_fs::access::Action::Fork,
        ],
        io,
        clock,
    )
    .await?;
    super::super::access::require_current(workspace, &access.0, io, clock).await
}

pub(super) async fn compile<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    grant: &ContentGrant,
    generation: Option<GenerationId>,
    operation: VolumeOperation,
    actions: &[acyclic_fs::access::Action],
    io: &'a PreparationIo,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<super::super::access::CompiledAccess<'a>> {
    grant.recheck(Some(io))?;
    let scope = grant.policy_scope();
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let view = super::super::access::access_view(
        workspace,
        scope,
        generation,
        super::super::access::capability_payload(label, operation, io)?,
        None,
        io,
        super::super::access::clock_now(clock)?,
    )
    .await?;
    super::super::access::require_unrestricted(&view.0, actions, "whole-volume copy", clock)?;
    if view.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(
            "whole-volume copy requires non-expiring authority",
        ));
    }
    grant.recheck(Some(io))?;
    Ok(view)
}
