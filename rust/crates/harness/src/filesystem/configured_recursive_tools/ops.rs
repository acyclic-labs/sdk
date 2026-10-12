use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io as fsio};

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn authenticate<'a>(
        &'a self,
        context: &ToolContext,
        project_read: bool,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<&'a ConfiguredRecursiveCaller> {
        check_cancellation(cancellation, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            let owner = self.source_owner(context.task(), cancellation, io).await?;
            let caller = self
                .authenticate_caller(context.task(), Some(&owner), project_read, io)
                .await?;
            let io = io.expect("native authentication observer");
            for volume in std::iter::once(&caller.private_volume)
                .chain(project_read.then_some(&caller.project))
            {
                crate::model::charge_items(Some(io), 1)?;
                if !io.contains_capability(
                    context.effective_scope().grants(),
                    volume,
                    "volume:read:",
                )? {
                    return Err(crate::executor::stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "native recursive tool effective scope denies its selected volume operation",
                    ));
                }
            }
            return Ok(caller);
        }
        self.authenticate_caller(context.task(), None, project_read, None)
            .await
    }
    pub(super) async fn read(
        &self,
        context: &ToolContext,
        file: &crate::conversation::FileRef,
        io: &PreparationIo,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return context.read_file_with_native_work(file, work).await;
        }
        io.receive_content(
            context
                .read_file_measured(file, io.remaining()?, io.cancellation())
                .await,
        )
    }
    pub(super) async fn source(
        &self,
        context: &ToolContext,
        caller: &ConfiguredRecursiveCaller,
        parent: Reducer,
        request: &ForkRequest,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<RetainedSourceBinding<A, O>> {
        let owner = self.source_owner(context.task(), cancellation, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut _selected_hold = None;
        let selected = match &request.authorized_source {
            Some(authorization) => {
                let selected = 'selection: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                        let (handle, custody) = owner
                            .restore_project_source_authorization_with_native_work(
                                context.task(),
                                authorization,
                                work,
                            )?
                            .await?;
                        _selected_hold = Some(custody);
                        break 'selection handle;
                    }
                    owner
                        .restore_project_source_authorization(context.task(), authorization)
                        .await?
                };
                Some(selected)
            }
            None => None,
        };
        let verifier =
            crate::executor::with_history_io(io, |work| caller.owner.issuer().verifier(work))?;
        let request = crate::executor::with_history_io(io, |work| request.copy(work))?.0;
        let resolver = self.resolver(context, owner, io)?;
        restore_recursive_fork_source(
            Arc::clone(&self.filesystem),
            &self.stream,
            parent,
            &verifier,
            &caller.owner.scope,
            &caller.project,
            &self.config.stream_provider,
            request,
            self.config.history_limits,
            selected.as_ref(),
            resolver,
            cancellation,
            io,
        )
        .await
    }

    pub(super) async fn open(
        &self,
        binding: &ConfiguredRecursiveAuthorityBinding,
        revision: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<StreamAggregate<P>> {
        let (authority, verifier) = crate::executor::with_history_io(io, |work| {
            let verifier = binding.issuer().verifier(work)?;
            verifier.verify_scope_for_with_work(binding.authority(), &binding.scope, work)?;
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(binding.authority())?,
                None => binding.authority().clone(),
            };
            Ok::<_, Error>((authority, verifier))
        })?;
        let mut history = io.map(PreparationIo::history_io);
        StreamAggregate::open(
            &self.stream,
            authority,
            verifier,
            SchemaRegistry::default(),
            None,
            self.config.history_limits,
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            revision,
            &mut history
                .as_mut()
                .map(|history| history as &mut dyn HistoryIo),
        )
        .await
    }
    pub(super) async fn project_generation(
        &self,
        volume: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<crate::resources::GenerationRef> {
        let reference_hold;
        let reference = fsio::content_workspace(volume.provider(), volume, io)?;
        reference_hold = io
            .map(|io| io.guard_owned(reference.logical_owned_bytes()?))
            .transpose()?;
        let observed = fsio::resolve(&self.filesystem, &reference, io).await?;
        let workspace_hold = io
            .map(|io| io.guard_owned(observed.workspace.logical_owned_bytes()?))
            .transpose()?;
        drop((observed.workspace, workspace_hold));
        drop(reference);
        drop(reference_hold);
        Ok(observed.generation)
    }
}
