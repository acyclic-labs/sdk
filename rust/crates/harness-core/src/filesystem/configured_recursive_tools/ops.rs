use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io as fsio};

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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
        let selected = match &request.authorized_source {
            Some(authorization) => Some(
                owner
                    .restore_project_source_authorization(context.task(), authorization, io)
                    .await?,
            ),
            None => None,
        };
        let verifier =
            crate::executor::with_history_io(io, |work| caller.owner.issuer.verifier(work))?;
        let request = crate::executor::with_history_io(io, |work| request.copy(work))?.0;
        let resolver = self.resolver(context, owner, io)?;
        let input = PreparationIo::temporary(io)?;
        restore_recursive_fork_source(
            Arc::clone(&self.filesystem),
            &self.stream,
            (parent, input),
            &verifier,
            &caller.owner.scope,
            &caller.project,
            &self.config.stream_provider,
            request,
            self.config.history_limits,
            selected.as_ref(),
            resolver,
            cancellation,
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
            let verifier = binding.issuer.verifier(work)?;
            verifier.verify_scope_for_with_work(binding.issuer.audience(), &binding.scope, work)?;
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(binding.issuer.audience())?,
                None => binding.issuer.audience().clone(),
            };
            Ok::<_, Error>((authority, verifier))
        })?;
        let mut history = io.map(PreparationIo::history_io);
        StreamAggregate::open(
            &self.stream,
            (authority, verifier, SchemaRegistry::default()),
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
        let (reference_hold, reference) = {
            let reference = fsio::content_workspace(volume.provider(), volume, io)?;
            let reference_hold = io
                .map(|io| io.guard_owned(reference.logical_owned_bytes()?))
                .transpose()?;
            (reference_hold, reference)
        };
        let observed = self.filesystem.resolve(&reference, io).await?;
        let workspace_hold = io
            .map(|io| io.guard_owned(observed.workspace.logical_owned_bytes()?))
            .transpose()?;
        drop((observed.workspace, workspace_hold));
        drop(reference);
        drop(reference_hold);
        Ok(observed.generation)
    }
}
