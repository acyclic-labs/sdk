use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Forks from the first durable cut and returns the original child snapshot.
    pub async fn fork(
        &self,
        destination: &str,
        operation: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        let (name, id) = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .volume
                    .fs
                    .workspace_name_identity_with_native_work(destination, original, cancellation)
                    .map_err(|(error, allocation)| {
                        LazyWorkspaceError::NativeWorkspace(error, allocation)
                    })?
            } else {
                accept_receipt(
                    self.workspace.volume.fs.workspace_name_identity(
                        destination,
                        accounting.remaining()?,
                        cancellation,
                    ),
                    accounting,
                )?
            }
            #[cfg(target_arch = "wasm32")]
            {
                accept_receipt(
                    self.workspace.volume.fs.workspace_name_identity(
                        destination,
                        accounting.remaining()?,
                        cancellation,
                    ),
                    accounting,
                )?
            }
        };
        drop(name);
        let frozen = if let Some(state) = self.store.load_lazy_workspace(id, accounting).await? {
            self.fork_origin(&state, id, operation.operation_id(), accounting)?
                .snapshot
        } else {
            self.snapshot(accounting).await?
        };
        self.fork_snapshot(destination, frozen, operation, accounting)
            .await
    }

    pub(super) async fn fork_snapshot(
        &self,
        destination: &str,
        frozen: LazySnapshotRef,
        operation: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        let frozen = self
            .prepare_fork_snapshot(destination, frozen, operation.operation_id(), accounting)
            .await?;
        let cancellation = cancellation(accounting);
        let generation = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .generation(frozen.authored_generation, original, cancellation)
                    .await?
            } else {
                accept_receipt(
                    self.workspace
                        .generation(
                            frozen.authored_generation,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await,
                    accounting,
                )?
            }
            #[cfg(target_arch = "wasm32")]
            {
                accept_receipt(
                    self.workspace
                        .generation(
                            frozen.authored_generation,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await,
                    accounting,
                )?
            }
        };
        let options = ForkOptions::from_generation(generation, operation);
        let child = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .fork_with_native_work(
                        destination,
                        options,
                        crate::PublicationPermit::Unrestricted,
                        original,
                        cancellation,
                        &mut |_| async { Ok::<(), LazyWorkspaceError>(()) },
                    )
                    .await?
            } else {
                accept_receipt(
                    self.workspace
                        .fork(
                            destination,
                            options,
                            crate::PublicationPermit::Unrestricted,
                            accounting.remaining()?,
                            cancellation,
                            |_| async {
                                Ok::<_, crate::OperationFailure<crate::FsError>>(OperationReceipt {
                                    value: (),
                                    work: WorkCounters::UNCHARGED,
                                })
                            },
                        )
                        .await,
                    accounting,
                )?
            }
            #[cfg(target_arch = "wasm32")]
            {
                accept_receipt(
                    self.workspace
                        .fork(
                            destination,
                            options,
                            crate::PublicationPermit::Unrestricted,
                            accounting.remaining()?,
                            cancellation,
                            |_| async {
                                Ok::<_, crate::OperationFailure<crate::FsError>>(OperationReceipt {
                                    value: (),
                                    work: WorkCounters::UNCHARGED,
                                })
                            },
                        )
                        .await,
                    accounting,
                )?
            }
        };
        let observed = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .volume
                    .fs
                    .workspace_for_creation_operation_with_native_work(
                        child.name.as_str(),
                        child.volume.config,
                        operation.operation_id(),
                        original,
                        cancellation,
                    )
                    .await
                    .map_err(|(error, allocation)| {
                        LazyWorkspaceError::NativeWorkspace(error, allocation)
                    })?
            } else {
                accept_receipt(
                    self.workspace
                        .volume
                        .fs
                        .workspace_for_creation_operation(
                            child.name.as_str(),
                            child.volume.config,
                            operation.operation_id(),
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await,
                    accounting,
                )?
            }
            #[cfg(target_arch = "wasm32")]
            {
                accept_receipt(
                    self.workspace
                        .volume
                        .fs
                        .workspace_for_creation_operation(
                            child.name.as_str(),
                            child.volume.config,
                            operation.operation_id(),
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await,
                    accounting,
                )?
            }
        }
        .ok_or(LazyWorkspaceError::Concurrent)?;
        let original = observed.id();
        drop(observed);
        self.bind_prepared_fork(
            frozen,
            child,
            original,
            operation.operation_id(),
            accounting,
        )
        .await
    }
}
