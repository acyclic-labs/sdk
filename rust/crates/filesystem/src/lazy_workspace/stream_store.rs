//! Durable sparse companions on the authored filesystem's original authority.
use super::*;
use crate::distributed::{
    AuthorityProvider, AuthorizedObservation,
    state_record::{CompanionKind, StateRecord},
};
use crate::kernel::{MetadataDecodeError, NativeAccounting, OwnedSourceChunk, Retained};
use crate::{AuthorityWork, StreamAuthorityStore, StreamStateStoreError};
use acyclic_stream::StreamPath;
use serde::de::DeserializeOwned;

fn failure(error: impl Into<StreamStateStoreError>) -> LazyWorkspaceError {
    LazyWorkspaceError::StatePublication(Box::new(error.into()))
}
fn incompatible() -> LazyWorkspaceError {
    failure(StreamStateStoreError::Incompatible)
}
fn path(
    family: &str,
    id: &[u8],
    ctx: &KernelAccounting<'_>,
) -> Result<StreamPath, LazyWorkspaceError> {
    let mut encoded = [0; 64];
    let encoded = encoded
        .get_mut(..id.len().checked_mul(2).ok_or_else(incompatible)?)
        .ok_or_else(incompatible)?;
    hex::encode_to_slice(id, encoded).map_err(|_| incompatible())?;
    let encoded = std::str::from_utf8(encoded).map_err(|_| incompatible())?;
    let work = original(ctx)?;
    StreamPath::new_native_parts(
        &["fs/sparse-v1/", family, "/", encoded],
        &work.runtime_meter(),
    )
    .map_err(|error| LazyWorkspaceError::from(work.runtime_error(error)))?
    .map_err(failure)
}
fn original(
    ctx: &KernelAccounting<'_>,
) -> Result<crate::NativeMaterializationWork, LazyWorkspaceError> {
    ctx.native_work()
        .cloned()
        .ok_or(LazyWorkspaceError::NativeLedgerMismatch)
}

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    async fn read_sparse<T: DeserializeOwned>(
        &self,
        path: StreamPath,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Option<Retained<T>>, AuthorizedObservation), LazyWorkspaceError> {
        let original = original(ctx)?;
        let observed = self
            .observe_record(
                path,
                &mut AuthorityWork::native(&original, original.cancellation()),
            )
            .await
            .map_err(failure)?;
        let value = match observed.record {
            None => None,
            Some(record) => {
                let recorded = StateRecord::decode(record.value).map_err(failure)?;
                let StateRecord::Companion {
                    kind: CompanionKind::SparseWorkspace,
                    payload,
                } = recorded.state
                else {
                    return Err(incompatible());
                };
                Some(ctx.decode_json(&payload).map_err(|error| match error {
                    MetadataDecodeError::Work(error) => error.into(),
                    MetadataDecodeError::Codec(error) => failure(error),
                })?)
            }
        };
        Ok((value, observed.observation))
    }

    async fn publish_sparse<T: Serialize + Sync>(
        &self,
        observation: AuthorizedObservation,
        dependencies: &[AuthorizedObservation],
        value: &T,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let payload = OwnedSourceChunk::produce(ctx, |sink| {
            serde_json::to_writer(sink, value).map_err(failure)
        })?;
        let original = original(ctx)?;
        self.publish_companion(
            &observation,
            dependencies,
            StateRecord::Companion {
                kind: CompanionKind::SparseWorkspace,
                payload: payload.bytes,
            },
            crate::PublicationHold::none(),
            &mut AuthorityWork::native(&original, original.cancellation()),
        )
        .await
        .map_err(failure)
    }
    async fn sparse_node<T: MemoryLazyNode + DeserializeOwned>(
        &self,
        family: &str,
        id: [u8; 32],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(T, KernelOwnedAllocation)>, LazyWorkspaceError> {
        let (value, _) = self.read_sparse::<T>(path(family, &id, ctx)?, ctx).await?;
        let Some(mut value) = value else {
            return Ok(None);
        };
        let (mut allocation, _) = value.allocation.take_owned(ctx)?;
        value.value.retain_decoded(&mut allocation, ctx)?;
        let actual = value.value.owned_bytes();
        let KernelOwnedAllocation::NativeOriginal(backing) = &mut allocation else {
            return Err(LazyWorkspaceError::NativeLedgerMismatch);
        };
        backing
            .reconcile_capacity(actual)
            .map_err(crate::FsError::from)?;
        if hash_lazy_value(&value.value, ctx)? != id {
            drop(value);
            ctx.release_owned(&mut allocation, actual)?;
            return Err(incompatible());
        }
        Ok(Some((value.value, allocation)))
    }

    async fn put_sparse_node<T: MemoryLazyNode + DeserializeOwned>(
        &self,
        family: &str,
        id: [u8; 32],
        value: &T,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        if hash_lazy_value(value, ctx)? != id {
            return Err(incompatible());
        }
        let (current, observation) = self.read_sparse::<T>(path(family, &id, ctx)?, ctx).await?;
        if let Some(current) = current {
            return if &current.value == value {
                Ok(())
            } else {
                Err(incompatible())
            };
        }
        if self.publish_sparse(observation, &[], value, ctx).await? {
            Ok(())
        } else {
            Err(LazyWorkspaceError::Concurrent)
        }
    }
}

impl<P: AuthorityProvider + ?Sized + 'static> StreamAuthorityStore<P> {
    async fn load_sparse_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        let (state, _) = self
            .read_sparse::<LazyWorkspaceState>(path("bindings", &id.into_bytes(), ctx)?, ctx)
            .await?;
        let Some(mut state) = state else {
            return Ok(None);
        };
        if state.workspace_id != id || state.schema_version != LAZY_STATE_SCHEMA {
            return Err(incompatible());
        }
        let (mut allocation, _) = state.allocation.take_owned(ctx)?;
        state.value.source.retain_decoded(&mut allocation, ctx)?;
        if let Some(origin) = &mut state.value.fork_origin {
            origin
                .snapshot
                .source
                .retain_decoded(&mut allocation, ctx)?;
        }
        if let Some(roots) = &mut state.value.selected_import_roots {
            for (snapshot, _) in roots {
                snapshot.source.retain_decoded(&mut allocation, ctx)?;
            }
        }
        // Sources own their decoded proof components; the remaining state is scalar.
        drop(allocation);
        Ok(Some(state.value))
    }
    async fn swap_sparse_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        if state.workspace_id != id
            || state.schema_version != LAZY_STATE_SCHEMA
            || state.revision != revision.checked_add(1).ok_or_else(incompatible)?
        {
            return Err(incompatible());
        }
        let (current, observation) = self
            .read_sparse::<LazyWorkspaceState>(path("bindings", &id.into_bytes(), ctx)?, ctx)
            .await?;
        if current.as_ref().is_some_and(|current| {
            current.workspace_id != id || current.schema_version != LAZY_STATE_SCHEMA
        }) {
            return Err(incompatible());
        }
        if current.as_ref().map_or(0, |state| state.revision) != revision {
            return Ok(false);
        }
        let parent = match &state.fork_origin {
            Some(origin)
                if current
                    .as_ref()
                    .is_none_or(|state| state.fork_origin.is_none()) =>
            {
                Some(
                    self.read_sparse::<LazyWorkspaceState>(
                        path("bindings", &origin.snapshot.workspace_id.into_bytes(), ctx)?,
                        ctx,
                    )
                    .await?,
                )
            }
            _ => None,
        };
        if !state.accepts_fork_transition(
            current.as_ref().map(|state| &state.value),
            parent
                .as_ref()
                .and_then(|(state, _)| state.as_ref())
                .map(|state| &state.value),
        ) {
            return Ok(false);
        }
        match parent {
            Some((_, parent_observation)) => {
                self.publish_sparse(observation, &[parent_observation], &state, ctx)
                    .await
            }
            None => self.publish_sparse(observation, &[], &state, ctx).await,
        }
    }
}

impl<P: AuthorityProvider + ?Sized + 'static> LazyWorkspaceStore for StreamAuthorityStore<P> {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        let owner = NativeAccounting::take(ctx)?;
        let result = self
            .load_sparse_workspace(
                id,
                &mut KernelAccounting::native(&owner.work, owner.work.cancellation()),
            )
            .await;
        owner.restore(ctx)?;
        result
    }
    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let owner = NativeAccounting::take(ctx)?;
        let result = self
            .swap_sparse_workspace(
                id,
                revision,
                state,
                &mut KernelAccounting::native(&owner.work, owner.work.cancellation()),
            )
            .await;
        owner.restore(ctx)?;
        result
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.load_sparse_node("overlays", id.into_bytes(), ctx)
            .await
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.load_sparse_node("shadows", id.into_bytes(), ctx).await
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.store_sparse_node("overlays", id.into_bytes(), value, ctx)
            .await
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.store_sparse_node("shadows", id.into_bytes(), value, ctx)
            .await
    }
}

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    async fn load_sparse_node<T: MemoryLazyNode + DeserializeOwned>(
        &self,
        family: &str,
        id: [u8; 32],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(T, KernelOwnedAllocation)>, LazyWorkspaceError> {
        let owner = NativeAccounting::take(ctx)?;
        let result = self
            .sparse_node::<T>(
                family,
                id,
                &mut KernelAccounting::native(&owner.work, owner.work.cancellation()),
            )
            .await;
        let result = result.and_then(|node| {
            node.map(|(value, allocation)| {
                let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                    return Err(LazyWorkspaceError::NativeLedgerMismatch);
                };
                let allocation = owner.return_allocation(allocation, value.owned_bytes())?;
                Ok((value, allocation))
            })
            .transpose()
        });
        owner.restore(ctx)?;
        result
    }

    async fn store_sparse_node<T: MemoryLazyNode + DeserializeOwned>(
        &self,
        family: &str,
        id: [u8; 32],
        value: (T, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let bytes = value.0.owned_bytes();
        let value = Retained::from_owned(Owned::new(value.0, value.1, bytes), ctx)?;
        let owner = NativeAccounting::take(ctx)?;
        let result = self
            .put_sparse_node(
                family,
                id,
                &value.value,
                &mut KernelAccounting::native(&owner.work, owner.work.cancellation()),
            )
            .await;
        drop(value);
        owner.restore(ctx)?;
        result
    }
}
