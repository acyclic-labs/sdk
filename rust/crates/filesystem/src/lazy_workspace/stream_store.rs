//! Durable sparse companions on the authored filesystem's original authority.
use super::*;
use crate::distributed::{
    AuthorityProvider, AuthorizedObservation,
    state_record::{CompanionKind, StateRecord},
};
use crate::kernel::{MetadataDecodeError, OwnedSourceChunk, Retained};
use crate::{
    AuthorityWork, FilesystemCommit, FilesystemCommitOutcome, StreamAuthorityStore,
    StreamStateStoreError,
};
use acyclic_stream::{CommitCondition, CommitEffect, CommitRequest, StreamPath};
use serde::de::DeserializeOwned;

fn failure(error: impl Into<StreamStateStoreError>) -> LazyWorkspaceError {
    LazyWorkspaceError::StatePublication(Box::new(error.into()))
}
fn incompatible() -> LazyWorkspaceError {
    failure(StreamStateStoreError::Incompatible)
}
fn path(family: &str, id: &[u8]) -> Result<StreamPath, LazyWorkspaceError> {
    StreamPath::new(format!("fs/sparse-v1/{family}/{}", hex::encode(id))).map_err(failure)
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
        value: &T,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let payload = OwnedSourceChunk::produce(ctx, |sink| {
            serde_json::to_writer(sink, value).map_err(failure)
        })?;
        let (path, sequence) = match observation.condition() {
            CommitCondition::Absent { path } => (path.clone(), 0),
            CommitCondition::Head { path, expected, .. } => (path.clone(), *expected),
            _ => return Err(incompatible()),
        };
        ctx.charge(WorkCounters {
            bytes_hashed: (path.as_str().len() + payload.bytes.len() + 8) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-sparse-publication-v1\0");
        hash.update(path.as_str().as_bytes());
        hash.update(&sequence.to_le_bytes());
        if let CommitCondition::Head { commit_id, .. } = observation.condition() {
            hash.update(commit_id.as_bytes());
        }
        hash.update(&payload.bytes);
        let key = acyclic_stream::IdempotencyKey::new(bytes::Bytes::copy_from_slice(
            hash.finalize().as_bytes(),
        ))
        .map_err(failure)?;
        let record = StateRecord::Companion {
            kind: CompanionKind::SparseWorkspace,
            payload: payload.bytes,
        };
        let mut commit = FilesystemCommit::new(CommitRequest {
            idempotency_key: key.clone(),
            effects: vec![CommitEffect::hard(Vec::new(), Vec::new())],
            deadline_unix_millis: None,
        });
        let original = original(ctx)?;
        let mut work = AuthorityWork::native(&original, original.cancellation());
        self.contribute_record(
            &mut commit,
            &observation,
            &record,
            crate::PublicationHold::none(),
            &mut work,
        )
        .map_err(failure)?;
        self.seal_observed(&mut commit, &[], None, &mut work)
            .map_err(failure)?;
        match self.submit(commit, &mut work).await.map_err(failure)? {
            FilesystemCommitOutcome::Committed(_) => Ok(true),
            FilesystemCommitOutcome::Rejected(_) => Ok(false),
            FilesystemCommitOutcome::Unknown { commit, error } => {
                match self
                    .reconcile_record(path, sequence, &key, &record, &mut work)
                    .await
                {
                    Ok(_) => {
                        drop(commit);
                        Ok(true)
                    }
                    Err(_) => Err(failure(StreamStateStoreError::Unknown {
                        commit: Box::new(move || commit),
                        error,
                    })),
                }
            }
        }
    }

    async fn sparse_node<T: MemoryLazyNode + DeserializeOwned>(
        &self,
        family: &str,
        id: [u8; 32],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(T, KernelOwnedAllocation)>, LazyWorkspaceError> {
        let (value, _) = self.read_sparse::<T>(path(family, &id)?, ctx).await?;
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
        value: (T, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let bytes = value.0.owned_bytes();
        let value = Retained::from_owned(Owned::new(value.0, value.1, bytes), ctx)?;
        if hash_lazy_value(&value.value, ctx)? != id {
            return Err(incompatible());
        }
        let (current, observation) = self.read_sparse::<T>(path(family, &id)?, ctx).await?;
        if let Some(current) = current {
            return if current.value == value.value {
                Ok(())
            } else {
                Err(incompatible())
            };
        }
        if self.publish_sparse(observation, &value.value, ctx).await? {
            Ok(())
        } else {
            Err(LazyWorkspaceError::Concurrent)
        }
    }
}

impl<P: AuthorityProvider + ?Sized + 'static> LazyWorkspaceStore for StreamAuthorityStore<P> {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        let (state, _) = self
            .read_sparse::<LazyWorkspaceState>(path("bindings", &id.into_bytes())?, ctx)
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
    async fn compare_and_swap_lazy_workspace(
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
            .read_sparse::<LazyWorkspaceState>(path("bindings", &id.into_bytes())?, ctx)
            .await?;
        if current.as_ref().map_or(0, |state| state.revision) != revision {
            return Ok(false);
        }
        self.publish_sparse(observation, &state, ctx).await
    }
    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.sparse_node("overlays", id.into_bytes(), ctx).await
    }
    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.sparse_node("shadows", id.into_bytes(), ctx).await
    }
    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.put_sparse_node("overlays", id.into_bytes(), value, ctx)
            .await
    }
    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.put_sparse_node("shadows", id.into_bytes(), value, ctx)
            .await
    }
}
