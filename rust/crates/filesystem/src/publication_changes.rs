//! Kernel-sealed namespace facts carried into the same Streams commit as the
//! authority record. Callers can retain a seal but cannot manufacture its paths.
use crate::kernel::allocation::KernelAccounting;
use crate::kernel::allocation::retained::{Retained, RetainedAllocation};
use crate::kernel::{
    DecodeLimits, GenerationRoot, Owned, PublicationError, PublishGenerationRequest, VolumeCreated,
};
use crate::workspace::{ChangedPath, NamespaceView};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FsError, Head, ObjectId,
    VolumeConfig, VolumeId, WorkError,
};
use std::sync::Arc;

mod context;
pub use context::PublicationContext;

struct ChangeData {
    volume: VolumeId,
    config: VolumeConfig,
    paths: Retained<Vec<ChangedPath>>,
    _allocation: RetainedAllocation,
}

/// Authenticated complete namespace delta, bound to one exact publication.
/// Only the filesystem kernel can construct this immutable seal. Storage
/// adapters validate its volume, previous head and candidate before atomically
/// appending its public change records.
#[derive(Clone)]
pub struct PublicationChanges(Arc<ChangeData>, Option<Head>, ObjectId);

pub(crate) struct PublicationBasis {
    pub(crate) request: PublishGenerationRequest,
    pub(crate) predecessor: Option<ObjectId>,
}

impl std::fmt::Debug for PublicationChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationChanges")
            .field("volume", &self.0.volume)
            .field("base", &self.1)
            .field("root", &self.2)
            .field("paths", &self.0.paths.value.len())
            .finish()
    }
}
impl PartialEq for PublicationChanges {
    fn eq(&self, other: &Self) -> bool {
        self.0.volume == other.0.volume
            && self.1 == other.1
            && self.2 == other.2
            && self.0.config == other.0.config
            && self.0.paths.value == other.0.paths.value
    }
}
impl Eq for PublicationChanges {}

impl PublicationChanges {
    pub(crate) async fn current_root<A: AsyncAuthorityStore>(
        authority: &A,
        request: PublishGenerationRequest,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Head, ObjectId), FsError> {
        accounting.check()?;
        let actor = crate::kernel::authority_actor(
            authority.operation_claims(),
            authority.actor_scope(),
            request.authority_id,
            accounting,
        )?;
        if let Some(observed) = actor.publication_root(request) {
            return Ok(observed);
        }
        let (head, root, _) = authority::base(authority, request, None, accounting, cancellation)
            .await?
            .ok_or(FsError::InvalidAuthorityHistory)?;
        Ok((head, root))
    }
    pub(crate) fn volume(&self) -> VolumeId {
        self.0.volume
    }
    pub(crate) fn root(&self) -> ObjectId {
        self.2
    }
    pub(crate) fn base(&self) -> Option<Head> {
        self.1
    }

    /// Only the kernel combiner may bind a prepared delta to its authenticated
    /// predecessor after the dense record digest has been computed.
    pub(crate) fn compose_base(
        &mut self,
        original: Head,
        predecessor: Head,
    ) -> Result<(), FsError> {
        if self.1 != Some(original)
            || original.epoch != predecessor.epoch
            || original.sequence > predecessor.sequence
        {
            return Err(FsError::InvalidDiff);
        }
        self.1 = Some(predecessor);
        Ok(())
    }
    pub(crate) fn composed(
        &self,
        original: Head,
        expected: Head,
        root: ObjectId,
    ) -> Result<Self, FsError> {
        if root.kind != crate::ObjectKind::GenerationRoot {
            return Err(FsError::InvalidDiff);
        }
        let mut composed = self.clone();
        composed.compose_base(original, expected)?;
        composed.2 = root;
        Ok(composed)
    }

    pub(crate) async fn proposed<O: AsyncObjectStore>(
        objects: &O,
        base: &GenerationRoot,
        candidate: &GenerationRoot,
        request: PublishGenerationRequest,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        Self::prepare(
            [
                Some(NamespaceView {
                    objects,
                    config,
                    root: base,
                }),
                Some(NamespaceView {
                    objects,
                    config,
                    root: candidate,
                }),
            ],
            Some(request.expected),
            request.generation_root,
            config,
            accounting,
            cancellation,
        )
        .await
    }

    pub(crate) fn config(&self) -> VolumeConfig {
        self.0.config
    }
    pub(crate) fn paths(&self) -> &[ChangedPath] {
        &self.0.paths.value
    }

    pub(crate) fn retire(self, _accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        drop(self);
        Ok(())
    }

    async fn prepare<O: AsyncObjectStore>(
        endpoints: [Option<NamespaceView<'_, O>>; 2],
        base: Option<Head>,
        root: ObjectId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let before = accounting
            .standalone_parts()
            .map(|(a, _, _, _)| a.live_bytes());
        let volume = endpoints
            .last()
            .and_then(Option::as_ref)
            .ok_or(FsError::InvalidDiff)?
            .root
            .volume_id;
        let maximum = u32::try_from(accounting.remaining()?.items_examined).unwrap_or(u32::MAX);
        let (paths, allocation) =
            crate::workspace::complete_changed_paths(endpoints, maximum, accounting, cancellation)
                .await
                .map_err(change_error)?;
        let standalone_bytes = if let Some(before) = before {
            let (allocations, _, _, _) =
                accounting.standalone_parts().ok_or(WorkError::Overflow)?;
            allocations
                .live_bytes()
                .checked_sub(before)
                .ok_or(WorkError::Overflow)?
        } else {
            0
        };
        let paths =
            Retained::from_owned(Owned::new(paths, allocation, standalone_bytes), accounting)?;
        let control_bytes =
            crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<ChangeData>())
                .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(control_bytes, 1, accounting)?;
        Ok(Self(
            Arc::new(ChangeData {
                volume,
                config,
                paths,
                _allocation: allocation,
            }),
            base,
            root,
        ))
    }

    pub(crate) async fn initial<O: AsyncObjectStore>(
        objects: &O,
        creation: VolumeCreated,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let limits = DecodeLimits::for_volume(creation.config);
        let (root, guard) = crate::kernel::read_generation_root_with_limits_and_accounting(
            objects,
            creation.initial_generation_root,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        if root.volume_id != creation.volume_id {
            return Err(FsError::InvalidDiff);
        }
        let bytes = (root.parents.capacity() * std::mem::size_of::<crate::GenerationId>()) as u64;
        let root = Owned::new(root, guard, bytes);
        let result = Self::prepare(
            [
                None,
                Some(NamespaceView {
                    objects,
                    config: creation.config,
                    root: &root.value,
                }),
            ],
            None,
            creation.initial_generation_root,
            creation.config,
            accounting,
            cancellation,
        )
        .await;
        root.retire(accounting)?;
        result
    }

    pub(crate) async fn between<O: AsyncObjectStore, A: AsyncAuthorityStore>(
        objects: &O,
        authority: &A,
        basis: PublicationBasis,
        candidate: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, FsError> {
        let request = basis.request;
        let Some((head, current_root, config)) =
            authority::facts(authority, request, accounting, cancellation).await?
        else {
            return Ok(None);
        };
        if candidate.volume_id != request.volume_id {
            return Err(FsError::InvalidDiff);
        }
        let limits = DecodeLimits::for_volume(config);
        let (before, guard) = crate::kernel::read_generation_root_with_limits_and_accounting(
            objects,
            basis.predecessor.unwrap_or(current_root),
            limits,
            accounting,
            cancellation,
        )
        .await?;
        if before.volume_id != request.volume_id {
            return Err(FsError::InvalidDiff);
        }
        let bytes = (before.parents.capacity() * std::mem::size_of::<crate::GenerationId>()) as u64;
        let before = Owned::new(before, guard, bytes);
        let result = Self::prepare(
            [
                Some(NamespaceView {
                    objects,
                    config,
                    root: &before.value,
                }),
                Some(NamespaceView {
                    objects,
                    config,
                    root: candidate,
                }),
            ],
            Some(head),
            request.generation_root,
            config,
            accounting,
            cancellation,
        )
        .await;
        before.retire(accounting)?;
        result.map(Some)
    }
}

mod authority;
#[cfg(any(test, feature = "distributed"))]
pub(crate) mod entry;
#[cfg(any(test, feature = "distributed"))]
pub use entry::PublicationEntry;

fn change_error(error: crate::WorkspaceError) -> FsError {
    match error {
        crate::WorkspaceError::Filesystem(error) => error,
        crate::WorkspaceError::Work(error) => error.into(),
        crate::WorkspaceError::Cancelled(error) => error.into(),
        crate::WorkspaceError::ChangedPathLimit => PublicationError::ChangeLimit.into(),
        _ => FsError::InvalidDiff,
    }
}

fn merge_backend(
    accounting: &mut KernelAccounting<'_>,
    backend: crate::WorkCounters,
) -> Result<(), FsError> {
    if let Some((allocations, spent, budget, _)) = accounting.standalone_parts() {
        *spent = spent.with_backend(backend, allocations.live_bytes())?;
        spent.verify(budget)?;
    }
    Ok(())
}

/// Longest change-stream path: `.acyclic/fs/<placement-id>/<volume>`.
pub(crate) const CHANGES_PATH_LEN: usize = 77;

/// The one change-stream path rule, shared by its writer and every reader.
/// `scope` is the authority placement's actor scope: the default placement
/// (all zero) keeps `.acyclic/fs/<volume>`, and any other inserts its fixed
/// 16-byte id so separate placements on one provider never share a stream.
pub(crate) fn changes_path_text(
    scope: [u8; 16],
    volume: VolumeId,
    text: &mut [u8; CHANGES_PATH_LEN],
) -> Option<&str> {
    text.get_mut(..12)?.copy_from_slice(b".acyclic/fs/");
    let mut end = 12;
    if scope != [0; 16] {
        hex::encode_to_slice(scope, text.get_mut(end..end + 32)?).ok()?;
        *text.get_mut(end + 32)? = b'/';
        end += 33;
    }
    hex::encode_to_slice(volume.into_bytes(), text.get_mut(end..end + 32)?).ok()?;
    std::str::from_utf8(text.get(..end + 32)?).ok()
}
