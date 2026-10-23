//! Kernel-sealed namespace facts carried into the same Streams commit as the
//! authority record. Callers can retain a seal but cannot manufacture its paths.
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::{
    DecodeLimits, GenerationRoot, Owned, PublicationError, PublishGenerationRequest, VolumeCreated,
};
use crate::workspace::{ChangedPath, NamespaceView};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FsError, Head, ObjectId,
    VolumeConfig, VolumeId, WorkError,
};
use std::sync::Arc;

struct ChangeData {
    volume: VolumeId,
    base: Option<Head>,
    root: ObjectId,
    config: VolumeConfig,
    paths: Vec<ChangedPath>,
    _allocation: KernelOwnedAllocation,
}

/// Authenticated complete namespace delta, bound to one exact publication.
/// Only the filesystem kernel can construct this immutable seal. Storage
/// adapters validate its volume, previous head and candidate before atomically
/// appending its public change records.
#[derive(Clone)]
pub struct PublicationChanges(Arc<ChangeData>);

impl std::fmt::Debug for PublicationChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationChanges")
            .field("volume", &self.0.volume)
            .field("base", &self.0.base)
            .field("root", &self.0.root)
            .field("paths", &self.0.paths.len())
            .finish()
    }
}
impl PartialEq for PublicationChanges {
    fn eq(&self, other: &Self) -> bool {
        self.0.volume == other.0.volume
            && self.0.base == other.0.base
            && self.0.root == other.0.root
            && self.0.config == other.0.config
            && self.0.paths == other.0.paths
    }
}
impl Eq for PublicationChanges {}

impl PublicationChanges {
    pub(crate) fn volume(&self) -> VolumeId {
        self.0.volume
    }
    pub(crate) fn root(&self) -> ObjectId {
        self.0.root
    }
    pub(crate) fn base(&self) -> Option<Head> {
        self.0.base
    }
    pub(crate) fn config(&self) -> VolumeConfig {
        self.0.config
    }
    pub(crate) fn paths(&self) -> &[ChangedPath] {
        &self.0.paths
    }

    async fn prepare<O: AsyncObjectStore>(
        endpoints: [Option<NamespaceView<'_, O>>; 2],
        base: Option<Head>,
        root: ObjectId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let volume = endpoints
            .last()
            .and_then(Option::as_ref)
            .ok_or(FsError::InvalidDiff)?
            .root
            .volume_id;
        let maximum = u32::try_from(accounting.remaining()?.items_examined).unwrap_or(u32::MAX);
        let (paths, mut allocation) =
            crate::workspace::complete_changed_paths(endpoints, maximum, accounting, cancellation)
                .await
                .map_err(change_error)?;
        let control_bytes =
            crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<ChangeData>())
                .map_err(|_| WorkError::Overflow)? as u64;
        let mut control = accounting.claim_owned(control_bytes, 1)?;
        allocation.absorb(&mut control)?;
        Ok(Self(Arc::new(ChangeData {
            volume,
            base,
            root,
            config,
            paths,
            _allocation: allocation,
        })))
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
        request: PublishGenerationRequest,
        candidate: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, FsError> {
        let mut work = match &*accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                crate::AuthorityWork::native(work, cancellation)
            }
            _ => crate::AuthorityWork::new(accounting.remaining()?, cancellation),
        };
        let observed = authority
            .head_payload(
                request.authority_id,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut work,
            )
            .await;
        let spent = work.spent();
        drop(work);
        merge_backend(accounting, spent)?;
        let ((head, payload), _head_custody) = observed?;
        if head != request.expected {
            return Ok(None);
        }
        let mut work = match &*accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                crate::AuthorityWork::native(work, cancellation)
            }
            _ => crate::AuthorityWork::new(accounting.remaining()?, cancellation),
        };
        let observed = authority
            .replay(
                request.authority_id,
                crate::Sequence::GENESIS,
                crate::ReplayLimit {
                    records: 1,
                    payload_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                },
                &mut work,
            )
            .await;
        let spent = work.spent();
        drop(work);
        merge_backend(accounting, spent)?;
        let (records, _record_custody) = observed?;
        let first = records.first().ok_or(FsError::InvalidDiff)?;
        let mut decode = crate::kernel::OriginalReadAccounting::new(accounting)?;
        let creation = crate::kernel::decode_volume_created(
            &first.payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut |s| decode.observe(s),
        )?;
        let payload = payload.ok_or(FsError::InvalidDiff)?;
        let (current_volume, current_root) = if head.sequence == crate::Sequence::new(1) {
            let value = crate::kernel::decode_volume_created(
                &payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut |s| decode.observe(s),
            )?;
            (value.volume_id, value.initial_generation_root)
        } else {
            let value = crate::kernel::decode_published_generation(
                &payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut |s| decode.observe(s),
            )?;
            (value.volume_id, value.generation_root)
        };
        let _decoded = decode.into_allocation();
        if creation.volume_id != request.volume_id
            || candidate.volume_id != request.volume_id
            || current_volume != request.volume_id
        {
            return Err(FsError::InvalidDiff);
        }
        let limits = DecodeLimits::for_volume(creation.config);
        let (before, guard) = crate::kernel::read_generation_root_with_limits_and_accounting(
            objects,
            current_root,
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
                    config: creation.config,
                    root: &before.value,
                }),
                Some(NamespaceView {
                    objects,
                    config: creation.config,
                    root: candidate,
                }),
            ],
            Some(head),
            request.generation_root,
            creation.config,
            accounting,
            cancellation,
        )
        .await;
        before.retire(accounting)?;
        result.map(Some)
    }
}

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
