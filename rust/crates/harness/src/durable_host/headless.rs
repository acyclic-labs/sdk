//! Selected resource metadata from the existing authenticated allocation receipts.
use super::*;
use crate::{
    conversation::{VolumeClass, VolumeRef},
    resources::GenerationRef,
};

/// Original immutable project/scratch snapshot proved by a retained allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct TaskVolumeSnapshot {
    /// Exact provider/owner volume identity authenticated by the creation receipt.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: VolumeRef,
    /// Original allocation generation; later source edits require a new selection.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
    pub generation: GenerationRef,
}

pub(super) fn copy_generation(
    source: &GenerationRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<GenerationRef> {
    if work.is_none() {
        return Ok(source.clone());
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let source = source.as_resource();
    let provider = crate::core::copy_provider(source.provider(), &mut quota)?;
    let key = crate::core::copy_copy_vector(source.key(), &mut quota)?;
    let version = source
        .version()
        .map(|value| crate::core::copy_text(value, &mut quota))
        .transpose()?;
    let reference =
        crate::resources::ResourceRef::new(source.kind(), provider, key, version, quota.work())?;
    let generation = GenerationRef::from_resource_with_work(reference, quota.work())?;
    quota.transfer();
    Ok(generation)
}

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    /// Returns selected metadata from authenticated creation receipts.
    pub async fn observe_volume_snapshots(&self, task: TaskId) -> Result<Vec<TaskVolumeSnapshot>> {
        self.volume_snapshots(task, &mut None)
            .await
            .map(|(snapshots, _)| snapshots)
    }

    /// Returns selected metadata with its original native custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn observe_volume_snapshots_with_native_work(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<TaskVolumeSnapshot>, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let (snapshots, retained) = self.volume_snapshots(task, &mut Some(&mut io)).await?;
        Ok((snapshots, io.take_native_custody(retained)?))
    }

    async fn volume_snapshots(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Vec<TaskVolumeSnapshot>, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (admission, held) = self
            .read_retained_admission_counted(
                OperationId::from_bytes(task.into_bytes()),
                quota.work(),
            )
            .await?;
        quota.adopt(held)?;
        let mut verification = resources::Verification {
            path: Vec::new(),
            remaining: None,
        };
        let receipts = self
            .created_receipts(&admission, &mut verification, quota.work())
            .await;
        quota.adopt((verification.path.capacity() * std::mem::size_of::<OperationId>()) as u64)?;
        drop(verification);
        let Some((mut receipts, origin)) = receipts? else {
            return Ok((Vec::new(), 0));
        };
        let mut snapshots = Vec::new();
        for receipt in &receipts {
            if Some(receipt.origin.intended_child) != origin {
                continue;
            }
            for content in &receipt.content {
                if let Some(io) = quota.work().as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if !matches!(
                    content.volume.class(),
                    VolumeClass::Project | VolumeClass::AgentPrivate
                ) {
                    continue;
                }
                let volume = content.volume.copy(quota.work())?;
                quota.adopt(volume.logical_owned_bytes()?)?;
                let generation = copy_generation(&receipt.allocation_generation, quota.work())?;
                quota.adopt(generation.logical_owned_bytes()?)?;
                let held = Self::reserve_proof_slots(&mut snapshots, quota.work())?;
                quota.adopt(held)?;
                snapshots.push(TaskVolumeSnapshot { volume, generation });
            }
        }
        let (access, _) = crate::resource_preparation::CreatedResourceAccess::consume(
            &admission,
            &mut receipts,
            origin,
            quota.work(),
        )?;
        let (scope, held) = self.root_scope.with_created_access_with_work(
            &admission,
            Some(access),
            quota.work(),
        )?;
        quota.adopt(held)?;
        let current = scope
            .current_created_content(quota.work())?
            .map(|binding| &binding.volume);
        snapshots.retain(|snapshot| {
            current.is_some_and(|private| {
                snapshot.volume.class() == VolumeClass::Project || &snapshot.volume == private
            })
        });
        let retained = snapshots.iter().try_fold(
            (snapshots.capacity() * std::mem::size_of::<TaskVolumeSnapshot>()) as u64,
            |bytes, snapshot| {
                bytes
                    .checked_add(snapshot.volume.logical_owned_bytes()?)
                    .and_then(|bytes| {
                        bytes.checked_add(snapshot.generation.logical_owned_bytes().ok()?)
                    })
                    .ok_or_else(|| {
                        guarded_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Invalid,
                            "volume snapshot output custody overflows",
                        )
                    })
            },
        )?;
        drop(scope);
        drop(admission);
        quota.replace(retained)?;
        quota.transfer();
        Ok((snapshots, retained))
    }
}
