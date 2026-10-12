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
    let reference = crate::resources::ResourceRef::new_with_work(
        source.kind(),
        provider,
        key,
        version,
        quota.work(),
    )?;
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
        let mut io = crate::store::history::native_work_io(work)?;
        let (snapshots, retained) = self.volume_snapshots(task, &mut Some(&mut io)).await?;
        Ok((snapshots, io.take_native_custody(retained)?))
    }

    async fn volume_snapshots(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Vec<TaskVolumeSnapshot>, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (admission, retained) = self
            .read_retained_admission_counted(
                OperationId::from_bytes(task.into_bytes()),
                quota.work(),
            )
            .await?;
        quota.adopt(retained)?;
        let (scope, retained) = self
            .authenticated_scope_with_work(&admission, quota.work())
            .await?;
        quota.adopt(retained)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let current_private = match native.as_ref() {
            Some(native) => scope.current_created_private_volume_with_native_work(native)?,
            None => scope.current_created_private_volume()?,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let current_private = scope.current_created_private_volume()?;
        let Some(current_private) = current_private else {
            return Ok((Vec::new(), 0));
        };
        let mut current_origin = None;
        let mut snapshots = Vec::new();
        for proof in &admission.resource_proofs {
            if let Some(io) = quota.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            let (preparation, retained) = self
                .read_retained_admission_counted(
                    OperationId::from_bytes(proof.preparation.into_bytes()),
                    quota.work(),
                )
                .await?;
            quota.adopt(retained)?;
            let (request, retained) = crate::resource_preparation::ResourcePreparationRequest::from_borrowed_value_with_work(
                &preparation.input, quota.work())?;
            quota.adopt(retained)?;
            let provider = self
                .resource_creation
                .iter()
                .find(|provider| provider.provider() == &proof.provider)
                .ok_or_else(|| {
                    Self::admission_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "resource proof verifier is not bound",
                    )
                })?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let verified = match native.as_ref() {
                Some(native) => {
                    let (receipt, custody) = provider
                        .verify_with_native_work(&request, proof, native)?
                        .await?;
                    if !custody.matches_work(native) {
                        drop(receipt);
                        return Err(Self::admission_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Unauthorized,
                            "volume receipt has foreign native custody",
                        ));
                    }
                    (receipt, Some(custody))
                }
                None => (provider.verify(&request, proof).await?, None),
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let receipt = &verified.0;
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let receipt = provider.verify(&request, proof).await?;
            if receipt.resource != proof.resource || receipt.evidence != proof.evidence {
                return Err(Self::admission_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "volume receipt differs from admitted proof",
                ));
            }
            for content in &receipt.content {
                let mut allocated = false;
                for operation in &receipt.operations {
                    if let Some(io) = quota.work().as_deref_mut() {
                        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                    }
                    allocated |= match operation {
                        crate::resource_preparation::CreatedResourceOperation::ReadVolume(
                            volume,
                        )
                        | crate::resource_preparation::CreatedResourceOperation::WriteVolume(
                            volume,
                        ) => volume == &content.volume,
                        _ => false,
                    };
                }
                if !allocated
                    || !matches!(
                        content.volume.class(),
                        VolumeClass::Project | VolumeClass::AgentPrivate
                    )
                {
                    continue;
                }
                let origin = receipt.origin.intended_child;
                if content.default_writer && &content.volume == current_private {
                    if current_origin.is_some_and(|current| current != origin) {
                        return Err(Self::admission_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Unauthorized,
                            "current actor allocation origin is ambiguous",
                        ));
                    }
                    current_origin = Some(origin);
                }
                let volume = content.volume.copy(quota.work())?;
                quota.adopt(volume.logical_owned_bytes()?)?;
                let generation = copy_generation(&receipt.allocation_generation, quota.work())?;
                quota.adopt(generation.logical_owned_bytes()?)?;
                let snapshot = (origin, TaskVolumeSnapshot { volume, generation });
                if !Self::proof_contains(&snapshots, &snapshot, quota.work())? {
                    let retained = Self::reserve_proof_slots(&mut snapshots, quota.work())?;
                    quota.adopt(retained)?;
                    snapshots.push(snapshot);
                }
            }
        }
        let origin = current_origin.ok_or_else(|| {
            Self::admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "current actor allocation receipt is absent",
            )
        })?;
        let mut selected = Vec::new();
        let mut retained = 0_u64;
        for (allocation, snapshot) in snapshots {
            if allocation == origin
                && (snapshot.volume.class() == VolumeClass::Project
                    || &snapshot.volume == current_private)
            {
                let slots = Self::reserve_proof_slots(&mut selected, quota.work())?;
                quota.adopt(slots)?;
                retained = retained
                    .checked_add(slots)
                    .and_then(|bytes| {
                        bytes.checked_add(snapshot.volume.logical_owned_bytes().ok()?)
                    })
                    .and_then(|bytes| {
                        bytes.checked_add(snapshot.generation.logical_owned_bytes().ok()?)
                    })
                    .ok_or_else(|| {
                        Self::admission_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Invalid,
                            "volume snapshot output custody overflows",
                        )
                    })?;
                selected.push(snapshot);
            }
        }
        // Selected payloads move into the caller's original output claim.
        drop(scope);
        drop(admission);
        quota.replace(retained)?;
        quota.transfer();
        Ok((selected, retained))
    }
}
