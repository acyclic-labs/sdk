//! Selected immutable task volume metadata.
use super::*;
#[cfg(feature = "filesystem")]
use crate::{conversation::VolumeClass, resources::GenerationRef};

/// Exact volume and generation selected by authenticated allocation metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct TaskVolumeSnapshot {
    /// Selected volume identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: crate::conversation::VolumeRef,
    /// Selected immutable generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
    pub generation: crate::resources::GenerationRef,
}

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Observes immutable snapshots and their retained output extent.
    #[allow(
        clippy::too_many_lines,
        reason = "Snapshot validation and retained output accounting share one original quota."
    )]
    pub async fn observe_volume_snapshots(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(Vec<TaskVolumeSnapshot>, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let admission = self
            .admission(OperationId::from_bytes(task.into_bytes()), quota.work())
            .await?;
        let retained = if quota.work().is_some() {
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?
        } else {
            0
        };
        quota.adopt(retained)?;
        let (scope, retained) = self.authenticated_scope(&admission, quota.work()).await?;
        quota.adopt(retained)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned();
        let current_private = scope
            .current_created_content(quota.work())?
            .map(|binding| &binding.volume);
        let Some(current_private) = current_private else {
            return Ok((Vec::new(), 0));
        };
        let mut current_origin = None;
        let mut snapshots = Vec::new();
        for proof in &admission.resource_proofs {
            if let Some(io) = quota.work().as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            let preparation = self
                .admission(
                    OperationId::from_bytes(proof.preparation.into_bytes()),
                    quota.work(),
                )
                .await?;
            let retained = if quota.work().is_some() {
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + preparation.logical_owned_bytes()?
            } else {
                0
            };
            quota.adopt(retained)?;
            let (request, retained) =
                crate::resource_preparation::ResourcePreparationRequest::decode(
                    &preparation.input,
                    quota.work(),
                )?;
            quota.adopt(retained)?;
            let provider = self
                .resource_creation
                .iter()
                .find(|provider| provider.provider() == &proof.provider)
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
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
                    if !custody.work().shares_allocation_context(native) {
                        drop(receipt);
                        return Err(crate::store::guarded_boundary(
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
                return Err(crate::store::guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unauthorized,
                    "volume receipt differs from admitted proof",
                ));
            }
            for content in &receipt.content {
                if !matches!(
                    content.volume.class(),
                    VolumeClass::Project | VolumeClass::AgentPrivate
                ) {
                    continue;
                }
                let origin = receipt.origin.intended_child;
                if content.default_writer && &content.volume == current_private {
                    if current_origin.is_some_and(|current| current != origin) {
                        return Err(crate::store::guarded_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Unauthorized,
                            "current actor allocation origin is ambiguous",
                        ));
                    }
                    current_origin = Some(origin);
                }
                let volume = content.volume.copy(quota.work())?;
                quota.adopt(volume.logical_owned_bytes()?)?;
                let reference = crate::core::copy_resource(
                    receipt.allocation_generation.as_resource(),
                    &mut quota,
                )?;
                let generation = GenerationRef::from_resource(reference, quota.work())?;
                let snapshot = (origin, TaskVolumeSnapshot { volume, generation });
                if !Self::proof_contains(&snapshots, &snapshot, quota.work())? {
                    let retained = Self::reserve_proof_slots(&mut snapshots, quota.work())?;
                    quota.adopt(retained)?;
                    snapshots.push(snapshot);
                }
            }
        }
        let origin = current_origin.ok_or_else(|| {
            crate::store::guarded_boundary(
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
                        crate::store::guarded_boundary(
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
