//! Ownership of resource preparation metadata.
use super::*;
use super::inventory::CloneInventory;
/// Closed logical ownership allowance, not allocator/copy counters or a new claim.
pub(crate) fn resource_preparation_owned_bytes(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|work| work.original_native_work().is_some()) {
        let child = &request.child;
        let mut bytes = 0u64;
        macro_rules! owned_add {
            ($owned:expr) => {
                bytes = bytes.checked_add($owned).ok_or_else(|| crate::fork::boundary_error(
                    work, crate::wire::ErrorCode::Invalid, "resource preparation ownership inventory overflows"))?;
            };
        }
        owned_add!(request.provider.logical_owned_bytes()?);
        owned_add!(request.creation_grant.capacity() as u64);
        for text in [&child.task.name, &child.task.version, &child.machine.name, &child.machine.version] {
            owned_add!(text.capacity() as u64);
        }
        for value in [&request.payload, &child.input, &child.input_schema, &child.output_schema] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            owned_add!(super::ownership::value_owned_bytes(value)?);
        }
        owned_add!(child.grants.logical_owned_bytes()?);
        if let Some(policy) = &child.policy {
            owned_add!(policy.name.capacity() as u64);
            owned_add!(policy.version.capacity() as u64);
        }
        if let Some(extensions) = &child.extensions {
            owned_add!(extensions.logical_owned_bytes()?);
        }
        if let Some(execution) = &child.execution {
            owned_add!(execution.provider.name.capacity() as u64);
            owned_add!(execution.provider.version.capacity() as u64);
            owned_add!(execution.build.as_resource().logical_owned_bytes()?);
            if let Some(environment) = &execution.environment {
                owned_add!(environment.as_resource().logical_owned_bytes()?);
            }
        }
        owned_add!((request.resources.capacity() as u64)
            .checked_mul(std::mem::size_of::<crate::resources::ResourceRef>() as u64)
            .ok_or_else(|| crate::fork::boundary_error(
                work, crate::wire::ErrorCode::Invalid, "resource preparation ownership inventory overflows"))?);
        for reference in &request.resources {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            owned_add!(reference.logical_owned_bytes()?);
        }
        owned_add!((child.resource_proofs.capacity() as u64)
            .checked_mul(std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>() as u64)
            .ok_or_else(|| crate::fork::boundary_error(
                work, crate::wire::ErrorCode::Invalid, "resource preparation ownership inventory overflows"))?);
        for proof in &child.resource_proofs {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            owned_add!(proof.provider.logical_owned_bytes()?);
            owned_add!(proof.resource.logical_owned_bytes()?);
            owned_add!(proof.evidence.logical_owned_bytes()?);
        }
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        return Ok(bytes);
    }
    let mut inventory = CloneInventory { bytes: Some(0), ..Default::default() };
    request.serialize(&mut inventory).map_err(|_| crate::fork::boundary_error(
        work, crate::wire::ErrorCode::Invalid, "resource preparation ownership inventory overflows"))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: inventory.slots })?;
    }
    inventory.bytes.expect("size inventory").checked_mul(4).ok_or_else(|| crate::fork::boundary_error(
        work, crate::wire::ErrorCode::Invalid, "resource preparation ownership inventory overflows"))
}

impl crate::resource_preparation::ResourceCreationProofRef {
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        if work.is_none() { return Ok((self.clone(), 0)); }
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            quota.work().as_deref_mut().expect("proof copy observer").check_cancellation()?;
            Ok(Self {
                provider: super::copy_provider(&self.provider, quota)?, preparation: self.preparation,
                resource: super::copy_resource(&self.resource, quota)?,
                evidence: super::copy_resource(&self.evidence, quota)?,
            })
        })
    }
}

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let mut inventory = CloneInventory { bytes: Some(0), work: work.as_deref_mut().map(|work| work as &mut dyn HistoryIo), ..Default::default() };
    let result = (request, proof, origin).serialize(&mut inventory);
    if let Some(error) = inventory.failure.take() { return Err(error); }
    result.map_err(|_| Error::Invalid("closed creator capture exceeds inventory bounds".into()))?;
    let bytes = inventory.bytes.expect("size inventory");
    drop(inventory);
    if let Some(work) = work.as_deref_mut() { work.retain_owned(bytes)?; }
    Ok(bytes)
}
