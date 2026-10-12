//! Ownership of resource preparation metadata.
use super::*;

pub(crate) fn resource_preparation_owned_bytes(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    preparation_item(work)?;
    if !work.as_deref().is_some_and(copying::is_native) {
        let mut inventory = inventory::CloneInventory {
            bytes: Some(0),
            ..Default::default()
        };
        request
            .serialize(&mut inventory)
            .map_err(|_| Error::Invalid("resource ownership overflows".into()))?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: 0,
                items: inventory.slots,
            })?;
        }
        return inventory
            .bytes
            .expect("size inventory")
            .checked_mul(4)
            .ok_or_else(|| Error::Invalid("resource ownership overflows".into()));
    }
    let child = &request.child;
    let mut bytes = owned_sum([
        request.provider.logical_owned_bytes()?,
        request.creation_grant.capacity() as u64,
        child.task.name.capacity() as u64,
        child.task.version.capacity() as u64,
        child.machine.name.capacity() as u64,
        child.machine.version.capacity() as u64,
        child.grants.logical_owned_bytes()?,
        child.policy.as_ref().map_or(Ok(0), |policy| {
            owned_sum([
                policy.name.capacity() as u64,
                policy.version.capacity() as u64,
            ])
        })?,
        child
            .extensions
            .as_ref()
            .map_or(Ok(0), |extensions| extensions.logical_owned_bytes())?,
        child.execution.as_ref().map_or(Ok(0), |execution| {
            owned_sum([
                execution.provider.name.capacity() as u64,
                execution.provider.version.capacity() as u64,
                execution.build.as_resource().logical_owned_bytes()?,
                execution.environment.as_ref().map_or(Ok(0), |reference| {
                    reference.as_resource().logical_owned_bytes()
                })?,
            ])
        })?,
        owned_buffer::<crate::resources::ResourceRef>(request.resources.capacity())?,
        owned_buffer::<crate::resource_preparation::ResourceCreationProofRef>(
            child.resource_proofs.capacity(),
        )?,
    ])?;
    for value in [
        &request.payload,
        &child.input,
        &child.input_schema,
        &child.output_schema,
    ] {
        preparation_item(work)?;
        bytes = owned_sum([bytes, json_value_logical_owned_bytes(value)?])?;
    }
    for reference in &request.resources {
        preparation_item(work)?;
        bytes = owned_sum([bytes, reference.logical_owned_bytes()?])?;
    }
    for proof in &child.resource_proofs {
        preparation_item(work)?;
        bytes = owned_sum([
            bytes,
            proof.provider.logical_owned_bytes()?,
            proof.resource.logical_owned_bytes()?,
            proof.evidence.logical_owned_bytes()?,
        ])?;
    }
    work.as_deref()
        .expect("native preparation observer")
        .check_cancellation()?;
    Ok(bytes)
}

fn preparation_item(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

impl crate::resource_preparation::ResourceCreationProofRef {
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        if work.is_none() {
            return Ok((self.clone(), 0));
        }
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            quota
                .work()
                .as_deref()
                .expect("proof copy observer")
                .check_cancellation()?;
            Ok(Self {
                provider: copy_provider(&self.provider, quota)?,
                preparation: self.preparation,
                resource: copy_resource(&self.resource, quota)?,
                evidence: copy_resource(&self.evidence, quota)?,
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
    let mut inventory = inventory::CloneInventory {
        bytes: Some(0),
        work: work.as_deref_mut().map(|io| io as &mut dyn HistoryIo),
        ..Default::default()
    };
    let result = (request, proof, origin).serialize(&mut inventory);
    if let Some(error) = inventory.failure.take() {
        return Err(error);
    }
    result.map_err(|_| Error::Invalid("closed creator capture exceeds inventory bounds".into()))?;
    let bytes = inventory.bytes.expect("size inventory");
    drop(inventory);
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(bytes)?;
    }
    Ok(bytes)
}
