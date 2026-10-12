use super::*;
use crate::kernel::codec::{CanonicalDecodeError, Decoder, EncodeWorkStep, Encoder, ReadAllocationStep};

const DOMAIN: &[u8] = b"APQ";
const HEADER_BYTES: usize = 5;

pub(crate) fn cache_key(generation: GenerationId, root: FileId, table: crate::ObjectId,
    ids: &[FileId], accounting: &mut KernelAccounting<'_>) -> Result<[u8; 32], FsError>
{
    let mut hasher = blake3::Hasher::new();
    for bytes in [b"acyclic-generation-path-root-v2\0".as_slice(), generation.digest().as_bytes(),
        &root.into_bytes(), &[table.kind.canonical_tag()], table.digest.as_bytes()]
    {
        let delta = WorkCounters { bytes_hashed: bytes.len() as u64, items_examined: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        hasher.update(bytes);
        accounting.record_completed(delta)?;
    }
    for id in ids {
        let delta = WorkCounters { bytes_hashed: 16, items_examined: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        hasher.update(&id.into_bytes());
        accounting.record_completed(delta)?;
    }
    Ok(*hasher.finalize().as_bytes())
}

pub(crate) fn encode_records(generation: GenerationId, ids: &[FileId],
    records: &[(FileId, Vec<(NamespacePath, FileRecord)>)], accounting: &mut KernelAccounting<'_>)
    -> Result<(Option<Vec<u8>>, KernelOwnedAllocation), FsError>
{
    accounting.check()?;
    let mut length = HEADER_BYTES + 32;
    let mut items = 1_u64;
    let mut next = 0;
    for id in ids {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        while records.get(next).is_some_and(|entry| entry.0 < *id) {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            next += 1;
        }
        length = length.checked_add(20).ok_or(WorkError::Overflow)?;
        items += 1;
        if let Some((_, paths)) = records.get(next).filter(|entry| entry.0 == *id) {
            for (path, _) in paths {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                length = length.checked_add(4).ok_or(WorkError::Overflow)?;
                items += 1;
                for name in path.components() {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    length = length.checked_add(5 + name.as_bytes().len()).ok_or(WorkError::Overflow)?;
                    items += 1;
                }
            }
        }
        if length > MAXIMUM_VALUE_BYTES { return Ok((None, accounting.claim_owned(0, 0)?)); }
    }
    let (bytes, allocation) = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
        let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, length, |step| {
            use crate::kernel::codec::EncodeAllocationStep;
            match step {
                EncodeAllocationStep::BeforeHeader { bytes } => {
                    observe(EncodeWorkStep::Admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED }))?;
                    observe(EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }))
                }
                EncodeAllocationStep::HeaderWritten { bytes } => {
                    observe(EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }))?;
                    observe(EncodeWorkStep::Completed(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED }))
                }
                step => observe(EncodeWorkStep::Allocation(step)),
            }
        }, allocation_error)?;
        let delta = WorkCounters { bytes_copied: (length - HEADER_BYTES) as u64,
            bytes_encoded: (length - HEADER_BYTES) as u64, items_examined: items, ..WorkCounters::UNCHARGED };
        observe(EncodeWorkStep::Admit(delta))?;
        encoder.fixed(generation.digest().as_bytes());
        let mut next = 0;
        for id in ids {
            while records.get(next).is_some_and(|entry| entry.0 < *id) { next += 1; }
            let paths = records.get(next).filter(|entry| entry.0 == *id).map_or(&[][..], |entry| entry.1.as_slice());
            encoder.fixed(&id.into_bytes());
            encoder.u32(u32::try_from(paths.len()).map_err(|_| WorkError::Overflow)?);
            for (path, _) in paths {
                encoder.u32(u32::try_from(path.components().len()).map_err(|_| WorkError::Overflow)?);
                for name in path.components() {
                    encoder.u8(name.encoding().tag());
                    encoder.bounded_bytes(name.as_bytes())?;
                }
            }
        }
        observe(EncodeWorkStep::Completed(delta))?;
        Ok(encoder.finish())
    })?;
    Ok((Some(bytes), allocation))
}

fn reserve<T>(count: usize, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), FsError>) -> Result<Vec<T>, FsError> {
    let bytes = count.checked_mul(size_of::<T>()).ok_or(WorkError::Overflow)? as u64;
    observe(ReadAllocationStep::Allocate { bytes, operations: u64::from(count != 0) })?;
    let mut values = Vec::<T>::new();
    if let Err(error) = values.try_reserve_exact(count) {
        observe(ReadAllocationStep::ReserveFailed(error))?;
        return Err(CanonicalDecodeError::AllocationFailed.into());
    }
    observe(ReadAllocationStep::Capacity { admitted_bytes: bytes, actual_bytes: (values.capacity() * size_of::<T>()) as u64 })?;
    Ok(values)
}

fn document<'a>(bytes: &'a [u8], generation: GenerationId, observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), FsError>)
    -> Result<Decoder<'a>, FsError>
{
    let mut decoder = Decoder::new(bytes, DOMAIN, 1, MAXIMUM_VALUE_BYTES as u64, observe)?;
    if decoder.fixed::<32, FsError>(observe)? != *generation.digest().as_bytes() { return Err(CanonicalDecodeError::WrongDomain.into()); }
    Ok(decoder)
}

fn invalid() -> FsError { CanonicalDecodeError::WrongDomain.into() }

pub(crate) fn decode(bytes: &[u8], generation: GenerationId, ids: &[FileId], limits: VolumeLimits,
    accounting: &mut KernelAccounting<'_>) -> Result<(Option<Vec<(FileId, NamespacePath)>>, KernelOwnedAllocation), FsError>
{
    accounting.check()?;
    let before = snapshot(accounting);
    let result = crate::kernel::decode_owned(accounting, |observe| {
        let observe = &mut |step| observe(step);
        let mut decoder = document(bytes, generation, observe)?;
        let mut count = 0_usize;
        for id in ids {
            if decoder.fixed::<16, FsError>(observe)? != id.into_bytes() { return Err(invalid()); }
            let paths = decoder.u32(observe)?;
            for _ in 0..paths {
                observe(ReadAllocationStep::BeforeItem)?;
                let depth = decoder.u32(observe)?;
                if depth > u32::from(limits.maximum_path_depth) { return Err(invalid()); }
                let mut path_bytes = 1_u64;
                for index in 0..depth {
                    NameEncoding::from_tag(decoder.u8(observe)?).map_err(|_| invalid())?;
                    let source = decoder.bounded_slice(limits.maximum_component_bytes, observe)?;
                    if source.is_empty() { return Err(invalid()); }
                    path_bytes = path_bytes.checked_add(source.len() as u64 + u64::from(index != 0)).ok_or(WorkError::Overflow)?;
                }
                if path_bytes > u64::from(limits.maximum_path_bytes) { return Err(invalid()); }
                count = count.checked_add(1).ok_or(WorkError::Overflow)?;
                observe(ReadAllocationStep::Item)?;
            }
        }
        decoder.finish()?;
        let mut output = reserve(count, observe)?;
        let mut decoder = document(bytes, generation, observe)?;
        for id in ids {
            decoder.fixed::<16, FsError>(observe)?;
            for _ in 0..decoder.u32(observe)? {
                let depth = decoder.u32(observe)? as usize;
                let mut components = reserve(depth, observe)?;
                for _ in 0..depth {
                    let encoding = NameEncoding::from_tag(decoder.u8(observe)?).map_err(|_| invalid())?;
                    let data = decoder.bounded_bytes(limits.maximum_component_bytes, observe)?;
                    let name = LogicalName::new(encoding, data, limits.maximum_component_bytes).map_err(|_| invalid())?;
                    observe(ReadAllocationStep::BeforeCopy(size_of::<LogicalName>() as u64))?;
                    components.push(name);
                    observe(ReadAllocationStep::Copied(size_of::<LogicalName>() as u64))?;
                }
                let path = NamespacePath::new(components, limits).map_err(|_| invalid())?;
                observe(ReadAllocationStep::BeforeCopy(size_of::<(FileId, NamespacePath)>() as u64))?;
                output.push((*id, path));
                observe(ReadAllocationStep::Copied(size_of::<(FileId, NamespacePath)>() as u64))?;
            }
        }
        decoder.finish()?;
        Ok(output)
    });
    let keep = result.as_ref().map_or(0, |(paths, _)| (paths.capacity() * size_of::<(FileId, NamespacePath)>()) as u64
        + paths.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>());
    restore(accounting, before, keep)?;
    match result {
        Ok((paths, allocation)) => Ok((Some(paths), allocation)),
        Err(FsError::Decode(CanonicalDecodeError::AllocationFailed)) => Err(CanonicalDecodeError::AllocationFailed.into()),
        Err(FsError::Decode(_)) => Ok((None, accounting.claim_owned(0, 0)?)),
        Err(error) => Err(error),
    }
}
