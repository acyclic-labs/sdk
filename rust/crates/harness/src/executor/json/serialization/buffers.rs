use super::*;

pub(super) fn reserve_output(
    output: &mut Vec<u8>,
    written: u64,
    maximum: u64,
    scope: &mut Scope<'_>,
) -> Result<()> {
    let required = usize::try_from(written)
        .map_err(|_| boundary("JSON output exceeds address space", scope.code))?;
    if required <= output.capacity() {
        return Ok(());
    }
    let capacity = required
        .max(output.capacity().saturating_mul(2).max(64))
        .min(usize::try_from(maximum).unwrap_or(usize::MAX));
    // Admit both the old-buffer copy and the incoming fragment before
    // allocating their replacement. The fragment itself is charged below.
    scope.event(JsonCodecCharge::CopyAdmission { bytes: written })?;
    scope.retain(capacity as u64)?;
    scope.event(JsonCodecCharge::AllocationOperation {
        bytes: capacity as u64,
    })?;
    let mut replacement = Vec::new();
    replacement
        .try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    scope.retain((replacement.capacity() - capacity) as u64)?;
    scope.event(JsonCodecCharge::Copy {
        bytes: output.len() as u64,
    })?;
    replacement.extend_from_slice(output);
    let old = std::mem::replace(output, replacement);
    let released = old.capacity() as u64;
    drop(old);
    scope.release(released)
}

pub(super) fn output<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    canonical: bool,
    indexed: bool,
) -> Result<(Vec<u8>, usize)> {
    let size = write(
        value,
        scope,
        None,
        None,
        None,
        scope.bounds.maximum_bytes,
        canonical,
    )?;
    let length = usize::try_from(size)
        .map_err(|_| boundary("JSON output exceeds address space", scope.code))?;
    let prefix = if indexed {
        super::super::arrays::bytes(scope.lexical.arrays)?
    } else {
        0
    };
    let capacity = checked(length.checked_add(prefix), "JSON indexed buffer overflow")?;
    scope.event(JsonCodecCharge::CopyAdmission {
        bytes: capacity as u64,
    })?;
    scope.retain(capacity as u64)?;
    if capacity != 0 {
        scope.event(JsonCodecCharge::AllocationOperation {
            bytes: capacity as u64,
        })?;
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    scope.retain((bytes.capacity() - capacity) as u64)?;
    scope.event(JsonCodecCharge::Copy {
        bytes: prefix as u64,
    })?;
    bytes.resize(prefix, 0);
    if write(value, scope, Some(&mut bytes), None, None, size, canonical)? != size {
        return Err(boundary(
            "JSON serialization changed between admission and encoding",
            scope.code,
        ));
    }
    Ok((bytes, prefix))
}
