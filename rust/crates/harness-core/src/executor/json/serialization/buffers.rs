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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn relocation(chunks: &[usize], headroom: usize) -> (Vec<u8>, u64) {
        let length = chunks.iter().sum::<usize>();
        let maximum = (length + headroom) as u64;
        let mut copied = 0;
        let mut charge = |event| {
            if let JsonCodecCharge::Copy { bytes } = event {
                copied += bytes;
            }
            Ok(())
        };
        let mut scope = super::super::scope(
            JsonBounds {
                maximum_bytes: maximum,
                maximum_items: u64::MAX,
                maximum_depth: 256,
            },
            None,
            &mut charge,
        );
        let mut output = Vec::new();
        for &chunk in chunks {
            let written = output.len() + chunk;
            reserve_output(&mut output, written as u64, maximum, &mut scope).unwrap();
            output.resize(written, 0);
        }
        drop(scope);
        (output, copied)
    }

    #[test]
    fn equal_input_lengths_do_not_determine_relocation_work() {
        let mut threes = vec![3; 43];
        threes.push(1);
        let mut fours = vec![4; 32];
        fours.push(2);
        let (first, first_copies) = relocation(&threes, 0);
        let (second, second_copies) = relocation(&fours, 0);
        assert_eq!(first, second);
        assert_ne!(first_copies, second_copies);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn relocation_is_linear_for_every_fragmentation(
            chunks in prop::collection::vec(1_usize..=64, 0..128), headroom in 0_usize..8192,
        ) {
            let (output, copied) = relocation(&chunks, headroom);
            // Earlier capacities form a geometric series. The final growth
            // can clamp to the input bound, adding at most one more prefix.
            prop_assert!(copied <= 2 * output.len() as u64);
        }
    }

    #[test]
    fn one_byte_fragments_cannot_recopy_every_prefix() {
        let (output, copied) = relocation(&[1; 130], 0);
        assert!(copied <= 2 * output.len() as u64);
    }
}
