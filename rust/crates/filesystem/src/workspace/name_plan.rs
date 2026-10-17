use super::*;

pub(super) fn inspect(
    name: &str,
    accounting: &mut KernelAccounting<'_>,
    ascii: impl Fn(&char) -> bool,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<(bool, bool, u64, u64), WorkspaceError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let KernelAccounting::NativeOriginal { work, cancellation } = accounting {
        // No allocation owners or external callbacks enter this pure planning
        // loop. Hold the original leaf (50); resource ancestors follow it.
        return work.state(|state| {
            collect(
                name,
                |count| {
                    let delta = item_work(count);
                    let native = crate::materializer::native_materialization_work_error;
                    state
                        .admit(crate::WorkCounters::UNCHARGED)
                        .map_err(native)?;
                    cancellation.check()?;
                    #[cfg(all(test, acyclic_fs_loom))]
                    if super::name_model::omit_plan_charge() {
                        return state.admit(delta).map_err(native).map_err(Into::into);
                    }
                    state.charge(delta).map_err(native).map_err(Into::into)
                },
                ascii,
                probe,
            )
        });
    }
    // Standalone item charges take the single-counter path.
    collect(name, |count| accounting.charge_items(count), ascii, probe)
}

fn collect(
    name: &str,
    mut charge: impl FnMut(u64) -> Result<(), FsError>,
    ascii: impl Fn(&char) -> bool,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<(bool, bool, u64, u64), WorkspaceError> {
    let mut all_ascii = true;
    let mut invalid_ascii = false;
    let (mut pending, mut maximum_pending, mut maximum_chunk) = (0_u64, 0_u64, 0_u64);
    for character in name.chars() {
        charge(1)?;
        probe(0, character as u64, 0);
        if ascii(&character) {
            // ASCII is already NFC, has combining class zero, and expands
            // to exactly itself. No decomposition or buffering is needed.
            pending = 0;
            maximum_chunk = maximum_chunk.max(1);
            invalid_ascii |= character.is_control() || matches!(character, '/' | '\\');
            continue;
        }
        all_ascii = false;
        let mut chunk = 0_u64;
        let mut failure = None;
        unicode_normalization::char::decompose_canonical(character, |scalar| {
            if failure.is_some() {
                return;
            }
            failure = (|| -> Result<(), WorkspaceError> {
                charge(1)?;
                probe(1, character as u64, scalar as u64);
                chunk = chunk.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                if unicode_normalization::char::canonical_combining_class(scalar) == 0 {
                    pending = 0;
                } else {
                    pending = pending.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                    maximum_pending = maximum_pending.max(pending);
                }
                Ok(())
            })()
            .err();
        });
        if let Some(error) = failure {
            return Err(error);
        }
        maximum_chunk = maximum_chunk.max(chunk);
    }
    Ok((all_ascii, invalid_ascii, maximum_pending, maximum_chunk))
}
