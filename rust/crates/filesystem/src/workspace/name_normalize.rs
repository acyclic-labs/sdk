use super::*;

pub(super) fn parse(
    name: &str,
    maximum_pending: u64,
    maximum_chunk: u64,
    accounting: &mut KernelAccounting<'_>,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<WorkspaceName, WorkspaceError> {
    // NFC's two TinyVec buffers and combining-mark sort scratch are admitted before iteration.
    let buffer = |count: u64, width: usize| -> Result<u64, WorkspaceError> {
        if count <= 4 {
            return Ok(0);
        }
        count
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_mul(width as u64))
            .ok_or(crate::WorkError::Overflow.into())
    };
    let scratch = buffer(
        maximum_pending
            .checked_add(maximum_chunk)
            .ok_or(crate::WorkError::Overflow)?,
        std::mem::size_of::<(u8, char)>(),
    )?
    .checked_add(buffer(maximum_pending, std::mem::size_of::<char>())?)
    .and_then(|n| {
        maximum_pending
            .checked_mul(std::mem::size_of::<(u8, char)>() as u64)
            .and_then(|sort| n.checked_add(sort))
    })
    .ok_or(crate::WorkError::Overflow)?;
    admit_scratch(scratch, accounting)?;
    let mut output = [0_u8; MAXIMUM_WORKSPACE_NAME_BYTES];
    let mut length = 0;
    let mut normalized = name.nfc();
    // Each character's charge also checks cancellation.
    while let Some(character) = normalized.next() {
        let end = length + character.len_utf8();
        let Some(slot) = output.get_mut(length..end) else {
            #[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
            let check = !name_tests::skip_length_cancellation();
            #[cfg(not(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32"))))]
            let check = true;
            if check {
                accounting.check()?;
            }
            return Err(WorkspaceNameError::TooLong.into());
        };
        encode(character, slot, accounting, probe)?;
        length = end;
    }
    drop(normalized);
    accounting.check()?;
    let canonical = std::str::from_utf8(output.get(..length).ok_or(WorkspaceNameError::TooLong)?)
        .map_err(WorkspaceError::engine)?;
    if canonical.is_empty() {
        return Err(WorkspaceNameError::Empty.into());
    }
    if canonical == "." || canonical == ".." {
        return Err(WorkspaceNameError::Reserved.into());
    }
    for character in canonical.chars() {
        accounting.check()?;
        probe(3, character as u64, 0);
        if character.is_control() || matches!(character, '/' | '\\') {
            return Err(WorkspaceNameError::InvalidCharacter.into());
        }
    }
    WorkspaceName::retain(canonical, accounting, probe)
}

pub(super) fn encode(
    character: char,
    output: &mut [u8],
    accounting: &mut KernelAccounting<'_>,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<(), FsError> {
    charged(accounting, copy_work(character.len_utf8() as u64), || {
        character.encode_utf8(output);
        probe(2, character as u64, character.len_utf8() as u64);
    })
}

pub(super) fn admit_scratch(
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let live = match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
        KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
    };
    accounting.admit(crate::WorkCounters {
        peak_allocation_bytes: live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    })?;
    Ok(())
}
