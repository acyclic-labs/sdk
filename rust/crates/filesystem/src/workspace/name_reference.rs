use super::*;

// Independent parser with the shared, funded normalization storage primitive.
pub(super) fn decode(
    name: &str,
    accounting: &mut KernelAccounting<'_>,
    project: bool,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<WorkspaceName, WorkspaceError> {
    accounting.check()?;
    let borrowed = project && name.chars().all(|character| (character as u32) < 128);
    let (mut pending, mut maximum_pending, mut maximum_chunk) = (0_u64, 0_u64, 0_u64);
    for character in name.chars() {
        accounting.charge(item_work(1))?;
        probe(0, character as u64, 0);
        let mut chunk = 0_u64;
        let mut failure = None;
        unicode_normalization::char::decompose_canonical(character, |scalar| {
            if failure.is_some() {
                return;
            }
            failure = (|| -> Result<(), WorkspaceError> {
                if !project || !character.is_ascii() {
                    accounting.charge(item_work(1))?;
                    probe(1, character as u64, scalar as u64);
                }
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
    let live = match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
        KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
    };
    accounting.admit(crate::WorkCounters {
        peak_allocation_bytes: live
            .checked_add(scratch)
            .ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    })?;
    if borrowed {
        if name.is_empty() {
            return Err(WorkspaceNameError::Empty.into());
        }
        if matches!(name, "." | "..") {
            return Err(WorkspaceNameError::Reserved.into());
        }
        if name
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
        {
            return Err(WorkspaceNameError::InvalidCharacter.into());
        }
        return WorkspaceName::retain(name, accounting, probe);
    }
    let mut output = normalize::NameBuffer::new();
    let result = (|| {
        let mut length = 0_usize;
        let mut normalized = name.nfc();
        loop {
            accounting.check()?;
            let Some(character) = normalized.next() else {
                break;
            };
            let end = length
                .checked_add(character.len_utf8())
                .ok_or(crate::WorkError::Overflow)?;
            let slot = output.slot(length, end, accounting)?;
            let copied = copy_work(character.len_utf8() as u64);
            if !borrowed {
                accounting.admit(copied)?;
            }
            character.encode_utf8(slot);
            length = end;
            if !borrowed {
                probe(2, character as u64, character.len_utf8() as u64);
                accounting.record_completed(copied)?;
            }
        }
        drop(normalized);
        let canonical =
            std::str::from_utf8(output.bytes(length)).map_err(WorkspaceError::engine)?;
        if canonical.is_empty() {
            return Err(WorkspaceNameError::Empty.into());
        }
        if canonical == "." || canonical == ".." {
            return Err(WorkspaceNameError::Reserved.into());
        }
        for character in canonical.chars() {
            if !borrowed {
                accounting.check()?;
                probe(3, character as u64, 0);
            }
            if character.is_control() || matches!(character, '/' | '\\') {
                return Err(WorkspaceNameError::InvalidCharacter.into());
            }
        }
        let copied = copy_work(length as u64);
        accounting.admit(copied)?;
        let mut owned = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
        owned.value.extend_from_slice(canonical.as_bytes());
        probe(4, canonical.len() as u64, 0);
        accounting.record_completed(copied)?;
        let name = String::from_utf8(owned.value).map_err(WorkspaceError::engine)?;
        let control = const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<(
                String,
                KernelOwnedAllocation,
            )>()) {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("workspace name Arc layout overflow"),
            }
        };
        owned
            .allocation
            .absorb(&mut accounting.claim_owned(control, 1)?)?;
        Ok(WorkspaceName(Arc::new((name, owned.allocation))))
    })();
    output.retire(accounting)?;
    result
}
