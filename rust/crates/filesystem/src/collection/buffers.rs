use super::*;

pub(super) fn root_parent_bytes(root: &GenerationRoot) -> u64 {
    crate::foundation::usize_to_u64(
        root.parents.capacity() * std::mem::size_of::<crate::GenerationId>(),
    )
}

pub(super) fn held_owned_bytes(held: &Held) -> u64 {
    let parents = match held {
        Held::Checkout { working, .. } => {
            working.parents.capacity() * std::mem::size_of::<crate::GenerationId>()
        }
        Held::Record { .. } | Held::Generation { .. } => 0,
    };
    crate::foundation::usize_to_u64(std::mem::size_of::<Slot>() + parents)
}

pub(crate) fn clone_generation_root(
    root: &GenerationRoot,
    accounting: &mut KernelAccounting<'_>,
    native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
) -> Result<(GenerationRoot, crate::kernel::ProofDataClaim), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let native_sink = native_owner
        .map(|owner| {
            accounting
                .native_work()
                .ok_or(crate::FsError::HandleAllocationFailed)?
                .checkout_control_sink(owner)
                .map_err(crate::FsError::from)
        })
        .transpose()?;
    let mut owned = clone_generation_root_with_accounting(root, accounting)?;
    let control = match (native_owner, &mut owned.1) {
        (None, KernelOwnedAllocation::Standalone) => crate::kernel::ProofDataClaim::default(),
        #[cfg(not(target_arch = "wasm32"))]
        (Some(owner), KernelOwnedAllocation::NativeOriginal(allocation)) => allocation
            .transfer_checkout_control(
                owner,
                native_sink
                    .as_ref()
                    .ok_or(crate::FsError::HandleAllocationFailed)?,
            )?,
        _ => return Err(crate::FsError::HandleAllocationFailed),
    };
    Ok((owned.0, control))
}

/// Clones the root outside accounting loans; the buffer precedes its guard.
pub(crate) fn clone_generation_root_with_accounting(
    root: &GenerationRoot,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(GenerationRoot, KernelOwnedAllocation), crate::FsError> {
    let requested = crate::foundation::usize_to_u64(
        root.parents.len() * std::mem::size_of::<crate::GenerationId>(),
    );
    let mut parents = (
        Vec::new(),
        accounting.claim_owned(requested, u64::from(requested != 0))?,
    );
    if parents.0.try_reserve_exact(root.parents.len()).is_err() {
        drop(parents.0);
        accounting.release_owned(&mut parents.1, requested)?;
        return Err(crate::FsError::HandleAllocationFailed);
    }
    let actual = crate::foundation::usize_to_u64(
        parents.0.capacity() * std::mem::size_of::<crate::GenerationId>(),
    );
    let copied = WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    };
    let completion = accounting
        .reconcile_owned(&mut parents.1, requested, actual)
        .and_then(|()| accounting.admit(copied));
    if let Err(error) = completion {
        drop(parents.0);
        accounting.release_owned(&mut parents.1, actual)?;
        return Err(error);
    }
    parents.0.extend_from_slice(&root.parents);
    if let Err(error) = accounting.record_completed(copied) {
        drop(parents.0);
        accounting.release_owned(&mut parents.1, actual)?;
        return Err(error);
    }
    Ok((
        GenerationRoot {
            volume_id: root.volume_id,
            root_file_id: root.root_file_id,
            file_table: root.file_table,
            parents: parents.0,
            required_features: root.required_features,
        },
        parents.1,
    ))
}
