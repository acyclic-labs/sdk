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
