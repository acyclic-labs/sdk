use super::*;

pub(super) fn reserve_slots<T>(
    count: usize,
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<ReadOwned<Vec<T>>, crate::FsError> {
    let requested = usize_to_u64(count)
        .checked_mul(size_of::<T>() as u64)
        .ok_or(WorkError::Overflow)?;
    let allocation = accounting.claim_owned(requested, u64::from(count != 0))?;
    let mut slots = ReadOwned {
        value: Vec::new(),
        custody: allocation,
    };
    if let Err(error) = slots.value.try_reserve_exact(count) {
        // Retain the allocator's actual cause on independent observation or
        // retirement refusal of this already-performed allocation attempt.
        if let Some(owner) = custody {
            let _ = owner.record_completed_leaf_work(WorkCounters {
                allocation_operations: u64::from(count != 0),
                peak_allocation_bytes: requested,
                ..WorkCounters::UNCHARGED
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        let error = if matches!(&slots.custody, KernelOwnedAllocation::NativeOriginal(_)) {
            crate::NativeTreeMaterializationError::AllocationFailed(error).into()
        } else {
            crate::FsError::HandleAllocationFailed
        };
        #[cfg(target_arch = "wasm32")]
        let error = {
            let _ = error;
            crate::FsError::HandleAllocationFailed
        };
        let _ = accounting.release_owned(&mut slots.custody, requested);
        return Err(error);
    }
    let actual = usize_to_u64(slots.value.capacity())
        .checked_mul(size_of::<T>() as u64)
        .ok_or(WorkError::Overflow)?;
    // The buffer already exists: keep its actual guard even on a late refusal.
    let completed = accounting.reconcile_owned(&mut slots.custody, requested, actual);
    let observed = custody.map_or(Ok(()), |owner| {
        owner
            .record_completed_leaf_work(WorkCounters {
                allocation_operations: u64::from(count != 0),
                peak_allocation_bytes: actual,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::FsError::from)
    });
    completed?;
    observed?;
    Ok(slots)
}

pub(super) fn examine_item<T>(
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&crate::kernel::ProofDataCustody>,
    examine: impl FnOnce() -> T,
) -> Result<T, crate::FsError> {
    let delta = WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(delta)?;
    let value = examine();
    let completed = accounting.record_completed(delta);
    let observed = custody.map_or(Ok(()), |owner| {
        owner
            .record_completed_leaf_work(delta)
            .map_err(crate::FsError::from)
    });
    completed?;
    observed?;
    Ok(value)
}

pub(super) struct FrameObservation<'a> {
    pub(super) owner: Option<&'a crate::kernel::ProofDataCustody>,
    pub(super) bytes: u64,
}
impl FrameObservation<'_> {
    fn admitted(&mut self, bytes: u64) -> Result<(), crate::FsError> {
        if let Some(owner) = self.owner {
            owner.record_heap_future_event(crate::heap_future::HeapFutureEvent::Admitted(
                WorkCounters {
                    allocation_operations: u64::from(bytes != 0),
                    peak_allocation_bytes: bytes,
                    ..WorkCounters::UNCHARGED
                },
            ))?;
            self.bytes = bytes;
        }
        Ok(())
    }
}
impl Drop for FrameObservation<'_> {
    #[allow(
        clippy::expect_used,
        reason = "The frame releases exactly the bytes admitted by its observer."
    )]
    fn drop(&mut self) {
        if let Some(owner) = self.owner.filter(|_| self.bytes != 0) {
            owner
                .record_heap_future_event(crate::heap_future::HeapFutureEvent::Released(self.bytes))
                .expect("the actual path frame dies before its observation retires");
        }
    }
}

pub(super) fn admit_path_frame(
    accounting: &mut KernelAccounting<'_>,
    bytes: u64,
    observation: &mut FrameObservation<'_>,
) -> Result<crate::heap_future::KernelFrameReservation, crate::FsError> {
    let guard = crate::heap_future::FrameLedger::reserve_frame(accounting, bytes)?;
    observation.admitted(bytes)?;
    Ok(guard)
}

pub(super) fn absorb_allocation(
    target: &mut KernelOwnedAllocation,
    incoming: &mut KernelOwnedAllocation,
) -> Result<(), crate::FsError> {
    match (target, incoming) {
        (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => Ok(()),
        #[cfg(not(target_arch = "wasm32"))]
        (
            KernelOwnedAllocation::NativeOriginal(target),
            KernelOwnedAllocation::NativeOriginal(incoming),
        ) => {
            target.absorb(incoming)?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        _ => Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
    }
}
pub(super) fn release_standalone(
    accounting: &mut KernelAccounting<'_>,
    bytes: u64,
) -> Result<(), crate::FsError> {
    let ledger = match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations,
        KernelAccounting::BorrowedStandalone { allocations, .. } => &mut **allocations,
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => return Ok(()),
    };
    ledger
        .release(bytes)
        .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))
}
pub(super) fn drop_slots<T>(
    values: ReadOwned<Vec<T>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let bytes = match &values.custody {
        KernelOwnedAllocation::Standalone => Some(
            usize_to_u64(values.value.capacity())
                .checked_mul(size_of::<T>() as u64)
                .ok_or(WorkError::Overflow)?,
        ),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => None,
    };
    drop(values);
    if let Some(bytes) = bytes {
        release_standalone(accounting, bytes)?;
    }
    Ok(())
}
pub(super) fn drop_bindings(
    values: ReadOwned<Vec<Option<crate::kernel::TreeEntry>>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let bytes = match &values.custody {
        KernelOwnedAllocation::Standalone => Some(
            usize_to_u64(values.value.capacity())
                .checked_mul(size_of::<Option<crate::kernel::TreeEntry>>() as u64)
                .and_then(|bytes| {
                    values
                        .value
                        .iter()
                        .flatten()
                        .try_fold(bytes, |bytes, entry| {
                            bytes.checked_add(usize_to_u64(entry.name.retained_bytes()))
                        })
                })
                .ok_or(WorkError::Overflow)?,
        ),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => None,
    };
    drop(values);
    if let Some(bytes) = bytes {
        release_standalone(accounting, bytes)?;
    }
    Ok(())
}
pub(super) fn drop_cache<S>(
    cache: OperationReadCache<'_, S>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => {
            drop(cache);
            Ok(())
        }
        _ => {
            let retained = cache.lock_state().accounted_bytes;
            drop(cache);
            release_standalone(accounting, retained)
        }
    }
}
pub(super) fn copy_name(
    source: &LogicalName,
    maximum: u32,
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(LogicalName, KernelOwnedAllocation), crate::FsError> {
    // Use the same canonical clone leaf, including its per-field observation.
    crate::kernel::persistent_io::clone_key::<crate::kernel::tree_mutation::TreeFormat>(
        source,
        maximum,
        accounting,
        custody,
        custody.map_or(0, crate::kernel::ProofDataCustody::operation_cache_bytes),
    )
}
pub(super) fn retire_name_bytes(
    allocation: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
    bytes: u64,
) -> Result<(), crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => release_standalone(accounting, bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            allocation.release(bytes).map_err(Into::into)
        }
    }
}
pub(super) fn standalone_live(
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<u64>, crate::FsError> {
    Ok(accounting
        .standalone_parts()
        .map(|(allocations, ..)| allocations.live_bytes() - allocations.frame_control_bytes()))
}
pub(super) fn cleanup_standalone(
    accounting: &mut KernelAccounting<'_>,
    initial: Option<u64>,
    retained: u64,
) -> Result<(), crate::FsError> {
    if let Some(initial) = initial {
        let live = match accounting {
            KernelAccounting::Standalone { allocations, .. } => {
                allocations.live_bytes() - allocations.frame_control_bytes()
            }
            KernelAccounting::BorrowedStandalone { allocations, .. } => {
                allocations.live_bytes() - allocations.frame_control_bytes()
            }
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return Ok(()),
        };
        let release = live
            .checked_sub(initial)
            .and_then(|bytes| bytes.checked_sub(retained))
            .ok_or(WorkError::Overflow)?;
        release_standalone(accounting, release)?;
    }
    Ok(())
}
pub(super) fn dependency_bytes(
    dependencies: &[Dependency],
    capacity: usize,
) -> Result<u64, WorkError> {
    dependencies.iter().try_fold(
        usize_to_u64(capacity)
            .checked_mul(size_of::<Dependency>() as u64)
            .ok_or(WorkError::Overflow)?,
        |bytes, dependency| {
            bytes
                .checked_add(match &dependency.region {
                    DependencyRegion::DirectoryName { name, .. } => {
                        usize_to_u64(name.retained_bytes())
                    }
                    _ => 0,
                })
                .ok_or(WorkError::Overflow)
        },
    )
}
