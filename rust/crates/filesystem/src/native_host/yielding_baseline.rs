//! Pre-optimization Windows initialized-slice read.
use super::*;
use windows::Win32::Storage::FileSystem::ReadFile;
impl YieldingFile {
    pub(crate) fn baseline_read_at<W: SourceDemandReadWork>(
        &self,
        offset: u64,
        destination: &mut [u8],
        work: &mut W,
    ) -> Result<usize, DemandError> {
        work.check()?;
        if destination.is_empty() {
            return Ok(0);
        }
        work.with_native_read_meter(|meter| {
            use acyclic_native_runtime::{
                NativeByteReservation, NativeOperationKind, NativeOperationReservation,
            };
            let mut operations =
                NativeOperationReservation::new(meter, NativeOperationKind::Read, 2)
                    .map_err(crate::demand::native_demand_error)?;
            let length = destination.len().min(u32::MAX as usize);
            let destination = destination
                .get_mut(..length)
                .ok_or(DemandError::InvalidRequest)?;
            let mut bytes = NativeByteReservation::new(meter, NativeOperationKind::Read, length)
                .map_err(crate::demand::native_demand_error)?;
            let held = self
                .held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let file = held.as_ref().ok_or_else(|| io::Error::other(Yielded))?;
            let handle = HANDLE(file.as_raw_handle());
            // SAFETY: creation transfers sole event ownership.
            let event = unsafe {
                Owned::new(CreateEventW(None, true, false, None).map_err(io::Error::other)?)
            };
            let mut overlapped = OVERLAPPED {
                hEvent: *event,
                ..Default::default()
            };
            overlapped.Anonymous.Anonymous.Offset = (offset & 0xffff_ffff) as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
            // SAFETY: storage remains live through terminal completion, including cancellation.
            let (started, start_charge) = operations.attempt(|| unsafe {
                ReadFile(
                    handle,
                    Some(&mut *destination),
                    None,
                    Some(&raw mut overlapped),
                )
            });
            let pending = started
                .as_ref()
                .is_err_and(|error| error.code() == ERROR_IO_PENDING.to_hresult());
            let file = Arc::clone(file);
            drop(held);
            let mut interrupted = None;
            if pending {
                loop {
                    #[cfg(test)]
                    acyclic_test_support::differential::record(("wait", 0_u64, 0_u64));
                    let status = meter
                        .examine(1, 0)
                        .map_err(crate::demand::native_demand_error)
                        .map(|()| {
                            // SAFETY: the event remains owned by this live read through the wait.
                            unsafe { WaitForSingleObject(*event, 10) }
                        });
                    match status {
                        Ok(WAIT_TIMEOUT) => continue,
                        Ok(WAIT_FAILED) => {
                            interrupted = Some(DemandError::Io(io::Error::last_os_error()));
                        }
                        Err(error) => interrupted = Some(error),
                        _ => {}
                    }
                    if interrupted.is_some() {
                        // SAFETY: the live request belongs to this held handle.
                        let _ = unsafe {
                            CancelIoEx(HANDLE(file.as_raw_handle()), Some(&raw const overlapped))
                        };
                    }
                    break;
                }
            }
            let mut transferred = 0;
            let (read, completion_charge) = if started.is_ok() || pending {
                operations.attempt(|| {
                    // SAFETY: waits for this request before its event or buffer can be destroyed.
                    unsafe {
                        GetOverlappedResult(
                            HANDLE(file.as_raw_handle()),
                            &raw const overlapped,
                            &raw mut transferred,
                            true,
                        )
                    }
                })
            } else {
                (started, Ok(()))
            };
            let recorded = bytes.record(transferred as usize);
            start_charge.map_err(crate::demand::native_demand_error)?;
            completion_charge.map_err(crate::demand::native_demand_error)?;
            recorded.map_err(crate::demand::native_demand_error)?;
            if let Some(error) = interrupted {
                return Err(error);
            }
            meter.check().map_err(crate::demand::native_demand_error)?;
            match read {
                Ok(()) => Ok(transferred as usize),
                Err(error) if error.code() == ERROR_HANDLE_EOF.to_hresult() => Ok(0),
                Err(_) if self.was_yielded() => Err(io::Error::other(Yielded).into()),
                Err(error) => Err(io::Error::from_raw_os_error(error.code().0 & 0xffff).into()),
            }
        })
    }
}

#[test]
fn native_yielding_storage_refunds_exactly_when_the_handle_retires()
-> Result<(), Box<dyn std::error::Error>> {
    for delayed in [false, true] {
        let token = crate::CancellationToken::new();
        let mut work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token)
            .map_err(|(cause, _)| cause)?;
        let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
        let mut file = YieldingFile::new(std::fs::File::open("NUL")?, &mut work)?;
        let storage = (size_of::<File>() + size_of::<YieldingHeld>()) as u64;
        assert_eq!(work.live_allocation_bytes(), baseline + storage);
        assert_eq!(work.receipt().allocation_operations, 4);
        assert_eq!(
            work.receipt().peak_allocation_bytes,
            baseline + storage + size_of::<OplockRequest>() as u64
        );
        // Negative control: leave the handle's charge alive after its storage.
        let delayed_guard = if delayed {
            file.allocation.take()
        } else {
            None
        };
        drop(file);
        if delayed {
            assert_ne!(work.live_allocation_bytes(), baseline);
        } else {
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        drop(delayed_guard);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
