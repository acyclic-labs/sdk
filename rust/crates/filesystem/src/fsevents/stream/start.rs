use super::*;

/// Unregistering, plus either the queue barrier or, when stopped on the
/// queue itself, the same-queue terminal suffix.
const STARTUP_CLEANUP: WorkCounters = WorkCounters {
    backend_write_operations: 2,
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};

impl EventStream {
    /// Registers canonical roots, as `FSEvents` reports them, on the hub.
    pub(crate) fn start<W: SourceDemandReadWork>(
        roots: &[PathBuf],
        handler: Arc<BorrowedEventHandlerFn>,
        published_bytes: Option<&AtomicU64>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let hub = Hub::get(work)?;
        let mut ownership = crate::demand::SourcePayloadOwnership::new(work)?;
        let mut registration = None;
        let mut owned_bytes = 0;
        let mut cleanup_reserved = false;
        let mut retained = false;
        let mut registered = false;
        let result = (|| {
            // Birth funds both ordinary drain and the same-queue self-drop
            // suffix. Whichever path does not run refunds its exact credit.
            work.check()?;
            work.with_original_leaf(|work| {
                crate::source_watch::reserve_cleanup_credits(work, STARTUP_CLEANUP)
            })?;
            cleanup_reserved = true;
            let (allocated, bytes) = allocate_registration(roots, handler, work)?;
            owned_bytes = bytes;
            let allocated = registration.insert(allocated);
            ownership.retain(owned_bytes, work)?;
            retained = ownership.has_native_custody();
            native_write(work, 1)?;
            if let Some(published) = published_bytes {
                published.store(owned_bytes, Ordering::Release);
            }
            hub.register(allocated)?;
            registered = true;
            work.check()?;
            if !retained {
                work.release(owned_bytes)?;
            }
            Ok(Self {
                hub,
                registration: registration.take(),
                terminal: None,
                owned_bytes,
                cleanup: if work.is_native_original() {
                    (1, 2)
                } else {
                    (0, 0)
                },
                birth: work.native_source_work().map(|work| work.allocation_work()),
                ownership: std::mem::take(&mut ownership),
            })
        })();
        if cleanup_reserved && !work.is_native_original() {
            work.with_original_leaf(|work| {
                crate::source_watch::cancel_cleanup_credits(work, STARTUP_CLEANUP);
            });
        }
        if result.is_err() {
            if let Some(published) = published_bytes {
                published.store(0, Ordering::Release);
            }
            Self::cleanup_failed_start(
                hub,
                (registration, registered, owned_bytes),
                (cleanup_reserved, retained),
                &mut ownership,
                work,
            );
            if !retained {
                let _cleanup = work.release(owned_bytes);
            }
        }
        result
    }

    fn cleanup_failed_start<W: SourceDemandReadWork>(
        hub: &'static Hub,
        (registration, registered, owned_bytes): (Option<Arc<Registration>>, bool, u64),
        (cleanup_reserved, retained): (bool, bool),
        ownership: &mut crate::demand::SourcePayloadOwnership,
        work: &mut W,
    ) {
        if !registered {
            drop(registration);
            if cleanup_reserved && work.is_native_original() {
                work.with_original_leaf(|work| {
                    crate::source_watch::cancel_cleanup_credits(work, STARTUP_CLEANUP);
                });
            }
            return;
        }
        let mut failed = Self {
            hub,
            registration,
            terminal: None,
            owned_bytes,
            ownership: if retained {
                std::mem::take(ownership)
            } else {
                Default::default()
            },
            cleanup: if cleanup_reserved && work.is_native_original() {
                (1, 2)
            } else {
                (0, 0)
            },
            birth: work.native_source_work().map(|work| work.allocation_work()),
        };
        let _cleanup = failed.cleanup_admitted(work);
    }
}

/// Copies the roots into the registration the hub routes by.
fn allocate_registration<W: SourceDemandReadWork>(
    roots: &[PathBuf],
    handler: Arc<BorrowedEventHandlerFn>,
    work: &mut W,
) -> Result<(Arc<Registration>, u64), DemandError> {
    let payload = roots
        .iter()
        .try_fold(0_u64, |total, root| {
            total.checked_add(root.as_os_str().len() as u64)
        })
        .ok_or(crate::WorkError::Overflow)?;
    let count = u64::try_from(roots.len()).map_err(|_| crate::WorkError::Overflow)?;
    let bytes = count
        .checked_mul(size_of::<PathBuf>() as u64)
        .and_then(|slots| slots.checked_add(payload))
        .and_then(|owned| {
            owned.checked_add(crate::source_watch::arc_control_bytes::<Registration>())
        })
        .ok_or(crate::WorkError::Overflow)?;
    let copy = work.reserve_source_copy(payload)?;
    work.claim_allocation(bytes, count + 2, crate::WorkCounters::UNCHARGED)?;
    let mut copied = Vec::new();
    if let Err(error) = copied.try_reserve_exact(roots.len()) {
        work.release(bytes)?;
        return Err(DemandError::Allocation(error));
    }
    copied.extend(roots.iter().cloned());
    let recorded = if let Some(copy) = copy {
        let recorded = copy.record_completed(crate::WorkCounters {
            bytes_copied: payload,
            ..crate::WorkCounters::UNCHARGED
        });
        drop(copy);
        recorded.map_err(crate::native_name::original_name_error)
    } else {
        work.record(WorkCounters {
            bytes_copied: payload,
            ..WorkCounters::UNCHARGED
        })
    };
    if let Err(error) = recorded {
        drop(copied);
        work.release(bytes)?;
        return Err(error);
    }
    Ok((Arc::new(Registration::new(copied, handler)), bytes))
}
