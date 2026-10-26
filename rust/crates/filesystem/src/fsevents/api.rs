use super::*;

/// The `CoreFoundation` and `CoreServices` functions that a watcher calls.
pub(super) struct Api {
    pub(super) string_create_with_bytes: StringCreateWithBytes,
    pub(super) array_create: ArrayCreate,
    pub(super) release: Release,
    pub(super) type_array_callbacks: *const c_void,
    pub(super) stream_create: StreamCreate,
    pub(super) stream_set_dispatch_queue: StreamSetDispatchQueue,
    pub(super) stream_start: StreamStart,
    pub(super) stream_stop: StreamAction,
    pub(super) stream_invalidate: StreamAction,
    pub(super) stream_release: StreamAction,
    pub(super) current_event_id: CurrentEventId,
}

// SAFETY: the table holds function addresses and the address of an immutable
// `CoreFoundation` constant, all valid for the life of the process.
unsafe impl Send for Api {}
// SAFETY: as for `Send`; nothing in the table is ever written after loading.
unsafe impl Sync for Api {}

static API: OnceLock<Api> = OnceLock::new();

impl Api {
    /// Bootstrap belongs to the process, before a finite original ledger exists.
    /// The warm path neither allocates an invocation nor acquires the loader lock.
    pub(super) fn initialize_services() -> notify::Result<()> {
        if API.get().is_some() {
            return Ok(());
        }
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::get(&mut work).map(|_| ()).map_err(notify_error)
    }

    pub(super) fn get<W: SourceDemandReadWork>(work: &mut W) -> Result<&'static Self, DemandError> {
        static LOADING: Mutex<()> = Mutex::new(());
        work.check()?;
        if let Some(api) = API.get() {
            return Ok(api);
        }
        // Process-global loader custody must be initialized outside a finite
        // original invocation; loading CoreServices has no bounded payload.
        if work.is_native_original() {
            return Err(DemandError::NativeWorkUnavailable);
        }
        let _loading = LOADING.lock().map_err(|_| DemandError::WorkerUnavailable)?;
        work.check()?;
        if let Some(api) = API.get() {
            return Ok(api);
        }
        let api = work.with_original_leaf(Self::load)?;
        let _ = API.set(api);
        API.get().ok_or(DemandError::WorkerUnavailable)
    }

    fn load(work: &mut DemandWork) -> Result<Self, DemandError> {
        const CORE_SERVICES: &CStr =
            c"/System/Library/Frameworks/CoreServices.framework/CoreServices";
        // Framework-private loader state has no canonical payload, but the
        // loader does allocate it. The successfully loaded handle lives for
        // the process, just as the function table does.
        work.check()?;
        work.work.admit(
            &WorkCounters {
                backend_read_operations: 1,
                backend_write_operations: 1,
                allocation_operations: 1,
                ..WorkCounters::default()
            },
            &work.budget,
        )?;
        work.started_io(WorkCounters {
            backend_read_operations: 1,
            allocation_operations: 1,
            ..WorkCounters::default()
        })?;
        // SAFETY: the path is a valid C string. A successful bootstrap keeps
        // its loader reference for the process; failed bootstrap closes it.
        let framework = unsafe { libc::dlopen(CORE_SERVICES.as_ptr(), libc::RTLD_LAZY) };
        if framework.is_null() {
            return Err(native_error("cannot load CoreServices for FSEvents"));
        }
        let result = (|| {
            let mut symbol = |name: &CStr| {
                work.started_io(WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::default()
                })?;
                // SAFETY: the handle is open and the name is a valid C string.
                // Lookups include the frameworks linked by CoreServices.
                let address = unsafe { libc::dlsym(framework, name.as_ptr()) };
                if address.is_null() {
                    Err(native_error("FSEvents lacks a required framework symbol"))
                } else {
                    Ok(address)
                }
            };
            macro_rules! function {
                ($name:literal as $signature:ty) => {
                    // SAFETY: the named function has the declared C signature.
                    unsafe { std::mem::transmute::<*mut c_void, $signature>(symbol($name)?) }
                };
            }
            Ok(Self {
                string_create_with_bytes: function!(
                    c"CFStringCreateWithBytes" as StringCreateWithBytes
                ),
                array_create: function!(c"CFArrayCreate" as ArrayCreate),
                release: function!(c"CFRelease" as Release),
                type_array_callbacks: symbol(c"kCFTypeArrayCallBacks")?.cast_const(),
                stream_create: function!(c"FSEventStreamCreate" as StreamCreate),
                stream_set_dispatch_queue: function!(
                    c"FSEventStreamSetDispatchQueue" as StreamSetDispatchQueue
                ),
                stream_start: function!(c"FSEventStreamStart" as StreamStart),
                stream_stop: function!(c"FSEventStreamStop" as StreamAction),
                stream_invalidate: function!(c"FSEventStreamInvalidate" as StreamAction),
                stream_release: function!(c"FSEventStreamRelease" as StreamAction),
                current_event_id: function!(c"FSEventsGetCurrentEventId" as CurrentEventId),
            })
        })();
        if result.is_err() {
            let retired = work.record(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::default()
            });
            // SAFETY: this attempt holds one loader reference and has exposed
            // no function table or stream. Successful bootstrap never closes it.
            unsafe { libc::dlclose(framework) };
            retired?;
        }
        result
    }

    /// The array owns the canonical UTF-8 path payload and its pointer slots.
    /// Opaque CF headers add allocation operations, not guessed payload sizes.
    pub(super) fn path_array<W: SourceDemandReadWork>(
        &self,
        paths: &[PathBuf],
        work: &mut W,
    ) -> Result<(CfTypeRef, u64), DemandError> {
        let slots = u64::try_from(paths.len())
            .ok()
            .and_then(|count| count.checked_mul(size_of::<CfTypeRef>() as u64))
            .ok_or(crate::WorkError::Overflow)?;
        let cleanup_records = u64::try_from(paths.len()).map_err(|_| crate::WorkError::Overflow)?;
        work.check()?;
        work.admit(WorkCounters {
            items_examined: cleanup_records,
            ..WorkCounters::default()
        })?;
        work.with_original_leaf(|work| {
            work.reserve(crate::materializer::NativeResourceCredit::Items.delta(cleanup_records))
        })?;
        let mut slot_allocation = match work.claim(slots) {
            Ok(allocation) => allocation,
            Err(error) => {
                work.with_original_leaf(|work| {
                    work.refund(
                        crate::materializer::NativeResourceCredit::Items.delta(cleanup_records),
                    )
                })?;
                return Err(error);
            }
        };
        let mut strings = Vec::new();
        let reserved = strings.try_reserve_exact(paths.len());
        let actual_slots = (strings.capacity() as u64)
            .checked_mul(size_of::<CfTypeRef>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        let reconciled = work.reconcile_allocation(&mut slot_allocation, actual_slots);
        if let Err(error) = reserved.map_err(DemandError::Allocation).and(reconciled) {
            drop(strings);
            work.release(slot_allocation)?;
            work.with_original_leaf(|work| {
                work.refund(crate::materializer::NativeResourceCredit::Items.delta(cleanup_records))
            })?;
            return Err(error);
        }
        let mut payload = 0_u64;
        let mut array_bytes = 0_u64;
        let mut created_array = std::ptr::null();
        let mut result = (|| {
            for path in paths {
                self.append_path_string(path, &mut strings, &mut payload, work)?;
            }
            let array = self.create_path_array(
                &strings,
                slots,
                &mut array_bytes,
                &mut created_array,
                work,
            )?;
            Ok(array)
        })();
        if let Err(error) = self.release_path_strings(&mut strings, cleanup_records, work) {
            result = Err(error);
        }
        drop(strings);
        work.release(slot_allocation)?;
        match result {
            Ok(array) => Ok((array, payload + array_bytes)),
            Err(error) => {
                if !created_array.is_null() {
                    // SAFETY: the failed attempt still owns the array reference.
                    unsafe { (self.release)(created_array) };
                    let recorded = work.record(WorkCounters {
                        backend_write_operations: 1,
                        ..WorkCounters::default()
                    });
                    work.release(payload + array_bytes)?;
                    return recorded.and(Err(error));
                }
                work.release(payload + array_bytes)?;
                Err(error)
            }
        }
    }
    fn append_path_string<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        strings: &mut Vec<CfTypeRef>,
        payload: &mut u64,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let bytes = path.as_os_str().as_bytes();
        let length = isize::try_from(bytes.len()).map_err(|_| DemandError::InvalidRequest)?;
        let owned = u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
        let copy = work.reserve_source_copy(owned)?;
        let cleanup = strings.len() as u64 + 1;
        work.admit(WorkCounters {
            backend_write_operations: cleanup + 1,
            ..WorkCounters::default()
        })?;
        if owned == 0 {
            work.started_io(WorkCounters {
                allocation_operations: 1,
                ..WorkCounters::default()
            })?;
        } else {
            work.claim_allocation(
                owned,
                u64::from((owned) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
        }
        if let Err(error) = native_write(work, cleanup) {
            work.release(owned)?;
            return Err(error);
        }
        // SAFETY: the string copies the bytes, valid for their length.
        let string = unsafe {
            (self.string_create_with_bytes)(
                std::ptr::null(),
                bytes.as_ptr(),
                length,
                CF_STRING_ENCODING_UTF8,
                0,
            )
        };
        if string.is_null() {
            work.release(owned)?;
            return Err(DemandError::Io(std::io::Error::from(
                std::io::ErrorKind::NotFound,
            )));
        }
        strings.push(string);
        *payload += owned;
        if let Some(copy) = copy {
            {
                let recorded = copy.record_completed(crate::WorkCounters {
                    bytes_copied: owned,
                    ..crate::WorkCounters::UNCHARGED
                });
                drop(copy);
                recorded
            }
            .map_err(crate::native_name::original_name_error)?;
        } else {
            work.record(WorkCounters {
                bytes_copied: owned,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(())
    }

    fn create_path_array<W: SourceDemandReadWork>(
        &self,
        strings: &[CfTypeRef],
        slots: u64,
        array_bytes: &mut u64,
        created_array: &mut CfTypeRef,
        work: &mut W,
    ) -> Result<CfTypeRef, DemandError> {
        let count = isize::try_from(strings.len()).map_err(|_| DemandError::InvalidRequest)?;
        work.admit(WorkCounters {
            backend_write_operations: strings.len() as u64 + 2,
            bytes_copied: slots,
            ..WorkCounters::default()
        })?;
        if slots == 0 {
            work.started_io(WorkCounters {
                allocation_operations: 1,
                ..WorkCounters::default()
            })?;
        } else {
            work.claim_allocation(
                slots,
                u64::from((slots) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
        }
        *array_bytes = slots;
        let copy = work.reserve_source_copy(slots)?;
        native_write(work, strings.len() as u64 + 1)?;
        // SAFETY: the array retains the valid strings with standard callbacks.
        let array = unsafe {
            (self.array_create)(
                std::ptr::null(),
                strings.as_ptr(),
                count,
                self.type_array_callbacks,
            )
        };
        *created_array = array;
        if array.is_null() {
            return Err(native_error("cannot create the FSEvents path array"));
        }
        if let Some(copy) = copy {
            {
                let recorded = copy.record_completed(crate::WorkCounters {
                    bytes_copied: slots,
                    ..crate::WorkCounters::UNCHARGED
                });
                drop(copy);
                recorded
            }
            .map_err(crate::native_name::original_name_error)?;
        } else {
            work.record(WorkCounters {
                bytes_copied: slots,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok(array)
    }
    fn release_path_strings<W: SourceDemandReadWork>(
        &self,
        strings: &mut Vec<CfTypeRef>,
        mut cleanup_remaining: u64,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        while let Some(string) = strings.pop() {
            cleanup_remaining -= 1;
            // SAFETY: each original string reference is released exactly once.
            unsafe { (self.release)(string) };
            let retired = work.record(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::default()
            });
            let visited = work.with_original_leaf(|work| {
                work.complete(
                    crate::materializer::NativeResourceCredit::Items.delta(1),
                    crate::materializer::NativeResourceCredit::Items.delta(1),
                )
            });
            if let Err(error) = retired.and(visited) {
                result = Err(error);
            }
        }
        let refunded = work.with_original_leaf(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items.delta(cleanup_remaining))
        });
        if let Err(error) = refunded {
            result = Err(error);
        }
        result
    }
}
