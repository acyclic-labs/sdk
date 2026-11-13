use super::*;

/// One native loop borrows this owner through every callback. A shared root
/// selects a leased view before any source guard, meter or origin is entered.
#[derive(Clone)]
pub(super) enum CallbackTarget {
    View(Arc<DarwinMountContext>),
    Shared(Arc<super::shared::SharedViews>),
}

impl CallbackTarget {
    pub(super) fn view(&self) -> Result<Arc<DarwinMountContext>, NativeMountError> {
        match self {
            Self::View(context) => Ok(Arc::clone(context)),
            Self::Shared(_) => Err(NativeMountError::Lifecycle(
                "shared root has no volume owner",
            )),
        }
    }

    pub(super) fn writable(&self) -> bool {
        match self {
            Self::View(context) => context.writable,
            Self::Shared(_) => true,
        }
    }

    pub(super) fn named_attributes(&self) -> bool {
        match self {
            Self::View(context) => context.source.supports_posix_named_attributes(),
            Self::Shared(_) => true,
        }
    }

    pub(super) fn record_failure(&self, cause: NativeMountError, work: WorkCounters) -> i32 {
        match self {
            Self::View(context) => context.record_failure(cause, work),
            Self::Shared(views) => views.record_failure(cause, work),
        }
    }
}

fn target(address: usize) -> Result<&'static CallbackTarget, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    // SAFETY: the synchronous native loop retains this typed target until all
    // native callbacks have joined. Tests retain the same owner through calls.
    Ok(unsafe { &*(address as *const CallbackTarget) })
}

fn run_view<T>(
    context: &DarwinMountContext,
    authored_write: bool,
    operation: impl FnOnce(&DarwinMountContext, &Work) -> Result<T, NativeMountError>,
) -> Result<T, i32> {
    let _callback = context.callbacks.enter();
    let _origin = context.origin.enter();
    context.run(authored_write, |work| operation(context, work))
}

pub(super) fn ffi<T: From<i32>>(
    address: usize,
    authored_write: bool,
    operation: impl FnOnce(&DarwinMountContext, &Work) -> Result<T, NativeMountError>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match target(address)? {
        CallbackTarget::View(context) => run_view(context, authored_write, operation),
        CallbackTarget::Shared(_) => Err(libc::ESTALE),
    }))
    .unwrap_or(Err(libc::EIO))
    .unwrap_or_else(|error| T::from(-error))
}

pub(super) fn ffi_path<T: From<i32>>(
    address: usize,
    path: *const c_char,
    handle: u64,
    authored_write: bool,
    operation: impl FnOnce(&DarwinMountContext, *const c_char, &Work) -> Result<T, NativeMountError>,
) -> T {
    ffi_path_with_root(address, path, handle, authored_write, operation, |_, _| {
        Err(driver_errno(libc::EISDIR))
    })
}

pub(super) fn ffi_path_with_root<T: From<i32>>(
    address: usize,
    path: *const c_char,
    handle: u64,
    authored_write: bool,
    operation: impl FnOnce(&DarwinMountContext, *const c_char, &Work) -> Result<T, NativeMountError>,
    root_operation: impl FnOnce(&super::shared::SharedViews, &Work) -> Result<T, NativeMountError>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        match target(address)? {
            CallbackTarget::View(context) => run_view(context, authored_write, |context, work| {
                operation(context, path, work)
            }),
            CallbackTarget::Shared(views) => {
                let path = if path.is_null() {
                    None
                } else {
                    // SAFETY: the bridge passes a terminated pathname retained
                    // until this callback returns; no source is touched here.
                    Some(unsafe { CStr::from_ptr(path) })
                };
                if handle == 0 && path.is_some_and(|path| path.to_bytes() == b"/") {
                    return views.run(|work| root_operation(views, work));
                }
                let routed = views.route(path, handle)?;
                run_view(&routed.context, authored_write, |context, work| {
                    operation(context, routed.path.map_or(ptr::null(), CStr::as_ptr), work)
                })
            }
        }
    }))
    .unwrap_or(Err(libc::EIO))
    .unwrap_or_else(|error| T::from(-error))
}

pub(super) fn ffi_pair<T: From<i32>>(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
    operation: impl FnOnce(
        &DarwinMountContext,
        *const c_char,
        *const c_char,
        &Work,
    ) -> Result<T, NativeMountError>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        match target(address)? {
            CallbackTarget::View(context) => run_view(context, true, |context, work| {
                operation(context, source, destination, work)
            }),
            CallbackTarget::Shared(views) => {
                if source.is_null() || destination.is_null() {
                    return Err(libc::ESTALE);
                }
                // SAFETY: both strings are retained by the synchronous bridge.
                let (source, destination) =
                    unsafe { (CStr::from_ptr(source), CStr::from_ptr(destination)) };
                let (routed, destination) = views.route_pair(source, destination)?;
                run_view(&routed.context, true, |context, work| {
                    operation(
                        context,
                        routed
                            .path
                            .ok_or_else(|| driver_errno(libc::ESTALE))?
                            .as_ptr(),
                        destination.as_ptr(),
                        work,
                    )
                })
            }
        }
    }))
    .unwrap_or(Err(libc::EIO))
    .unwrap_or_else(|error| T::from(-error))
}
impl DarwinMountContext {
    pub(super) fn record_failure(&self, cause: NativeMountError, work: WorkCounters) -> i32 {
        let status = callback_errno(&cause);
        if fatal(&cause) {
            let mut state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.first_fatal.is_none() {
                state.first_fatal = Some((Some(cause), work));
            }
        }
        status
    }

    pub(super) fn run<T>(
        &self,
        authored_write: bool,
        operation: impl FnOnce(&Work) -> Result<T, NativeMountError>,
    ) -> Result<T, i32> {
        let active = self.callback_meter().map_err(|cause| {
            let work = self
                .source_original_work
                .as_ref()
                .map_or(WorkCounters::UNCHARGED, |work| work.state.lock().work);
            self.record_failure(cause, work)
        })?;
        {
            let state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some((cause, _)) = &state.first_fatal {
                return Err(cause.as_ref().map_or(libc::EIO, callback_errno));
            }
        }
        let operation = |work: &Work| {
            work.admit(WorkCounters::UNCHARGED)?;
            if authored_write {
                self.admit_write(work)?;
            }
            let value = operation(work)?;
            if authored_write {
                self.admit_write(work)?;
            }
            Ok(value)
        };
        let future_request = active.is_none();
        let work = match active {
            Some(work) => work,
            None => Work::new(self.handler_budget, &self.handler_cancellation)
                .map_err(|(cause, work)| self.record_failure(cause.into(), *work))?,
        };
        let result = crate::native_mount::run_native_mount_handler_on_meter(&work, operation);
        let completed = work.receipt();
        if future_request
            && let Err(overflow) = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .completed_future_request_work
                .try_add_assign(&completed)
        {
            match &result {
                Ok(_) => return Err(self.record_failure(overflow.into(), completed)),
                Err(cause) if !fatal(cause) => {
                    self.record_failure(overflow.into(), completed);
                }
                Err(_) => {}
            }
        }
        result.map_err(|cause| self.record_failure(cause, completed))
    }
}
