use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::process::{Child, ExitStatus};
use std::time::Instant;
use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Power::{
    DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS, RegisterSuspendResumeNotification,
    UnregisterSuspendResumeNotification,
};
use windows_sys::Win32::System::Threading::{
    CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, CreateEventW, CreateWaitableTimerExW, INFINITE,
    SYNCHRONIZATION_SYNCHRONIZE, SetEvent, SetWaitableTimerEx, TIMER_MODIFY_STATE,
    WaitForMultipleObjects,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    DEVICE_NOTIFY_CALLBACK, PBT_APMRESUMEAUTOMATIC, PBT_APMRESUMESUSPEND,
};

pub(in crate::process_tree) struct ResumeNotification {
    pub(in crate::process_tree) event: OwnedHandle,
    registration: isize,
}
impl ResumeNotification {
    pub(in crate::process_tree) fn new() -> io::Result<Self> {
        // SAFETY: unnamed auto-reset event, owned on success.
        let event = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transfers one fresh owned event handle.
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        let parameters = DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS {
            Callback: Some(resume_notification),
            Context: event.as_raw_handle(),
        };
        // SAFETY: registration copies the callback parameters; its context is
        // only this retained kernel handle, with no borrowed Rust allocation.
        let registration = unsafe {
            RegisterSuspendResumeNotification(
                (&raw const parameters).cast_mut().cast(),
                DEVICE_NOTIFY_CALLBACK,
            )
        };
        if registration == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            event,
            registration,
        })
    }
}
impl Drop for ResumeNotification {
    fn drop(&mut self) {
        // SAFETY: this exclusively owned registration is cancelled before
        // releasing the event handle carried by its callback.
        unsafe { UnregisterSuspendResumeNotification(self.registration) };
    }
}

pub(in crate::process_tree) unsafe extern "system" fn resume_notification(
    context: *const c_void,
    kind: u32,
    _setting: *const c_void,
) -> u32 {
    if matches!(kind, PBT_APMRESUMEAUTOMATIC | PBT_APMRESUMESUSPEND) {
        // SAFETY: context is the event handle retained by ResumeNotification.
        unsafe { SetEvent(context.cast_mut()) };
    }
    0
}

pub(in crate::process_tree) fn deadline_timer() -> Option<OwnedHandle> {
    // SAFETY: unnamed timer with default security, owned on success.
    let timer = unsafe {
        CreateWaitableTimerExW(
            std::ptr::null(),
            std::ptr::null(),
            CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
            SYNCHRONIZATION_SYNCHRONIZE | TIMER_MODIFY_STATE,
        )
    };
    (!timer.is_null()).then(|| {
        // SAFETY: successful creation transfers one fresh owned timer handle.
        unsafe { OwnedHandle::from_raw_handle(timer) }
    })
}

pub(in crate::process_tree) fn wait_for_exit(
    child: &mut Child,
    deadline: Instant,
) -> io::Result<ExitStatus> {
    wait(child, deadline, true)
}

#[cfg(test)]
pub(in crate::process_tree) fn wait_for_exit_coarse(
    child: &mut Child,
    deadline: Instant,
) -> io::Result<ExitStatus> {
    wait(child, deadline, false)
}

fn wait(child: &mut Child, deadline: Instant, precise: bool) -> io::Result<ExitStatus> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    if Instant::now() >= deadline {
        return Err(exit_timeout());
    }
    let resume = ResumeNotification::new()?;
    let timer = precise.then(deadline_timer).flatten();
    wait_with_resume(child, deadline, timer, &resume, Instant::now)
}

pub(in crate::process_tree) fn wait_with_resume(
    child: &mut Child,
    deadline: Instant,
    mut timer: Option<OwnedHandle>,
    resume: &ResumeNotification,
    mut now: impl FnMut() -> Instant,
) -> io::Result<ExitStatus> {
    loop {
        let remaining = deadline.saturating_duration_since(now());
        if remaining.is_zero() {
            return child.try_wait()?.ok_or_else(exit_timeout);
        }
        if let Some(handle) = &timer {
            let ticks = i64::try_from(remaining.as_nanos().div_ceil(100)).unwrap_or(i64::MAX);
            let due = -ticks;
            // SAFETY: this owned timer and relative due time outlive the call.
            if unsafe {
                SetWaitableTimerEx(
                    handle.as_raw_handle(),
                    &raw const due,
                    0,
                    None,
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                )
            } == 0
            {
                timer = None;
            }
        }
        let handles: [HANDLE; 3] = [
            child.as_raw_handle(),
            resume.event.as_raw_handle(),
            timer
                .as_ref()
                .map_or(std::ptr::null_mut(), |timer| timer.as_raw_handle()),
        ];
        // A relative timer and a DWORD timeout both exclude suspend time.
        // The resume event wakes either path to recheck the original QPC deadline.
        let milliseconds = if timer.is_some() {
            INFINITE
        } else {
            u32::try_from(remaining.as_nanos().div_ceil(1_000_000))
                .unwrap_or(u32::MAX - 1)
                .min(u32::MAX - 1)
        };
        let count = if timer.is_some() { 3 } else { 2 };
        // SAFETY: the selected handles stay owned for the complete wait.
        match unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, milliseconds) } {
            WAIT_OBJECT_0 => return child.wait(),
            result
                if result == WAIT_OBJECT_0 + 1
                    || result == WAIT_OBJECT_0 + 2
                    || result == WAIT_TIMEOUT =>
            {
                if let Some(status) = child.try_wait()? {
                    return Ok(status);
                }
            }
            WAIT_FAILED => return Err(io::Error::last_os_error()),
            _ => return Err(io::Error::other("unexpected process wait result")),
        }
    }
}

fn exit_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "process exit deadline exceeded; termination may be unresolved",
    )
}
