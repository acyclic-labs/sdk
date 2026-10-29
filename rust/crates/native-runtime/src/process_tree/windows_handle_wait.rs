//! A finite native handle wait retains its original deadline and resume event.
use super::*;
use std::os::windows::io::AsRawHandle;
use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{INFINITE, SetWaitableTimerEx, WaitForMultipleObjects};
#[path = "windows_resources.rs"]
pub(super) mod resources;
pub(super) struct HandleWait<'a> {
    meter: Option<&'a dyn crate::NativeReadMeter>,
    resume: super::platform::wait::ResumeNotification<'a>,
    timer: Option<resources::Event<'a>>,
}

impl<'a> HandleWait<'a> {
    pub(super) fn new(meter: Option<&'a dyn crate::NativeReadMeter>) -> io::Result<Self> {
        Ok(Self {
            meter,
            resume: super::platform::wait::ResumeNotification::new_with_meter(meter)?,
            timer: resources::Event::timer(meter)?,
        })
    }

    pub(in crate::process_tree) fn wait_handle(
        &mut self,
        process: &impl AsRawHandle,
        deadline: Instant,
    ) -> io::Result<()> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Some(timer) = &self.timer {
            let ticks = i64::try_from(remaining.as_nanos().div_ceil(100))
                .unwrap_or(i64::MAX)
                .max(1);
            let due = -ticks;
            let (armed, recorded) =
                pipe_operation_attempt(self.meter, crate::NativeOperationKind::Write, || {
                    // SAFETY: retained unnamed timer and valid relative one-shot deadline.
                    Ok(unsafe {
                        SetWaitableTimerEx(
                            timer.as_raw_handle(),
                            &raw const due,
                            0,
                            None,
                            std::ptr::null(),
                            std::ptr::null(),
                            0,
                        )
                    } != 0)
                })?;
            recorded?;
            if !armed? {
                self.timer = None;
            }
        }
        let handles = [
            process.as_raw_handle(),
            self.resume.event.as_raw_handle(),
            self.timer
                .as_ref()
                .map_or(std::ptr::null_mut(), AsRawHandle::as_raw_handle),
        ];
        // Relative waits exclude suspend time. Resume returns control to the
        // handle owner, which rechecks its original Instant deadline.
        let milliseconds = if self.timer.is_some() {
            INFINITE
        } else {
            u32::try_from(remaining.as_nanos().div_ceil(1_000_000))
                .unwrap_or(u32::MAX - 1)
                .min(u32::MAX - 1)
        };
        let count = if self.timer.is_some() { 3 } else { 2 };
        let (answer, recorded) =
            pipe_operation_attempt(self.meter, crate::NativeOperationKind::Read, || {
                // SAFETY: all handles remain owned throughout this wait.
                let answer =
                    unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, milliseconds) };
                if answer == WAIT_FAILED {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(answer)
                }
            })?;
        let answer = answer?;
        recorded?;
        if answer == WAIT_TIMEOUT || answer < count {
            Ok(())
        } else {
            Err(io::Error::other("invalid native handle wait response"))
        }
    }
}
