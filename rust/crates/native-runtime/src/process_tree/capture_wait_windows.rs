//! Readiness waits retain pending input across output and control events.
use super::*;
use std::os::windows::io::AsRawHandle;
use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{INFINITE, SetWaitableTimerEx, WaitForMultipleObjects};
#[path = "capture_pending_windows.rs"]
pub(super) mod pending;
use pending::Pending;
pub(super) struct CaptureWait<'a> {
    stdin: Option<Pending<'a>>,
    meter: Option<&'a dyn crate::NativeReadMeter>,
    resume: super::platform::wait::ResumeNotification<'a>,
    timer: Option<pending::Event<'a>>,
}

impl<'a> CaptureWait<'a> {
    pub(super) fn new(meter: Option<&'a dyn crate::NativeReadMeter>) -> io::Result<Self> {
        Ok(Self {
            stdin: None,
            meter,
            resume: super::platform::wait::ResumeNotification::new_with_meter(meter)?,
            timer: pending::Event::timer(meter)?,
        })
    }

    fn input_completed(&mut self, actual: Option<u32>) -> (Option<usize>, io::Result<()>) {
        let Some(actual) = actual else {
            return (None, Ok(()));
        };
        drop(self.stdin.take());
        (Some(actual as usize), Ok(()))
    }

    pub(super) fn finish_input(&mut self) -> io::Result<()> {
        let (actual, result) = self
            .stdin
            .as_mut()
            .map_or((None, Ok(())), Pending::complete);
        self.input_completed(actual).1.and(result)
    }

    pub(in crate::process_tree) fn wait_handle(
        &mut self,
        process: &impl AsRawHandle,
        deadline: Instant,
    ) -> io::Result<()> {
        let mut handles = [std::ptr::null_mut(); 8];
        handles[0] = process.as_raw_handle();
        self.await_handles(&mut handles, 1, deadline)?
    }

    fn await_handles(
        &mut self,
        handles: &mut [HANDLE; 8],
        mut count: usize,
        deadline: Instant,
    ) -> io::Result<io::Result<()>> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        *handles.get_mut(count).ok_or(io::ErrorKind::InvalidInput)? =
            self.resume.event.as_raw_handle();
        count += 1;
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
        if let Some(timer) = &self.timer {
            *handles.get_mut(count).ok_or(io::ErrorKind::InvalidInput)? = timer.as_raw_handle();
            count += 1;
        }
        // Relative waits exclude suspend time. Resume returns control to the
        // capture owner, which rechecks its original Instant deadline.
        let milliseconds = if self.timer.is_some() {
            INFINITE
        } else {
            u32::try_from(remaining.as_nanos().div_ceil(1_000_000))
                .unwrap_or(u32::MAX - 1)
                .min(u32::MAX - 1)
        };
        let count = u32::try_from(count).map_err(io::Error::other)?;
        let (answer, recorded) =
            pipe_operation_attempt(self.meter, crate::NativeOperationKind::Read, || {
                // SAFETY: every handle and pending IO operation stays retained until
                // this wait and its subsequent completion/cancellation finish.
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
        Ok(if answer == WAIT_TIMEOUT || answer < count {
            Ok(())
        } else {
            Err(io::Error::other("invalid capture readiness response"))
        })
    }

    pub(super) fn wait(
        &mut self,
        child: &Child,
        pipes: [Option<HANDLE>; 2],
        input: Option<(HANDLE, &[u8])>,
        wake: Option<&[super::wake::ProcessWake]>,
        exited: bool,
        deadline: Instant,
    ) -> (Option<usize>, io::Result<()>) {
        let result = (|| {
            if self.stdin.is_none()
                && let Some((pipe, bytes)) = input
            {
                self.stdin = Some(Pending::new(pipe, Some(bytes), self.meter)?);
            }
            let mut stdout = pipes[0]
                .map(|pipe| Pending::new(pipe, None, self.meter))
                .transpose()?;
            let mut stderr = pipes[1]
                .map(|pipe| Pending::new(pipe, None, self.meter))
                .transpose()?;
            // Broken-pipe initiation did not queue an operation; the capture
            // owner now drains EOF through the ordinary bounded read path.
            if stdout.as_ref().is_some_and(|pending| !pending.active())
                || stderr.as_ref().is_some_and(|pending| !pending.active())
                || self.stdin.as_ref().is_some_and(|pending| !pending.active())
            {
                return Ok(());
            }
            if wake.is_some_and(|wakes| wakes.len() > 2) {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            let mut handles = [std::ptr::null_mut(); 8];
            let mut count = 0;
            for pending in [&stdout, &stderr, &self.stdin].into_iter().flatten() {
                *handles.get_mut(count).ok_or(io::ErrorKind::InvalidInput)? = pending.event()?;
                count += 1;
            }
            if !exited {
                *handles.get_mut(count).ok_or(io::ErrorKind::InvalidInput)? = child.as_raw_handle();
                count += 1;
            }
            for wake in wake.into_iter().flatten() {
                *handles.get_mut(count).ok_or(io::ErrorKind::InvalidInput)? = wake.raw();
                count += 1;
            }
            let waited = self.await_handles(&mut handles, count, deadline)?;
            let stdout_result = stdout
                .as_mut()
                .map_or(Ok(()), |pending| pending.complete().1);
            let stderr_result = stderr
                .as_mut()
                .map_or(Ok(()), |pending| pending.complete().1);
            stdout_result?;
            stderr_result?;
            for wake in wake.into_iter().flatten() {
                wake.consume()?;
            }
            waited
        })();
        // Output, owner notifications and resume events leave an unfinished
        // write in place. Its owned buffer and byte credit survive this call.
        let (actual, completed) = self.stdin.as_mut().map_or((None, Ok(())), Pending::poll);
        let (written, recorded) = self.input_completed(actual);
        (written, recorded.and(completed).and(result))
    }
}

impl Drop for CaptureWait<'_> {
    fn drop(&mut self) {
        let _ = self.finish_input();
    }
}
