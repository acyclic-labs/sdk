//! Retained overlapped pipe requests and their original metered custody.
use super::*;
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::{
    ERROR_BROKEN_PIPE, ERROR_IO_INCOMPLETE, ERROR_IO_PENDING, ERROR_NO_DATA, ERROR_NOT_FOUND,
    ERROR_OPERATION_ABORTED, HANDLE,
};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Threading::CreateEventW;

#[cfg(test)]
#[path = "capture_pending_windows/tests.rs"]
mod tests;

pub(in crate::process_tree) struct Cleanup<'a> {
    meter: Option<&'a dyn crate::NativeReadMeter>,
    token: Option<u64>,
}

impl<'a> Cleanup<'a> {
    pub(in crate::process_tree) fn new(
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        Ok(Self {
            meter,
            token: meter
                .map(crate::NativeReadMeter::reserve_cleanup_write)
                .transpose()?,
        })
    }

    pub(in crate::process_tree) fn record(&mut self) -> io::Result<()> {
        self.token.take().map_or(Ok(()), |token| {
            self.meter
                .ok_or(io::ErrorKind::InvalidInput)?
                .record_cleanup_write(token)
        })
    }
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if let (Some(meter), Some(token)) = (self.meter, self.token.take()) {
            meter.cancel_cleanup_write(token);
        }
    }
}

pub(in crate::process_tree) struct Event<'a> {
    handle: OwnedHandle,
    cleanup: Cleanup<'a>,
}

impl<'a> Event<'a> {
    pub(in crate::process_tree) fn new(
        meter: Option<&'a dyn crate::NativeReadMeter>,
        manual_reset: bool,
    ) -> io::Result<Self> {
        let cleanup = Cleanup::new(meter)?;
        let (opened, recorded) =
            pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
                // SAFETY: unnamed manual-reset event, exclusively owned on success.
                let raw = unsafe {
                    CreateEventW(
                        std::ptr::null(),
                        i32::from(manual_reset),
                        0,
                        std::ptr::null(),
                    )
                };
                if raw.is_null() {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: CreateEventW transferred this fresh handle.
                Ok(unsafe { OwnedHandle::from_raw_handle(raw) })
            })?;
        let event = Self {
            handle: opened?,
            cleanup,
        };
        recorded?;
        Ok(event)
    }

    fn raw(&self) -> io::Result<HANDLE> {
        Ok(self.handle.as_raw_handle())
    }

    pub(in crate::process_tree) fn timer(
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Option<Self>> {
        let cleanup = Cleanup::new(meter)?;
        let (opened, recorded) =
            pipe_operation_attempt(meter, crate::NativeOperationKind::Write, || {
                Ok(super::super::platform::wait::deadline_timer())
            })?;
        let event = opened?.map(|handle| Self { handle, cleanup });
        recorded?;
        Ok(event)
    }
}

impl AsRawHandle for Event<'_> {
    fn as_raw_handle(&self) -> HANDLE {
        self.handle.as_raw_handle()
    }
}

impl AsHandle for Event<'_> {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.handle.as_handle()
    }
}

impl Drop for Event<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup.record();
    }
}

struct Memory<'a> {
    meter: Option<&'a dyn crate::NativeReadMeter>,
    bytes: u64,
}

impl<'a> Memory<'a> {
    fn new(meter: Option<&'a dyn crate::NativeReadMeter>) -> Self {
        Self { meter, bytes: 0 }
    }

    fn allocate(&mut self, bytes: usize) -> io::Result<()> {
        if let Some(meter) = self.meter {
            meter.allocate(bytes as u64)?;
        }
        self.bytes += bytes as u64;
        Ok(())
    }

    fn excess(&mut self, bytes: usize) -> io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some(meter) = self.meter {
            let (retained, recorded) = meter.record_completed_adoption(bytes as u64);
            self.bytes += retained;
            recorded?;
        }
        Ok(())
    }

    fn failed(&self, error: std::collections::TryReserveError) -> io::Error {
        match self.meter {
            Some(meter) => meter.allocation_failed(error),
            None => io::Error::other(error),
        }
    }
}

impl Drop for Memory<'_> {
    fn drop(&mut self) {
        if let Some(meter) = self.meter {
            meter.release(self.bytes);
        }
    }
}

struct State<'a> {
    overlapped: OVERLAPPED,
    event: Event<'a>,
    byte: u8,
    input: Vec<u8>,
    credit: Option<crate::NativeByteReservation<&'a dyn crate::NativeReadMeter>>,
    cancel: Cleanup<'a>,
    settle: Cleanup<'a>,
    _memory: Memory<'a>,
}

pub(super) struct Pending<'a> {
    // One fallibly allocated state. Never resized after queuing native IO.
    state: Vec<State<'a>>,
    pipe: HANDLE,
    active: bool,
    completed: Option<u32>,
    cleanup_attempted: bool,
    meter: Option<&'a dyn crate::NativeReadMeter>,
}

impl<'a> Pending<'a> {
    pub(super) fn new(
        pipe: HANDLE,
        input: Option<&[u8]>,
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        let mut memory = Memory::new(meter);
        let event = Event::new(meter, true)?;
        let cancel = Cleanup::new(meter)?;
        let settle = Cleanup::new(meter)?;
        let mut retained = Vec::new();
        if let Some(bytes) = input {
            memory.allocate(bytes.len())?;
            retained
                .try_reserve_exact(bytes.len())
                .map_err(|error| memory.failed(error))?;
            memory.excess(retained.capacity().saturating_sub(bytes.len()))?;
            if let Some(meter) = meter {
                meter.admit_examination(0, bytes.len() as u64)?;
            }
            retained.extend_from_slice(bytes);
            if let Some(meter) = meter {
                meter.record_examination(0, bytes.len() as u64)?;
            }
        }
        let credit = input
            .and_then(|bytes| meter.map(|meter| (meter, bytes.len())))
            .map(|(meter, maximum)| {
                crate::NativeByteReservation::new(meter, crate::NativeOperationKind::Write, maximum)
            })
            .transpose()?;
        memory.allocate(std::mem::size_of::<State<'_>>())?;
        let mut state = Vec::new();
        state
            .try_reserve_exact(1)
            .map_err(|error| memory.failed(error))?;
        memory.excess((state.capacity() - 1) * std::mem::size_of::<State<'_>>())?;
        // SAFETY: zero is a valid initial OVERLAPPED control block.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.hEvent = event.raw()?;
        state.push(State {
            overlapped,
            event,
            byte: 0,
            input: retained,
            credit,
            cancel,
            settle,
            _memory: memory,
        });
        let mut pending = Self {
            state,
            pipe,
            active: false,
            completed: None,
            cleanup_attempted: false,
            meter,
        };
        pending.start(input.is_some())?;
        Ok(pending)
    }

    fn start(&mut self, writing: bool) -> io::Result<()> {
        if writing {
            let mode = windows_sys::Win32::System::Pipes::PIPE_WAIT;
            let (changed, recorded) =
                pipe_operation_attempt(self.meter, crate::NativeOperationKind::Write, || {
                    // SAFETY: capture exclusively owns this byte-mode pipe.
                    if unsafe {
                        windows_sys::Win32::System::Pipes::SetNamedPipeHandleState(
                            self.pipe,
                            &raw const mode,
                            std::ptr::null(),
                            std::ptr::null(),
                        )
                    } == 0
                    {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                })?;
            recorded?;
            match changed {
                Err(cause) if closed_pipe(&cause) => return self.finish(0),
                result => result?,
            }
        }
        let state = self.state.first_mut().ok_or(io::ErrorKind::NotFound)?;
        let mut actual = 0;
        let kind = if writing {
            crate::NativeOperationKind::Write
        } else {
            crate::NativeOperationKind::Read
        };
        let (started, recorded) = pipe_operation_attempt(self.meter, kind, || {
            // SAFETY: std process pipes are overlapped; both this buffer and
            // the control block remain stable until confirmed completion.
            let answer = unsafe {
                if writing {
                    windows_sys::Win32::Storage::FileSystem::WriteFile(
                        self.pipe,
                        state.input.as_ptr(),
                        u32::try_from(state.input.len()).map_err(io::Error::other)?,
                        &raw mut actual,
                        &raw mut state.overlapped,
                    )
                } else {
                    windows_sys::Win32::Storage::FileSystem::ReadFile(
                        self.pipe,
                        &raw mut state.byte,
                        0,
                        &raw mut actual,
                        &raw mut state.overlapped,
                    )
                }
            };
            if answer == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
        match started {
            Ok(()) => {
                self.active = true;
            }
            Err(cause) if cause.raw_os_error() == Some(ERROR_IO_PENDING.cast_signed()) => {
                self.active = true;
            }
            Err(cause) => {
                let charged = self.finish(actual);
                recorded?;
                charged?;
                if !closed_pipe(&cause) {
                    return Err(cause);
                }
                return Ok(());
            }
        }
        recorded
    }

    pub(super) fn event(&self) -> io::Result<HANDLE> {
        self.state
            .first()
            .ok_or(io::ErrorKind::NotFound)?
            .event
            .raw()
    }
    pub(super) fn active(&self) -> bool {
        self.active
    }

    fn finish(&mut self, actual: u32) -> io::Result<()> {
        self.active = false;
        self.completed = Some(actual);
        self.state
            .first_mut()
            .and_then(|state| state.credit.take())
            .map_or(Ok(()), |mut credit| credit.record(actual as usize))
    }

    pub(super) fn poll(&mut self) -> (Option<u32>, io::Result<()>) {
        if !self.active {
            return (self.completed, Ok(()));
        }
        let queried = self.query(false);
        (self.completed, queried)
    }

    fn query(&mut self, cleanup: bool) -> io::Result<()> {
        let state = self.state.first_mut().ok_or(io::ErrorKind::NotFound)?;
        let mut actual = 0;
        let mut operation = || {
            // SAFETY: retained pipe/control block; cleanup waits for the
            // kernel to release all references before retiring their storage.
            if unsafe {
                GetOverlappedResult(
                    self.pipe,
                    &raw const state.overlapped,
                    &raw mut actual,
                    i32::from(cleanup),
                )
            } == 0
            {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        };
        let (answer, recorded) = if cleanup {
            let recorded = state.settle.record();
            (operation(), recorded)
        } else {
            pipe_operation_attempt(self.meter, crate::NativeOperationKind::Read, operation)?
        };
        self.accept_completion(answer, recorded, actual)
    }

    fn accept_completion(
        &mut self,
        answer: io::Result<()>,
        recorded: io::Result<()>,
        actual: u32,
    ) -> io::Result<()> {
        if answer
            .as_ref()
            .err()
            .is_some_and(|cause| cause.raw_os_error() == Some(ERROR_IO_INCOMPLETE.cast_signed()))
        {
            return recorded;
        }
        let terminal = answer.is_ok()
            || answer.as_ref().err().is_some_and(|cause| {
                matches!(
                    cause.raw_os_error().map(i32::cast_unsigned),
                    Some(ERROR_BROKEN_PIPE | ERROR_NO_DATA | ERROR_OPERATION_ABORTED)
                )
            });
        if !terminal {
            recorded?;
            return answer.map(|_| ());
        }
        let charged = self.finish(actual);
        recorded?;
        charged?;
        match answer {
            Err(cause)
                if !matches!(
                    cause.raw_os_error().map(i32::cast_unsigned),
                    Some(ERROR_BROKEN_PIPE | ERROR_NO_DATA | ERROR_OPERATION_ABORTED)
                ) =>
            {
                Err(cause)
            }
            _ => Ok(()),
        }
    }

    pub(super) fn complete(&mut self) -> (Option<u32>, io::Result<()>) {
        if !self.active {
            return (self.completed, Ok(()));
        }
        if self.cleanup_attempted {
            return (
                None,
                Err(io::Error::other("pipe cancellation did not settle")),
            );
        }
        self.cleanup_attempted = true;
        let result = self.cancel_and_settle().and_then(|()| {
            if self.active {
                Err(io::Error::other("pipe cancellation did not settle"))
            } else {
                Ok(())
            }
        });
        (self.completed, result)
    }

    fn cancel_and_settle(&mut self) -> io::Result<()> {
        let state = self.state.first_mut().ok_or(io::ErrorKind::NotFound)?;
        let recorded = state.cancel.record();
        // SAFETY: cancellation targets only this retained request. A lost
        // cancellation race still requires observing terminal completion.
        let cancelled = unsafe { CancelIoEx(self.pipe, &raw const state.overlapped) };
        let cancellation = if cancelled == 0 {
            let cause = io::Error::last_os_error();
            if cause.raw_os_error() == Some(ERROR_NOT_FOUND.cast_signed()) {
                Ok(())
            } else {
                Err(cause)
            }
        } else {
            Ok(())
        };
        let settled = self.query(true);
        settled.and(recorded).and(cancellation)
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.complete();
            if self.active {
                // Unconfirmed kernel custody retains the exact buffers,
                // handles, byte reservations and allocation owner together.
                std::mem::forget(std::mem::take(&mut self.state));
            }
        }
    }
}

// Both statuses are terminal native pipe closure. Unknown errors retain the
// exact OVERLAPPED request until the kernel confirms completion.
fn closed_pipe(cause: &io::Error) -> bool {
    matches!(
        cause.raw_os_error().map(i32::cast_unsigned),
        Some(ERROR_BROKEN_PIPE | ERROR_NO_DATA)
    )
}
