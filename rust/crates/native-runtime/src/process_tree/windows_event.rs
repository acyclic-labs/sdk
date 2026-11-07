//! Owned Windows events and their original metered cleanup custody.
use super::cleanup::Cleanup;
use super::*;
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::Threading::CreateEventW;

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
}

impl AsHandle for Event<'_> {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.handle.as_handle()
    }
}

impl AsRawHandle for Event<'_> {
    fn as_raw_handle(&self) -> HANDLE {
        self.handle.as_raw_handle()
    }
}

impl Drop for Event<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup.record();
    }
}
