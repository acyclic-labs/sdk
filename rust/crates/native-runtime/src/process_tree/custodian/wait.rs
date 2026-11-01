//! One physical process exit delivered by the kernel to the original port.
//!
//! Custody retains each association until its packet is dequeued. Unlike a
//! cancellable readiness subscription, there is no callback, user buffer or
//! user-mode completion post between the process exit and that packet. The
//! key and message are integer identities, never dereferenced pointers.
//!
//! Native contracts: <https://learn.microsoft.com/en-us/windows/win32/devnotes/ntcreatewaitcompletionpacket>
//! and <https://learn.microsoft.com/en-us/windows/win32/devnotes/ntassociatewaitcompletionpacket>.

use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle as _, BorrowedHandle, FromRawHandle as _, OwnedHandle};
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::{
    GENERIC_READ, GENERIC_WRITE, HANDLE, NTSTATUS, RtlNtStatusToDosError,
};
use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryA};

type Create = unsafe extern "system" fn(*mut HANDLE, u32, *mut c_void) -> NTSTATUS;
type Associate = unsafe extern "system" fn(
    HANDLE,
    HANDLE,
    HANDLE,
    *mut c_void,
    *mut c_void,
    NTSTATUS,
    usize,
    *mut u8,
) -> NTSTATUS;

struct Api {
    create: Create,
    associate: Associate,
}

impl Api {
    fn load() -> io::Result<Self> {
        // These documented exports have no import library. Retain the loaded
        // system module for the process lifetime, just like the cached API.
        // SAFETY: a fixed, NUL-terminated system-library name.
        let module = unsafe { LoadLibraryA(c"ntdll.dll".as_ptr().cast()) };
        if module.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the fixed names identify the documented ntdll exports. The
        // module remains loaded and both signatures match their native ABI.
        let create =
            unsafe { GetProcAddress(module, c"NtCreateWaitCompletionPacket".as_ptr().cast()) }
                .ok_or_else(io::Error::last_os_error)?;
        // SAFETY: same retained module and documented export contract.
        let associate =
            unsafe { GetProcAddress(module, c"NtAssociateWaitCompletionPacket".as_ptr().cast()) }
                .ok_or_else(io::Error::last_os_error)?;
        Ok(Self {
            // SAFETY: the exact exported signatures are declared above.
            create: unsafe {
                std::mem::transmute::<unsafe extern "system" fn() -> isize, Create>(create)
            },
            // SAFETY: the exact exported signatures are declared above.
            associate: unsafe {
                std::mem::transmute::<unsafe extern "system" fn() -> isize, Associate>(associate)
            },
        })
    }
}

static API: OnceLock<io::Result<Api>> = OnceLock::new();

fn api() -> io::Result<&'static Api> {
    API.get_or_init(Api::load).as_ref().map_err(|error| {
        error.raw_os_error().map_or_else(
            || io::Error::new(error.kind(), error.to_string()),
            io::Error::from_raw_os_error,
        )
    })
}

fn checked(status: NTSTATUS) -> io::Result<()> {
    if status >= 0 {
        Ok(())
    } else {
        // SAFETY: translates this actual failing native status, not LastError
        // from an unrelated Win32 operation.
        let error = unsafe { RtlNtStatusToDosError(status) };
        Err(io::Error::from_raw_os_error(error.cast_signed()))
    }
}

/// An owned one-shot association. Its custodian retains it through dequeue;
/// no pending association is reused or withdrawn to infer physical exit.
pub(super) struct ExitWait {
    _packet: OwnedHandle,
}

impl ExitWait {
    pub(super) fn new(
        port: BorrowedHandle<'_>,
        process: BorrowedHandle<'_>,
        key: usize,
        message: u32,
    ) -> io::Result<Self> {
        let api = api()?;
        let mut packet = std::ptr::null_mut();
        // SAFETY: writable handle output, no object attributes or user data.
        checked(unsafe {
            (api.create)(
                &raw mut packet,
                GENERIC_READ | GENERIC_WRITE,
                std::ptr::null_mut(),
            )
        })?;
        // SAFETY: successful creation transferred this one owned handle.
        let packet = unsafe { OwnedHandle::from_raw_handle(packet) };
        // SAFETY: all three handles remain live for association. The kernel
        // retains its object references, and the completion contains only
        // integer identity fields. Already-signaled still queues the packet.
        checked(unsafe {
            (api.associate)(
                packet.as_raw_handle(),
                port.as_raw_handle(),
                process.as_raw_handle(),
                std::ptr::without_provenance_mut(key),
                std::ptr::null_mut(),
                0,
                message as usize,
                std::ptr::null_mut(),
            )
        })?;
        Ok(Self { _packet: packet })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::io::AsHandle as _;
    use windows_sys::Win32::Foundation::{
        ERROR_ACCESS_DENIED, INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::IO::{CreateIoCompletionPort, GetQueuedCompletionStatus};
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, INFINITE, PROCESS_QUERY_LIMITED_INFORMATION, SetEvent,
        WaitForSingleObject,
    };

    #[test]
    fn kernel_exit_packets_retain_identity_and_refuse_invalid_admission() -> io::Result<()> {
        // SAFETY: create one original IOCP, retained through every dequeue.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transferred this owned port.
        let port = unsafe { OwnedHandle::from_raw_handle(port) };
        for already_signaled in [false, true] {
            let process = super::super::event(true, None)?;
            if already_signaled {
                // SAFETY: this original manual event is still owned here.
                if unsafe { SetEvent(process.as_raw_handle()) } == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            let wait = ExitWait::new(port.as_handle(), process.as_handle(), usize::MAX - 1, 47)?;
            if !already_signaled {
                // SAFETY: one admission observation, not a readiness loop.
                // An unsignaled target must not manufacture physical exit.
                let (mut message, mut key, mut context) = (0, 0, std::ptr::null_mut());
                assert_eq!(
                    unsafe {
                        GetQueuedCompletionStatus(
                            port.as_raw_handle(),
                            &raw mut message,
                            &raw mut key,
                            &raw mut context,
                            0,
                        )
                    },
                    0
                );
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(WAIT_TIMEOUT.cast_signed())
                );
                // SAFETY: the same producer signals the retained event before
                // its original kernel packet is awaited; no user-mode post.
                if unsafe { SetEvent(process.as_raw_handle()) } == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            let (mut message, mut key, mut context) = (0, 0, std::ptr::null_mut());
            // SAFETY: the same original port and writable packet outputs.
            if unsafe {
                GetQueuedCompletionStatus(
                    port.as_raw_handle(),
                    &raw mut message,
                    &raw mut key,
                    &raw mut context,
                    INFINITE,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            assert_eq!((key, message), (usize::MAX - 1, 47));
            assert!(context.is_null());
            // Both target and packet retire only after the actual dequeue.
            drop(wait);
            drop(process);
        }

        // Refused owner synchronization cannot be converted to owner death.
        // SAFETY: duplicate this process with query access, omitting SYNCHRONIZE.
        let restricted = unsafe {
            super::super::duplicate(
                GetCurrentProcess(),
                GetCurrentProcess(),
                PROCESS_QUERY_LIMITED_INFORMATION,
                0,
            )?
        };
        // SAFETY: duplication transferred this restricted owned process handle.
        let restricted = unsafe { OwnedHandle::from_raw_handle(restricted) };
        // SAFETY: one native observation of this retained original handle.
        assert_eq!(
            unsafe { WaitForSingleObject(restricted.as_raw_handle(), 0) },
            WAIT_FAILED
        );
        let original_error = io::Error::last_os_error();
        assert_eq!(
            original_error.raw_os_error(),
            Some(ERROR_ACCESS_DENIED.cast_signed())
        );
        let error = ExitWait::new(port.as_handle(), restricted.as_handle(), usize::MAX, 0)
            .err()
            .expect("synchronization denied");
        assert_eq!(error.raw_os_error(), original_error.raw_os_error());

        // A real pipe is not an IOCP. The kernel must refuse this original
        // association rather than accept an undeliverable exit subscription.
        let (read, write) = std::io::pipe()?;
        let wrong_port: OwnedHandle = read.into();
        let target = super::super::event(true, None)?;
        let error = ExitWait::new(wrong_port.as_handle(), target.as_handle(), usize::MAX, 0)
            .err()
            .expect("non-port refused");
        assert!(error.raw_os_error().is_some());
        drop(write);
        // SAFETY: one final observation proves neither refused association
        // published a terminal to the valid original completion port.
        let (mut message, mut key, mut context) = (0, 0, std::ptr::null_mut());
        assert_eq!(
            unsafe {
                GetQueuedCompletionStatus(
                    port.as_raw_handle(),
                    &raw mut message,
                    &raw mut key,
                    &raw mut context,
                    0,
                )
            },
            0
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(WAIT_TIMEOUT.cast_signed())
        );
        Ok(())
    }
}
