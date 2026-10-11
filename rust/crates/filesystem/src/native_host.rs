//! Capability-rooted native filesystem access shared by capture and materialization.
#![allow(
    unsafe_code,
    reason = "isolates the reviewed platform calls of native filesystem access"
)]

use cap_fs_ext::DirExt as _;
use cap_std::fs::{Dir, Metadata, OpenOptions, Permissions, ReadDir};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::Path;
use crate::demand::{DemandError, DemandWork};
use crate::performance::{WorkBudget, WorkCounters};
use crate::NativeRootIdentity;

/// A closed, preaccepted replacement of one conditional materialization
/// record. Core state owns the genuine namespace admission, revision check,
/// record lock and durable previous-image copy before preparing this packet.
/// No path/descriptor escapes and completion opens/allocates/converts nothing.
pub(crate) struct PreparedMaterializationRecord {
    file: File,
    parent: Dir,
    #[cfg(unix)]
    temporary_name: NativeEnvironmentBuffer<u8>,
    #[cfg(unix)]
    current_name: NativeEnvironmentBuffer<u8>,
    #[cfg(windows)]
    rename_packet: NativeEnvironmentBuffer<u64>,
    #[cfg(windows)]
    rename_length: u32,
    #[cfg(windows)]
    parent_sync: File,
    maximum_bytes: usize,
    published: bool,
    credit: crate::materializer::NativeWorkReservation,
    cleanup: Option<crate::materializer::NativeCleanupWrite>,
    _allocation: crate::materializer::NativeAllocation,
}

impl PreparedMaterializationRecord {
    pub(crate) fn prepare(
        parent_path: &Path, current: &Path, previous: &Path, temporary: &Path,
        has_current: bool, work: &crate::materializer::NativeMaterializationWork, maximum_bytes: usize,
    ) -> Result<Self, crate::materializer::NativeTreeMaterializationError> {
        use crate::materializer::NativeTreeMaterializationError as E;
        require_leaf(current)?;
        require_leaf(previous)?;
        require_leaf(temporary)?;
        if !has_current || current == previous || current == temporary || previous == temporary {
            return Err(E::InvalidLayout);
        }
        let allocation = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let parent = open_root_directory_with_work(parent_path, &mut &*work)?;
        if entry_identity_with_work(&parent, current, &mut &*work)?.is_none() {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }
        // A previous stale temporary belongs to this already-locked record;
        // its disposal is admitted now, never after the physical action.
        match remove_leaf_with_work(&parent, temporary, &mut &*work) {
            Ok(()) => {}
            Err(E::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        let temporary_name = prepared_materialization_name(temporary, work)?;
        #[cfg(unix)]
        let current_name = prepared_materialization_name(current, work)?;
        #[cfg(windows)]
        let (rename_packet, rename_length) = prepared_materialization_rename_packet(&parent, current, work)?;
        #[cfg(windows)]
        let parent_sync = {
            use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
            use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
            open_windows_held_with_work(&parent, Path::new("."), FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, &mut &*work)?
        };
        let credit = work.reserve_work(WorkCounters {
            backend_write_operations: (maximum_bytes as u64).checked_add(1).ok_or(crate::WorkError::Overflow)?,
            object_bytes_written: maximum_bytes as u64,
            durability_operations: 2,
            ..WorkCounters::UNCHARGED
        })?;
        let cleanup = work.reserve_cleanup_write()?;
        #[cfg(unix)]
        let file = open_unix_named_with(&parent,
            std::ffi::CStr::from_bytes_with_nul(&temporary_name.data).expect("prepared temporary leaf is terminated"),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            || work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }))?;
        #[cfg(windows)]
        let file = with_wide_path_with_work(temporary, &mut &*work, |name, work| {
            create_windows_provisional_file_with_work(&parent, name, work)
        })?;
        let prepared = Self {
            file, parent,
            #[cfg(unix)] temporary_name,
            #[cfg(unix)] current_name,
            #[cfg(windows)] rename_packet,
            #[cfg(windows)] rename_length,
            #[cfg(windows)] parent_sync,
            maximum_bytes, published: false, credit, cleanup: Some(cleanup), _allocation: allocation,
        };
        #[cfg(windows)]
        keep_windows_created_file_with_work(&prepared.file, &mut &*work)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }

    /// Completes only the previously accepted CAS packet. Accounting credit
    /// does not authorize any additional action or renew the caller's scope.
    pub(crate) fn complete(mut self, bytes: &[u8]) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
        use std::io::Write as _;
        use crate::materializer::NativeTreeMaterializationError as E;
        if bytes.len() > self.maximum_bytes { return Err(E::InvalidLayout); }
        let file = &mut self.file;
        let mut offset = 0;
        while offset < bytes.len() {
            self.credit.record_completed(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
            let count = file.write(&bytes[offset..]).map_err(E::Io)?;
            if count > bytes.len() - offset { return Err(E::InvalidLayout); }
            self.credit.record_completed(WorkCounters { object_bytes_written: count as u64, ..WorkCounters::UNCHARGED })?;
            if count == 0 { return Err(io::Error::from(io::ErrorKind::WriteZero).into()); }
            offset += count;
        }
        self.credit.record_completed(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })?;
        sync_held_descriptor_once(file)?;
        self.credit.record_completed(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
        #[cfg(unix)]
        {
            // SAFETY: this retained name was validated/terminated at prepare.
            let temporary = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: preparation validated and terminated these owned names;
            // borrowing them performs no late scan, encoding or allocation.
            let current = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.current_name.data) };
            rename_unix_names_once(&self.parent, temporary, &self.parent, current, true)?;
        }
        #[cfg(windows)]
        windows_rename_packet_once(file, &self.rename_packet.data, self.rename_length)?;
        self.published = true;
        drop(self.cleanup.take());
        self.credit.record_completed(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })?;
        #[cfg(unix)]
        sync_held_descriptor_once(&self.parent)?;
        #[cfg(windows)]
        sync_held_descriptor_once(&self.parent_sync)?;
        Ok(())
    }
}

impl Drop for PreparedMaterializationRecord {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else { return; };
        if self.published { drop(cleanup); return; }
        #[cfg(unix)]
        let _ = cleanup.attempt_recorded(|| {
            use std::os::fd::AsRawFd as _;
            // SAFETY: preparation validated and terminated this retained name.
            let name = unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(&self.temporary_name.data) };
            // SAFETY: the exact held parent and prepared name remain alive;
            // no cancelled allocator/path traversal can gate this cleanup.
            if unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
                Ok(())
            } else { Err(io::Error::last_os_error()) }
        });
        #[cfg(windows)]
        let _ = cleanup.attempt_recorded(|| {
            use windows::Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation};
            windows_set_information_held(&self.file,
                &FILE_DISPOSITION_INFORMATION { DeleteFile: true }, FileDispositionInformation)
        });
    }
}

#[cfg(unix)]
fn prepared_materialization_name(
    leaf: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<NativeEnvironmentBuffer<u8>, crate::materializer::NativeTreeMaterializationError> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = leaf.as_os_str().as_bytes();
    for byte in bytes {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *byte == 0 { return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout); }
    }
    let copy = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let mut name = native_environment_buffer::<u8>(bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?, work)?;
    name.data.extend_from_slice(bytes);
    name.data.push(0);
    work.record_admitted(copy)?;
    Ok(name)
}

#[cfg(windows)]
fn prepared_materialization_rename_packet(
    parent: &Dir, leaf: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(NativeEnvironmentBuffer<u64>, u32), crate::materializer::NativeTreeMaterializationError> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
    let units = wide_units_with(leaf.as_os_str(), || work.charge(
        WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
    ))?;
    let name_bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let header = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
    let length = header.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?;
    let words = length.max(std::mem::size_of::<FILE_RENAME_INFORMATION>()).div_ceil(8);
    let encoded = WorkCounters { bytes_encoded: name_bytes as u64, ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut buffer = native_environment_buffer::<u64>(words, work)?;
    buffer.data.resize(words, 0);
    let information = buffer.data.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: pre-funded aligned storage contains the full header/name tail.
    // The leading BOOLEAN is ReplaceIfExists, set only for this locked CAS.
    unsafe {
        information.cast::<u8>().write(1);
        std::ptr::addr_of_mut!((*information).RootDirectory).write(windows::Win32::Foundation::HANDLE(parent.as_handle().as_raw_handle()));
        std::ptr::addr_of_mut!((*information).FileNameLength).write(u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?);
        let tail = buffer.data.as_mut_ptr().cast::<u8>().add(header).cast::<u16>();
        for (index, unit) in leaf.as_os_str().encode_wide().enumerate() {
            if unit == 0 { return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout); }
            tail.add(index).write(unit);
        }
    }
    work.record_admitted(encoded)?;
    Ok((buffer, u32::try_from(length).map_err(|_| crate::WorkError::Overflow)?))
}

#[cfg(unix)]
fn rename_unix_names_once(
    from: &Dir, source: &std::ffi::CStr, to: &Dir, destination: &std::ffi::CStr, replace: bool,
) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: actual held parents and prepared names outlive this syscall.
    #[cfg(target_os = "linux")]
    let result = unsafe { libc::syscall(libc::SYS_renameat2, from.as_raw_fd(), source.as_ptr(),
        to.as_raw_fd(), destination.as_ptr(), if replace { 0 } else { libc::RENAME_NOREPLACE }) };
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renameatx_np(from.as_raw_fd(), source.as_ptr(), to.as_raw_fd(),
        destination.as_ptr(), if replace { 0 } else { libc::RENAME_EXCL }) } as libc::c_long;
    if result == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(windows)]
fn windows_rename_packet_once(file: &File, buffer: &[u64], length: u32) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{FileRenameInformation, NtSetInformationFile};
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: the held DELETE-capable file and complete aligned prebuilt
    // packet remain alive; the parent handle stored in the packet is pinned.
    let status = unsafe { NtSetInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
        &raw mut block, buffer.as_ptr().cast(), length, FileRenameInformation) };
    if status.is_err() { Err(status_error(status)) } else { Ok(()) }
}

#[cfg(unix)]
fn sync_held_descriptor_once(file: &impl std::os::fd::AsRawFd) -> io::Result<()> {
    // SAFETY: the exact held file/directory descriptor remains live.
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(windows)]
fn sync_held_descriptor_once(file: &impl std::os::windows::io::AsRawHandle) -> io::Result<()> {
    // SAFETY: this held file/directory handle has genuine write access.
    unsafe { windows::Win32::Storage::FileSystem::FlushFileBuffers(
        windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
    ) }.map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}

fn native_read(work: &mut DemandWork) -> Result<(), DemandError> {
    work.charge(WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    })
}

fn native_buffer<W: crate::demand::SourceDemandReadWork>(work: &mut W, bytes: u64, copied: u64, encoded: u64) -> Result<(), DemandError> {
    let delta = WorkCounters { bytes_copied: copied, bytes_encoded: encoded, ..WorkCounters::UNCHARGED };
    work.check()?;
    work.preflight(delta)?;
    work.allocate(bytes)?;
    work.record(delta)?;
    Ok(())
}

pub(crate) trait NativeHostWork {
    type Error: std::error::Error + Send + Sync + 'static + From<io::Error> + From<crate::WorkError>;
    type Allocation;
    type CleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind>;
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error;
    fn invalid_request() -> Self::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid native source request").into()
    }
    fn check(&self) -> Result<(), Self::Error>;
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    /// Atomically admits and records one actual begun host stage. No shared
    /// original-meter lock may remain held while the following syscall runs.
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error>;
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error>;
    /// Reserves bounded logical output for an opaque standard-library decode.
    /// Its hidden allocator behavior is not reported as a fabricated attempt.
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error>;
    /// Records completed physical backing and updates its existing guard
    /// before any late refusal; errors must not erase newly owned bytes.
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64) -> Result<(), Self::Error>;
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error>;
    /// Records the actual started request before invoking host IO. Shared
    /// meters must release their admission leaf lock before `operation`.
    fn attempt_io<T>(
        &mut self, delta: WorkCounters, operation: impl FnOnce() -> io::Result<T>,
    ) -> Result<T, Self::Error> {
        self.started_io(delta)?;
        operation().map_err(Into::into)
    }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error>;
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite);
    /// Always invokes cleanup, including when recording its actual attempt fails.
    fn attempt_cleanup_write<T>(
        &mut self, reservation: Self::CleanupWrite, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>);
}

impl NativeHostWork for DemandWork {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        match error { DemandError::Io(error) => Some(error.kind()), _ => None }
    }
    fn invalid_request() -> Self::Error { DemandError::InvalidRequest }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error { DemandError::Allocation(error) }
    fn check(&self) -> Result<(), Self::Error> { DemandWork::check(self) }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        self.check()?;
        Ok(self.work.admit(&delta, &self.budget)?)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { DemandWork::record(self, delta) }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.charge(delta) }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> { self.allocate(bytes)?; Ok(bytes) }
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        self.adopt(logical_max)?;
        Ok(logical_max)
    }
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64) -> Result<(), Self::Error> {
        self.reconcile_native_allocation(allocation, actual)
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> { DemandWork::release(self, bytes) }
    fn reserve_cleanup_write(&mut self) -> Result<u64, Self::Error> {
        self.reserve_native_cleanup_write()
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.cancel_native_cleanup_write(reservation);
    }
    fn attempt_cleanup_write<T>(&mut self, reservation: u64, operation: impl FnOnce() -> T)
        -> (T, Result<(), Self::Error>) {
        let recorded = self.record_native_cleanup_write(reservation);
        (operation(), recorded)
    }
}

impl NativeHostWork for &crate::materializer::NativeMaterializationWork {
    type Error = crate::materializer::NativeTreeMaterializationError;
    type Allocation = crate::materializer::NativeAllocation;
    type CleanupWrite = crate::materializer::NativeCleanupWrite;
    fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
        match error { crate::materializer::NativeTreeMaterializationError::Io(error) => Some(error.kind()), _ => None }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        crate::materializer::NativeTreeMaterializationError::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error {
        crate::materializer::NativeTreeMaterializationError::InvalidLayout
    }
    fn check(&self) -> Result<(), Self::Error> {
        crate::materializer::NativeMaterializationWork::admit(self, WorkCounters::UNCHARGED)
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
        crate::materializer::NativeMaterializationWork::admit(self, delta)
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.record_admitted(delta) }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.charge(delta) }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> {
        crate::materializer::NativeMaterializationWork::claim(self, bytes)
    }
    fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        self.retain_owned(logical_max)
    }
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64) -> Result<(), Self::Error> {
        allocation.reconcile_capacity(actual)
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> { drop(allocation); Ok(()) }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        crate::materializer::NativeMaterializationWork::reserve_cleanup_write(self)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) { drop(reservation); }
    fn attempt_cleanup_write<T>(&mut self, reservation: Self::CleanupWrite, operation: impl FnOnce() -> T)
        -> (T, Result<(), Self::Error>) {
        reservation.attempt_recorded(operation)
    }
}

fn native_write<W: crate::demand::SourceDemandReadWork>(work: &mut W) -> Result<(), DemandError> {
    work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })
}

fn native_attempt<W: NativeHostWork, T>(
    work: &mut W, delta: WorkCounters, operation: impl FnOnce() -> io::Result<T>,
) -> Result<T, W::Error> {
    work.attempt_io(delta, operation)
}

fn native_read_with_work<W: NativeHostWork>(work: &mut W) -> Result<(), W::Error> {
    work.started_io(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
}

// Field order keeps concrete native storage alive under its original ledger
// until the storage itself is destroyed, including on every early return.
struct NativeEnvironmentBuffer<T> {
    data: Vec<T>,
    allocation: crate::materializer::NativeAllocation,
}

fn native_environment_buffer<T>(
    capacity: usize, mut work: &crate::materializer::NativeMaterializationWork,
) -> Result<NativeEnvironmentBuffer<T>, crate::materializer::NativeTreeMaterializationError> {
    let bytes = capacity.checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut data = Vec::new();
    let reserved = data.try_reserve_exact(capacity);
    let actual = data.capacity().checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?;
    // Even a failed reservation can leave completed backing. Observe it before
    // returning either the real allocator cause or a late original-work error.
    let reconciled = work.reconcile_allocation(&mut allocation, actual as u64);
    reserved.map_err(<&crate::materializer::NativeMaterializationWork as NativeHostWork>::allocation_failed)?;
    reconciled?;
    Ok(NativeEnvironmentBuffer { data, allocation })
}

/// Reads current-process control metadata, never application BODY bytes or a
/// filesystem root capability. The caller still owns all authority/proof checks.
#[cfg(unix)]
pub(crate) fn environment_variable_with_work(
    name: &OsStr, maximum_bytes: usize, work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(OsString, crate::materializer::NativeAllocation)>, crate::materializer::NativeTreeMaterializationError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    let name = name.as_bytes();
    for byte in name {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *byte == 0 { return Err(io::Error::from(io::ErrorKind::InvalidInput).into()); }
    }
    let copy = WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let mut terminated = native_environment_buffer::<u8>(
        name.len().checked_add(1).ok_or(crate::WorkError::Overflow)?, work,
    )?;
    terminated.data.extend_from_slice(name);
    terminated.data.push(0);
    work.record_admitted(copy)?;
    let value = native_attempt(&mut &*work,
        WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            // SAFETY: the name is live and NUL-terminated. POSIX getenv returns
            // the actual environment string, not an owned or temporary copy.
            // Concurrent readers are allowed; no reader here modifies it.
            //
            // As with std's Unix environment readers and native process
            // inheritance, unsafe process environment mutation must not race
            // this entire synchronous borrow/scan/copy. Rust set_var/remove_var
            // explicitly require no concurrent reads outside std::env; foreign
            // setenv/putenv/environ mutation must uphold the same requirement.
            // A private SDK mutex could not enforce that foreign/std contract.
            // https://doc.rust-lang.org/std/env/fn.set_var.html#safety
            // https://pubs.opengroup.org/onlinepubs/9799919799/functions/getenv.html
            Ok(unsafe { libc::getenv(terminated.data.as_ptr().cast()) })
        })?;
    work.admit(WorkCounters::UNCHARGED)?;
    if value.is_null() { return Ok(None); }
    let mut length = 0;
    loop {
        let step = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(step)?;
        // SAFETY: getenv's live string remains valid under the mutation
        // contract above. Each byte is inspected before advancing; we never
        // pass its terminating NUL or inspect more than maximum_bytes + 1.
        let byte = unsafe { *value.add(length).cast::<u8>() };
        work.record_admitted(step)?;
        if byte == 0 { break; }
        if length == maximum_bytes {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge).into());
        }
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    let copy = WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let mut output = native_environment_buffer::<u8>(length, work)?;
    // SAFETY: the bounded scan established exactly this many initialized
    // non-NUL bytes. Unsafe environment mutation still cannot race the copy.
    output.data.extend_from_slice(unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) });
    work.record_admitted(copy)?;
    work.admit(WorkCounters::UNCHARGED)?;
    // Unix OsString consumes the same concrete Vec allocation without decoding
    // or copying it. The returned tuple destroys data before its guard.
    Ok(Some((OsString::from_vec(output.data), output.allocation)))
}

#[cfg(windows)]
fn native_environment_query(
    name: &[u16], buffer: Option<&mut [u16]>,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<usize>, crate::materializer::NativeTreeMaterializationError> {
    use windows::Win32::Foundation::{ERROR_ENVVAR_NOT_FOUND, ERROR_SUCCESS, GetLastError, SetLastError};
    use windows::Win32::System::Environment::GetEnvironmentVariableW;
    native_attempt(&mut &*work,
        WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            // SAFETY: the validated terminated name and optional initialized
            // buffer remain live through this synchronous call. Its checked
            // slice length fits DWORD; Windows serializes environment changes.
            let (count, error) = unsafe {
                SetLastError(ERROR_SUCCESS);
                let count = GetEnvironmentVariableW(windows::core::PCWSTR(name.as_ptr()), buffer);
                (count, GetLastError())
            };
            if count != 0 { return Ok(Some(count as usize)); }
            if error == ERROR_ENVVAR_NOT_FOUND { return Ok(None); }
            if error == ERROR_SUCCESS { return Ok(Some(0)); }
            Err(io::Error::from_raw_os_error(error.0 as i32))
        })
}

#[cfg(windows)]
fn native_environment_encoded_length(
    wide: &[u16], work: &crate::materializer::NativeMaterializationWork,
) -> Result<usize, crate::materializer::NativeTreeMaterializationError> {
    let mut length = 0usize;
    let mut index = 0;
    while index < wide.len() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let unit = wide[index];
        let mut bytes = if unit < 0x80 { 1 } else if unit < 0x800 { 2 } else { 3 };
        if (0xD800..=0xDBFF).contains(&unit) && index + 1 < wide.len() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if (0xDC00..=0xDFFF).contains(&wide[index + 1]) {
                bytes = 4;
                index += 1;
            }
        }
        length = length.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        index += 1;
    }
    Ok(length)
}

/// Bounds native UTF-16 requests and the logical OsString result separately:
/// neither raw control-buffer bytes nor hidden std allocator calls are BODY
/// reads or fabricated allocation-operation counts.
#[cfg(windows)]
pub(crate) fn environment_variable_with_work(
    name: &OsStr, maximum_bytes: usize, mut work: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(OsString, crate::materializer::NativeAllocation)>, crate::materializer::NativeTreeMaterializationError> {
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    let mut units = 0usize;
    for unit in name.encode_wide() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if unit == 0 { return Err(io::Error::from(io::ErrorKind::InvalidInput).into()); }
        units = units.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    let encoded = WorkCounters {
        bytes_encoded: units.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(encoded)?;
    let mut terminated = native_environment_buffer::<u16>(
        units.checked_add(1).ok_or(crate::WorkError::Overflow)?, work,
    )?;
    terminated.data.extend(name.encode_wide());
    terminated.data.push(0);
    work.record_admitted(encoded)?;
    let Some(mut capacity) = native_environment_query(&terminated.data, None, work)? else {
        work.admit(WorkCounters::UNCHARGED)?;
        return Ok(None);
    };
    work.admit(WorkCounters::UNCHARGED)?;
    if capacity == 0 { return Ok(Some((OsString::new(), work.retain_owned(0)?))); }
    // One UTF-16 unit produces at least one encoded byte. Include only the
    // native NUL in this request cap, not in the caller's logical value cap.
    let maximum_units = maximum_bytes.saturating_add(1).min(u32::MAX as usize);
    loop {
        if capacity > maximum_units {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge).into());
        }
        work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
        // This claim uses the original remaining live-byte allowance, before
        // the allocator or next OS request. No retry can grow beyond that root
        // allowance or the declared logical cap.
        let mut buffer = native_environment_buffer::<u16>(capacity, work)?;
        buffer.data.resize(capacity, 0);
        let Some(count) = native_environment_query(&terminated.data, Some(&mut buffer.data), work)? else {
            work.admit(WorkCounters::UNCHARGED)?;
            return Ok(None);
        };
        work.admit(WorkCounters::UNCHARGED)?;
        if count >= capacity {
            // The OS reports the required capacity including NUL. Contents
            // are undefined in this branch and are never examined or decoded.
            drop(buffer);
            capacity = count;
            continue;
        }
        buffer.data.truncate(count);
        let logical_bytes = native_environment_encoded_length(&buffer.data, work)?;
        if logical_bytes > maximum_bytes {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge).into());
        }
        let encoded = WorkCounters { bytes_encoded: logical_bytes as u64, ..WorkCounters::UNCHARGED };
        work.admit(encoded)?;
        // Reserve logical decode output before calling the opaque safe std
        // conversion. Its private allocations are not fabricated SDK attempts.
        let mut allocation = work.reserve_opaque_decode(logical_bytes as u64)?;
        let output = OsString::from_wide(&buffer.data);
        // Both completed facts must survive any late capacity/cancellation
        // refusal. Output is declared after its guard, so it drops first.
        let reconciled = work.reconcile_allocation(&mut allocation, output.capacity() as u64);
        let recorded = work.record_admitted(encoded);
        reconciled?;
        recorded?;
        work.admit(WorkCounters::UNCHARGED)?;
        return Ok(Some((output, allocation)));
    }
}

/// Copies an existing native encoding without decoding UTF-16 or fabricating
/// encoded bytes. The returned allocation token owns the live path bytes.
pub(crate) fn clone_path_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    let bytes = path.as_os_str().len();
    let delta = WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::UNCHARGED };
    let mut allocation = work.claim(bytes as u64)?;
    let mut name = OsString::new();
    let result = (|| {
        let reserved = name.try_reserve_exact(bytes);
        let reconciled = work.reconcile_allocation(&mut allocation, name.capacity() as u64);
        reserved.map_err(W::allocation_failed)?;
        reconciled?;
        // No allocator or callback can intervene between this atomic begun-copy
        // charge and the exact copy into the completed reserved native buffer.
        work.started_io(delta)?;
        name.push(path.as_os_str());
        Ok(())
    })();
    if let Err(error) = result {
        drop(name);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((name.into(), allocation))
}

pub(crate) fn clone_path_measured(path: &Path, work: &mut DemandWork) -> Result<std::path::PathBuf, DemandError> {
    clone_path_with_work(path, work).map(|(path, _live_bytes)| path)
}

/// Observes the actual held directory's physical spelling. This shares the
/// root admission and bounded ancestry scanner, rather than normalizing text.
#[cfg(unix)]
pub(crate) fn canonical_directory_path_with_work(
    path: &Path, work: &mut DemandWork,
) -> Result<std::path::PathBuf, DemandError> {
    let directory = open_root_directory_with_work(path, work)?;
    canonical_directory_path_held_with_work(directory, work).map(|(path, _allocation)| path)
}

#[cfg(unix)]
pub(crate) fn canonical_directory_path_held_with_work<W: NativeHostWork>(
    directory: Dir, work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    let (canonical, allocation) = cwd_ancestry_with_work(directory, work)?;
    if let Err(error) = work.check() {
        drop(canonical);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((canonical, allocation))
}

#[cfg(windows)]
pub(crate) fn canonical_directory_wide_path_with_work(
    path: &Path, work: &mut DemandWork,
) -> Result<Vec<u16>, DemandError> {
    let directory = open_root_directory_with_work(path, work)?;
    canonical_directory_wide_with_work(&directory, work).map(|(name, _allocation)| name)
}

/// Returns normalized native UTF-16 without the terminating NUL and its live
/// capacity allocation. Drop the vector before releasing the allocation.
#[cfg(windows)]
pub(crate) fn canonical_directory_wide_with_work<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_NAME_NORMALIZED, VOLUME_NAME_DOS, GETFINALPATHNAMEBYHANDLE_FLAGS, GetFinalPathNameByHandleW,
    };
    let query = |buffer: &mut [u16], work: &mut W| -> Result<usize, W::Error> {
        native_read_with_work(work)?;
        // SAFETY: the held handle and admitted mutable native buffer remain
        // live; the API supports an empty slice for its required-size query.
        let count = unsafe { GetFinalPathNameByHandleW(HANDLE(directory.as_raw_handle()),
            buffer, GETFINALPATHNAMEBYHANDLE_FLAGS(FILE_NAME_NORMALIZED.0 | VOLUME_NAME_DOS.0)) };
        if count == 0 { return Err(io::Error::last_os_error().into()); }
        work.check()?;
        Ok(count as usize)
    };
    let mut capacity = query(&mut [], work)?;
    loop {
        let bytes = capacity.checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)? as u64;
        let mut allocation = work.claim(bytes)?;
        let mut buffer = Vec::new();
        let reserved = buffer.try_reserve_exact(capacity);
        let actual = buffer.capacity().checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)? as u64;
        let reconciled = work.reconcile_allocation(&mut allocation, actual);
        if let Err(error) = reserved {
            drop(buffer);
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        if let Err(error) = reconciled {
            drop(buffer);
            work.release(allocation)?;
            return Err(error);
        }
        buffer.resize(capacity, 0u16);
        let count = match query(&mut buffer, work) {
            Ok(count) => count,
            Err(error) => {
                drop(buffer);
                work.release(allocation)?;
                return Err(error);
            }
        };
        if count < buffer.len() {
            buffer.truncate(count);
            return Ok((buffer, allocation));
        }
        drop(buffer);
        work.release(allocation)?;
        capacity = count.checked_add(usize::from(count == capacity)).ok_or(crate::WorkError::Overflow)?;
        // The generated native binding takes a DWORD capacity. Never let its
        // checked conversion become an unbounded or panicking adapter.
        if capacity > u32::MAX as usize { return Err(crate::WorkError::Overflow.into()); }
    }
}

/// Resolves a source root without hiding CWD retries or native control I/O.
/// The returned native path's capacity remains claimed in `work`.
#[cfg(unix)]
pub(crate) fn normalize_root_path_measured<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    path: &Path, work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.check()?;
    if path.is_absolute() { return clone_path_with_work(path, work).map(|(path, _allocation)| path); }
    let cwd = cwd_path_measured(work)?;
    let cwd_capacity = cwd.capacity() as u64;
    let result = (|| {
        let prefix = cwd.as_os_str().as_bytes();
        let suffix = path.as_os_str().as_bytes();
        let separator = usize::from(!prefix.ends_with(b"/"));
        let length = prefix.len().checked_add(separator)
            .and_then(|length| length.checked_add(suffix.len()))
            .ok_or(crate::WorkError::Overflow)?;
        let copied = prefix.len().checked_add(suffix.len()).ok_or(crate::WorkError::Overflow)?;
        let delta = WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let mut bytes = cwd_buffer_measured(length, work)?;
        bytes.extend_from_slice(prefix);
        if separator != 0 { bytes.push(b'/'); }
        bytes.extend_from_slice(suffix);
        if let Err(error) = work.record(delta) {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            work.release(capacity)?;
            return Err(error);
        }
        Ok(std::path::PathBuf::from(OsString::from_vec(bytes)))
    })();
    drop(cwd);
    work.release(cwd_capacity)?;
    result
}

#[cfg(windows)]
pub(crate) fn current_directory_with_work<W: NativeHostWork>(
    work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    use std::os::windows::ffi::OsStringExt as _;
    use windows::Win32::System::Environment::GetCurrentDirectoryW;
    let read = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
    // The zero-length probe reads the real process CWD control without an
    // allocator. A concurrent CWD change can require another bounded attempt.
    let mut requested = work.attempt_io(read, || {
        // SAFETY: None advertises zero writable units.
        let count = unsafe { GetCurrentDirectoryW(None) };
        if count == 0 { Err(io::Error::last_os_error()) } else { Ok(count as usize) }
    })?;
    loop {
        let bytes = requested.checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)?;
        let mut scratch_allocation = work.claim(bytes as u64)?;
        let mut scratch = Vec::<u16>::new();
        if let Err(error) = scratch.try_reserve_exact(requested) {
            work.release(scratch_allocation)?;
            return Err(W::allocation_failed(error));
        }
        let actual = scratch.capacity().checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)?;
        if let Err(error) = work.reconcile_allocation(&mut scratch_allocation, actual as u64) {
            drop(scratch);
            work.release(scratch_allocation)?;
            return Err(error);
        }
        scratch.resize(requested, 0);
        let result = (|| {
            let count = work.attempt_io(read, || {
                // SAFETY: the initialized slice advertises exactly its writable
                // units; Win32 reports a required count when it does not fit.
                let count = unsafe { GetCurrentDirectoryW(Some(&mut scratch)) };
                if count == 0 { Err(io::Error::last_os_error()) } else { Ok(count as usize) }
            })?;
            if count >= scratch.len() { return Ok(Err(count)); }
            let logical_max = count.checked_mul(3).ok_or(crate::WorkError::Overflow)?;
            let copy = WorkCounters { bytes_copied: (count as u64).checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::UNCHARGED };
            work.admit(copy)?;
            // Safe std decoding is opaque, not a fabricated allocator attempt.
            // Keep the raw input resident until actual output is reconciled.
            let mut allocation = work.reserve_opaque_decode(logical_max as u64)?;
            let path = std::path::PathBuf::from(OsString::from_wide(&scratch[..count]));
            if let Err(error) = work.reconcile_allocation(&mut allocation, path.capacity() as u64) {
                drop(path);
                work.release(allocation)?;
                return Err(error);
            }
            if let Err(error) = work.record(copy).and_then(|()| work.check()) {
                drop(path);
                work.release(allocation)?;
                return Err(error);
            }
            Ok(Ok((path, allocation)))
        })();
        drop(scratch);
        let released = work.release(scratch_allocation);
        match result {
            Ok(Ok((path, allocation))) => {
                if let Err(error) = released {
                    drop(path);
                    work.release(allocation)?;
                    return Err(error);
                }
                return Ok((path, allocation));
            }
            Ok(Err(next)) => { released?; requested = next; }
            Err(error) => { released?; return Err(error); }
        }
    }
}

#[cfg(unix)]
fn cwd_buffer_measured<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    length: usize, work: &mut W,
) -> Result<Vec<u8>, DemandError> {
    cwd_buffer_with_work(length, work).map(|(bytes, _allocation)| bytes)
}

#[cfg(unix)]
fn cwd_buffer_with_work<W: NativeHostWork>(
    length: usize, work: &mut W,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
    let mut allocation = work.claim(length as u64)?;
    let mut bytes = Vec::new();
    let reserved = bytes.try_reserve_exact(length);
    let reconciled = work.reconcile_allocation(&mut allocation, bytes.capacity() as u64);
    let result = reserved.map_err(W::allocation_failed).and(reconciled);
    if let Err(error) = result {
        drop(bytes);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((bytes, allocation))
}

#[cfg(unix)]
fn cwd_open_measured<W: NativeHostWork>(name: &std::ffi::CStr, work: &mut W) -> Result<Dir, W::Error> {
    use std::os::fd::FromRawFd as _;
    let flags = cwd_metadata_flags();
    loop {
        let result = native_attempt(work,
            WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                // SAFETY: `name` is terminated and borrowed across open; a
                // successful descriptor is fresh and immediately owned by File.
                let fd = unsafe { libc::open(name.as_ptr(), flags) };
                if fd < 0 { Err(io::Error::last_os_error()) }
                else { Ok(unsafe { File::from_raw_fd(fd) }) }
            });
        match result {
            Ok(file) => return Ok(Dir::from_std_file(file)),
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(unix)]
fn cwd_metadata_flags() -> i32 {
    libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
        | {
            #[cfg(target_os = "linux")]
            { libc::O_PATH }
            #[cfg(target_os = "macos")]
            { libc::O_EVTONLY }
        }
}

#[cfg(target_os = "linux")]
pub(crate) fn cwd_path_measured<W: NativeHostWork<Error = DemandError, Allocation = u64>>(work: &mut W) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::OsStringExt as _;
    // The Linux syscall itself stops at PATH_MAX; glibc's hidden ancestry
    // fallback is deliberately replaced by the shared measured reader below.
    let mut capacity = 256;
    loop {
        work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
        let mut bytes = cwd_buffer_measured(capacity, work)?;
        let result = loop {
            let result = native_attempt(work,
                WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                    // SAFETY: the live vector has the advertised writable
                    // capacity; SYS_getcwd never writes beyond that byte bound.
                    // Only the validated successful initialized prefix gets len.
                    let count = unsafe {
                        libc::syscall(libc::SYS_getcwd, bytes.as_mut_ptr(), bytes.capacity())
                    };
                    if count < 0 { Err(io::Error::last_os_error()) }
                    else { Ok(count as usize) }
                });
            match result {
                Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                result => break result,
            }
        };
        if let Ok(count) = &result {
            if *count > 0 && *count <= bytes.capacity() {
                // SYS_getcwd initialized exactly the returned byte count.
                unsafe { bytes.set_len(*count); }
            }
        }
        match result {
            Ok(count) if count > 0 && count <= bytes.len() && bytes[count - 1] == 0
                && bytes[0] == b'/' => {
                bytes.truncate(count - 1);
                return Ok(OsString::from_vec(bytes).into());
            }
            Ok(_) => {
                let capacity = bytes.capacity() as u64;
                drop(bytes);
                work.release(capacity)?;
                // Linux prefixes an inaccessible/chroot-external CWD with
                // "(unreachable)"; std current_dir rejects that as ENOENT.
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            Err(error) => {
                let allocation = bytes.capacity() as u64;
                drop(bytes);
                work.release(allocation)?;
                match error {
                    DemandError::Io(error) if error.raw_os_error() == Some(libc::ERANGE)
                        && capacity < libc::PATH_MAX as usize => {
                        capacity = capacity.checked_mul(2).ok_or(crate::WorkError::Overflow)?
                            .min(libc::PATH_MAX as usize);
                    }
                    DemandError::Io(error) if matches!(error.raw_os_error(),
                        Some(libc::ERANGE | libc::ENAMETOOLONG)) => {
                        let current = cwd_open_measured(c".", work)?;
                        return cwd_ancestry_measured(current, work);
                    }
                    error => return Err(error),
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn cwd_path_measured<W: NativeHostWork<Error = DemandError, Allocation = u64>>(work: &mut W) -> Result<std::path::PathBuf, DemandError> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStringExt as _;
    let current = cwd_open_measured(c".", work)?;
    work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    let mut bytes = cwd_buffer_measured(libc::PATH_MAX as usize, work)?;
    bytes.resize(libc::PATH_MAX as usize, 0);
    let result = (|| {
        loop {
            let result = native_attempt(work,
                WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                    // SAFETY: the held descriptor stays live and the initialized
                    // buffer is PATH_MAX bytes, as required by F_GETPATH.
                    if unsafe { libc::fcntl(current.as_raw_fd(), libc::F_GETPATH, bytes.as_mut_ptr()) } < 0 {
                        Err(io::Error::last_os_error())
                    } else { Ok(()) }
                });
            match result {
                Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                result => { result?; break; }
            }
        }
        for (index, byte) in bytes.iter().enumerate() {
            let step = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(step)?;
            work.record(step)?;
            if *byte == 0 {
                if index == 0 || bytes[0] != b'/' {
                    return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
                }
                return Ok(index);
            }
        }
        Err(io::Error::new(io::ErrorKind::InvalidData, "unterminated native CWD path").into())
    })();
    match result {
        Ok(length) => {
            bytes.truncate(length);
            Ok(OsString::from_vec(bytes).into())
        }
        Err(error) => {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            work.release(capacity)?;
            match error {
                DemandError::Io(error) if matches!(error.raw_os_error(),
                    Some(libc::ERANGE | libc::ENAMETOOLONG | libc::ENOSPC)) =>
                    cwd_ancestry_measured(current, work),
                error => Err(error),
            }
        }
    }
}

#[cfg(unix)]
struct CwdAncestor<A> {
    name: OsString,
    name_allocation: A,
    next: Option<Box<CwdAncestor<A>>>,
    slot_allocation: A,
}

#[cfg(unix)]
fn cwd_ancestor_name_measured<W: NativeHostWork>(
    parent: &Dir, child: &HostStat, work: &mut W,
) -> Result<(OsString, W::Allocation), W::Error> {
    // Reuse the one raw-record decoder/stat engine, never an unmetered
    // readdir/opendir scanner. A fresh "." description has its own cursor.
    let directory = Dir::from_std_file(open_unix_named_with(parent, c".",
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
            | { #[cfg(target_os = "linux")] { libc::O_NOATIME }
                #[cfg(not(target_os = "linux"))] { 0 } },
        || native_read_with_work(work))?);
    let mut allocation = work.claim(HostStatReader::BUFFER_BYTES as u64)?;
    let units = HostStatReader::BUFFER_BYTES / std::mem::size_of::<u64>();
    let mut buffer = Vec::<u64>::new();
    let reserved = buffer.try_reserve_exact(units);
    let reconciled = work.reconcile_allocation(&mut allocation,
        (buffer.capacity() * std::mem::size_of::<u64>()) as u64);
    if let Err(error) = reserved.map_err(W::allocation_failed).and(reconciled) {
        drop(buffer);
        work.release(allocation)?;
        return Err(error);
    }
    buffer.resize(units, 0);
    let mut reader = HostStatReader {
        directory,
        buffer,
        next: 0, filled: 0, exhausted: false,
        #[cfg(target_os = "macos")]
        base: 0,
    };
    let mut found = None;
    let result = (|| {
        while let Some((entry, entry_allocation)) = reader.next_with_work(work)? {
            if entry.stat.as_ref().is_some_and(|stat|
                stat.is_dir() && stat.dev() == child.dev() && stat.ino() == child.ino()) {
                found = Some((entry.name, entry_allocation));
                return Ok(());
            }
            drop(entry);
            work.release(entry_allocation)?;
        }
        Err(io::Error::from_raw_os_error(libc::ENOENT).into())
    })();
    drop(reader);
    let released = work.release(allocation);
    if let Err(error) = result.and(released) {
        if let Some((name, allocation)) = found {
            drop(name);
            work.release(allocation)?;
        }
        return Err(error);
    }
    Ok(found.expect("matching held directory entry owns its native name"))
}

#[cfg(unix)]
fn cwd_ancestry_measured<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    current: Dir, work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    cwd_ancestry_with_work(current, work).map(|(path, _allocation)| path)
}

#[cfg(unix)]
fn cwd_ancestry_with_work<W: NativeHostWork>(
    mut current: Dir, work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    let root = cwd_open_measured(c"/", work)?;
    let root_stat = stat_unix_fd_at(root.as_raw_fd(), OsStr::new(""), true, work)?;
    let mut ancestors: Option<Box<CwdAncestor<W::Allocation>>> = None;
    let slot_bytes = std::mem::size_of::<CwdAncestor<W::Allocation>>() as u64;
    let result = (|| {
        let mut length = 1usize;
        let mut name_bytes = 0usize;
        loop {
            work.check()?;
            let child = stat_unix_fd_at(current.as_raw_fd(), OsStr::new(""), true, work)?;
            if child.dev() == root_stat.dev() && child.ino() == root_stat.ino() { break; }
            let parent = Dir::from_std_file(open_unix_named_with(&current, c"..",
                cwd_metadata_flags(), || native_read_with_work(work))?);
            let parent_stat = stat_unix_fd_at(parent.as_raw_fd(), OsStr::new(""), true, work)?;
            if parent_stat.dev() == child.dev() && parent_stat.ino() == child.ino() {
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            let (name, name_allocation) = cwd_ancestor_name_measured(&parent, &child, work)?;
            let retained = (|| {
                length = length.checked_add(name.len()).and_then(|length|
                    length.checked_add(usize::from(ancestors.is_some())))
                    .ok_or(crate::WorkError::Overflow)?;
                name_bytes = name_bytes.checked_add(name.len()).ok_or(crate::WorkError::Overflow)?;
                work.claim(slot_bytes)
            })();
            let slot_allocation = match retained {
                Ok(allocation) => allocation,
                Err(error) => {
                    drop(name);
                    work.release(name_allocation)?;
                    return Err(error);
                }
            };
            let mut ancestor = Box::new(CwdAncestor {
                name, name_allocation, next: None, slot_allocation,
            });
            if let Err(error) = work.reconcile_allocation(&mut ancestor.slot_allocation, slot_bytes) {
                let CwdAncestor { name, name_allocation, slot_allocation, .. } = *ancestor;
                drop(name);
                let released_name = work.release(name_allocation);
                let released_slot = work.release(slot_allocation);
                released_name.and(released_slot)?;
                return Err(error);
            }
            ancestor.next = ancestors.take();
            ancestors = Some(ancestor);
            current = parent;
        }
        let delta = WorkCounters { bytes_copied: name_bytes as u64, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let (mut bytes, allocation) = cwd_buffer_with_work(length, work)?;
        bytes.push(b'/');
        let copied = (|| {
            let mut next = ancestors.as_deref();
            while let Some(ancestor) = next {
                let delta = WorkCounters {
                    bytes_copied: ancestor.name.len() as u64, ..WorkCounters::UNCHARGED
                };
                work.admit(delta)?;
                if bytes.len() != 1 { bytes.push(b'/'); }
                bytes.extend_from_slice(ancestor.name.as_bytes());
                work.record(delta)?;
                next = ancestor.next.as_deref();
            }
            Ok::<_, W::Error>(())
        })();
        if let Err(error) = copied {
            drop(bytes);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((std::path::PathBuf::from(OsString::from_vec(bytes)), allocation))
    })();
    // Iterative teardown also bounds stack use for paths of arbitrary depth.
    let mut cleanup_error = None;
    while let Some(mut ancestor) = ancestors.take() {
        ancestors = ancestor.next.take();
        let CwdAncestor { name, name_allocation, slot_allocation, .. } = *ancestor;
        drop(name);
        let released_name = work.release(name_allocation);
        let released_slot = work.release(slot_allocation);
        if let Err(error) = released_name.and(released_slot) {
            if cleanup_error.is_none() { cleanup_error = Some(error); }
        }
    }
    if let Some(error) = cleanup_error {
        if let Ok((path, allocation)) = result {
            drop(path);
            work.release(allocation)?;
        }
        return Err(error);
    }
    result
}

#[cfg(unix)]
pub(crate) fn unix_name_with_work<W: NativeHostWork, T>(
    name: &OsStr, work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    unix_name_checked_with_work(name, false, work, operation)
}

#[cfg(unix)]
fn unix_name_checked_with_work<W: NativeHostWork, T>(
    name: &OsStr, single_leaf: bool, work: &mut W,
    operation: impl FnOnce(&std::ffi::CStr, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = name.as_bytes();
    if single_leaf && matches!(bytes, b"" | b"." | b"..") {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "expected one owned native child").into());
    }
    for byte in bytes {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let invalid = *byte == 0 || single_leaf && *byte == b'/';
        work.record(delta)?;
        if invalid {
            return Err(W::invalid_request());
        }
    }
    let size = bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let delta = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
    work.admit(delta)?;
    let mut allocation = work.claim(size as u64)?;
    let mut owned = Vec::new();
    let reserved = owned.try_reserve_exact(size);
    let reconciled = work.reconcile_allocation(&mut allocation, owned.capacity() as u64);
    let result = (|| {
        reserved.map_err(W::allocation_failed).and(reconciled)?;
        owned.extend_from_slice(bytes);
        owned.push(0);
        let name = std::ffi::CStr::from_bytes_with_nul(&owned)
            .expect("validated name and one appended terminator");
        work.record(delta)?;
        operation(name, work)
    })();
    drop(owned);
    work.release(allocation)?;
    result
}

#[cfg(unix)]
fn create_unix_directory_leaf_with_work<W: NativeHostWork>(
    parent: &Dir, leaf: &std::ffi::CStr, work: &mut W,
) -> Result<(), W::Error> {
    use std::os::fd::AsRawFd as _;
    loop {
        let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)?;
        // SAFETY: held directory and terminated single-component name remain live.
        if unsafe { libc::mkdirat(parent.as_raw_fd(), leaf.as_ptr(), 0o777) } == 0 { return Ok(()); }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
    }
}

#[cfg(unix)]
pub(crate) fn open_unix_directory_with_work<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, create: bool, work: &mut W,
) -> Result<Dir, W::Error> {
    unix_name_with_work(name, work, |name, work| {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let open = |work: &mut W| open_unix_named_with(parent, name, flags, || {
            let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
            work.started_io(delta)
        }).map(Dir::from_std_file);
        match open(work) {
            Ok(directory) => return Ok(directory),
            Err(error) if create && W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        match create_unix_directory_leaf_with_work(parent, name, work) {
            Ok(()) => {}
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::AlreadyExists) => {}
            Err(error) => return Err(error),
        }
        open(work)
    })
}

/// Used only for creator-proven private scratch disposal. Unlike ordinary
/// source/materializer walks, every child must remain on its held parent's
/// physical mount, even when a Linux bind mount retains the same device ID.
#[cfg(unix)]
pub(crate) fn open_owned_child_directory_with_work<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, work: &mut W,
) -> Result<Dir, W::Error> {
    let child = open_owned_child_file_with_work(parent, name, true, work)?;
    work.check()?;
    Ok(Dir::from_std_file(child))
}

#[cfg(unix)]
pub(crate) fn verify_owned_child_no_mount_with_work<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, work: &mut W,
) -> Result<(), W::Error> {
    let child = open_owned_child_file_with_work(parent, name, false, work)?;
    let stat = stat_file_with_work(&child, work)?;
    if stat.file_type().is_symlink() {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "owned scratch child is a symbolic link").into());
    }
    work.check()
}

#[cfg(unix)]
fn open_owned_child_file_with_work<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, directory: bool, work: &mut W,
) -> Result<File, W::Error> {
    unix_name_checked_with_work(name, true, work, |name, work| {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::{AsRawFd as _, FromRawFd as _};
            // SAFETY: open_how contains only integer fields and zero is valid.
            let mut how: libc::open_how = unsafe { std::mem::zeroed() };
            how.flags = (libc::O_CLOEXEC | libc::O_NOFOLLOW
                | if directory { libc::O_RDONLY | libc::O_DIRECTORY } else { libc::O_PATH }) as u64;
            how.resolve = libc::RESOLVE_NO_XDEV | libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS;
            loop {
                let result = native_attempt(work,
                    WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                        // SAFETY: live held parent, admitted terminated leaf,
                        // and initialized versioned kernel ABI storage.
                        let fd = unsafe { libc::syscall(libc::SYS_openat2, parent.as_raw_fd(),
                            name.as_ptr(), &raw const how, std::mem::size_of::<libc::open_how>()) };
                        if fd < 0 {
                            let error = io::Error::last_os_error();
                            if matches!(error.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) {
                                return Err(io::Error::new(io::ErrorKind::Unsupported,
                                    "owned scratch requires native no-cross resolution"));
                            }
                            return Err(error);
                        }
                        // SAFETY: successful openat2 returned a new owner.
                        Ok(unsafe { File::from_raw_fd(fd as i32) })
                    });
                match result {
                    Err(error) if matches!(W::io_error_kind(&error),
                        Some(io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock)) => continue,
                    result => return result,
                }
            }
        }
        #[cfg(target_os = "macos")]
        {
            let parent_mount = held_mount_identity_with_work(parent, work)?;
            let flags = libc::O_EVTONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
                | if directory { libc::O_DIRECTORY } else { 0 };
            let child = open_unix_named_with(parent, name, flags, || native_read_with_work(work))?;
            if held_mount_identity_with_work(&child, work)? != parent_mount {
                return Err(io::Error::from_raw_os_error(libc::EXDEV).into());
            }
            Ok(child)
        }
    })
}

#[cfg(target_os = "macos")]
fn held_mount_identity_with_work<W: NativeHostWork>(
    file: &impl std::os::fd::AsRawFd, work: &mut W,
) -> Result<[i32; 2], W::Error> {
    loop {
        let result = native_attempt(work,
            WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                let mut metadata = std::mem::MaybeUninit::<libc::statfs>::uninit();
                // SAFETY: live held descriptor and exactly sized native output.
                if unsafe { libc::fstatfs(file.as_raw_fd(), metadata.as_mut_ptr()) } < 0 {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: successful fstatfs initialized its public result.
                let metadata = unsafe { metadata.assume_init() };
                // SAFETY: Darwin fsid_t is repr(C) with exactly two i32 words;
                // this preserves its opaque mounted-filesystem identity bits.
                Ok(unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(metadata.f_fsid) })
            });
        match result {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}

#[cfg(windows)]
pub(crate) fn open_owned_child_directory_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W,
) -> Result<Dir, W::Error> {
    let child = open_owned_child_file_with_work(parent, name, true, work)?;
    work.check()?;
    Ok(Dir::from_std_file(child))
}

#[cfg(windows)]
pub(crate) fn verify_owned_child_no_mount_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W,
) -> Result<(), W::Error> {
    drop(open_owned_child_file_with_work(parent, name, false, work)?);
    work.check()
}

#[cfg(windows)]
fn open_owned_child_file_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], directory: bool, work: &mut W,
) -> Result<File, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION,
        FILE_GENERIC_READ, FILE_READ_ATTRIBUTES, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle, SYNCHRONIZE};
    let access = if directory { FILE_GENERIC_READ } else { FILE_READ_ATTRIBUTES | SYNCHRONIZE };
    let child = open_windows_leaf_handle_with_work(parent, name, access,
        if directory { Some(true) } else { None }, work)?;
    let parent_identity = directory_identity_with_work(parent, work)?;
    native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut metadata = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: the exact nofollow-opened child and native output remain live.
        unsafe { GetFileInformationByHandle(HANDLE(child.as_raw_handle()), &raw mut metadata) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        if metadata.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
            || directory && metadata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0 {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "owned scratch child redirects"));
        }
        if u64::from(metadata.dwVolumeSerialNumber) != parent_identity.device {
            return Err(io::Error::new(io::ErrorKind::CrossesDevices, "owned scratch child crosses a volume"));
        }
        Ok(())
    })?;
    Ok(child)
}

#[cfg(unix)]
fn open_unix_directory_path_with_work<W: NativeHostWork>(
    root: &Dir, path: &Path, create: bool, work: &mut W,
) -> Result<Dir, W::Error> {
    let mut current = clone_directory_with(root, || {
        let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
        work.started_io(delta)
    })?;
    for component in path.components() {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let name = match component {
            std::path::Component::Normal(name) => name,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid held path component").into()),
        };
        work.record(delta)?;
        current = open_unix_directory_with_work(&current, name, create, work)?;
    }
    Ok(current)
}

#[cfg(unix)]
fn with_unix_parent_leaf<W: crate::demand::SourceDemandReadWork, T>(
    root: &Dir, path: &Path, work: &mut W,
    operation: impl FnOnce(&Dir, &std::ffi::CStr, &mut W) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    let leaf = path.file_name().ok_or(DemandError::InvalidRequest)?;
    let parent = open_unix_directory_path_with_work(root, path.parent().ok_or(DemandError::InvalidRequest)?, false, work)?;
    unix_name_with_work(leaf, work, |leaf, work| operation(&parent, leaf, work))
}

pub(crate) enum NativeOpenOptions {
    Read,
    ReadWrite,
    CreateNew,
    /// Creates only an absent regular leaf, retaining a read/write descriptor.
    ReadWriteCreateNew,
    ReadWriteCreate,
}

pub(crate) fn create_directories_materialization_measured(
    path: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    create_directories_with_work(path, &mut &*work)
}

pub(crate) fn create_directories_with_work<W: NativeHostWork>(path: &Path, work: &mut W) -> Result<(), W::Error> {
    let mut base = Path::new(".");
    if path.is_absolute() {
        base = path;
        while let Some(parent) = base.parent() {
            let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            work.record(delta)?;
            base = parent;
        }
    }
    let current = open_root_directory_with_work(base, work)?;
    let relative = if path.is_absolute() {
        path.strip_prefix(base).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid materialization root"))?
    } else { path };
    create_directories_owned_with_work(current, relative, work).map(|_| ())
}

pub(crate) fn create_directories_held_with_work<W: NativeHostWork>(
    parent: &Dir, relative: &Path, work: &mut W,
) -> Result<Dir, W::Error> {
    let current = clone_directory_with(parent, || {
        let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
        work.started_io(delta)
    })?;
    create_directories_owned_with_work(current, relative, work)
}

pub(crate) fn create_directories_held_materialization_measured(
    parent: &Dir, relative: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<Dir, crate::materializer::NativeTreeMaterializationError> {
    create_directories_held_with_work(parent, relative, &mut &*work)
}

pub(crate) fn create_new_directory_held_with_work<W: NativeHostWork>(
    parent: &Dir, leaf: &Path, work: &mut W,
) -> Result<Dir, W::Error> {
    require_leaf(leaf)?;
    #[cfg(unix)]
    let directory = unix_name_with_work(leaf.as_os_str(), work, |leaf, work| {
        create_unix_directory_leaf_with_work(parent, leaf, work)?;
        let file = open_unix_named_with(parent, leaf,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC, || {
                let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
                work.started_io(delta)
            })?;
        Ok(Dir::from_std_file(file))
    })?;
    #[cfg(windows)]
    let directory = with_wide_path_with_work(leaf, work, |leaf, work| {
        // FILE_CREATE returns the exact newly created directory handle; an
        // existing binding is never adopted, including after a race.
        create_windows_wide_at_measured(parent, leaf, true, false, false, false, work)
            .map(Dir::from_std_file)
    })?;
    sync_directory_held_with_work(parent, work)?;
    Ok(directory)
}

fn create_directories_owned_with_work<W: NativeHostWork>(
    mut current: Dir, relative: &Path, work: &mut W,
) -> Result<Dir, W::Error> {
    for component in relative.components() {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)?;
        let name = match component {
            std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => name,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid held materialization directory component").into()),
        };
        #[cfg(unix)]
        { current = open_unix_directory_with_work(&current, name, true, work)?; }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt as _;
            let units = wide_units_with(name, || {
                let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                work.admit(delta)?;
                work.record(delta)
            })?;
            let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64;
            let delta = WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let mut allocation = work.claim(bytes)?;
            let mut wide = Vec::new();
            let reserved = wide.try_reserve_exact(units);
            let reconciled = work.reconcile_allocation(&mut allocation, wide.capacity() as u64 * 2);
            let result = (|| {
                reserved.map_err(W::allocation_failed).and(reconciled)?;
                wide.extend(name.encode_wide());
                work.record(delta)?;
                create_windows_directory_with_work(&current, &wide, work)
            })();
            drop(wide);
            work.release(allocation)?;
            current = Dir::from_std_file(result?);
        }
    }
    Ok(current)
}

pub(crate) fn open_leaf_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, options: &NativeOpenOptions, work: &mut W,
) -> Result<File, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let flags = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | match options {
            NativeOpenOptions::Read => libc::O_RDONLY,
            NativeOpenOptions::CreateNew => libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            NativeOpenOptions::ReadWrite => libc::O_RDWR,
            NativeOpenOptions::ReadWriteCreateNew => libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            NativeOpenOptions::ReadWriteCreate => libc::O_RDWR | libc::O_CREAT,
        };
        unix_name_with_work(name.as_os_str(), work, |name, work| {
            let delta = if matches!(options, NativeOpenOptions::Read) {
                WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }
            } else { WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED } };
            let file = open_unix_named_with(parent, name, flags, || {
                work.started_io(delta)
            })?;
            loop {
                let result = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                    // SAFETY: held descriptor and complete output storage remain live.
                    if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 { return Err(io::Error::last_os_error()); }
                    Ok(unsafe { stat.assume_init() })
                });
                match result {
                    Ok(stat) if stat.st_mode & libc::S_IFMT as libc::mode_t == libc::S_IFREG as libc::mode_t => return Ok(file),
                    Ok(_) => return Err(W::invalid_request()),
                    Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
                    Err(error) => return Err(error),
                }
            }
        })
    }
    #[cfg(windows)]
    { with_wide_path_with_work(name, work, |name, work| open_windows_leaf_with_options_with_work(parent, name, options, work)) }
}
/// Fixed private-owner predicates shared by rooted state-file producers.
/// No raw handle, provider callback or dynamically constructed policy error
/// crosses the public SDK boundary.
#[cfg(unix)]
fn validate_private_state_descriptor_with_work<W: NativeHostWork>(
    file: &impl std::os::fd::AsRawFd, directory: bool, work: &mut W,
) -> Result<(), W::Error> {
    let stat = loop {
        let result = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: live held descriptor and correctly sized output storage.
            if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 { return Err(io::Error::last_os_error()); }
            Ok(unsafe { stat.assume_init() })
        });
        match result {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {}
            result => break result?,
        }
    };
    let user = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        // SAFETY: the effective-user query has no arguments or retained data.
        Ok(unsafe { libc::geteuid() })
    })?;
    let kind = if directory { libc::S_IFDIR } else { libc::S_IFREG } as libc::mode_t;
    let mode = stat.st_mode & 0o777;
    if stat.st_mode & libc::S_IFMT as libc::mode_t != kind
        || stat.st_uid != user
        || (directory && mode & 0o022 != 0)
        || (!directory && (mode != 0o600 || stat.st_nlink != 1))
    {
        return Err(W::invalid_request());
    }
    work.check()
}

#[cfg(windows)]
fn validate_private_state_descriptor_with_work<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, directory: bool, work: &mut W,
) -> Result<(), W::Error> {
    use windows::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
    };
    let information = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: the exact held handle and fixed output storage remain live.
        unsafe { GetFileInformationByHandle(windows::Win32::Foundation::HANDLE(file.as_raw_handle()), &raw mut information) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        Ok(information)
    })?;
    if information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        || (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0) != directory
    {
        return Err(W::invalid_request());
    }
    // The existing host policy checks regular/reparse metadata on Windows;
    // DPAPI's current-user protection belongs to its existing caller engine.
    work.check()
}

pub(crate) fn validate_private_state_file_with_work<W: NativeHostWork>(
    file: &File, work: &mut W,
) -> Result<(), W::Error> {
    validate_private_state_descriptor_with_work(file, false, work)
}

/// Holds a prepared cleanup name and reservation before exclusive creation.
/// The only callback is crate-private SDK code; no consumer receives a File.
pub(crate) fn with_private_state_temporary(
    parent: &Dir, name: &Path, original: &crate::materializer::NativeMaterializationWork,
    operation: impl FnOnce(&mut File, &mut bool) -> Result<(), crate::materializer::NativeTreeMaterializationError>,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    require_leaf(name)?;
    #[cfg(unix)]
    return unix_name_with_work(name.as_os_str(), &mut &*original, |name, work| {
        use std::os::fd::AsRawFd as _;
        let cleanup = original.reserve_cleanup_write()?;
        let mut file = open_unix_named_with_mode(parent, name,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0o600, || work.started_io(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }))?;
        let mut published = false;
        let result = validate_private_state_file_with_work(&file, work)
            .and_then(|()| operation(&mut file, &mut published));
        drop(file);
        if published {
            drop(cleanup);
            return result;
        }
        let (removed, recorded) = cleanup.attempt_recorded(|| {
            // SAFETY: held parent and pre-funded terminated leaf outlive this
            // mandatory cleanup, even after quota refusal or cancellation.
            if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } != 0 {
                Err(io::Error::last_os_error())
            } else { Ok(()) }
        });
        match result {
            Err(error) => Err(error),
            Ok(()) => { recorded?; removed?; Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout) }
        }
    });
    #[cfg(windows)]
    return with_wide_path_with_work(name, &mut &*original, |name, work| {
        use windows::Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation};
        let cleanup = original.reserve_cleanup_write()?;
        let mut file = create_windows_provisional_file_with_work(parent, name, work)?;
        let mut published = false;
        let result = keep_windows_created_file_with_work(&file, work)
            .and_then(|()| validate_private_state_file_with_work(&file, work))
            .and_then(|()| operation(&mut file, &mut published));
        if published {
            drop(cleanup);
            drop(file);
            return result;
        }
        let (removed, recorded) = cleanup.attempt_recorded(|| windows_set_information_held(
            &file, &FILE_DISPOSITION_INFORMATION { DeleteFile: true }, FileDispositionInformation,
        ));
        drop(file);
        match result {
            Err(error) => Err(error),
            Ok(()) => { recorded?; removed?; Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout) }
        }
    });
}


/// Opens exactly one held directory component; no ambient relative reopen.
pub(crate) fn open_builtin_directory_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, create: bool, work: &mut W,
) -> Result<Dir, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    { open_unix_directory_with_work(parent, name.as_os_str(), create, work) }
    #[cfg(windows)]
    {
        with_wide_path_with_work(name, work, |name, work| {
            if create {
                return create_windows_directory_with_work(parent, name, work).map(Dir::from_std_file);
            }
            use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
            open_windows_leaf_handle_with_work(parent, name, FILE_GENERIC_READ, Some(true), work)
                .map(Dir::from_std_file)
        })
    }
}

/// Records completed byte transfer even when cancellation arrives during IO.
pub(crate) fn read_builtin_file(
    file: &File, destination: &mut [u8], work: &crate::materializer::NativeMaterializationWork,
) -> Result<usize, crate::materializer::NativeTreeMaterializationError> {
    use std::io::Read as _;
    let mut transfer = acyclic_native_runtime::NativeByteReservation::new(
        work.native_read_meter().as_ref(), acyclic_native_runtime::NativeOperationKind::Read, destination.len(),
    ).map_err(native_builtin_runtime_error)?;
    let copy = work.reserve_copy(destination.len() as u64)?;
    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    let count = (&*file).read(destination)?;
    let transferred = transfer.record(count).map_err(native_builtin_runtime_error);
    let copied = copy.record(count as u64);
    transferred?;
    copied?;
    Ok(count)
}

pub(crate) fn write_builtin_file(
    file: &File, source: &[u8], work: &crate::materializer::NativeMaterializationWork,
) -> Result<usize, crate::materializer::NativeTreeMaterializationError> {
    use std::io::Write as _;
    let mut transfer = acyclic_native_runtime::NativeByteReservation::new(
        work.native_read_meter().as_ref(), acyclic_native_runtime::NativeOperationKind::Write, source.len(),
    ).map_err(native_builtin_runtime_error)?;
    let copy = work.reserve_copy(source.len() as u64)?;
    work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
    let count = (&*file).write(source)?;
    let transferred = transfer.record(count).map_err(native_builtin_runtime_error);
    let copied = copy.record(count as u64);
    transferred?;
    copied?;
    Ok(count)
}

fn native_builtin_runtime_error(error: io::Error) -> crate::materializer::NativeTreeMaterializationError {
    crate::materializer::native_materialization_work_error(crate::demand::native_demand_error(error))
}

/// Serializes these SDK leaf operations without claiming an OS-wide content
/// transaction. Unrelated writers may ignore advisory Unix locks.
pub(crate) fn lock_builtin_file(
    file: &File, writable: bool, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    use crate::materializer::NativeTreeMaterializationError;
    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
    let result = if writable { file.try_lock() } else { file.try_lock_shared() };
    match result {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(NativeTreeMaterializationError::PreimageConflict),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

pub(crate) fn remove_leaf_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, work: &mut W,
) -> Result<(), W::Error> {
    remove_leaf_kind_with_work(parent, name, false, work)
}

pub(crate) fn remove_empty_directory_held_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, work: &mut W,
) -> Result<(), W::Error> {
    remove_leaf_kind_with_work(parent, name, true, work)
}

fn remove_leaf_kind_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, directory: bool, work: &mut W,
) -> Result<(), W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        unix_name_with_work(name.as_os_str(), work, |name, work| loop {
            let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            work.record(delta)?;
            // SAFETY: held directory and terminated single-component name remain live.
            if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), if directory { libc::AT_REMOVEDIR } else { 0 }) } == 0 { return Ok(()); }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
        })
    }
    #[cfg(windows)]
    { with_wide_path_with_work(name, work, |name, work| remove_windows_leaf_with_work(parent, name, directory, work)) }
}

#[cfg(unix)]
fn unix_mutation<W: crate::demand::SourceDemandReadWork>(work: &mut W, mut operation: impl FnMut() -> libc::c_int) -> Result<(), DemandError> {
    loop {
        native_write(work)?;
        if operation() == 0 { return Ok(()); }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
    }
}

#[cfg(unix)]
fn fence_cookie_held_with_work<W: NativeHostWork>(
    parent: &Dir, leaf: &std::ffi::CStr, work: &mut W,
) -> Result<(), W::Error> {
    use std::os::fd::AsRawFd as _;
    let cleanup = work.reserve_cleanup_write()?;
    let created = match open_unix_named_with(parent, leaf,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        || {
            let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            work.record(delta)
        }) {
        Ok(file) => file,
        Err(error) => {
            work.cancel_cleanup_write(cleanup);
            return Err(error);
        }
    };
    let completion = work.check();
    let (removed, recorded) = work.attempt_cleanup_write(cleanup, || {
        // SAFETY: the same held parent and prepared leaf remain live, even
        // after cancellation; no path/name allocation can gate this cleanup.
        if unsafe { libc::unlinkat(parent.as_raw_fd(), leaf.as_ptr(), 0) } < 0 {
            Err(io::Error::last_os_error())
        } else { Ok(()) }
    });
    drop(created);
    recorded?;
    removed?;
    completion?;
    work.check()
}

#[cfg(windows)]
fn fence_cookie_held_with_work<W: NativeHostWork>(
    parent: &Dir, leaf: &[u16], work: &mut W,
) -> Result<(), W::Error> {
    use windows::Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation};
    let cleanup = work.reserve_cleanup_write()?;
    let created = match create_windows_provisional_file_with_work(parent, leaf, work) {
        Ok(file) => file,
        Err(error) => {
            work.cancel_cleanup_write(cleanup);
            return Err(error);
        }
    };
    let completion = work.check();
    let (removed, recorded) = work.attempt_cleanup_write(cleanup, || {
        windows_set_information_held(&created,
            &FILE_DISPOSITION_INFORMATION { DeleteFile: true }, FileDispositionInformation)
    });
    // The original delete-capable FILE_CREATE handle owns final disposal:
    // neither cleanup nor close reopens a replaced name after cancellation.
    drop(created);
    recorded?;
    removed?;
    completion?;
    work.check()
}

impl HostRoot {
    /// Emits an exclusive ordered watcher cookie, reserving its mandatory
    /// removal before the namespace is mutated. Cleanup still runs on cancel.
    pub(crate) fn fence_cookie_measured(
        &self, relative: &Path, work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, relative, work, |parent, leaf, work| {
                fence_cookie_held_with_work(parent, leaf, work)
            })
        }
        #[cfg(windows)]
        {
            with_wide_path(relative, work, |relative, work| {
                let (parent, leaf) = wide_parent_leaf(relative)?;
                let parent = self.open_dir_held_wide_measured(parent, work)?;
                fence_cookie_held_with_work(&parent, leaf, work)
            })
        }
    }

    pub(crate) fn create_file_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, work: &mut W) -> Result<File, DemandError> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.create_file_wide_measured(path, work)) }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                open_unix_named_with(parent, leaf,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    || native_write(work))
            })
        }
    }

    pub(crate) fn create_dir_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, work: &mut W) -> Result<(), DemandError> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.create_dir_wide_measured(path, work)) }
        #[cfg(unix)]
        {
            with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                create_unix_directory_leaf_with_work(parent, leaf, work)
            })
        }
    }

    pub(crate) fn create_dir_all_held_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, work: &mut W) -> Result<HostDirectory, DemandError> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.create_dir_all_held_wide_measured(path, work)) }
        #[cfg(unix)]
        { Ok(HostDirectory { directory: open_unix_directory_path_with_work(&self.directory, path, true, work)? }) }
    }

    pub(crate) fn hard_link_measured<W: crate::demand::SourceDemandReadWork>(&self, source: &Path, destination: &Path, work: &mut W) -> Result<(), DemandError> {
        #[cfg(windows)]
        {
            with_wide_path_with_work(source, work, |source, work| with_wide_path_with_work(destination, work,
                |destination, work| self.hard_link_wide_measured(source, destination, work)))
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            with_unix_parent_leaf(&self.directory, source, work, |source_parent, source_leaf, work| {
                with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
                    // SAFETY: both capability parents and terminated names outlive linkat.
                    unix_mutation(work, || unsafe { libc::linkat(source_parent.as_raw_fd(), source_leaf.as_ptr(),
                        parent.as_raw_fd(), leaf.as_ptr(), 0) })
                })
            })
        }
    }

    #[cfg(unix)]
    pub(crate) fn symlink_measured<W: crate::demand::SourceDemandReadWork>(&self, target: &OsStr, destination: &Path, work: &mut W) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            unix_name_with_work(target, work, |target, work| {
                // SAFETY: held directory and both terminated strings are live.
                unix_mutation(work, || unsafe { libc::symlinkat(target.as_ptr(), parent.as_raw_fd(), leaf.as_ptr()) })
            })
        })
    }

    #[cfg(unix)]
    pub(crate) fn set_permissions_without_open_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, mode: u32, work: &mut W) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        let mode = libc::mode_t::try_from(mode & 0o7777).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name outlive the nofollow operation.
            unix_mutation(work, || unsafe { libc::fchmodat(parent.as_raw_fd(), leaf.as_ptr(), mode, libc::AT_SYMLINK_NOFOLLOW) })
        })
    }

    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn create_fifo_held_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, mode: u32, work: &mut W) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        let mode = libc::mode_t::try_from(mode & 0o7777).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name remain live.
            unix_mutation(work, || unsafe { libc::mkfifoat(parent.as_raw_fd(), leaf.as_ptr(), mode) })
        })
    }

    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", all(test, target_os = "macos")))]
    pub(crate) fn create_device_held_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, mode: u32, device: libc::dev_t, work: &mut W) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        let mask = mode & libc::S_IFMT as u32;
        if mask != libc::S_IFCHR as u32 && mask != libc::S_IFBLK as u32 { return Err(DemandError::InvalidRequest); }
        let mode = libc::mode_t::try_from(mode).map_err(|_| DemandError::InvalidRequest)?;
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
            // SAFETY: held parent and terminated name remain live.
            unix_mutation(work, || unsafe { libc::mknodat(parent.as_raw_fd(), leaf.as_ptr(), mode, device) })
        })
    }

    #[cfg(unix)]
    pub(crate) fn bind_unix_socket_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, work: &mut W) -> Result<(), DemandError> {
        with_unix_parent_leaf(&self.directory, path, work, |parent, leaf, work| bind_unix_socket_in_measured(parent, leaf, work))
    }
}

#[cfg(unix)]
fn bind_unix_socket_in_measured<W: crate::demand::SourceDemandReadWork>(parent: &Dir, leaf: &std::ffi::CStr, work: &mut W) -> Result<(), DemandError> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let mut path = [0_u8; libc::PATH_MAX as usize];
    #[cfg(target_os = "linux")]
    let base = {
        let prefix = b"/proc/self/fd/";
        let mut number = parent.as_raw_fd() as u32;
        let encoded = u64::from(if number == 0 { 1 } else { number.ilog10() + 1 });
        work.charge(WorkCounters { bytes_encoded: encoded, ..WorkCounters::UNCHARGED })?;
        let mut digits = [0_u8; 10];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (number % 10) as u8;
            number /= 10;
            if number == 0 { break; }
        }
        let length = digits.len() - start;
        work.charge(WorkCounters {
            bytes_copied: (prefix.len() + length) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        path[..prefix.len()].copy_from_slice(prefix);
        path[prefix.len()..prefix.len() + length].copy_from_slice(&digits[start..]);
        prefix.len() + length
    };
    #[cfg(target_os = "macos")]
    let base = {
        native_read_with_work(work)?;
        // SAFETY: F_GETPATH writes no more than PATH_MAX into this live buffer.
        if unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        path.iter().position(|byte| *byte == 0).ok_or(DemandError::InvalidRequest)?
    };
    let length = base.checked_add(1).and_then(|n| n.checked_add(leaf.to_bytes().len()))
        .ok_or(crate::WorkError::Overflow)?;
    if length >= address.sun_path.len() { return Err(DemandError::InvalidRequest); }
    work.charge(WorkCounters { bytes_copied: (length + leaf.to_bytes().len()) as u64, ..WorkCounters::UNCHARGED })?;
    path[base] = b'/';
    path[base + 1..length].copy_from_slice(leaf.to_bytes());
    for (destination, byte) in address.sun_path.iter_mut().zip(&path[..length]) { *destination = *byte as libc::c_char; }
    #[cfg(target_os = "macos")]
    { address.sun_len = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1) as u8; }
    native_write(work)?;
    #[cfg(target_os = "linux")]
    let kind = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let kind = libc::SOCK_STREAM;
    // SAFETY: socket returns a fresh owned descriptor or -1.
    let fd = unsafe { libc::socket(libc::AF_UNIX, kind, 0) };
    if fd < 0 { return Err(io::Error::last_os_error().into()); }
    let socket = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
    #[cfg(target_os = "macos")]
    {
        native_write(work)?;
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    // SAFETY: correctly initialized sockaddr_un storage remains live.
    unix_mutation(work, || unsafe { libc::bind(socket.as_raw_fd(), (&raw const address).cast(),
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + length + 1) as libc::socklen_t) })?;
    unix_mutation(work, || unsafe { libc::listen(socket.as_raw_fd(), 128) })?;
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let through_descriptor = stat_unix_at(parent, OsStr::from_bytes(leaf.to_bytes()), work)?;
        let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
        native_read_with_work(work)?;
        if unsafe { libc::lstat(path.as_ptr().cast(), observed.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let observed = unsafe { observed.assume_init() };
        if through_descriptor.dev() != observed.st_dev as u64 || through_descriptor.ino() != observed.st_ino as u64 {
            unix_mutation(work, || unsafe { libc::unlink(path.as_ptr().cast()) })?;
            return Err(io::Error::other("socket parent directory moved during bind").into());
        }
    }
    Ok(())
}


fn ordinary_cancellation() -> &'static crate::CancellationToken {
    static CANCELLATION: std::sync::LazyLock<crate::CancellationToken> = std::sync::LazyLock::new(crate::CancellationToken::new);
    &CANCELLATION
}

fn ordinary_work() -> DemandWork {
    DemandWork::new(WorkBudget::UNBOUNDED, ordinary_cancellation())
}

fn ordinary_error(error: DemandError) -> io::Error {
    match error {
        DemandError::Io(error) => error,
        error => io::Error::other(error),
    }
}


fn require_leaf(path: &Path) -> io::Result<()> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_))) || components.next().is_some() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(())
}

pub(crate) fn durable_rename_held(from: &Dir, from_name: &Path, to: &Dir, to_name: &Path) -> io::Result<()> {
    rename_no_replace_held(from, from_name, to, to_name)?;
    // Attempt both barriers even if the first one fails.
    let source = sync_directory_held(from);
    let destination = sync_directory_held(to);
    source.and(destination)
}

pub(crate) fn rename_no_replace_held(from: &Dir, from_name: &Path, to: &Dir, to_name: &Path) -> io::Result<()> {
    rename_no_replace_held_with_work(from, from_name, to, to_name, &mut ordinary_work()).map_err(ordinary_error)
}

pub(crate) fn rename_no_replace_held_measured(
    from: &Dir, from_name: &Path, to: &Dir, to_name: &Path,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    rename_no_replace_held_with_work(from, from_name, to, to_name, &mut &*work)
}

pub(crate) fn rename_no_replace_held_with_work<W: NativeHostWork>(
    from: &Dir, from_name: &Path, to: &Dir, to_name: &Path, work: &mut W,
) -> Result<(), W::Error> {
    rename_no_replace_held_core(from, from_name, to, to_name, None, work)
}

#[cfg(windows)]
pub(crate) fn rename_no_replace_held_if_identity_with_work<W: NativeHostWork>(
    from: &Dir, from_name: &Path, to: &Dir, to_name: &Path,
    expected: NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error> {
    rename_no_replace_held_core(from, from_name, to, to_name, Some(expected), work)
}

fn rename_no_replace_held_core<W: NativeHostWork>(
    from: &Dir, from_name: &Path, to: &Dir, to_name: &Path,
    expected: Option<NativeRootIdentity>, work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    debug_assert!(expected.is_none());
    require_leaf(from_name)?;
    require_leaf(to_name)?;
    #[cfg(unix)]
    {
        unix_name_with_work(from_name.as_os_str(), work, |from_name, work| {
            unix_name_with_work(to_name.as_os_str(), work, |to_name, work| {
                native_attempt(work, WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, || {
                    rename_unix_names_once(from, from_name, to, to_name, false)
                })
            })
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Wdk::Storage::FileSystem::{
            FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, FILE_RENAME_INFORMATION,
        };
        use windows::Win32::Storage::FileSystem::{DELETE, SYNCHRONIZE, FILE_READ_ATTRIBUTES};
        use windows::Win32::Foundation::HANDLE;
        let access = DELETE | SYNCHRONIZE | if expected.is_some() { FILE_READ_ATTRIBUTES } else { Default::default() };
        let file = open_windows_held_with_work(from, from_name, access,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work)?;
        if let Some(expected) = expected {
            if file_identity_and_length_with_work(&file, work)?.0 != expected {
                return Err(io::Error::other("rename source identity changed").into());
            }
        }
        let units = wide_units_with(to_name.as_os_str(), || {
            let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            work.record(delta)
        })?;
        let name_bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
        let header = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
        let bytes = header.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?;
        let words = bytes.max(std::mem::size_of::<FILE_RENAME_INFORMATION>()).div_ceil(8);
        let mut allocation = work.claim(words as u64 * 8)?;
        let mut buffer = Vec::new();
        let reserved = buffer.try_reserve_exact(words);
        let reconciled = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64 * 8);
        let result = (|| {
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            buffer.resize(words, 0u64);
            let encoded = WorkCounters { bytes_encoded: name_bytes as u64, ..WorkCounters::UNCHARGED };
            work.admit(encoded)?;
            work.record(encoded)?;
            let information = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
            // SAFETY: aligned storage covers the fields and entire UTF-16 tail.
            // Zero ReplaceIfExists means NO-REPLACE.
            unsafe {
                std::ptr::addr_of_mut!((*information).RootDirectory).write(HANDLE(to.as_handle().as_raw_handle()));
                std::ptr::addr_of_mut!((*information).FileNameLength).write(u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?);
                let tail = buffer.as_mut_ptr().cast::<u8>().add(header).cast::<u16>();
                for (index, unit) in to_name.as_os_str().encode_wide().enumerate() {
                    if unit == 0 { return Err(W::invalid_request()); }
                    tail.add(index).write(unit);
                }
            }
            let length = u32::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
            native_attempt(work, WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, || {
                windows_rename_packet_once(&file, &buffer, length)
            })
        })();
        drop(buffer);
        work.release(allocation)?;
        result
    }
}

pub(crate) fn sync_directory_held(directory: &Dir) -> io::Result<()> {
    sync_directory_held_with_work(directory, &mut ordinary_work()).map_err(ordinary_error)
}

pub(crate) fn sync_directory_held_measured(directory: &Dir, work: &crate::materializer::NativeMaterializationWork)
    -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    sync_directory_held_with_work(directory, &mut &*work)
}

pub(crate) fn sync_directory_held_with_work<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        loop {
            let error = native_attempt(work, WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED }, || {
                Ok(sync_held_descriptor_once(directory).err())
            })?;
            match error {
                Some(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Some(error) => return Err(error.into()),
                None => return Ok(()),
            }
        }
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
        // FlushFileBuffers requires write access. Reopen the exact held
        // directory itself, not a path which may now name a replacement.
        let file = open_windows_held_with_work(directory, Path::new("."), FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, work)?;
        native_attempt(work, WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED }, || {
            sync_held_descriptor_once(&file)
        })
    }
}

#[cfg(windows)]
fn open_windows_materialization_at(
    directory: &Dir, path: &Path, access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<File, crate::materializer::NativeTreeMaterializationError> {
    open_windows_held_with_work(directory, path, access, options, &mut &*work)
}

#[cfg(windows)]
fn open_windows_held_with_work<W: NativeHostWork>(
    directory: &Dir, path: &Path, access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS, work: &mut W,
) -> Result<File, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN, NtCreateFile};
    use windows::Win32::Foundation::{HANDLE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    if path != Path::new(".") { require_leaf(path)?; }
    let units = wide_units_with(path.as_os_str(), || {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)
    })?;
    let length = u16::try_from(units.checked_mul(2).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "native name too long"))?;
    let encoded = WorkCounters { bytes_encoded: u64::from(length), ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut allocation = work.claim(u64::from(length))?;
    let mut name = Vec::new();
    let result = (|| {
        let reserved = name.try_reserve_exact(units);
        let actual = name.capacity().checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)? as u64;
        let reconciled = work.reconcile_allocation(&mut allocation, actual);
        reserved.map_err(W::allocation_failed)?;
        reconciled?;
        name.extend(path.as_os_str().encode_wide());
        work.record(encoded)?;
        if name.contains(&0) { return Err(io::Error::new(io::ErrorKind::InvalidInput, "native name contains NUL").into()); }
        let name = UNICODE_STRING { Length: length, MaximumLength: length, Buffer: windows::core::PWSTR(name.as_mut_ptr()) };
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: HANDLE(directory.as_handle().as_raw_handle()), ObjectName: &raw const name,
            Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE, ..Default::default()
        };
        native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            let mut block = IO_STATUS_BLOCK::default();
            let mut handle = HANDLE::default();
            let status = unsafe { NtCreateFile(&raw mut handle, access, &raw const attributes, &raw mut block,
                None, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, FILE_OPEN, options, None, 0) };
            if status.is_err() { return Err(status_error(status)); }
            Ok(unsafe { File::from_raw_handle(handle.0) })
        })
    })();
    drop(name);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
pub(crate) fn set_file_attributes_held(parent: &Dir, name: &Path, attributes: u32) -> io::Result<()> {
    sync_windows_metadata_held_with_work(parent, name, Some(attributes), None, &mut ordinary_work()).map_err(ordinary_error)
}

#[cfg(windows)]
pub(crate) fn set_file_attributes_held_measured(parent: &Dir, name: &Path, attributes: u32,
    expected: Option<NativeRootIdentity>,
    work: &crate::materializer::NativeMaterializationWork) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    sync_windows_metadata_held_with_work(parent, name, Some(attributes), expected, &mut &*work)
}

/// Flushes metadata through the exact held leaf. A readonly Windows leaf is
/// temporarily unlocked only with a separately reserved mandatory restoration.
pub(crate) fn sync_metadata_held_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, work: &mut W,
) -> Result<(), W::Error> {
    if name != Path::new(".") { require_leaf(name)?; }
    #[cfg(unix)]
    {
        let file = unix_name_with_work(name.as_os_str(), work, |name, work| {
            open_unix_named_with(parent, name, libc::O_RDONLY | libc::O_NOFOLLOW
                | libc::O_NONBLOCK | libc::O_CLOEXEC, || {
                let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
                work.started_io(delta)
            })
        })?;
        sync_file_held_with_work(&file, work)
    }
    #[cfg(windows)]
    { sync_windows_metadata_held_with_work(parent, name, None, None, work) }
}

#[cfg(windows)]
#[derive(Debug, thiserror::Error)]
#[error("metadata operation failed: {primary:?}; restoration accounting failed: {accounting:?}; restoration failed: {restoration:?}")]
struct MetadataRestorationFailure<E: std::error::Error + 'static> {
    #[source]
    primary: Option<E>,
    accounting: Option<E>,
    restoration: Option<io::Error>,
}

#[cfg(windows)]
fn sync_windows_metadata_held_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, requested: Option<u32>,
    expected: Option<NativeRootIdentity>, work: &mut W,
) -> Result<(), W::Error> {
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::{
        FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
        FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_NORMAL, FILE_BASIC_INFO,
    };
    let file = open_windows_held_with_work(parent, name,
        FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work)?;
    // Admission evidence must match the actual opened inode before even a
    // temporary readonly change. A lexical replacement is never adopted.
    if let Some(expected) = expected {
        if file_identity_and_length_with_work(&file, work)?.0 != expected {
            return Err(io::Error::other("metadata target identity changed").into());
        }
    }
    let current = query_windows_basic_info_with_work(&file, work)?;
    let writable = if current.FileAttributes & FILE_ATTRIBUTE_READONLY.0 == 0 {
        reopen_windows_writable_with_work(&file, work)?
    } else {
        // The reservation withholds a real write slot from all shared-meter
        // clones. Neither cancellation nor another caller can steal cleanup.
        let restore = work.reserve_cleanup_write()?;
        let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
        if let Err(error) = work.admit(delta).and_then(|()| work.record(delta)) {
            work.cancel_cleanup_write(restore);
            return Err(error);
        }
        let unlocked = current.FileAttributes & !FILE_ATTRIBUTE_READONLY.0;
        let information = FILE_BASIC_INFO {
            FileAttributes: if unlocked == 0 { FILE_ATTRIBUTE_NORMAL.0 } else { unlocked },
            ..Default::default()
        };
        let reopened = match set_windows_basic_info_held(&file, information) {
            Ok(()) => reopen_windows_writable_with_work(&file, work),
            Err(error) => Err(error.into()),
        };
        // ReOpenFile acts on this same pinned object, never its current path.
        // Restore even when reopening, cancellation, or recording failed.
        let (restored, recorded) = work.attempt_cleanup_write(restore, || {
            set_windows_basic_info_held(&file, FILE_BASIC_INFO {
                FileAttributes: current.FileAttributes, ..Default::default()
            })
        });
        if restored.is_err() || recorded.is_err() {
            return Err(io::Error::other(MetadataRestorationFailure {
                primary: reopened.err(), accounting: recorded.err(), restoration: restored.err(),
            }).into());
        }
        reopened?
    };
    if let Some(attributes) = requested {
        set_windows_basic_info_with_work(&file,
            FILE_BASIC_INFO { FileAttributes: attributes, ..Default::default() }, work)?;
    }
    sync_file_held_with_work(&writable, work)
}

#[cfg(windows)]
fn reopen_windows_writable_with_work<W: NativeHostWork>(file: &File, work: &mut W)
    -> Result<File, W::Error> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        ReOpenFile, FILE_GENERIC_WRITE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_SHARE_DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };
    native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        // SAFETY: the live handle pins the same inode across rename/replacement.
        let handle = unsafe { ReOpenFile(HANDLE(file.as_raw_handle()), FILE_GENERIC_WRITE.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        // SAFETY: successful ReOpenFile transferred a new exclusively owned handle.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })
}

#[cfg(windows)]
fn query_windows_basic_info_with_work<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
) -> Result<windows::Win32::Storage::FileSystem::FILE_BASIC_INFO, W::Error> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx};
    native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut information = FILE_BASIC_INFO::default();
        // SAFETY: the held handle and correctly sized output remain live.
        unsafe { GetFileInformationByHandleEx(HANDLE(file.as_raw_handle()), FileBasicInfo,
            (&raw mut information).cast(), std::mem::size_of::<FILE_BASIC_INFO>() as u32) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        Ok(information)
    })
}

#[cfg(windows)]
fn set_windows_basic_info_held(
    file: &impl std::os::windows::io::AsRawHandle,
    information: windows::Win32::Storage::FileSystem::FILE_BASIC_INFO,
) -> io::Result<()> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{FILE_BASIC_INFO, FileBasicInfo, SetFileInformationByHandle};
    // SAFETY: the held handle and complete fixed-size input remain live.
    unsafe { SetFileInformationByHandle(HANDLE(file.as_raw_handle()), FileBasicInfo,
        (&raw const information).cast(), std::mem::size_of::<FILE_BASIC_INFO>() as u32) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}

#[cfg(windows)]
fn set_windows_basic_info_with_work<W: NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle,
    information: windows::Win32::Storage::FileSystem::FILE_BASIC_INFO, work: &mut W,
) -> Result<(), W::Error> {
    native_attempt(work, WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED },
        || set_windows_basic_info_held(file, information))
}

pub(crate) fn sync_file_held_with_work<W: NativeHostWork>(file: &File, work: &mut W) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        loop {
            let error = native_attempt(work, WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED }, || {
                Ok(sync_held_descriptor_once(file).err())
            })?;
            match error {
                Some(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Some(error) => return Err(error.into()),
                None => return Ok(()),
            }
        }
    }
    #[cfg(windows)]
    {
        native_attempt(work, WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED }, || {
            sync_held_descriptor_once(file)
        })
    }
}
#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
use windows::Win32::Storage::FileSystem::FILE_BASIC_INFO;

#[cfg(target_os = "linux")]
#[derive(Debug, thiserror::Error)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum LinuxMetadataError {
    #[error("Linux native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) struct LinuxMetadataTarget {
    inode: std::os::fd::OwnedFd,
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
#[derive(Debug, thiserror::Error)]
pub(crate) enum WindowsMetadataError {
    #[error("Windows native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A no-follow leaf handle pinned before metadata work is deferred.
#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) struct WindowsMetadataTarget {
    file: cap_std::fs::File,
}

#[cfg(target_os = "macos")]
#[derive(Debug, thiserror::Error)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum MacMetadataError {
    #[error("macOS native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// An exact, no-follow target pinned before metadata work is deferred.
#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum MacMetadataTarget {
    Held(File),
    Noop {
        parent: Dir,
        leaf: std::path::PathBuf,
        device: u64,
        inode: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// One object's kind, size, times, and identity, read without following its
/// name's final link.
#[cfg(unix)]
#[derive(Clone, Debug)]
pub struct HostStat {
    mode: u32,
    len: u64,
    dev: u64,
    ino: u64,
    nlink: u64,
    uid: u32,
    gid: u32,
    rdev: u64,
    atime: i64,
    atime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
    created: Option<cap_std::time::SystemTime>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostFileType(u32);

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostFileType {
    Native(cap_std::fs::FileType),
    Link { directory: bool },
}

#[cfg(windows)]
impl HostFileType {
    pub fn is_dir(self) -> bool { matches!(self, Self::Native(kind) if kind.is_dir()) }
    pub fn is_file(self) -> bool { matches!(self, Self::Native(kind) if kind.is_file()) }
    pub fn is_symlink(self) -> bool { match self { Self::Native(kind) => kind.is_symlink(), Self::Link { .. } => true } }
    pub fn is_char_device(self) -> bool {
        use cap_fs_ext::FileTypeExt as _;
        matches!(self, Self::Native(kind) if kind.is_char_device())
    }
    pub fn is_fifo(self) -> bool {
        use cap_fs_ext::FileTypeExt as _;
        matches!(self, Self::Native(kind) if kind.is_fifo())
    }
    pub fn is_block_device(self) -> bool { false }
    pub fn is_socket(self) -> bool { false }
}

#[cfg(unix)]
impl HostFileType {
    pub fn is_dir(self) -> bool { self.0 == libc::S_IFDIR as u32 }
    pub fn is_file(self) -> bool { self.0 == libc::S_IFREG as u32 }
    pub fn is_symlink(self) -> bool { self.0 == libc::S_IFLNK as u32 }
    pub fn is_fifo(self) -> bool { self.0 == libc::S_IFIFO as u32 }
    pub fn is_socket(self) -> bool { self.0 == libc::S_IFSOCK as u32 }
    pub fn is_char_device(self) -> bool { self.0 == libc::S_IFCHR as u32 }
    pub fn is_block_device(self) -> bool { self.0 == libc::S_IFBLK as u32 }
}

#[cfg(unix)]
fn unix_time(seconds: i64, nanos: i64) -> io::Result<cap_std::time::SystemTime> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let magnitude = nanos.unsigned_abs();
    let duration = std::time::Duration::new(
        u64::try_from(magnitude / 1_000_000_000).map_err(|_| io::Error::other("host time overflow"))?,
        u32::try_from(magnitude % 1_000_000_000).map_err(|_| io::Error::other("host time overflow"))?,
    );
    let time = if nanos < 0 {
        std::time::UNIX_EPOCH.checked_sub(duration)
    } else {
        std::time::UNIX_EPOCH.checked_add(duration)
    };
    time.map(cap_std::time::SystemTime::from_std)
        .ok_or_else(|| io::Error::other("host time overflow"))
}

#[cfg(unix)]
impl HostStat {
    pub fn file_type(&self) -> HostFileType { HostFileType(self.mode & libc::S_IFMT as u32) }
    pub fn is_dir(&self) -> bool { self.file_type().is_dir() }
    pub fn len(&self) -> u64 { self.len }
    pub fn mode(&self) -> u32 { self.mode }
    pub fn dev(&self) -> u64 { self.dev }
    pub fn ino(&self) -> u64 { self.ino }
    pub fn nlink(&self) -> u64 { self.nlink }
    pub fn uid(&self) -> u32 { self.uid }
    pub fn gid(&self) -> u32 { self.gid }
    pub fn rdev(&self) -> u64 { self.rdev }
    pub fn atime(&self) -> i64 { self.atime }
    pub fn atime_nsec(&self) -> i64 { self.atime_nsec }
    pub fn mtime(&self) -> i64 { self.mtime }
    pub fn mtime_nsec(&self) -> i64 { self.mtime_nsec }
    pub fn ctime(&self) -> i64 { self.ctime }
    pub fn ctime_nsec(&self) -> i64 { self.ctime_nsec }
    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        self.created.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "birth time unavailable"))
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> { unix_time(self.mtime, self.mtime_nsec) }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> { unix_time(self.atime, self.atime_nsec) }

    #[cfg(target_os = "linux")]
    fn from_stat(stat: rustix::fs::Stat) -> io::Result<Self> {
        Ok(Self {
            mode: stat.st_mode as u32,
            len: u64::try_from(stat.st_size).map_err(|_| io::Error::other("negative file length"))?,
            dev: stat.st_dev as u64, ino: stat.st_ino as u64, nlink: stat.st_nlink as u64,
            uid: stat.st_uid, gid: stat.st_gid, rdev: stat.st_rdev as u64,
            atime: stat.st_atime as i64, atime_nsec: stat.st_atime_nsec as i64,
            mtime: stat.st_mtime as i64, mtime_nsec: stat.st_mtime_nsec as i64,
            ctime: stat.st_ctime as i64, ctime_nsec: stat.st_ctime_nsec as i64,
            created: None,
        })
    }

    #[cfg(target_os = "macos")]
    fn from_stat(stat: libc::stat) -> io::Result<Self> {
        Ok(Self {
            mode: u32::from(stat.st_mode),
            len: u64::try_from(stat.st_size).map_err(|_| io::Error::other("negative file length"))?,
            dev: stat.st_dev as u64, ino: stat.st_ino, nlink: u64::from(stat.st_nlink),
            uid: stat.st_uid, gid: stat.st_gid, rdev: stat.st_rdev as u64,
            atime: stat.st_atime, atime_nsec: stat.st_atime_nsec,
            mtime: stat.st_mtime, mtime_nsec: stat.st_mtime_nsec,
            ctime: stat.st_ctime, ctime_nsec: stat.st_ctime_nsec,
            created: unix_time(stat.st_birthtime, stat.st_birthtime_nsec).ok(),
        })
    }
}


/// One object's kind, size, times, attributes, and identity, read without
/// following its name's final link. Every plain object's facts come from one
/// `FileStatInformation` query of its file record, by name or by handle, so
/// a name and a handle on it always agree. An NTFS directory index is never
/// a source: it may describe an older state of a file written through
/// another link or through a handle still open.
#[cfg(windows)]
#[derive(Clone, Copy, Debug)]
pub struct HostStat {
    file_type: HostFileType,
    len: u64,
    attributes: u32,
    creation_time: u64,
    last_access_time: u64,
    last_write_time: u64,
    volume_serial_number: u32,
    file_index: u64,
    number_of_links: Option<u32>,
}

#[cfg(windows)]
impl HostStat {
    /// # Errors
    ///
    /// Fails when the metadata does not identify its file, which a file
    /// record always does: a file's identity is never assumed.
    pub fn from_metadata(metadata: &Metadata) -> io::Result<Self> {
        use cap_primitives::fs::_WindowsByHandle;
        use cap_std::fs::MetadataExt;
        let unidentified = || io::Error::other("host metadata does not identify its file");
        Ok(Self {
            file_type: HostFileType::Native(metadata.file_type()),
            len: metadata.len(),
            attributes: MetadataExt::file_attributes(metadata),
            creation_time: metadata.creation_time(),
            last_access_time: metadata.last_access_time(),
            last_write_time: metadata.last_write_time(),
            volume_serial_number: _WindowsByHandle::volume_serial_number(metadata)
                .ok_or_else(unidentified)?,
            file_index: _WindowsByHandle::file_index(metadata).ok_or_else(unidentified)?,
            number_of_links: _WindowsByHandle::number_of_links(metadata),
        })
    }

    /// The facts one `FileStatInformation` query reports for an object
    /// with no reparse point, on the volume `volume_serial_number` names.
    fn from_stat_information(
        information: &windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION,
        volume_serial_number: u32,
    ) -> io::Result<Self> {
        use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;

        let unsigned =
            |value: i64| u64::try_from(value).map_err(|_| io::Error::other("negative stat field"));
        Ok(Self {
            file_type: if matches!(information.ReparseTag, 0xA000_0003 | 0xA000_000C) {
                HostFileType::Link { directory: information.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 }
            } else if information.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
                HostFileType::Native(cap_std::fs::FileType::dir())
            } else {
                HostFileType::Native(cap_std::fs::FileType::file())
            },
            len: unsigned(information.EndOfFile)?,
            attributes: information.FileAttributes,
            creation_time: unsigned(information.CreationTime)?,
            last_access_time: unsigned(information.LastAccessTime)?,
            last_write_time: unsigned(information.LastWriteTime)?,
            volume_serial_number,
            // File IDs are opaque bit patterns, not signed sizes or times.
            file_index: information.FileId as u64,
            number_of_links: Some(information.NumberOfLinks),
        })
    }

    #[must_use]
    pub const fn file_type(&self) -> HostFileType {
        self.file_type
    }

    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    #[must_use]
    #[allow(
        clippy::len_without_is_empty,
        reason = "mirrors Metadata::len: the byte length of the named object"
    )]
    pub const fn len(&self) -> u64 {
        self.len
    }

    #[must_use]
    pub const fn file_attributes(&self) -> u32 {
        self.attributes
    }

    #[must_use]
    pub const fn creation_time(&self) -> u64 {
        self.creation_time
    }

    #[must_use]
    pub const fn last_write_time(&self) -> u64 {
        self.last_write_time
    }

    #[must_use]
    pub const fn volume_serial_number(&self) -> u32 {
        self.volume_serial_number
    }

    #[must_use]
    pub const fn file_index(&self) -> u64 {
        self.file_index
    }

    /// How many names the file record has, which the file record keeps
    /// exact for every one of them.
    #[must_use]
    pub const fn number_of_links(&self) -> Option<u32> {
        self.number_of_links
    }

    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.creation_time))
    }

    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_write_time))
    }

    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_access_time))
    }
}

/// `path` as the UTF-16 name a kernel call resolves relative to a held
/// directory, with its byte length; `None` for the directory itself or any
/// component that is not a plain name.
#[cfg(windows)]
fn relative_kernel_name(path: &Path) -> Option<(Vec<u16>, u16)> {
    use std::os::windows::ffi::OsStrExt as _;

    let mut name = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(component) = component else {
            return None;
        };
        if !name.is_empty() {
            name.push(u16::from(b'\\'));
        }
        name.extend(component.encode_wide());
    }
    let length = u16::try_from(name.len() * 2)
        .ok()
        .filter(|length| *length > 0)?;
    Some((name, length))
}

/// `FILETIME` ticks (100 ns since 1601-01-01 UTC) at the Unix epoch.
#[cfg(windows)]
pub(crate) const UNIX_EPOCH_FILETIME: i64 = 116_444_736_000_000_000;

/// Unix nanoseconds one `FILETIME` names. Every tick is a whole number of
/// nanoseconds, so this is exact or `None` beyond the `i64` nanosecond range.
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn filetime_to_unix_nanoseconds(ticks: i64) -> Option<i64> {
    ticks.checked_sub(UNIX_EPOCH_FILETIME)?.checked_mul(100)
}

/// The `FILETIME` tick containing one Unix-nanosecond instant: the instant
/// rounded toward the past, so ordering is preserved, including before 1970.
/// It never overflows, because `i64` nanoseconds span far fewer ticks than
/// `i64`. A writer that must reproduce an instant exactly rejects
/// `nanoseconds % 100 != 0` first.
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn unix_nanoseconds_to_filetime(nanoseconds: i64) -> i64 {
    nanoseconds.div_euclid(100) + UNIX_EPOCH_FILETIME
}

/// The instant one `FILETIME` names, as the standard library reads it.
#[cfg(windows)]
fn windows_time(ticks: u64) -> cap_std::time::SystemTime {
    const UNIX_EPOCH_TICKS: u64 = UNIX_EPOCH_FILETIME.unsigned_abs();
    let since = |ticks: u64| std::time::Duration::from_nanos(ticks.saturating_mul(100));
    cap_std::time::SystemTime::from_std(if ticks >= UNIX_EPOCH_TICKS {
        std::time::UNIX_EPOCH + since(ticks - UNIX_EPOCH_TICKS)
    } else {
        std::time::UNIX_EPOCH - since(UNIX_EPOCH_TICKS - ticks)
    })
}

/// One enumerated name with its facts. `stat` is `None` when enumeration
/// cannot report the facts exactly as [`HostRoot::stat`] would, and the name
/// must be stat'ed on its own.
pub struct HostListedEntry {
    pub name: OsString,
    pub stat: Option<HostStat>,
}

impl HostListedEntry {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.name.as_encoded_bytes().len() as u64
    }
}

#[cfg(windows)]
pub(crate) struct HostListedWideEntry {
    pub(crate) name: Vec<u16>,
    pub(crate) stat: Option<HostStat>,
}

#[cfg(windows)]
impl HostListedWideEntry {
    pub(crate) fn logical_owned_bytes(&self) -> u64 { self.name.len() as u64 * 2 }
}

/// The names of one held directory, each with its facts, read relative to
/// the directory itself so no entry costs a path walk.
#[cfg(unix)]
pub struct HostStatReader {
    directory: Dir,
    buffer: Vec<u64>,
    next: usize,
    filled: usize,
    exhausted: bool,
    #[cfg(target_os = "macos")]
    base: libc::off_t,
}

#[cfg(unix)]
impl Iterator for HostStatReader {
    type Item = io::Result<HostListedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_measured(&mut ordinary_work()).map_err(ordinary_error).transpose()
    }
}

/// The names of one held directory, read one buffer of index records per
/// kernel call, each stat'ed by name relative to the held directory exactly
/// as a lookup stats it.
#[cfg(windows)]
pub struct HostStatReader {
    directory: Dir,
    volume_serial_number: u32,
    /// `u64` storage keeps every entry record 8-byte aligned.
    buffer: Vec<u64>,
    /// Byte offset of the next unread record in `buffer`, if any.
    next: Option<usize>,
    exhausted: bool,
}

#[cfg(windows)]
impl HostStatReader {
    const BUFFER_BYTES: usize = 64 * 1024;

    /// Reads the next buffer of records; `false` once none remain.
    fn refill(&mut self) -> io::Result<bool> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Win32::Foundation::{ERROR_NO_MORE_FILES, HANDLE};
        use windows::Win32::Storage::FileSystem::{
            FileIdExtdDirectoryInfo, GetFileInformationByHandleEx,
        };

        if self.exhausted {
            return Ok(false);
        }
        let bytes = u32::try_from(self.buffer.len() * std::mem::size_of::<u64>())
            .map_err(|_| io::Error::other("enumeration buffer size"))?;
        // SAFETY: the held directory handle and the owned buffer outlive this
        // synchronous call, and `bytes` is exactly the buffer's length.
        let result = unsafe {
            GetFileInformationByHandleEx(
                HANDLE(self.directory.as_handle().as_raw_handle()),
                FileIdExtdDirectoryInfo,
                self.buffer.as_mut_ptr().cast(),
                bytes,
            )
        };
        match result {
            Ok(()) => {
                self.next = Some(0);
                Ok(true)
            }
            Err(error)
                if error.code() == windows::core::HRESULT::from_win32(ERROR_NO_MORE_FILES.0) =>
            {
                self.exhausted = true;
                Ok(false)
            }
            Err(error) => Err(io::Error::from_raw_os_error(error.code().0 & 0xffff)),
        }
    }

}

#[cfg(windows)]
fn decode_host_directory_record(
    buffer: &[u64],
    offset: usize,
) -> io::Result<(Option<usize>, &[u16])> {
    use std::mem::{align_of, offset_of, size_of};
    use windows::Win32::Storage::FileSystem::FILE_ID_EXTD_DIR_INFO;

    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
    let total = buffer.len().checked_mul(size_of::<u64>()).ok_or_else(malformed)?;
    let header = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
    if !offset.is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>())
        || offset.checked_add(header).is_none_or(|end| end > total)
    {
        return Err(malformed());
    }
    // SAFETY: only the two checked fixed-header fields are read. A Windows
    // record has a variable UTF-16 tail, so forming a reference to the entire
    // Rust struct would also require its trailing field and padding to exist.
    let record = unsafe { buffer.as_ptr().cast::<u8>().add(offset).cast::<FILE_ID_EXTD_DIR_INFO>() };
    let (name_bytes, step) = unsafe {
        (
            std::ptr::addr_of!((*record).FileNameLength).read_unaligned(),
            std::ptr::addr_of!((*record).NextEntryOffset).read_unaligned(),
        )
    };
    let name_bytes = usize::try_from(name_bytes).map_err(|_| malformed())?;
    let step = usize::try_from(step).map_err(|_| malformed())?;
    let next = if step == 0 {
        None
    } else {
        let next = offset.checked_add(step).ok_or_else(malformed)?;
        if !step.is_multiple_of(align_of::<FILE_ID_EXTD_DIR_INFO>()) || next >= total {
            return Err(malformed());
        }
        Some(next)
    };
    let end = next.unwrap_or(total);
    let start = offset.checked_add(header).ok_or_else(malformed)?;
    if name_bytes == 0 || !name_bytes.is_multiple_of(size_of::<u16>())
        || start.checked_add(name_bytes).is_none_or(|name_end| name_end > end)
    {
        return Err(malformed());
    }
    // SAFETY: every UTF-16 unit is initialized owned storage inside this
    // record, not merely somewhere in a following record's buffer.
    let name = unsafe {
        std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>().add(start).cast::<u16>(), name_bytes / 2)
    };
    Ok((next, name))
}

#[cfg(windows)]
impl Iterator for HostStatReader {
    type Item = io::Result<HostListedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::os::windows::ffi::OsStringExt as _;
        self.next_wide_measured(&mut ordinary_work())
            .map(|entry| entry.map(|entry| HostListedEntry {
                name: OsString::from_wide(&entry.name), stat: entry.stat,
            }))
            .map_err(ordinary_error).transpose()
    }
}

/// Whether Windows attributes `before` and `after` describe the same file.
/// `ProjFS` marks a file it manages (reparse point, sparse, recall bits)
/// and drops the marks when it stops managing it; hydrating a placeholder
/// also sets its archive bit. Its content and metadata stay what they were.
#[cfg(windows)]
pub(crate) const fn same_windows_attributes(before: u32, after: u32) -> bool {
    const SPARSE: u32 = 0x0000_0200;
    const REPARSE_POINT: u32 = 0x0000_0400;
    const RECALL_ON_OPEN: u32 = 0x0004_0000;
    const RECALL_ON_DATA_ACCESS: u32 = 0x0040_0000;
    const ARCHIVE: u32 = 0x0000_0020;
    const PROJFS_MARKS: u32 = SPARSE | REPARSE_POINT | RECALL_ON_OPEN | RECALL_ON_DATA_ACCESS;
    // Marks are ignored only as they leave: a mark gained is a change.
    let marks = before & PROJFS_MARKS;
    let hydration = if before & RECALL_ON_DATA_ACCESS == 0 {
        0
    } else {
        ARCHIVE
    };
    let ignored = marks | hydration;
    before & !ignored == after & !ignored && after & PROJFS_MARKS & !marks == 0
}

/// The error a failed `NTSTATUS` reports. A name deleted while another
/// handle holds its file stays listed until that handle closes, yet nothing
/// can open it again: it is gone, where Win32 would report denied access.
#[cfg(windows)]
fn status_error(status: windows::Win32::Foundation::NTSTATUS) -> io::Error {
    const DELETE_PENDING: windows::Win32::Foundation::NTSTATUS =
        windows::Win32::Foundation::NTSTATUS(0xC000_0056_u32.cast_signed());
    if status == DELETE_PENDING {
        return io::ErrorKind::NotFound.into();
    }
    // SAFETY: a pure status-code translation.
    let code = unsafe { windows::Win32::Foundation::RtlNtStatusToDosError(status) };
    i32::try_from(code).map_or_else(
        |_| io::Error::other(format!("{status:?}")),
        io::Error::from_raw_os_error,
    )
}

/// One `FileStatInformation` query naming `path` relative to `directory`,
/// which no reparse point may redirect; its object's own reparse tag is
/// reported, not resolved. `None` when the path names the directory itself
/// or redirection was refused, which only a held walk may resolve.
#[cfg(windows)]
fn stat_information_at(
    directory: &Dir,
    path: &Path,
) -> io::Result<Option<windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION>> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationByName,
    };
    use windows::Win32::Foundation::{
        HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING,
    };
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    const REPARSE_POINT_ENCOUNTERED: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());

    let Some((mut name, length)) = relative_kernel_name(path) else {
        return Ok(None);
    };
    let name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(name.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(std::mem::size_of::<OBJECT_ATTRIBUTES>())
            .map_err(|_| io::Error::other("object attributes size"))?,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const name,
        // Win32 names are case-insensitive unless the directory says
        // otherwise; no reparse point may redirect the query.
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut information = FILE_STAT_INFORMATION::default();
    // SAFETY: every pointer names a live, correctly sized value for this
    // synchronous query, and the held directory handle outlives it.
    let status = unsafe {
        NtQueryInformationByName(
            &raw const attributes,
            &raw mut status_block,
            (&raw mut information).cast(),
            u32::try_from(std::mem::size_of::<FILE_STAT_INFORMATION>())
                .map_err(|_| io::Error::other("stat information size"))?,
            FileStatInformation,
        )
    };
    if status == REPARSE_POINT_ENCOUNTERED {
        return Ok(None);
    }
    if status.is_err() {
        return Err(status_error(status));
    }
    Ok(Some(information))
}

/// Stats `path` relative to `directory` with one [`stat_information_at`]
/// query, on the volume `volume_serial_number` names. `None` when the path
/// crosses or ends in a reparse point, which only a held walk may resolve.
#[cfg(all(windows, test))]
fn stat_at(
    directory: &Dir,
    path: &Path,
    volume_serial_number: u32,
) -> io::Result<Option<HostStat>> {
    match stat_information_at(directory, path)? {
        Some(information) if information.ReparseTag == 0 => {
            HostStat::from_stat_information(&information, volume_serial_number).map(Some)
        }
        _ => Ok(None),
    }
}

/// How an opened file will be read: through its cursor, or only at
/// explicit offsets.
#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileReads {
    Cursor,
    Positional,
}

/// A held directory capability whose relative operations cannot escape through
/// path traversal or an intermediate symbolic link/reparse point.
pub struct HostRoot {
    directory: Dir,
    identity: crate::NativeRootIdentity,
}

pub(crate) struct HostDirectoryEntry {
    pub(crate) name: OsString,
    pub(crate) is_dir: bool,
}

pub(crate) enum HostReadDir<'a> {
    #[cfg(target_os = "linux")]
    Linux {
        entries: rustix::fs::Dir,
        held: &'a Dir,
    },
    #[cfg(not(target_os = "linux"))]
    Native(ReadDir, std::marker::PhantomData<&'a Dir>),
}

impl Iterator for HostReadDir<'_> {
    type Item = io::Result<HostDirectoryEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Linux { entries, held } => loop {
                let entry = match entries.next()? {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error.into())),
                };
                use std::os::unix::ffi::OsStrExt as _;
                let name = OsStr::from_bytes(entry.file_name().to_bytes());
                if name == "." || name == ".." {
                    continue;
                }
                let is_dir = match entry.file_type() {
                    rustix::fs::FileType::Directory => true,
                    rustix::fs::FileType::Unknown => match held.symlink_metadata(name) {
                        Ok(metadata) => metadata.is_dir(),
                        Err(error) => return Some(Err(error)),
                    },
                    _ => false,
                };
                return Some(Ok(HostDirectoryEntry {
                    name: name.to_os_string(),
                    is_dir,
                }));
            },
            #[cfg(not(target_os = "linux"))]
            Self::Native(entries, _) => entries.next().map(|entry| {
                let entry = entry?;
                let file_type = entry.file_type()?;
                Ok(HostDirectoryEntry {
                    name: entry.file_name(),
                    is_dir: file_type.is_dir() && !file_type.is_symlink(),
                })
            }),
        }
    }
}

/// A held directory capability for race-free leaf operations.
pub struct HostDirectory {
    directory: Dir,
}

impl HostDirectory {
    /// Exchanges native entries without releasing either original parent.
    pub(crate) fn exchange_to_measured(
        &self, name: &Path, destination: &Self, destination_name: &Path,
        work: &mut impl crate::demand::SourceDemandReadWork,
    ) -> Result<(), DemandError> {
        crate::native_exchange::exchange_native_entries_held_measured(
            &self.directory, name, &destination.directory, destination_name, work,
        )
    }

    /// Resolves the existing identity-bearing entry exchange witness.
    pub(crate) fn recover_entry_exchange_measured(
        &self, name: &Path, work: &mut impl crate::demand::SourceDemandReadWork,
    ) -> Result<(), DemandError> {
        crate::native_exchange::recover_native_entry_exchange_held_measured(
            &self.directory, name, work,
        )
    }
}


#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
impl LinuxMetadataTarget {
    fn open_measured<W: crate::demand::SourceDemandReadWork>(root: &Dir, path: &Path, work: &mut W) -> Result<Self, DemandError> {
        let file = with_unix_parent_leaf(root, path, work, |parent, leaf, work| {
            open_unix_named_with(parent, leaf, libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                || native_read_with_work(work))
        })?;
        Ok(Self { inode: file.into() })
    }

    fn observed_measured<W: crate::demand::SourceDemandReadWork>(&self, work: &mut W) -> Result<libc::stat, DemandError> {
        use std::os::fd::AsRawFd as _;

        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        native_read_with_work(work)?;
        // SAFETY: the held inode is live and fstat initializes stat on success.
        if unsafe { libc::fstat(self.inode.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: fstat succeeded and initialized the complete structure.
        Ok(unsafe { stat.assume_init() })
    }

    /// Sets POSIX extended attributes on the pinned inode, each by its full
    /// raw name, and reads each back. Linux gives a symbolic link no user
    /// attributes, so asking for any on one fails closed.
    #[cfg(feature = "native-mount")]
    pub(crate) fn set_extended_attributes(
        &self,
        attributes: &[(Vec<u8>, bytes::Bytes)],
    ) -> Result<(), LinuxMetadataError> {
        self.set_extended_attributes_measured(attributes, &mut ordinary_work())
            .map_err(|error| match error {
                DemandError::LinuxMetadata(error) => error,
                error => LinuxMetadataError::Io(ordinary_error(error)),
            })
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn set_extended_attributes_measured<W: crate::demand::SourceDemandReadWork>(
        &self,
        attributes: &[(Vec<u8>, bytes::Bytes)],
        work: &mut W,
    ) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::OsStrExt as _;

        work.check()?;
        if attributes.is_empty() { return Ok(()); }
        if self.observed_measured(work)?.st_mode & libc::S_IFMT == libc::S_IFLNK {
            return Err(LinuxMetadataError::Unsupported("named_attributes on a symbolic link").into());
        }
        // O_PATH cannot receive xattr calls. The proc link names the pinned
        // inode; a fixed stack buffer avoids allocating a formatted pathname.
        let mut pinned = [0_u8; 32];
        let prefix = b"/proc/self/fd/";
        let fd = self.inode.as_raw_fd() as u32;
        let digits = fd.checked_ilog10().unwrap_or(0) as usize + 1;
        let length = prefix.len() + digits;
        work.charge(WorkCounters { bytes_copied: prefix.len() as u64,
            bytes_encoded: digits as u64, ..WorkCounters::UNCHARGED })?;
        pinned[..prefix.len()].copy_from_slice(prefix);
        let mut number = fd;
        for index in (prefix.len()..length).rev() {
            pinned[index] = b'0' + (number % 10) as u8;
            number /= 10;
        }
        let pinned = std::ffi::CStr::from_bytes_with_nul(&pinned[..=length])
            .map_err(|_| LinuxMetadataError::Unsupported("named_attributes"))?;
        for (name, value) in attributes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let mut entered = false;
            let result = unix_name_with_work(OsStr::from_bytes(name), work, |name, work| {
                entered = true;
                native_write(work)?;
                // SAFETY: names and the exact value slice remain live.
                if unsafe { libc::setxattr(pinned.as_ptr(), name.as_ptr(),
                    value.as_ptr().cast(), value.len(), 0) } != 0 {
                    return Err(io::Error::last_os_error().into());
                }
                let length = value.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
                work.allocate(length as u64)?;
                let mut readback = vec![0_u8; length];
                let result = (|| {
                    work.check()?;
                    work.preflight(WorkCounters { source_bytes_read: length as u64, ..WorkCounters::UNCHARGED })?;
                    native_read_with_work(work)?;
                    // SAFETY: the output buffer is writable for its full length.
                    let read = unsafe { libc::getxattr(pinned.as_ptr(), name.as_ptr(),
                        readback.as_mut_ptr().cast(), readback.len()) };
                    if let Ok(read) = u64::try_from(read) {
                        work.record(WorkCounters { source_bytes_read: read, ..WorkCounters::UNCHARGED })?;
                    }
                    if usize::try_from(read).ok() != Some(value.len()) {
                        return Err(LinuxMetadataError::Unsupported("named_attributes readback mismatch").into());
                    }
                    for (actual, expected) in readback.iter().zip(value.iter()) {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        if actual != expected {
                            return Err(LinuxMetadataError::Unsupported("named_attributes readback mismatch").into());
                        }
                    }
                    Ok(())
                })();
                drop(readback);
                work.release(length as u64)?;
                result
            });
            match result {
                Err(DemandError::Io(error)) if !entered && error.kind() == io::ErrorKind::InvalidInput =>
                    return Err(LinuxMetadataError::Unsupported("named_attributes").into()),
                result => result?,
            }
        }
        Ok(())
    }

    pub(crate) fn apply(
        &self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), LinuxMetadataError> {
        self.apply_measured(metadata, &mut ordinary_work()).map_err(|error| match error {
            DemandError::LinuxMetadata(error) => error,
            error => LinuxMetadataError::Io(ordinary_error(error)),
        })
    }

    /// Charges each pinned-inode read, capability probe, and mutation before
    /// attempting it, preserving unsupported metadata as a typed failure.
    pub(crate) fn apply_measured<W: crate::demand::SourceDemandReadWork>(
        &self, metadata: crate::kernel::FileMetadata, work: &mut W,
    ) -> Result<(), DemandError> {
        use crate::kernel::MetadataField;
        use std::os::fd::AsRawFd as _;

        let mut observed = self.observed_measured(work)?;
        validate_linux_metadata(observed, metadata, work)?;
        let requested_uid = match metadata.posix_uid {
            MetadataField::Value(uid) if uid != observed.st_uid => uid,
            _ => libc::uid_t::MAX,
        };
        let requested_gid = match metadata.posix_gid {
            MetadataField::Value(gid) if gid != observed.st_gid => gid,
            _ => libc::gid_t::MAX,
        };
        if requested_uid != libc::uid_t::MAX || requested_gid != libc::gid_t::MAX {
            native_write(work)?;
            // SAFETY: AT_EMPTY_PATH acts on the pinned inode, including a final symlink.
            if unsafe {
                libc::fchownat(
                    self.inode.as_raw_fd(),
                    c"".as_ptr(),
                    requested_uid,
                    requested_gid,
                    libc::AT_EMPTY_PATH,
                )
            } != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            observed = self.observed_measured(work)?;
        }
        if let MetadataField::Value(mode) = metadata.posix_mode {
            let desired = mode & 0o7777;
            if desired != observed.st_mode & 0o7777 {
                native_write(work)?;
                // SAFETY: fchmodat2 with AT_EMPTY_PATH acts only on the held inode.
                if unsafe {
                    libc::syscall(
                        LINUX_FCHMODAT2_SYSCALL,
                        self.inode.as_raw_fd(),
                        c"".as_ptr(),
                        desired,
                        libc::AT_EMPTY_PATH,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error().into());
                }
                observed = self.observed_measured(work)?;
            }
        }
        if matches!(metadata.modified_ns, MetadataField::Value(_))
            || matches!(metadata.accessed_ns, MetadataField::Value(_))
        {
            let times = [
                linux_time_spec(metadata.accessed_ns)?,
                linux_time_spec(metadata.modified_ns)?,
            ];
            native_write(work)?;
            // SAFETY: AT_EMPTY_PATH targets the held inode and the timespecs
            // remain live until the syscall returns.
            if unsafe {
                libc::utimensat(
                    self.inode.as_raw_fd(),
                    c"".as_ptr(),
                    times.as_ptr(),
                    libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            observed = self.observed_measured(work)?;
        }
        verify_linux_metadata(observed, metadata).map_err(Into::into)
    }
}

#[cfg(target_os = "linux")]
#[cfg(target_arch = "aarch64")]
// libc does not expose SYS_fchmodat2 on aarch64 yet; asm-generic/unistd.h does.
#[cfg(any(feature = "native-mount", test))]
const LINUX_FCHMODAT2_SYSCALL: libc::c_long = 452;

#[cfg(target_os = "linux")]
#[cfg(not(target_arch = "aarch64"))]
#[cfg(any(feature = "native-mount", test))]
const LINUX_FCHMODAT2_SYSCALL: libc::c_long = libc::SYS_fchmodat2;

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn validate_linux_metadata<W: crate::demand::SourceDemandReadWork>(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
    work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;

    for (present, field) in [
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.posix_flags, MetadataField::Value(_)),
            "posix_flags",
        ),
        (
            matches!(metadata.named_attributes, MetadataField::Value(_)),
            "named_attributes",
        ),
        (matches!(metadata.acl, MetadataField::Value(_)), "acl"),
        (
            matches!(metadata.security_descriptor, MetadataField::Value(_)),
            "security_descriptor",
        ),
        (
            matches!(metadata.created_ns, MetadataField::Value(_)),
            "created_ns",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns",
        ),
    ] {
        if present {
            return Err(LinuxMetadataError::Unsupported(field).into());
        }
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        let kind = mode & libc::S_IFMT;
        if kind != 0 && kind != observed.st_mode & libc::S_IFMT {
            return Err(LinuxMetadataError::Unsupported("posix_mode file type").into());
        }
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            if observed.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(LinuxMetadataError::Unsupported("posix_mode symlink").into());
            }
            linux_require_fchmodat2(work)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn linux_require_fchmodat2<W: crate::demand::SourceDemandReadWork>(work: &mut W) -> Result<(), DemandError> {
    native_write(work)?;
    // An invalid descriptor makes the capability probe non-mutating. A kernel
    // with fchmodat2 and AT_EMPTY_PATH support returns EBADF; an older kernel
    // returns ENOSYS or EINVAL before any ownership change is attempted.
    // SAFETY: the probe passes only descriptor -1 and a static empty C string.
    let result = unsafe {
        libc::syscall(
            LINUX_FCHMODAT2_SYSCALL,
            -1,
            c"".as_ptr(),
            0,
            libc::AT_EMPTY_PATH,
        )
    };
    if result == 0 {
        return Err(LinuxMetadataError::Unsupported("fchmodat2 probe").into());
    }
    match io::Error::last_os_error().raw_os_error() {
        Some(libc::EBADF) => Ok(()),
        Some(libc::ENOSYS | libc::EINVAL) => {
            Err(LinuxMetadataError::Unsupported("fchmodat2 AT_EMPTY_PATH").into())
        }
        _ => Err(io::Error::last_os_error().into()),
    }
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn verify_linux_metadata(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
) -> Result<(), LinuxMetadataError> {
    use crate::kernel::MetadataField;

    if let MetadataField::Value(uid) = metadata.posix_uid
        && observed.st_uid != uid
    {
        return Err(LinuxMetadataError::Unsupported("posix_uid"));
    }
    if let MetadataField::Value(gid) = metadata.posix_gid
        && observed.st_gid != gid
    {
        return Err(LinuxMetadataError::Unsupported("posix_gid"));
    }
    if let MetadataField::Value(mode) = metadata.posix_mode
        && observed.st_mode & 0o7777 != mode & 0o7777
    {
        return Err(LinuxMetadataError::Unsupported("posix_mode"));
    }
    if let MetadataField::Value(ns) = metadata.accessed_ns
        && linux_observed_ns(observed.st_atime, observed.st_atime_nsec)? != ns
    {
        return Err(LinuxMetadataError::Unsupported("accessed_ns"));
    }
    if let MetadataField::Value(ns) = metadata.modified_ns
        && linux_observed_ns(observed.st_mtime, observed.st_mtime_nsec)? != ns
    {
        return Err(LinuxMetadataError::Unsupported("modified_ns"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn linux_time_spec(
    field: crate::kernel::MetadataField<i64>,
) -> Result<libc::timespec, LinuxMetadataError> {
    let crate::kernel::MetadataField::Value(ns) = field else {
        return Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        });
    };
    Ok(libc::timespec {
        tv_sec: libc::time_t::try_from(ns.div_euclid(1_000_000_000))
            .map_err(|_| LinuxMetadataError::Unsupported("timestamp range"))?,
        tv_nsec: libc::c_long::try_from(ns.rem_euclid(1_000_000_000))
            .map_err(|_| LinuxMetadataError::Unsupported("timestamp range"))?,
    })
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn linux_observed_ns(seconds: i64, nanos: i64) -> Result<i64, LinuxMetadataError> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|whole| whole.checked_add(nanos))
        .ok_or(LinuxMetadataError::Unsupported("timestamp range"))
}

#[cfg(windows)]
fn wide_units_with<E: From<crate::WorkError>>(
    name: &OsStr, mut examine: impl FnMut() -> Result<(), E>,
) -> Result<usize, E> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut units = 0usize;
    for _ in name.encode_wide() {
        examine()?;
        units = units.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(units)
}

#[cfg(windows)]
fn with_wide_path<T>(path: &Path, work: &mut DemandWork,
    operation: impl FnOnce(&[u16], &mut DemandWork) -> Result<T, DemandError>) -> Result<T, DemandError> {
    with_wide_path_with_work(path, work, operation)
}

#[cfg(windows)]
pub(crate) fn with_wide_path_with_work<W: NativeHostWork, T>(
    path: &Path, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    work.check()?;
    let units = wide_units_with(path.as_os_str(), || {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)
    })?;
    let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64;
    let encoded = WorkCounters { bytes_encoded: bytes, ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut allocation = work.claim(bytes)?;
    let mut name = Vec::new();
    let reserved = name.try_reserve_exact(units);
    let reconciled = work.reconcile_allocation(&mut allocation, name.capacity() as u64 * 2);
    let result = (|| {
        reserved.map_err(W::allocation_failed).and(reconciled)?;
        name.extend(path.as_os_str().encode_wide().map(|unit| if unit == u16::from(b'/') { u16::from(b'\\') } else { unit }));
        work.record(encoded)?;
        operation(&name, work)
    })();
    drop(name);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
fn windows_wide_name<T>(
    path: &[u16], work: &mut DemandWork,
    operation: impl FnOnce(&mut windows::Win32::Foundation::UNICODE_STRING, &mut DemandWork) -> Result<T, DemandError>,
) -> Result<Option<T>, DemandError> {
    windows_wide_name_with_work(path, work, || DemandError::InvalidRequest, operation)
}

#[cfg(windows)]
fn windows_wide_name_with_work<W: NativeHostWork, T>(
    path: &[u16], work: &mut W, invalid_length: impl FnOnce() -> W::Error,
    operation: impl FnOnce(&mut windows::Win32::Foundation::UNICODE_STRING, &mut W) -> Result<T, W::Error>,
) -> Result<Option<T>, W::Error> {
    work.check()?;
    if path.is_empty() { return Ok(None); }
    if path != [u16::from(b'.')] {
        let mut begin = 0usize;
        let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(item)?;
        work.record(item)?;
        for (index, unit) in path.iter().enumerate() {
            work.admit(item)?;
            work.record(item)?;
            if matches!(*unit, 0 | 47 | 58) { return Ok(None); }
            if *unit == 92 {
                let component = &path[begin..index];
                if component.is_empty() || component == [46] || component == [46, 46] { return Ok(None); }
                begin = index + 1;
                work.admit(item)?;
                work.record(item)?;
            }
        }
        let component = &path[begin..];
        if component.is_empty() || component == [46] || component == [46, 46] { return Ok(None); }
    }
    let length = u16::try_from(path.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| invalid_length())?;
    // NT treats the input UNICODE_STRING name as read-only, despite the
    // historical mutable pointer type of its Buffer field.
    let mut string = windows::Win32::Foundation::UNICODE_STRING {
        Length: length, MaximumLength: length, Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
    };
    operation(&mut string, work).map(Some)
}


#[cfg(windows)]
fn stat_windows_wide_at_measured<W: NativeHostWork>(directory: &Dir, path: &[u16], volume: u32, work: &mut W) -> Result<Option<HostStat>, W::Error> {
    Ok(windows_wide_name_with_work(path, work, W::invalid_request, |name, work| {
        stat_windows_name_with_work(directory, name, volume, work)
    })?.flatten())
}

#[cfg(windows)]
fn stat_windows_name_with_work<W: NativeHostWork>(
    directory: &Dir, name: &windows::Win32::Foundation::UNICODE_STRING,
    volume: u32, work: &mut W,
) -> Result<Option<HostStat>, W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationByName};
    use windows::Win32::Foundation::{HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    const REPARSE: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const *name, Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut block = IO_STATUS_BLOCK::default();
    let mut information = FILE_STAT_INFORMATION::default();
    native_read_with_work(work)?;
    // SAFETY: held capability and fixed-size query storage outlive the call.
    let status = unsafe { NtQueryInformationByName(&raw const attributes, &raw mut block,
        (&raw mut information).cast(), std::mem::size_of::<FILE_STAT_INFORMATION>() as u32, FileStatInformation) };
    if status == REPARSE { return Ok(None); }
    if status.is_err() { return Err(status_error(status).into()); }
    if information.ReparseTag != 0 { return Ok(None); }
    Ok(Some(HostStat::from_stat_information(&information, volume)?))
}


#[cfg(windows)]
fn open_windows_wide_at_measured(
    directory: &Dir, path: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut DemandWork,
) -> Result<Option<cap_std::fs::File>, DemandError> {
    open_windows_wide_at_with_work(directory, path, access, options, work, || DemandError::InvalidRequest)
}

#[cfg(windows)]
fn open_windows_wide_at_with_work<W: NativeHostWork>(
    directory: &Dir, path: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    work: &mut W, invalid_length: impl FnOnce() -> W::Error,
) -> Result<Option<cap_std::fs::File>, W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN, NtCreateFile};
    use windows::Win32::Foundation::{HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    use windows::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    const REPARSE: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());
    Ok(windows_wide_name_with_work(path, work, invalid_length, |name, work| {
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
            ObjectName: &raw const *name, Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
            ..Default::default()
        };
        let mut block = IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        native_read_with_work(work)?;
        let status = unsafe { NtCreateFile(&raw mut handle, access, &raw const attributes, &raw mut block,
            None, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN, options, None, 0) };
        if status == REPARSE { return Ok(None); }
        if status.is_err() { return Err(status_error(status).into()); }
        // SAFETY: success transferred exclusive ownership of this new handle.
        Ok(Some(cap_std::fs::File::from_std(unsafe { File::from_raw_handle(handle.0) })))
    })?.flatten())
}

#[cfg(windows)]
fn stat_windows_file_measured<W: NativeHostWork>(file: &impl std::os::windows::io::AsHandle, volume: u32, work: &mut W) -> Result<HostStat, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationFile};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    let mut block = IO_STATUS_BLOCK::default();
    let mut information = FILE_STAT_INFORMATION::default();
    native_read_with_work(work)?;
    // SAFETY: live held file and correctly sized query storage.
    let status = unsafe { NtQueryInformationFile(HANDLE(file.as_handle().as_raw_handle()),
        &raw mut block, (&raw mut information).cast(),
        std::mem::size_of::<FILE_STAT_INFORMATION>() as u32, FileStatInformation) };
    if status.is_err() { return Err(status_error(status).into()); }
    Ok(HostStat::from_stat_information(&information, volume)?)
}

#[cfg(windows)]
pub(crate) fn stat_windows_leaf_with_work<W: NativeHostWork>(
    directory: &Dir, path: &[u16], work: &mut W,
) -> Result<HostStat, W::Error> {
    use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let volume = u32::try_from(directory_identity_with_work(directory, work)?.device)
        .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
    let name = windows_leaf_name_with_work(path, work)?;
    if let Some(stat) = stat_windows_name_with_work(directory, &name, volume, work)? {
        return Ok(stat);
    }
    let file = open_windows_leaf_handle_with_work(directory, path,
        FILE_READ_ATTRIBUTES | SYNCHRONIZE, None, work)?;
    stat_windows_file_measured(&file, volume, work)
}

#[cfg(windows)]
fn wide_parent_leaf(path: &[u16]) -> Result<(&[u16], &[u16]), DemandError> {
    let (parent, leaf) = match path.iter().rposition(|unit| *unit == u16::from(b'\\')) {
        Some(index) => (&path[..index], &path[index + 1..]),
        None => (&[][..], path),
    };
    if leaf.is_empty() { return Err(DemandError::InvalidRequest); }
    Ok((parent, leaf))
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn stat_file_measured(&self, file: &File, work: &mut DemandWork) -> Result<HostStat, DemandError> {
        self.stat_file_with_work(file, work)
    }

    pub(crate) fn open_dir_held_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<Dir, DemandError> {
        self.open_dir_held_wide_with_identity_measured(path, work).map(|(directory, _)| directory)
    }

    pub(crate) fn open_dir_held_wide_with_work<W: NativeHostWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<Dir, W::Error> {
        self.open_dir_held_wide_with_identity_with_work(path, work).map(|(directory, _)| directory)
    }

    pub(crate) fn open_dir_held_wide_with_identity_measured(
        &self, path: &[u16], work: &mut DemandWork,
    ) -> Result<(Dir, NativeRootIdentity), DemandError> {
        self.open_dir_held_wide_with_identity_with_work(path, work)
    }

    pub(crate) fn open_dir_held_wide_with_identity_with_work<W: NativeHostWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<(Dir, NativeRootIdentity), W::Error> {
        let mut current = clone_directory_with(&self.directory, || native_read_with_work(work))?;
        let mut identity = self.identity;
        if path.is_empty() { return Ok((current, identity)); }
        for component in path.split(|unit| *unit == u16::from(b'\\')) {
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(item)?;
            work.record(item)?;
            let (next, next_identity) = self.open_dir_component_wide_with_work(&current, component, work)?;
            identity = next_identity;
            current = next;
        }
        Ok((current, identity))
    }

    pub(crate) fn open_dir_component_wide_measured(
        &self, parent: &Dir, name: &[u16], work: &mut DemandWork,
    ) -> Result<(Dir, NativeRootIdentity), DemandError> {
        self.open_dir_component_wide_with_work(parent, name, work)
    }

    pub(crate) fn open_dir_component_wide_with_work<W: NativeHostWork>(
        &self, parent: &Dir, name: &[u16], work: &mut W,
    ) -> Result<(Dir, NativeRootIdentity), W::Error> {
        use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_ATTRIBUTE_REPARSE_POINT};
        if name.is_empty() || name == [46] || name == [46, 46] { return Err(W::invalid_request()); }
        let file = open_windows_wide_at_with_work(parent, name, FILE_GENERIC_READ,
            FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work,
            W::invalid_request)?.ok_or_else(W::invalid_request)?.into_std();
        let stat = self.stat_file_with_work(&file, work)?;
        if stat.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 { return Err(W::invalid_request()); }
        let identity = NativeRootIdentity {
            device: u64::from(stat.volume_serial_number()), object: stat.file_index(),
        };
        Ok((Dir::from_std_file(file), identity))
    }

    pub(crate) fn stat_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<HostStat, DemandError> {
        self.stat_wide_with_work(path, work)
    }

    pub(crate) fn stat_wide_with_work<W: NativeHostWork>(&self, path: &[u16], work: &mut W) -> Result<HostStat, W::Error> {
        use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let volume = self.volume_serial_number()?;
        if path.is_empty() { return stat_windows_file_measured(&self.directory, volume, work); }
        if let Some(stat) = stat_windows_wide_at_measured(&self.directory, path, volume, work)? { return Ok(stat); }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        let file = open_windows_wide_at_with_work(&parent, leaf, FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work,
            W::invalid_request)?.ok_or_else(W::invalid_request)?.into_std();
        self.stat_file_with_work(&file, work)
    }

    pub(crate) fn read_dir_stats_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<HostStatReader, DemandError> {
        self.read_dir_stats_wide_with_work(path, work).map(|(reader, _allocation)| reader)
    }

    pub(crate) fn read_dir_stats_wide_with_work<W: NativeHostWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        let volume_serial_number = self.volume_serial_number()?;
        let directory = self.open_dir_held_wide_with_work(path, work)?;
        read_dir_held_with_volume_with_work(&directory, volume_serial_number, work)
    }

    fn open_file_wide_for_with_work<W: NativeHostWork>(&self, path: &[u16], reads: FileReads, work: &mut W) -> Result<cap_std::fs::File, W::Error> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, NTCREATEFILE_CREATE_OPTIONS,
        };
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        let options = FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | match reads {
            FileReads::Cursor => FILE_SYNCHRONOUS_IO_NONALERT,
            FileReads::Positional => NTCREATEFILE_CREATE_OPTIONS(0),
        };
        if let Some(file) = open_windows_wide_at_with_work(&self.directory, path, FILE_GENERIC_READ, options, work, W::invalid_request)? {
            return Ok(file);
        }
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        open_windows_wide_at_with_work(&parent, leaf, FILE_GENERIC_READ, options, work, W::invalid_request)?
            .ok_or_else(W::invalid_request)
    }

    pub(crate) fn open_file_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<cap_std::fs::File, DemandError> {
        self.open_file_wide_for_with_work(path, FileReads::Cursor, work)
    }

    pub(crate) fn open_file_positional_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<cap_std::fs::File, DemandError> {
        self.open_file_positional_wide_with_work(path, work)
    }

    pub(crate) fn open_file_positional_wide_with_work<W: NativeHostWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        self.open_file_wide_for_with_work(path, FileReads::Positional, work)
    }

    pub(crate) fn open_file_yielding_wide_measured(&self, path: &[u16], work: &mut DemandWork) -> Result<YieldingFile, DemandError> {
        self.open_file_yielding_wide_with_work(path, work)
    }

    pub(crate) fn open_file_yielding_wide_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<YieldingFile, DemandError> {
        use windows::Wdk::Storage::FileSystem::{FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_OPEN_REQUIRING_OPLOCK, NTCREATEFILE_CREATE_OPTIONS};
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        let options = FILE_NON_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT;
        let file = match open_windows_wide_at_with_work(&self.directory, path, FILE_GENERIC_READ,
            options | FILE_OPEN_REQUIRING_OPLOCK, work, || DemandError::InvalidRequest) {
            Ok(file) => file,
            Err(DemandError::Io(_)) => open_windows_wide_at_with_work(&self.directory, path, FILE_GENERIC_READ,
                options | NTCREATEFILE_CREATE_OPTIONS(0), work, || DemandError::InvalidRequest)?,
            Err(error) => return Err(error),
        };
        match file {
            Some(file) => YieldingFile::new_with_work(file.into_std(), work),
            None => YieldingFile::plain_with_work(
                self.open_file_wide_for_with_work(path, FileReads::Positional, work)?.into_std(), work,
            ),
        }
    }

    pub(crate) fn open_file_yielding_measured(&self, path: &Path, work: &mut DemandWork) -> Result<YieldingFile, DemandError> {
        with_wide_path(path, work, |path, work| self.open_file_yielding_wide_measured(path, work))
    }

    pub(crate) fn read_link_wide_measured(&self, path: &[u16], maximum_bytes: u64, work: &mut DemandWork) -> Result<Vec<u16>, DemandError> {
        self.read_link_wide_with_work(path, maximum_bytes, work).map(|(target, _allocation)| target)
    }

    pub(crate) fn read_link_wide_with_work<W: NativeHostWork>(
        &self, path: &[u16], maximum_bytes: u64, work: &mut W,
    ) -> Result<(Vec<u16>, W::Allocation), W::Error> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        read_link_held_wide_with_work(&parent, leaf, maximum_bytes, work)
    }

    pub(crate) fn read_source_link_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, path: &[u16], maximum_bytes: u64, work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| DemandError::InvalidRequest)?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        read_source_link_held_wide_with_work(&parent, leaf, maximum_bytes, work)
    }

    pub(crate) fn read_link_units_measured(&self, path: &Path, maximum_bytes: u64, work: &mut DemandWork) -> Result<Vec<u16>, DemandError> {
        with_wide_path(path, work, |path, work| self.read_link_wide_measured(path, maximum_bytes, work))
    }
}

#[cfg(windows)]
impl HostStatReader {
    pub(crate) fn next_wide_measured(&mut self, work: &mut DemandWork) -> Result<Option<HostListedWideEntry>, DemandError> {
        self.next_wide_with_work(work).map(|entry| entry.map(|(entry, _live_bytes)| entry))
    }

    pub(crate) fn next_wide_with_work<W: NativeHostWork>(
        &mut self, work: &mut W,
    ) -> Result<Option<(HostListedWideEntry, W::Allocation)>, W::Error> {
        loop {
            work.check()?;
            let Some(offset) = self.next else {
                if self.exhausted { return Ok(None); }
                // Directory control data is metadata: count native IO and
                // decoded entries, not application/source BODY bytes.
                native_read_with_work(work)?;
                if !self.refill()? { return Ok(None); }
                continue;
            };
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(item)?;
            work.record(item)?;
            let (next, name) = decode_host_directory_record(&self.buffer, offset)?;
            let dot = u16::from(b'.');
            if name == [dot] || name == [dot, dot] { self.next = next; continue; }
            let bytes = name.len() as u64 * 2;
            let delta = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let mut allocation = work.claim(bytes)?;
            let mut copied = Vec::new();
            let reserved = copied.try_reserve_exact(name.len());
            let reconciled = work.reconcile_allocation(&mut allocation, copied.capacity() as u64 * 2);
            if let Err(error) = reserved.map_err(W::allocation_failed).and(reconciled) {
                drop(copied);
                work.release(allocation)?;
                return Err(error);
            }
            copied.extend_from_slice(name);
            let name = copied;
            let stat = match (|| {
                work.record(delta)?;
                Ok(windows_wide_name_with_work(&name, work,
                    || io::Error::new(io::ErrorKind::InvalidInput, "native name exceeds NT length").into(),
                    |string, work| stat_windows_name_with_work(&self.directory, string, self.volume_serial_number, work))?
                    .flatten())
            })() {
                Ok(stat) => stat,
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => None,
                Err(error) => {
                    drop(name);
                    work.release(allocation)?;
                    if W::io_error_kind(&error).is_some() { self.exhausted = true; self.next = None; }
                    return Err(error);
                }
            };
            self.next = next;
            return Ok(Some((HostListedWideEntry { name, stat }, allocation)));
        }
    }
}

#[cfg(windows)]
fn read_windows_link_wide_measured(parent: &Dir, name: &[u16], maximum: u64, work: &mut DemandWork) -> Result<Vec<u16>, DemandError> {
    read_windows_link_wide_with_work(parent, name, maximum, work, || DemandError::InvalidRequest)
        .map(|(target, _)| target)
}

#[cfg(windows)]
pub(crate) fn read_link_held_wide_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], maximum: u64, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    read_windows_link_wide_with_work(parent, name, maximum, work, || {
        io::Error::new(io::ErrorKind::InvalidInput, "native link exceeds its bounded buffer").into()
    })
}

#[cfg(windows)]
pub(crate) fn read_link_held_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, maximum: u64, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    require_leaf(name)?;
    with_wide_path_with_work(name, work, |name, work| read_link_held_wide_with_work(parent, name, maximum, work))
}

#[cfg(windows)]
fn read_windows_link_wide_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], maximum: u64, work: &mut W,
    invalid_request: impl Fn() -> W::Error,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let file = open_windows_link_with_work(parent, name, work, &invalid_request)?;
    let size = maximum.saturating_mul(4).saturating_add(32).min(16 * 1024);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    let allocation = work.claim(size)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(size as usize) {
        work.release(allocation)?;
        return Err(W::allocation_failed(error));
    }
    buffer.resize(size as usize, 0u8);
    let result = (|| {
        let returned = read_windows_link_record_with_work(&file, &mut buffer, work, &invalid_request)?;
        let target = windows_link_target_range::<W>(&buffer[..returned], maximum, &invalid_request)?;
        let target = &buffer[target];
        let length = target.len();
        let copied = WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let allocation = work.claim(length as u64)?;
        let mut wide = Vec::new();
        if let Err(error) = wide.try_reserve_exact(length / 2) {
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        wide.extend(target.chunks_exact(2).map(|pair| u16::from_le_bytes([pair[0], pair[1]])));
        if let Err(error) = work.record(copied).and_then(|()| work.check()) {
            drop(wide);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((wide, allocation))
    })();
    drop(buffer);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
fn open_windows_link_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W, invalid_request: impl Fn() -> W::Error,
) -> Result<cap_std::fs::File, W::Error> {
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    open_windows_wide_at_with_work(parent, name, FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work, &invalid_request)?
        .ok_or_else(invalid_request)
}

#[cfg(windows)]
fn read_windows_link_record_with_work<W: NativeHostWork>(
    file: &cap_std::fs::File, buffer: &mut [u8], work: &mut W, invalid_request: impl Fn() -> W::Error,
) -> Result<usize, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;
    let mut returned = 0u32;
    work.admit(WorkCounters { source_bytes_read: buffer.len() as u64, ..WorkCounters::UNCHARGED })?;
    native_read_with_work(work)?;
    // SAFETY: the held no-follow leaf and initialized fixed output remain live.
    let read = unsafe { DeviceIoControl(HANDLE(file.as_raw_handle()), FSCTL_GET_REPARSE_POINT,
        None, 0, Some(buffer.as_mut_ptr().cast()), buffer.len() as u32, Some(&raw mut returned), None) };
    work.record(WorkCounters { source_bytes_read: u64::from(returned), ..WorkCounters::UNCHARGED })?;
    if let Err(error) = read {
        let code = error.code().0 & 0xffff;
        if code == 122 || code == 234 { return Err(invalid_request()); }
        return Err(io::Error::from_raw_os_error(code).into());
    }
    let returned = returned as usize;
    if returned > buffer.len() { return Err(io::Error::from(io::ErrorKind::InvalidData).into()); }
    Ok(returned)
}

#[cfg(windows)]
fn windows_link_target_range<W: NativeHostWork>(
    bytes: &[u8], maximum: u64, invalid_request: impl Fn() -> W::Error,
) -> Result<std::ops::Range<usize>, W::Error> {
    let malformed = || W::Error::from(io::Error::from(io::ErrorKind::InvalidData));
    let u16_at = |offset| -> Result<u16, W::Error> {
        let data = bytes.get(offset..offset + 2).ok_or_else(&malformed)?;
        Ok(u16::from_le_bytes([data[0], data[1]]))
    };
    let data = bytes.get(..4).ok_or_else(&malformed)?;
    let tag = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let header = match tag {
        0xA000_000C => 20,
        0xA000_0003 => 16,
        _ => return Err(invalid_request()),
    };
    let offset = usize::from(u16_at(8)?);
    let length = usize::from(u16_at(10)?);
    if !offset.is_multiple_of(2) || !length.is_multiple_of(2) { return Err(malformed()); }
    let mut start = header + offset;
    let end = start + length;
    let target = bytes.get(start..end).ok_or_else(&malformed)?;
    let relative = tag == 0xA000_000C && u16_at(16)? & 1 != 0;
    if !relative && target.starts_with(&[92, 0, 63, 0, 63, 0, 92, 0]) { start += 8; }
    if (end - start) as u64 > maximum { return Err(invalid_request()); }
    Ok(start..end)
}

fn prepare_source_link_buffer<W: crate::demand::SourceDemandReadWork>(
    size: u64, work: &mut W,
) -> Result<acyclic_stream::PreparedOwnedBytes<crate::demand::SourcePayloadOwnership>, DemandError> {
    let original = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?
        .materialization_work()?;
    let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
    let mut allocation = work.claim(size)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(size as usize) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(W::allocation_failed(error));
    }
    if let Err(error) = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(error);
    }
    if let Err(error) = ownership.retain(allocation, work) {
        drop(buffer);
        let _cleanup = work.release(allocation);
        return Err(error);
    }
    let copied = original.reserve_copy(allocation).map_err(crate::native_name::original_name_error)?;
    buffer.resize(buffer.capacity(), 0);
    copied.record(allocation).map_err(crate::native_name::original_name_error)?;
    acyclic_stream::PreparedOwnedBytes::new(buffer, ownership, original.runtime_meter_owner())
        .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                crate::native_name::original_name_error(original.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            | acyclic_stream::PreparedOwnedBytesError::NotUnique => DemandError::InvalidRequest,
        })
}

#[cfg(windows)]
pub(crate) fn read_source_link_held_with_work<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &Path, maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    require_leaf(name)?;
    with_wide_path_with_work(name, work, |name, work| {
        read_source_link_held_wide_with_work(parent, name, maximum_bytes, work)
    })
}

#[cfg(windows)]
fn read_source_link_held_wide_with_work<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &[u16], maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    let file = open_windows_link_with_work(parent, name, work, W::invalid_request)?;
    let size = maximum_bytes.saturating_mul(4).saturating_add(32).min(16 * 1024);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    let mut prepared = prepare_source_link_buffer(size, work)?;
    let returned = read_windows_link_record_with_work(
        &file, &mut prepared.writable()[..size as usize], work, W::invalid_request,
    )?;
    let target = windows_link_target_range::<W>(
        &prepared.writable()[..returned], maximum_bytes, W::invalid_request,
    )?;
    work.check()?;
    let bytes = prepared.freeze(returned).map_err(|failed| {
        drop(failed);
        DemandError::InvalidRequest
    })?;
    Ok(bytes.slice(target))
}

#[cfg(unix)]
pub(crate) fn read_source_link_held_with_work<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &Path, maximum_bytes: u64, work: &mut W,
) -> Result<acyclic_stream::OwnedBytes, DemandError> {
    require_leaf(name)?;
    let maximum = maximum_bytes.saturating_add(1).min(isize::MAX as u64);
    let mut size = maximum.min(256);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    unix_name_with_work(name.as_os_str(), work, |name, work| loop {
        work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
        let mut prepared = prepare_source_link_buffer(size, work)?;
        let count = read_unix_link_into_with_work(
            parent, name, &mut prepared.writable()[..size as usize], work,
        )?;
        if count < size as usize && count as u64 <= maximum_bytes {
            work.check()?;
            return prepared.freeze(count).map_err(|failed| {
                drop(failed);
                DemandError::InvalidRequest
            });
        }
        drop(prepared);
        if size == maximum { return Err(DemandError::InvalidRequest); }
        size = size.saturating_mul(2).min(maximum);
    })
}

#[cfg(unix)]
pub(crate) fn read_link_held_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, maximum: u64, work: &mut W,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
    require_leaf(name)?;
    read_unix_link_with_work(parent, name.as_os_str(), maximum, work, || {
        io::Error::new(io::ErrorKind::InvalidInput, "native link exceeds its bounded buffer").into()
    })
}

#[cfg(unix)]
fn read_unix_link_with_work<W: NativeHostWork>(
    parent: &Dir, name: &OsStr, maximum_bytes: u64, work: &mut W,
    invalid_request: impl Fn() -> W::Error,
) -> Result<(Vec<u8>, W::Allocation), W::Error> {
    let maximum = maximum_bytes.saturating_add(1).min(isize::MAX as u64);
    let mut size = maximum.min(256);
    work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
    // Keep one terminated spelling through retries/growth, then release it
    // before constructing the exact owned result.
    let (buffer, buffer_allocation, count) = unix_name_with_work(name, work, |name, work| loop {
        work.admit(WorkCounters { source_bytes_read: size, ..WorkCounters::UNCHARGED })?;
        let allocation = work.claim(size)?;
        let mut buffer = Vec::new();
        if let Err(error) = buffer.try_reserve_exact(size as usize) {
            work.release(allocation)?;
            return Err(W::allocation_failed(error));
        }
        buffer.resize(size as usize, 0u8);
        let result = read_unix_link_into_with_work(parent, name, &mut buffer, work);
        let count = match result {
            Ok(count) => count,
            Err(error) => {
                drop(buffer);
                work.release(allocation)?;
                return Err(error);
            }
        };
        if count < buffer.len() && count as u64 <= maximum_bytes {
            return Ok((buffer, allocation, count));
        }
        drop(buffer);
        work.release(allocation)?;
        if size == maximum { return Err(invalid_request()); }
        size = size.saturating_mul(2).min(maximum);
    })?;
    let result = (|| {
        let copied = WorkCounters { bytes_copied: count as u64, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let allocation = work.claim(count as u64)?;
        let mut exact = Vec::new();
        if let Err(error) = exact.try_reserve_exact(count) {
            work.release(allocation)?;
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, error).into());
        }
        exact.extend_from_slice(&buffer[..count]);
        if let Err(error) = work.record(copied).and_then(|()| work.check()) {
            drop(exact);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((exact, allocation))
    })();
    drop(buffer);
    work.release(buffer_allocation)?;
    result
}

#[cfg(unix)]
fn read_unix_link_into_with_work<W: NativeHostWork>(
    parent: &Dir, name: &std::ffi::CStr, buffer: &mut [u8], work: &mut W,
) -> Result<usize, W::Error> {
    use std::os::fd::AsRawFd as _;
    let count = loop {
        work.admit(WorkCounters { source_bytes_read: buffer.len() as u64, ..WorkCounters::UNCHARGED })?;
        native_read_with_work(work)?;
        // SAFETY: held parent, terminated name and initialized output are live.
        let count = unsafe { libc::readlinkat(parent.as_raw_fd(), name.as_ptr(),
            buffer.as_mut_ptr().cast(), buffer.len()) };
        if count >= 0 { break count as usize; }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
    };
    work.record(WorkCounters { source_bytes_read: count as u64, ..WorkCounters::UNCHARGED })?;
    Ok(count)
}

pub(crate) fn root_path_identity_measured(path: &Path, work: &mut DemandWork) -> Result<crate::NativeRootIdentity, DemandError> {
    root_path_identity_with_work(path, work)
}

pub(crate) fn root_path_identity_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    #[cfg(unix)]
    {
        let stat = stat_unix_fd_at(libc::AT_FDCWD, path.as_os_str(), false, work)?;
        if !stat.is_dir() { return Err(io::Error::new(io::ErrorKind::NotADirectory, "native root is not a directory").into()); }
        Ok(crate::NativeRootIdentity { device: stat.dev(), object: stat.ino() })
    }
    #[cfg(windows)]
    {
        with_root_path_name_with_work(path, false, work, |name, work| root_path_identity_wide_with_work(name, work))
    }
}

fn admit_native_bytes(work: &mut DemandWork, bytes: u64) -> Result<(), DemandError> {
    work.check()?;
    work.work.admit(&WorkCounters { source_bytes_read: bytes, ..WorkCounters::UNCHARGED }, &work.budget)?;
    Ok(())
}

#[cfg(unix)]
fn unix_name<T>(
    name: &OsStr,
    work: &mut DemandWork,
    operation: impl FnOnce(&std::ffi::CStr, &mut DemandWork) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    use std::os::unix::ffi::OsStrExt as _;
    let bytes = name.as_bytes();
    for byte in bytes {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *byte == 0 { return Err(DemandError::InvalidRequest); }
    }
    let owned = (bytes.len() as u64).checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let capacity = usize::try_from(owned).map_err(|_| crate::WorkError::Overflow)?;
    native_buffer(work, owned, bytes.len() as u64, 0)?;
    let mut terminated = Vec::with_capacity(capacity);
    terminated.extend_from_slice(bytes);
    terminated.push(0);
    // SAFETY: the scan excludes interior NULs and the exact buffer ends in one NUL.
    let name = unsafe { std::ffi::CString::from_vec_with_nul_unchecked(terminated) };
    let result = operation(&name, work);
    drop(name);
    work.release(owned)?;
    result
}

fn clone_directory_with<E: From<io::Error>>(
    directory: &Dir, mut before: impl FnMut() -> Result<(), E>,
) -> Result<Dir, E> {
    #[cfg(unix)]
    {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        loop {
            before()?;
            // SAFETY: the held descriptor remains live and fcntl returns a new owner.
            let fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
            if fd >= 0 { return Ok(Dir::from_std_file(unsafe { File::from_raw_fd(fd) })); }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
        use windows::Win32::Foundation::{DuplicateHandle, HANDLE, DUPLICATE_SAME_ACCESS};
        use windows::Win32::System::Threading::GetCurrentProcess;
        before()?;
        let mut duplicated = HANDLE::default();
        // SAFETY: both handles belong to this process and the output receives a new owner.
        unsafe {
            let process = GetCurrentProcess();
            DuplicateHandle(process, HANDLE(directory.as_raw_handle()), process, &mut duplicated, 0, false, DUPLICATE_SAME_ACCESS)
                .map_err(|error| E::from(io::Error::from_raw_os_error(error.code().0 & 0xffff)))?;
            Ok(Dir::from_std_file(File::from_raw_handle(duplicated.0)))
        }
    }
}

#[cfg(unix)]
fn open_unix_named_with<E: From<io::Error>>(
    directory: &Dir, name: &std::ffi::CStr, flags: i32,
    before: impl FnMut() -> Result<(), E>,
) -> Result<File, E> {
    open_unix_named_with_mode(directory, name, flags, 0o666, before)
}

#[cfg(unix)]
fn open_unix_named_with_mode<E: From<io::Error>>(
    directory: &Dir, name: &std::ffi::CStr, mut flags: i32, mode: libc::mode_t,
    mut before: impl FnMut() -> Result<(), E>,
) -> Result<File, E> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    loop {
        before()?;
        // SAFETY: the held parent and terminated single-component name are live.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags, mode) };
        if fd >= 0 {
            // SAFETY: openat returned a new owned descriptor.
            return Ok(unsafe { File::from_raw_fd(fd) });
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted { continue; }
        #[cfg(target_os = "linux")]
        if error.kind() == io::ErrorKind::PermissionDenied && flags & libc::O_NOATIME != 0 {
            flags &= !libc::O_NOATIME;
            continue;
        }
        return Err(error.into());
    }
}

#[cfg(unix)]
fn open_unix_at<W: NativeHostWork>(
    directory: &Dir, name: &OsStr, flags: i32, work: &mut W,
) -> Result<cap_std::fs::File, W::Error> {
    unix_name_with_work(name, work, |name, work| {
        open_unix_named_with(directory, name, flags, || native_read_with_work(work)).map(cap_std::fs::File::from_std)
    })
}

#[cfg(unix)]
fn stat_unix_at<W: NativeHostWork>(directory: &Dir, name: &OsStr, work: &mut W) -> Result<HostStat, W::Error> {
    use std::os::fd::AsRawFd as _;
    stat_unix_fd_at(directory.as_raw_fd(), name, false, work)
}

#[cfg(unix)]
pub(crate) fn stat_leaf_with_work<W: NativeHostWork>(
    directory: &Dir, name: &OsStr, work: &mut W,
) -> Result<HostStat, W::Error> {
    use std::os::fd::AsRawFd as _;
    require_leaf(Path::new(name))?;
    stat_unix_fd_at(directory.as_raw_fd(), name, false, work)
}

#[cfg(unix)]
fn stat_unix_fd_at<W: NativeHostWork>(fd: i32, name: &OsStr, empty_path: bool, work: &mut W) -> Result<HostStat, W::Error> {
    #[cfg(target_os = "linux")]
    let directory = if fd == libc::AT_FDCWD { rustix::fs::CWD } else {
        // SAFETY: callers pass a live held descriptor, except the explicit CWD
        // sentinel above, supplied by rustix itself.
        unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
    };
    #[cfg(target_os = "linux")]
    let flags = rustix::fs::AtFlags::SYMLINK_NOFOLLOW
        | if empty_path { rustix::fs::AtFlags::EMPTY_PATH } else { rustix::fs::AtFlags::empty() };
    unix_name_with_work(name, work, |name, work| {
        #[cfg(target_os = "linux")]
        loop {
            native_read_with_work(work)?;
            match rustix::fs::statx(directory, name, flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME) {
                Ok(stat) => return Ok(HostStat {
                    mode: u32::from(stat.stx_mode), len: stat.stx_size,
                    dev: rustix::fs::makedev(stat.stx_dev_major, stat.stx_dev_minor),
                    ino: stat.stx_ino, nlink: u64::from(stat.stx_nlink), uid: stat.stx_uid, gid: stat.stx_gid,
                    rdev: rustix::fs::makedev(stat.stx_rdev_major, stat.stx_rdev_minor),
                    atime: stat.stx_atime.tv_sec, atime_nsec: i64::from(stat.stx_atime.tv_nsec),
                    mtime: stat.stx_mtime.tv_sec, mtime_nsec: i64::from(stat.stx_mtime.tv_nsec),
                    ctime: stat.stx_ctime.tv_sec, ctime_nsec: i64::from(stat.stx_ctime.tv_nsec),
                    created: if stat.stx_mask & rustix::fs::StatxFlags::BTIME.bits() != 0 {
                        unix_time(stat.stx_btime.tv_sec, i64::from(stat.stx_btime.tv_nsec)).ok()
                    } else { None },
                }),
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
        loop {
            native_read_with_work(work)?;
            #[cfg(target_os = "linux")]
            let result = if empty_path { rustix::fs::fstat(directory) }
                else { rustix::fs::statat(directory, name, flags) };
            #[cfg(target_os = "macos")]
            let result = {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                let result = unsafe {
                    if empty_path { libc::fstat(fd, stat.as_mut_ptr()) }
                    else { libc::fstatat(fd, name.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) }
                };
                if result == 0 { Ok(unsafe { stat.assume_init() }) }
                else { Err(io::Error::last_os_error()) }
            };
            match result {
                Ok(stat) => return Ok(HostStat::from_stat(stat)?),
                #[cfg(target_os = "linux")]
                Err(rustix::io::Errno::INTR) => {}
                #[cfg(target_os = "macos")]
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
    })
}

pub(crate) fn stat_file_with_work<W: NativeHostWork>(
    file: &File, work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        stat_unix_fd_at(file.as_raw_fd(), OsStr::new(""), true, work)
    }
    #[cfg(windows)]
    {
        let (identity, _) = file_identity_and_length_with_work(file, work)?;
        let volume = u32::try_from(identity.device)
            .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
        stat_windows_file_measured(file, volume, work)
    }
}

impl HostStatReader {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (self.buffer.len() * std::mem::size_of::<u64>()) as u64
    }
}

/// Opens a fresh enumeration cursor relative to an already held capability.
/// The caller retains the allocation token for the reader's entire lifetime.
pub(crate) fn read_dir_held_with_work<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<(HostStatReader, W::Allocation), W::Error> {
    #[cfg(unix)]
    {
        let directory = Dir::from_std_file(open_unix_named_with(directory, c".",
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY
                | { #[cfg(target_os = "linux")] { libc::O_NOATIME } #[cfg(not(target_os = "linux"))] { 0 } },
            || native_read_with_work(work))?);
        let (buffer, allocation) = directory_buffer_with_work(work)?;
        Ok((HostStatReader {
            directory, buffer, next: 0, filled: 0, exhausted: false,
            #[cfg(target_os = "macos")]
            base: 0,
        }, allocation))
    }
    #[cfg(windows)]
    {
        let volume = u32::try_from(directory_identity_with_work(directory, work)?.device)
            .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
        read_dir_held_with_volume_with_work(directory, volume, work)
    }
}

fn directory_buffer_with_work<W: NativeHostWork>(
    work: &mut W,
) -> Result<(Vec<u64>, W::Allocation), W::Error> {
    let mut allocation = work.claim(HostStatReader::BUFFER_BYTES as u64)?;
    let mut buffer = Vec::new();
    let reserved = buffer.try_reserve_exact(HostStatReader::BUFFER_BYTES / std::mem::size_of::<u64>());
    let reconciled = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64 * 8);
    if let Err(error) = reserved.map_err(W::allocation_failed).and(reconciled) {
        drop(buffer);
        work.release(allocation)?;
        return Err(error);
    }
    buffer.resize(HostStatReader::BUFFER_BYTES / std::mem::size_of::<u64>(), 0);
    Ok((buffer, allocation))
}

#[cfg(windows)]
fn read_dir_held_with_volume_with_work<W: NativeHostWork>(
    directory: &Dir, volume_serial_number: u32, work: &mut W,
) -> Result<(HostStatReader, W::Allocation), W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{
        NtCreateFile, FILE_OPEN, FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows::Win32::Foundation::{HANDLE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::{
        FILE_GENERIC_READ, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_SHARE_DELETE,
    };
    // DuplicateHandle shares enumeration state; opening "." creates a fresh
    // file object without resolving the original root path again.
    let dot = [46_u16];
    let name = UNICODE_STRING { Length: 2, MaximumLength: 2, Buffer: windows::core::PWSTR(dot.as_ptr().cast_mut()) };
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<windows::Wdk::Foundation::OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const name, Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let file = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        // SAFETY: the held parent, literal UTF-16 name and all output storage
        // remain live for the synchronous relative directory open.
        let status = unsafe { NtCreateFile(&raw mut handle, FILE_GENERIC_READ, &raw const attributes,
            &raw mut block, None, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN, FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, None, 0) };
        if status.is_err() { return Err(status_error(status)); }
        // SAFETY: successful open transferred ownership of this handle.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })?;
    let directory = Dir::from_std_file(file);
    let (buffer, allocation) = directory_buffer_with_work(work)?;
    Ok((HostStatReader { directory, volume_serial_number, buffer, next: None, exhausted: false }, allocation))
}

#[cfg(unix)]
impl HostStatReader {
    const BUFFER_BYTES: usize = 64 * 1024;

    pub(crate) fn next_measured(&mut self, work: &mut DemandWork) -> Result<Option<HostListedEntry>, DemandError> {
        self.next_with_work(work).map(|entry| entry.map(|(entry, _live_bytes)| entry))
    }

    pub(crate) fn next_with_work<W: NativeHostWork>(
        &mut self, work: &mut W,
    ) -> Result<Option<(HostListedEntry, W::Allocation)>, W::Error> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
        loop {
            work.check()?;
            if self.next == self.filled {
                if self.exhausted { return Ok(None); }
                // Metadata transfer is bounded by the owned directory buffer;
                // source_bytes_read counts BODY bytes, not directory controls.
                native_read_with_work(work)?;
                // SAFETY: the descriptor and aligned owned buffer are live;
                // neither platform call writes more than its specified length.
                #[cfg(target_os = "linux")]
                let count = unsafe {
                    libc::syscall(libc::SYS_getdents64, self.directory.as_raw_fd(),
                        self.buffer.as_mut_ptr(), Self::BUFFER_BYTES)
                } as isize;
                #[cfg(target_os = "macos")]
                let count = unsafe {
                    unsafe extern "C" {
                        fn getdirentries64(fd: libc::c_int, buf: *mut libc::c_char, size: libc::size_t,
                            base: *mut libc::off_t) -> libc::ssize_t;
                    }
                    getdirentries64(self.directory.as_raw_fd(), self.buffer.as_mut_ptr().cast(),
                        Self::BUFFER_BYTES, &raw mut self.base)
                };
                if count < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted { continue; }
                    return Err(error.into());
                }
                self.next = 0;
                self.filled = usize::try_from(count).map_err(|_| io::Error::other("negative directory refill size"))?;
                if self.filled == 0 { self.exhausted = true; return Ok(None); }
            }
            // Raw records (including dots) are control items. Canonical page
            // traversal owns source_entries_visited, including replay.
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(item)?;
            work.record(item)?;
            let bytes = unsafe {
                std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled)
            };
            #[cfg(target_os = "linux")]
            let (length_offset, name_offset) = (16, 19);
            #[cfg(target_os = "macos")]
            let (length_offset, name_offset) = (16, 21);
            let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
            let header = bytes.get(self.next..self.next + name_offset).ok_or_else(malformed)?;
            let length = usize::from(u16::from_ne_bytes([header[length_offset], header[length_offset + 1]]));
            if length <= name_offset { return Err(malformed().into()); }
            let end = self.next.checked_add(length).ok_or_else(malformed)?;
            let tail = bytes.get(self.next + name_offset..end).ok_or_else(malformed)?;
            let name_length = tail.iter().position(|byte| *byte == 0).ok_or_else(malformed)?;
            let name = &tail[..name_length];
            if name == b"." || name == b".." { self.next = end; continue; }
            if name.is_empty() || name.contains(&b'/') { return Err(malformed().into()); }
            let delta = WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let mut allocation = work.claim(name.len() as u64)?;
            let mut copied = Vec::new();
            let reserved = copied.try_reserve_exact(name.len());
            let reconciled = work.reconcile_allocation(&mut allocation, copied.capacity() as u64);
            if let Err(error) = reserved.map_err(W::allocation_failed).and(reconciled) {
                drop(copied);
                work.release(allocation)?;
                return Err(error);
            }
            copied.extend_from_slice(name);
            let name = OsString::from_vec(copied);
            let stat = match (|| {
                work.record(delta)?;
                stat_leaf_with_work(&self.directory, OsStr::from_bytes(name.as_bytes()), work)
            })() {
                Ok(stat) => Some(stat),
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => None,
                Err(error) => {
                    drop(name);
                    work.release(allocation)?;
                    if W::io_error_kind(&error).is_some() { self.next = end; }
                    return Err(error);
                }
            };
            self.next = end;
            return Ok(Some((HostListedEntry { name, stat }, allocation)));
        }
    }
}

/// Projects lossless private-state names through the same held directory/stat
/// engine. Windows uses the existing opaque std native-name decode boundary;
/// its logical capacity is funded/reconciled, not fabricated allocator calls.
pub(crate) fn next_private_state_entry_with_work(
    reader: &mut HostStatReader, directory: &Dir,
    original: &crate::materializer::NativeMaterializationWork,
) -> Result<Option<(HostListedEntry, crate::materializer::NativeAllocation)>, crate::materializer::NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        let Some(mut entry) = reader.next_with_work(&mut &*original)? else { return Ok(None); };
        if entry.0.stat.is_none() {
            entry.0.stat = Some(stat_leaf_with_work(directory, &entry.0.name, &mut &*original)?);
        }
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(Some(entry))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let Some(wide) = reader.next_wide_with_work(&mut &*original)? else { return Ok(None); };
        let stat = match wide.0.stat {
            Some(stat) => stat,
            None => stat_windows_leaf_with_work(directory, &wide.0.name, &mut &*original)?,
        };
        let logical = native_environment_encoded_length(&wide.0.name, original)?;
        let encoded = WorkCounters { bytes_encoded: logical as u64, ..WorkCounters::UNCHARGED };
        original.admit(encoded)?;
        let mut allocation = original.retain_owned(logical as u64)?;
        let name = OsString::from_wide(&wide.0.name);
        let reconciled = allocation.reconcile_capacity(name.capacity() as u64);
        let recorded = original.record_admitted(encoded);
        reconciled?;
        recorded?;
        drop(wide);
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(Some((HostListedEntry { name, stat: Some(stat) }, allocation)))
    }
}

impl HostRoot {
    /// Creates only relative directories beneath this already-held state root.
    pub(crate) fn create_directories_for_state_with_work<W: NativeHostWork>(
        &self, relative: &Path, work: &mut W,
    ) -> Result<(), W::Error> {
        create_directories_held_with_work(&self.directory, relative, work).map(|_| ())
    }

    pub(crate) fn validate_private_state_directory_with_work<W: NativeHostWork>(
        &self, relative: &Path, work: &mut W,
    ) -> Result<(), W::Error> {
        let directory = self.open_dir_held_with_work(relative, work)?;
        validate_private_state_descriptor_with_work(&directory, true, work)
    }

    pub(crate) fn open_dir_held_measured(&self, path: &Path, work: &mut DemandWork) -> Result<Dir, DemandError> {
        self.open_dir_held_with_work(path, work)
    }

    pub(crate) fn open_dir_held_with_work<W: NativeHostWork>(&self, path: &Path, work: &mut W) -> Result<Dir, W::Error> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.open_dir_held_wide_with_work(path, work)) }
        #[cfg(unix)]
        {
            let mut current = clone_directory_with(&self.directory, || native_read_with_work(work))?;
            for component in path.components() {
                let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                work.admit(item)?;
                work.record(item)?;
                let std::path::Component::Normal(name) = component else { return Err(W::invalid_request()); };
                let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW;
                #[cfg(target_os = "linux")]
                let flags = flags | libc::O_NOATIME;
                current = Dir::from_std_file(open_unix_at(&current, name, flags, work)?.into_std());
            }
            Ok(current)
        }
    }

    pub(crate) fn open_dir_materialization_measured(
        &self, path: &Path, work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Dir, crate::materializer::NativeTreeMaterializationError> {
        let mut current = clone_directory_with(&self.directory, || {
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
        })?;
        for component in path.components() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let std::path::Component::Normal(name) = component else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid held directory component").into());
            };
            #[cfg(unix)]
            let next = {
                use std::os::unix::ffi::OsStrExt as _;
                let bytes = name.as_bytes();
                let _allocation = work.claim(bytes.len() as u64 + 1)?;
                work.charge(WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED })?;
                let name = std::ffi::CString::new(bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "native name contains NUL"))?;
                let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
                    | { #[cfg(target_os = "linux")] { libc::O_NOATIME } #[cfg(not(target_os = "linux"))] { 0 } };
                open_unix_named_with(&current, &name, flags, || {
                    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
                })?
            };
            #[cfg(windows)]
            let next = {
                use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
                use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
                open_windows_materialization_at(&current, Path::new(name), FILE_GENERIC_READ,
                    FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work)?
            };
            let next = Dir::from_std_file(next);
            #[cfg(windows)]
            directory_identity_materialization_measured(&next, work)?;
            current = next;
        }
        Ok(current)
    }

    pub(crate) fn read_dir_stats_measured(&self, path: &Path, work: &mut DemandWork) -> Result<HostStatReader, DemandError> {
        self.read_dir_stats_with_work(path, work).map(|(reader, _allocation)| reader)
    }

    pub(crate) fn read_dir_stats_with_work<W: NativeHostWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<(HostStatReader, W::Allocation), W::Error> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.read_dir_stats_wide_with_work(path, work)) }
        #[cfg(unix)]
        {
            let directory = self.open_dir_held_with_work(path, work)?;
            read_dir_held_with_work(&directory, work)
        }
    }

    pub(crate) fn stat_measured(&self, path: &Path, work: &mut DemandWork) -> Result<HostStat, DemandError> {
        self.stat_with_work(path, work)
    }

    pub(crate) fn stat_with_work<W: NativeHostWork>(&self, path: &Path, work: &mut W) -> Result<HostStat, W::Error> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.stat_wide_with_work(path, work)) }
        #[cfg(unix)]
        {
            work.check()?;
            if path.as_os_str().is_empty() { return stat_unix_at(&self.directory, OsStr::new("."), work); }
            let leaf = path.file_name().ok_or_else(W::invalid_request)?;
            let parent = self.open_dir_held_with_work(path.parent().ok_or_else(W::invalid_request)?, work)?;
            stat_unix_at(&parent, leaf, work)
        }
    }

    pub(crate) fn open_file_measured(&self, path: &Path, work: &mut DemandWork) -> Result<cap_std::fs::File, DemandError> {
        self.open_file_with_work(path, work)
    }

    pub(crate) fn open_file_with_work<W: NativeHostWork>(&self, path: &Path, work: &mut W) -> Result<cap_std::fs::File, W::Error> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.open_file_wide_for_with_work(path, FileReads::Cursor, work)) }
        #[cfg(unix)]
        {
            work.check()?;
            let leaf = path.file_name().ok_or_else(W::invalid_request)?;
            let parent = self.open_dir_held_with_work(path.parent().ok_or_else(W::invalid_request)?, work)?;
            // A type-changing race must not block opening a FIFO or acquire a
            // controlling terminal before the caller validates the held inode.
            let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY;
            #[cfg(target_os = "linux")]
            let flags = flags | libc::O_NOATIME;
            open_unix_at(&parent, leaf, flags, work)
        }
    }

    pub(crate) fn open_file_positional_measured(&self, path: &Path, work: &mut DemandWork) -> Result<cap_std::fs::File, DemandError> {
        self.open_file_positional_with_work(path, work)
    }

    pub(crate) fn open_file_positional_with_work<W: NativeHostWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<cap_std::fs::File, W::Error> {
        #[cfg(windows)]
        { with_wide_path_with_work(path, work, |path, work| self.open_file_positional_wide_with_work(path, work)) }
        #[cfg(unix)]
        { self.open_file_with_work(path, work) }
    }

    #[cfg(unix)]
    pub(crate) fn stat_file_measured(&self, file: &File, work: &mut DemandWork) -> Result<HostStat, DemandError> {
        self.stat_file_with_work(file, work)
    }

    pub(crate) fn stat_file_with_work<W: NativeHostWork>(&self, file: &File, work: &mut W) -> Result<HostStat, W::Error> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            stat_unix_fd_at(file.as_raw_fd(), OsStr::new(""), true, work)
        }
        #[cfg(windows)]
        { stat_windows_file_measured(file, self.volume_serial_number()?, work) }
    }

    #[cfg(unix)]
    pub(crate) fn read_link_measured(&self, path: &Path, maximum_bytes: u64, work: &mut DemandWork) -> Result<std::path::PathBuf, DemandError> {
        let (target, _) = self.read_link_with_work(path, maximum_bytes, work)?;
        use std::os::unix::ffi::OsStringExt as _;
        Ok(OsString::from_vec(target).into())
    }

    #[cfg(unix)]
    pub(crate) fn read_link_with_work<W: NativeHostWork>(
        &self, path: &Path, maximum_bytes: u64, work: &mut W,
    ) -> Result<(Vec<u8>, W::Allocation), W::Error> {
        let leaf = path.file_name().ok_or_else(W::invalid_request)?;
        let parent = self.open_dir_held_with_work(path.parent().ok_or_else(W::invalid_request)?, work)?;
        read_unix_link_with_work(&parent, leaf, maximum_bytes, work, W::invalid_request)
    }

    #[cfg(unix)]
    pub(crate) fn read_source_link_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, path: &Path, maximum_bytes: u64, work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let leaf = path.file_name().ok_or(DemandError::InvalidRequest)?;
        let parent = self.open_dir_held_with_work(
            path.parent().ok_or(DemandError::InvalidRequest)?, work,
        )?;
        read_source_link_held_with_work(&parent, Path::new(leaf), maximum_bytes, work)
    }
}

impl HostStat {
    pub(crate) fn identity(&self) -> Result<crate::NativeRootIdentity, DemandError> {
        #[cfg(unix)]
        { Ok(crate::NativeRootIdentity { device: self.dev(), object: self.ino() }) }
        #[cfg(windows)]
        { Ok(crate::NativeRootIdentity { device: u64::from(self.volume_serial_number()), object: self.file_index() }) }
    }
}

fn held_root_identity(directory: &Dir) -> io::Result<crate::NativeRootIdentity> {
    query_held_root_identity(directory, || io::Error::new(io::ErrorKind::InvalidInput, "host root is not a real directory"))
}

fn query_held_root_identity<E: From<io::Error>>(
    directory: &Dir, invalid_root: impl FnOnce() -> E,
) -> Result<NativeRootIdentity, E> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: live held descriptor and correctly sized output.
        if unsafe { libc::fstat(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(invalid_root());
        }
        Ok(crate::NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        };
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        unsafe { GetFileInformationByHandle(HANDLE(directory.as_handle().as_raw_handle()), &raw mut information) }
            .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
        if information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0
            || information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(invalid_root());
        }
        Ok(crate::NativeRootIdentity { device: u64::from(information.dwVolumeSerialNumber),
            object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow) })
    }
}

#[cfg(unix)]
fn open_unix_root_with<E: From<io::Error>>(name: &std::ffi::CStr, mut before: impl FnMut() -> Result<(), E>) -> Result<File, E> {
    use std::os::fd::FromRawFd as _;
    let mut flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    #[cfg(target_os = "linux")]
    { flags |= libc::O_NOATIME; }
    loop {
        before()?;
        // SAFETY: live terminated ambient name, newly owned descriptor on success.
        let fd = unsafe { libc::open(name.as_ptr(), flags, 0) };
        if fd >= 0 { return Ok(unsafe { File::from_raw_fd(fd) }); }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted { continue; }
        #[cfg(target_os = "linux")]
        if error.kind() == io::ErrorKind::PermissionDenied && flags & libc::O_NOATIME != 0 {
            flags &= !libc::O_NOATIME;
            continue;
        }
        return Err(error.into());
    }
}

#[cfg(windows)]
fn open_windows_root_name(name: &[u16]) -> io::Result<File> {
    open_windows_root_name_with_access(name, windows::Win32::Storage::FileSystem::FILE_GENERIC_READ)
}

#[cfg(windows)]
fn open_windows_root_name_with_access(
    name: &[u16], access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_SHARE_DELETE,
        OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };
    // SAFETY: caller supplies a terminated name, handle returned is newly owned.
    let handle = unsafe { CreateFileW(windows::core::PCWSTR(name.as_ptr()), access.0,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, None, OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, None) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

#[cfg(windows)]
fn with_normalized_windows_root_name<W: NativeHostWork, T>(
    name: &[u16], force_absolute: bool, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use windows::Win32::Storage::FileSystem::GetFullPathNameW;
    const VERBATIM: &[u16] = &[92, 92, 63, 92];
    const NT: &[u16] = &[92, 63, 63, 92];
    const UNC: &[u16] = &[92, 92, 63, 92, 85, 78, 67, 92];
    const MAX_UNITS: usize = 32768;
    let short_absolute = name.len() < 248 && matches!(name,
        [drive, 58, 0] | [drive, 58, 92 | 47, ..] if *drive != 92 && *drive != 47)
        || name.len() < 248 && matches!(name, [92 | 47, 92 | 47, ..]);
    if name.starts_with(VERBATIM) || name.starts_with(NT)
        || !force_absolute && (short_absolute || name == [0]) {
        return operation(name, work);
    }
    let input = if name == [0] { &[46u16, 0][..] } else { name };
    // GetFullPathNameW only normalizes process/path controls; it neither opens
    // nor verifies a filesystem entry. Meter each real control attempt as work,
    // not application BODY bytes or an invented backend filesystem read.
    let step = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    let mut requested = native_attempt(work, step, || {
        let required = unsafe { GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()), None, None) };
        if required == 0 { return Err(io::Error::last_os_error()); }
        Ok(required as usize)
    })?;
    let mut operation = Some(operation);
    loop {
        if requested > MAX_UNITS { return Err(io::Error::from_raw_os_error(206).into()); }
        // Leave eight units ahead of the native output for the longest prefix.
        // Prefixing then needs no hidden Vec growth or second full-path copy.
        let capacity = requested.checked_add(8).ok_or(crate::WorkError::Overflow)?;
        let mut allocation = work.claim(capacity as u64 * 2)?;
        let mut buffer = Vec::new();
        let result = (|| {
            let reserved = buffer.try_reserve_exact(capacity);
            let reconciled = work.reconcile_allocation(&mut allocation, buffer.capacity() as u64 * 2);
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            buffer.resize(capacity, 0u16);
            let returned = native_attempt(work, step, || {
                let count = unsafe { GetFullPathNameW(windows::core::PCWSTR(input.as_ptr()),
                    Some(&mut buffer[8..]), None) };
                if count == 0 { return Err(io::Error::last_os_error()); }
                Ok(count as usize)
            })?;
            if returned >= requested { return Ok(Err(returned)); }
            let absolute = &buffer[8..8 + returned];
            let (prefix, skip) = match absolute {
                [_, 58, 92, ..] => (VERBATIM, 0),
                [92, 92, 46, 92, ..] => (VERBATIM, 4),
                [92, 92, 63, 92, ..] | [92, 63, 63, 92, ..] => (&[][..], 0),
                [92, 92, ..] => (UNC, 2),
                _ => (&[][..], 0),
            };
            let begin = 8 + skip - prefix.len();
            let copied = WorkCounters { bytes_copied: prefix.len() as u64 * 2, ..WorkCounters::UNCHARGED };
            work.admit(copied)?;
            buffer[begin..begin + prefix.len()].copy_from_slice(prefix);
            work.record(copied)?;
            operation.take().expect("normalization callback runs only after a complete name")(&buffer[begin..8 + returned + 1], work).map(Ok)
        })();
        drop(buffer);
        work.release(allocation)?;
        match result? {
            Ok(value) => return Ok(value),
            Err(required) if required > MAX_UNITS || requested == MAX_UNITS => return Err(io::Error::from_raw_os_error(206).into()),
            Err(required) => requested = requested.saturating_mul(2).max(required).min(MAX_UNITS),
        }
    }
}

#[cfg(windows)]
fn with_root_path_name_with_work<W: NativeHostWork, T>(
    path: &Path, force_absolute: bool, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let (name, allocation) = encode_root_path_name_with_work(path, work)?;
    let result = with_normalized_windows_root_name(&name, force_absolute, work, operation);
    drop(name);
    work.release(allocation)?;
    result
}

#[cfg(windows)]
pub(crate) fn encode_root_path_name_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let units = wide_units_with(path.as_os_str(), || {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)
    })?.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let encoded = WorkCounters { bytes_encoded: (bytes - 2) as u64, ..WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut name = Vec::new();
    let result = (|| {
        let reserved = name.try_reserve_exact(units);
        let reconciled = work.reconcile_allocation(&mut allocation, name.capacity() as u64 * 2);
        reserved.map_err(W::allocation_failed).and(reconciled)?;
        name.extend(path.as_os_str().encode_wide());
        work.record(encoded)?;
        if name.contains(&0) { return Err(io::Error::new(io::ErrorKind::InvalidInput, "host path contains NUL").into()); }
        name.push(0);
        Ok(())
    })();
    if let Err(error) = result {
        drop(name);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((name, allocation))
}

/// Returns the absolute native root spelling including its final NUL. Keeping
/// this exact owned encoding lets later root proofs borrow it without growth.
#[cfg(windows)]
pub(crate) fn normalize_root_wide_measured(path: &Path, work: &mut DemandWork) -> Result<Vec<u16>, DemandError> {
    normalize_root_wide_with_work(path, work).map(|(name, _allocation)| name)
}

/// Returns the owned spelling and its live allocation lease. Release the lease
/// only after dropping the spelling.
#[cfg(windows)]
pub(crate) fn normalize_root_wide_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let (name, live_bytes) = encode_root_path_name_with_work(path, work)?;
    let short_absolute = path.is_absolute() && name.len() < 248;
    if short_absolute || name.starts_with(&[92, 92, 63, 92]) || name.starts_with(&[92, 63, 63, 92]) {
        return Ok((name, live_bytes));
    }
    let result = normalize_root_wide_name_allocated_with_work(&name, path.is_absolute(), work);
    drop(name);
    if let Err(error) = work.release(live_bytes) {
        if let Ok((output, allocation)) = result {
            drop(output);
            work.release(allocation)?;
        }
        return Err(error);
    }
    result
}

/// Normalizes an already-owned raw UTF-16 input without re-encoding it.
/// The caller keeps input custody and owns the returned exact native capacity.
#[cfg(windows)]
pub(crate) fn normalize_root_wide_name_with_work<W: NativeHostWork<Allocation = u64, Error = DemandError>>(
    name: &[u16], absolute: bool, work: &mut W,
) -> Result<Vec<u16>, DemandError> {
    normalize_root_wide_name_allocated_with_work(name, absolute, work)
        .map(|(output, _allocation)| output)
}

#[cfg(windows)]
fn normalize_root_wide_name_allocated_with_work<W: NativeHostWork>(
    name: &[u16], absolute: bool, work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let mut owned = None;
    let result = with_normalized_windows_root_name(name, !absolute, work, |name, work| {
        let bytes = name.len() as u64 * 2;
        let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let mut allocation = work.claim(bytes)?;
        let mut output = Vec::new();
        let copied_result = (|| {
            let reserved = output.try_reserve_exact(name.len());
            let reconciled = work.reconcile_allocation(&mut allocation, output.capacity() as u64 * 2);
            reserved.map_err(W::allocation_failed).and(reconciled)?;
            output.extend_from_slice(name);
            work.record(copied)
        })();
        if let Err(error) = copied_result {
            drop(output);
            work.release(allocation)?;
            return Err(error);
        }
        owned = Some((output, allocation));
        Ok(())
    });
    if let Err(error) = result {
        if let Some((output, allocation)) = owned {
            drop(output);
            work.release(allocation)?;
        }
        return Err(error);
    }
    Ok(owned.expect("normalization callback owns the completed native name"))
}

#[cfg(windows)]
fn check_root_wide_name<W: NativeHostWork>(name: &[u16], work: &mut W) -> Result<(), W::Error> {
    work.check()?;
    let Some((last, body)) = name.split_last() else { return Err(W::invalid_request()); };
    if *last != 0 { return Err(W::invalid_request()); }
    for unit in body {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        work.record(delta)?;
        if *unit == 0 { return Err(W::invalid_request()); }
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn root_path_identity_wide_measured(name: &[u16], work: &mut DemandWork) -> Result<NativeRootIdentity, DemandError> {
    root_path_identity_wide_with_work(name, work)
}

#[cfg(windows)]
pub(crate) fn root_path_identity_wide_with_work<W: NativeHostWork>(
    name: &[u16], work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    check_root_wide_name(name, work)?;
    let file = native_attempt(work,
        WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
        || open_windows_root_name_with_access(name, windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES))?;
    let directory = Dir::from_std_file(file);
    directory_identity_with_work(&directory, work)
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn open_root_wide_measured<W: NativeHostWork>(name: &[u16], work: &mut W) -> Result<Self, W::Error> {
        check_root_wide_name(name, work)?;
        let file = native_attempt(work,
            WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
            || open_windows_root_name(name))?;
        let directory = Dir::from_std_file(file);
        let identity = directory_identity_with_work(&directory, work)?;
        Ok(Self::from_held_dir_with_identity(directory, identity))
    }
}

pub(crate) fn directory_identity_materialization_measured(
    directory: &Dir, work: &crate::materializer::NativeMaterializationWork,
) -> Result<NativeRootIdentity, crate::materializer::NativeTreeMaterializationError> {
    directory_identity_with_work(directory, &mut &*work)
}

pub(crate) fn directory_identity_with_work<W: NativeHostWork>(
    directory: &Dir, work: &mut W,
) -> Result<NativeRootIdentity, W::Error> {
    native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
        || held_root_identity(directory))
}

/// Observes physical Unix ancestry through held parent descriptors, not a
/// spelling that may have been replaced or reached through a lexical alias.
#[cfg(unix)]
pub(crate) fn directory_has_ancestor_with_work<W: NativeHostWork>(
    directory: &Dir, ancestor: NativeRootIdentity, work: &mut W,
) -> Result<bool, W::Error> {
    let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    work.admit(examined)?;
    work.record(examined)?;
    let mut identity = loop {
        match directory_identity_with_work(directory, work) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => break result?,
        }
    };
    if identity == ancestor { return Ok(true); }
    let mut current = None::<Dir>;
    loop {
        work.admit(examined)?;
        work.record(examined)?;
        let child = current.as_ref().unwrap_or(directory);
        let parent = Dir::from_std_file(open_unix_named_with(child, c"..",
            cwd_metadata_flags(), || native_read_with_work(work))?);
        let parent_identity = loop {
            match directory_identity_with_work(&parent, work) {
                Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
                result => break result?,
            }
        };
        if parent_identity == ancestor { return Ok(true); }
        if parent_identity == identity { return Ok(false); }
        identity = parent_identity;
        current = Some(parent);
    }
}

pub(crate) fn file_identity_and_length_with_work<W: NativeHostWork>(
    file: &File, work: &mut W,
) -> Result<(NativeRootIdentity, u64), W::Error> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        loop {
            let result = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
                    return Ok(Err(io::Error::last_os_error()));
                }
                let stat = unsafe { stat.assume_init() };
                let length = u64::try_from(stat.st_size)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative file length"));
                Ok(length.map(|length| (NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 }, length)))
            })?;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
        native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            unsafe { GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &raw mut information) }
                .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
            Ok((NativeRootIdentity { device: u64::from(information.dwVolumeSerialNumber),
                object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow) },
                (u64::from(information.nFileSizeHigh) << 32) | u64::from(information.nFileSizeLow)))
        })
    }
}

pub(crate) fn entry_identity_with_work<W: NativeHostWork>(
    parent: &Dir, name: &Path, work: &mut W,
) -> Result<Option<NativeRootIdentity>, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        unix_name_with_work(name.as_os_str(), work, |name, work| loop {
            let result = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
                let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                // SAFETY: held parent, terminated leaf, and correctly sized
                // output stay live; the entry itself is never followed.
                if unsafe { libc::fstatat(parent.as_raw_fd(), name.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
                    return Ok(Err(io::Error::last_os_error()));
                }
                let stat = unsafe { stat.assume_init() };
                Ok(Ok(NativeRootIdentity { device: stat.st_dev as u64, object: stat.st_ino as u64 }))
            })?;
            match result {
                Ok(identity) => return Ok(Some(identity)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        })
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let file = match open_windows_held_with_work(parent, name, FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work) {
            Ok(file) => file,
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        file_identity_and_length_with_work(&file, work).map(|(identity, _)| Some(identity))
    }
}

pub(crate) fn open_root_directory_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<Dir, W::Error> {
    open_root_directory_identity_with_work(path, work).map(|(directory, _)| directory)
}

pub(crate) fn open_root_directory_identity_with_work<W: NativeHostWork>(
    path: &Path, work: &mut W,
) -> Result<(Dir, NativeRootIdentity), W::Error> {
    let cleanup = work.reserve_cleanup_write()?;
    // Keep successful open custody outside temporary-name helpers so a later
    // allocation-release error still reaches the reserved physical close.
    let mut opened = None;
    #[cfg(unix)]
    let result = unix_name_with_work(path.as_os_str(), work, |name, work| {
        opened = Some(open_unix_root_with(name, || {
            let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED };
            work.started_io(delta)
        })?);
        Ok(())
    });
    #[cfg(windows)]
    let result = with_root_path_name_with_work(path, false, work, |name, work| {
        opened = Some(native_attempt(work,
            WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED },
            || open_windows_root_name(name))?);
        Ok(())
    });
    if let Err(error) = result {
        if let Some(file) = opened {
            let ((), recorded) = work.attempt_cleanup_write(cleanup, || drop(file));
            recorded?;
        } else {
            work.cancel_cleanup_write(cleanup);
        }
        return Err(error);
    }
    let directory = Dir::from_std_file(opened.expect("successful native root open retains its file"));
    match directory_identity_with_work(&directory, work) {
        Ok(identity) => {
            work.cancel_cleanup_write(cleanup);
            Ok((directory, identity))
        }
        Err(error) => {
            let ((), recorded) = work.attempt_cleanup_write(cleanup, || drop(directory));
            recorded?;
            Err(error)
        }
    }
}

impl HostRoot {
    pub(crate) fn from_held_dir(directory: Dir) -> io::Result<Self> {
        let identity = held_root_identity(&directory)?;
        Ok(Self::from_held_dir_with_identity(directory, identity))
    }

    /// Binds a directory whose kind, no-reparse authority, and identity have
    /// already been verified by the native constructor retaining this handle.
    pub(crate) fn from_held_dir_with_identity(directory: Dir, identity: crate::NativeRootIdentity) -> Self {
        Self { directory, identity }
    }

    pub(crate) fn open_measured(path: &Path, work: &mut DemandWork) -> Result<Self, DemandError> {
        Self::open_with_work(path, work)
    }

    pub(crate) fn open_with_work<W: NativeHostWork>(path: &Path, work: &mut W) -> Result<Self, W::Error> {
        let (directory, identity) = open_root_directory_identity_with_work(path, work)?;
        Ok(Self::from_held_dir_with_identity(directory, identity))
    }

    pub(crate) fn open_materialization_measured(path: &Path, work: &crate::materializer::NativeMaterializationWork)
        -> Result<Self, crate::materializer::NativeTreeMaterializationError> {
        let (directory, identity) = open_root_directory_identity_with_work(path, &mut &*work)?;
        Ok(Self::from_held_dir_with_identity(directory, identity))
    }
}

impl HostRoot {
    /// Opens an existing real directory without following the final path.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[must_use]
    pub const fn identity(&self) -> crate::NativeRootIdentity {
        self.identity
    }

    pub(crate) fn held_identity_with_work<W: NativeHostWork>(
        &self, work: &mut W,
    ) -> Result<crate::NativeRootIdentity, W::Error> {
        directory_identity_with_work(&self.directory, work)
    }

    /// Whether this root's filesystem is local. A network or user-space
    /// filesystem can stall a request indefinitely, so only local host I/O
    /// may block a native callback thread that must answer within its
    /// timeout. A filesystem that cannot be classified counts as remote.
    pub(crate) fn is_local(&self) -> bool {
        filesystem_is_local(&self.directory)
    }

    pub(crate) fn is_local_measured(&self, work: &mut DemandWork) -> Result<bool, DemandError> {
        self.is_local_with_work(work)
    }

    pub(crate) fn is_local_with_work<W: NativeHostWork>(&self, work: &mut W) -> Result<bool, W::Error> {
        filesystem_is_local_measured(&self.directory, work)
    }

    /// The held root directory's handle, for volume queries that must be
    /// bound to exactly this root.
    #[cfg(windows)]
    pub(crate) fn directory_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        use std::os::windows::io::AsHandle as _;
        self.directory.as_handle()
    }

    /// The held root directory's descriptor, for host queries that must be
    /// bound to exactly this root.
    #[cfg(unix)]
    pub(crate) fn directory_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd as _;
        self.directory.as_raw_fd()
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.directory.entries()?.next().is_none())
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        if path.as_os_str().is_empty() {
            self.directory.entries()
        } else {
            self.directory.read_dir(path)
        }
    }

    /// Enumerates the directory at `path`, reached without following any
    /// link, with each entry's facts as [`Self::stat`] reports them, or with
    /// `None` where only a separate stat of that name can report them.
    pub fn read_dir_stats(&self, path: &Path) -> io::Result<HostStatReader> {
        self.read_dir_stats_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Enumerates through held directory capabilities without following any
    /// intermediate symlink or reparse point.
    pub fn open_dir_held(&self, path: &Path) -> io::Result<Dir> {
        self.open_dir_held_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    pub(crate) fn scan_held_dir(directory: &Dir) -> io::Result<HostReadDir<'_>> {
        #[cfg(target_os = "linux")]
        {
            let entries = rustix::fs::Dir::read_from(directory)?;
            Ok(HostReadDir::Linux {
                entries,
                held: directory,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(HostReadDir::Native(
                directory.entries()?,
                std::marker::PhantomData,
            ))
        }
    }

    pub fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        if path.as_os_str().is_empty() {
            self.directory.dir_metadata()
        } else {
            self.directory.symlink_metadata(path)
        }
    }

    /// Stats `path` without following its final link, as
    /// [`Self::symlink_metadata`] does.
    #[cfg(not(windows))]
    pub fn stat(&self, path: &Path) -> io::Result<HostStat> {
        self.stat_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Stats `path` without following its final link, as
    /// [`Self::symlink_metadata`] does. A path free of reparse points is
    /// answered by one kernel query against the held root, without opening
    /// a handle per component; any reparse point on it takes the held walk.
    #[cfg(windows)]
    pub fn stat(&self, path: &Path) -> io::Result<HostStat> {
        self.stat_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// One `FileStatInformation` query naming `path` relative to the held
    /// root. `None` when the path names the root itself or crosses or ends
    /// in any reparse point, which only the held walk may resolve.
    #[cfg(all(windows, test))]
    fn stat_by_name(&self, path: &Path) -> io::Result<Option<HostStat>> {
        stat_at(&self.directory, path, self.volume_serial_number()?)
    }

    /// One `FileStatInformation` query naming `path` relative to the held
    /// root, which no reparse point may redirect; its object's own reparse
    /// tag is reported, not resolved. `None` when the path names the root
    /// itself or redirection was refused, which only the held walk may
    /// resolve. A name the query answers lies on the root's volume.
    #[cfg(all(windows, feature = "native-mount"))]
    pub(crate) fn stat_information_by_name(
        &self,
        path: &Path,
    ) -> io::Result<Option<windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION>> {
        stat_information_at(&self.directory, path)
    }

    /// The volume every name resolved without a reparse point lies on.
    #[cfg(windows)]
    fn volume_serial_number(&self) -> io::Result<u32> {
        u32::try_from(self.identity.device)
            .map_err(|_| io::Error::other("the root's volume has no serial number"))
    }

    /// Stats one file this root opened, exactly as [`Self::stat`] stats its
    /// name: the same query reports the same facts for the same object.
    #[cfg(not(windows))]
    pub fn stat_file(&self, file: &File) -> io::Result<HostStat> {
        self.stat_file_measured(file, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Stats one file this root opened, exactly as [`Self::stat`] stats its
    /// name: one file-record query, including the leaf's reparse tag.
    #[cfg(windows)]
    pub fn stat_file(&self, file: &File) -> io::Result<HostStat> {
        self.stat_file_measured(file, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Reads leaf metadata while refusing every intermediate link or reparse point.
    /// On Windows a path free of reparse points is opened by name in one call
    /// instead of a handle per component, with the same refusal.
    pub fn symlink_metadata_held(&self, path: &Path) -> io::Result<Metadata> {
        if path.as_os_str().is_empty() {
            return self.directory.dir_metadata();
        }
        #[cfg(windows)]
        {
            use windows::Wdk::Storage::FileSystem::{
                FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
            };
            use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
            if let Some(file) = self.open_by_name(
                path,
                FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            )? {
                return Metadata::from_file(&file.into_std());
            }
        }
        let mut current = self.directory.try_clone()?;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            let std::path::Component::Normal(name) = component else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid metadata path",
                ));
            };
            if components.peek().is_none() {
                return current.symlink_metadata(name);
            }
            current = current.open_dir_nofollow(name)?;
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "metadata path has no leaf",
        ))
    }

    pub fn open_file(&self, path: &Path) -> io::Result<cap_std::fs::File> {
        self.open_file_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Opens `path` for reading so that it gives way to whoever renames or
    /// deletes it: see [`YieldingFile`].
    #[cfg(windows)]
    pub fn open_file_yielding(&self, path: &Path) -> io::Result<YieldingFile> {
        self.open_file_yielding_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Opens `path` for positional reads only, as [`Self::open_file`] does
    /// otherwise. On Windows the handle is overlapped: it has no cursor, and
    /// the native runtime serves its positional I/O in place on a thread
    /// that admits inline blocking instead of reopening the file per call.
    pub fn open_file_positional(&self, path: &Path) -> io::Result<cap_std::fs::File> {
        self.open_file_positional_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }


    /// Opens `path` for reading with one `NtCreateFile` relative to the held
    /// root, which no reparse point may redirect; a final reparse point is
    /// opened as itself, as the held walk opens it. `None` when the path
    /// names the root itself or redirection was refused, which only the held
    /// walk may resolve.
    #[cfg(all(windows, test))]
    fn open_file_by_name(
        &self,
        path: &Path,
        reads: FileReads,
    ) -> io::Result<Option<cap_std::fs::File>> {
        use windows::Wdk::Storage::FileSystem::{
            FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
            NTCREATEFILE_CREATE_OPTIONS,
        };
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;

        self.open_by_name(
            path,
            FILE_GENERIC_READ,
            FILE_NON_DIRECTORY_FILE
                | FILE_OPEN_REPARSE_POINT
                | match reads {
                    FileReads::Cursor => FILE_SYNCHRONOUS_IO_NONALERT,
                    FileReads::Positional => NTCREATEFILE_CREATE_OPTIONS(0),
                },
        )
    }

    /// Opens `path` with `access` and `options` by one `NtCreateFile`
    /// relative to the held root, which no reparse point may redirect.
    /// `None` when the path names the root itself or redirection was
    /// refused, which only the held walk may resolve.
    #[cfg(windows)]
    fn open_by_name(
        &self,
        path: &Path,
        access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
        options: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS,
    ) -> io::Result<Option<cap_std::fs::File>> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
        use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
        use windows::Wdk::Storage::FileSystem::{FILE_OPEN, NtCreateFile};
        use windows::Win32::Foundation::{
            HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING,
        };
        use windows::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        use windows::Win32::System::IO::IO_STATUS_BLOCK;
        const REPARSE_POINT_ENCOUNTERED: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());

        let Some((mut name, length)) = relative_kernel_name(path) else {
            return Ok(None);
        };
        let name = UNICODE_STRING {
            Length: length,
            MaximumLength: length,
            Buffer: windows::core::PWSTR(name.as_mut_ptr()),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: u32::try_from(std::mem::size_of::<OBJECT_ATTRIBUTES>())
                .map_err(|_| io::Error::other("object attributes size"))?,
            RootDirectory: HANDLE(self.directory.as_handle().as_raw_handle()),
            ObjectName: &raw const name,
            Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
            ..OBJECT_ATTRIBUTES::default()
        };
        let mut status_block = IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        // SAFETY: every pointer names a live, correctly sized value for this
        // synchronous call, and the held root handle outlives it.
        let status = unsafe {
            NtCreateFile(
                &raw mut handle,
                access,
                &raw const attributes,
                &raw mut status_block,
                None,
                FILE_ATTRIBUTE_NORMAL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN,
                options,
                None,
                0,
            )
        };
        if status == REPARSE_POINT_ENCOUNTERED {
            return Ok(None);
        }
        if status.is_err() {
            return Err(status_error(status));
        }
        // SAFETY: the call succeeded, so `handle` is a new handle this
        // function exclusively owns.
        let file = unsafe { File::from_raw_handle(handle.0) };
        Ok(Some(cap_std::fs::File::from_std(file)))
    }

    pub fn create_file(&self, path: &Path) -> io::Result<File> {
        self.create_file_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Creates a capability-rooted Windows file ready for native overlapped I/O.
    /// The returned handle has not been associated with a completion port.
    #[cfg(windows)]
    pub fn create_overlapped_file(&self, path: &Path) -> io::Result<File> {
        self.create_overlapped_file_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    pub fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.create_dir_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Opens or creates each real directory component without following links.
    pub fn create_dir_all_held(&self, path: &Path) -> io::Result<HostDirectory> {
        self.create_dir_all_held_measured(path, &mut ordinary_work()).map_err(ordinary_error)
    }

    pub fn hard_link(&self, source: &Path, destination: &Path) -> io::Result<()> {
        self.hard_link_measured(source, destination, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Creates an APFS copy-on-write clone when the hosting volume supports it.
    /// The source and destination remain rooted in held directory capabilities.
    #[cfg(target_os = "macos")]
    pub fn clone_file(&self, source: &Path, destination: &Path) -> io::Result<bool> {
        self.clone_file_from(self, source, destination)
    }

    /// Clones one regular file from a held source root into this held destination.
    /// This does not seal a multi-file source view; the caller must do that.
    #[cfg(target_os = "macos")]
    pub fn clone_file_from(
        &self, source_root: &HostRoot, source: &Path, destination: &Path,
    ) -> io::Result<bool> {
        self.clone_file_from_measured(source_root, source, destination, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn clone_file_measured<W: crate::demand::SourceDemandReadWork>(&self, source: &Path, destination: &Path, work: &mut W) -> Result<bool, DemandError> {
        self.clone_file_from_measured(self, source, destination, work)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn clone_file_from_measured<W: crate::demand::SourceDemandReadWork>(
        &self, source_root: &HostRoot, source: &Path, destination: &Path, work: &mut W,
    ) -> Result<bool, DemandError> {
        use std::os::fd::AsRawFd as _;
        let source = match source_root.open_file_with_work(source, work) {
            Ok(source) => source,
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied => return Ok(false),
            Err(error) => return Err(error),
        };
        let stat = stat_unix_fd_at(source.as_raw_fd(), OsStr::new(""), true, work)?;
        if !stat.file_type().is_file() { return Err(DemandError::NotRegularFile); }
        with_unix_parent_leaf(&self.directory, destination, work, |parent, leaf, work| {
            let result = unix_mutation(work, || {
                // SAFETY: source inode, destination parent and terminated leaf are live.
                unsafe { libc::fclonefileat(source.as_raw_fd(), parent.as_raw_fd(), leaf.as_ptr(), 0) }
            });
            match result {
                Ok(()) => Ok(true),
                Err(DemandError::Io(error)) if matches!(error.raw_os_error(), Some(libc::ENOTSUP | libc::ENOSYS | libc::EXDEV)) => Ok(false),
                Err(error) => Err(error),
            }
        })
    }

    /// Creates a same-volume Windows copy-on-write clone when the filesystem
    /// accepts the complete file as one block-clone range. An unsupported or
    /// unaligned request leaves no destination so callers can write normally.
    #[cfg(windows)]
    pub fn clone_file(&self, source: &Path, destination: &Path) -> io::Result<bool> {
        self.clone_file_from(self, source, destination)
    }

    /// Clones a regular file from a held source root into this root when the
    /// destination filesystem supports block cloning. Neither root is sealed
    /// here; callers must keep the source immutable for the operation.
    #[cfg(windows)]
    pub fn clone_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<bool> {
        self.clone_file_from_identity(source_root, source, destination, None)
    }

    #[cfg(windows)]
    fn clone_file_from_identity(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
        expected: Option<crate::NativeRootIdentity>,
    ) -> io::Result<bool> {
        let mut work = ordinary_work();
        with_wide_path(source, &mut work, |source, work| {
            with_wide_path(destination, work, |destination, work| {
                self.clone_file_from_identity_wide_measured(source_root, source, destination, expected, work)
            })
        }).map_err(ordinary_error)
    }

    /// Copies one pinned regular source into a new file using `ReFS` block
    /// cloning when available, then owned-buffer overlapped I/O otherwise.
    /// The caller must keep the source immutable; the copy fails rather than
    /// replace an existing destination. This byte primitive does not capture
    /// SDK lineage or preserve multi-file hard-link topology.
    ///
    /// The destination name only ever holds the complete copy: bytes are
    /// written under a [`StagedWindowsFile`] beside it and linked into place
    /// once complete. If copying ends before publication, the kernel removes
    /// the staged name and the destination stays absent. A cancelled caller
    /// can race with a completed publication, so it may observe either an
    /// absent destination or the complete copy; it can never observe partial
    /// contents. Process death likewise leaves no staged name behind.
    #[cfg(windows)]
    pub async fn copy_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<()> {
        let source_root = HostRoot {
            directory: source_root.directory.try_clone()?,
            identity: source_root.identity,
        };
        let (parent, name) = open_windows_parent(&self.directory, destination)?;
        let name = name.to_os_string();
        let source = source.to_path_buf();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let context = crate::obs::caller_context();
        // The detached owner drains admitted kernel I/O before it closes the
        // staged file, even when its awaiting caller is cancelled.
        tokio::spawn(async move {
            let result = crate::obs::in_span(&context, async {
                let staged = std::sync::Arc::new(
                    acyclic_native_runtime::run_blocking_io(move || {
                        StagedWindowsFile::create(parent)
                    })
                    .await??,
                );
                copy_windows_file_worker(&source_root, &source, &staged, &sender).await?;
                if sender.is_closed() {
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
                }
                acyclic_native_runtime::run_blocking_io(move || {
                    let result = staged.publish(&name);
                    drop(staged);
                    result
                })
                .await?
            })
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| io::Error::other("native copy owner stopped before completion"))?
    }

    pub fn read_link(&self, path: &Path) -> io::Result<std::path::PathBuf> {
        #[cfg(unix)]
        { self.read_link_measured(path, u64::MAX, &mut ordinary_work()).map_err(ordinary_error) }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt as _;
            let target = with_wide_path(path, &mut ordinary_work(), |path, work| self.read_link_wide_measured(path, u64::MAX, work))
                .map_err(ordinary_error)?;
            Ok(OsString::from_wide(&target).into())
        }
    }

    pub fn set_permissions(&self, path: &Path, permissions: Permissions) -> io::Result<()> {
        self.directory.set_permissions(
            if path.as_os_str().is_empty() {
                Path::new(".")
            } else {
                path
            },
            permissions,
        )
    }

    /// Pins one ordinary macOS inode before an offloaded metadata mutation.
    #[cfg(target_os = "macos")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_macos_metadata_target(
        &self,
        path: &Path,
    ) -> Result<MacMetadataTarget, MacMetadataError> {
        let mut work = ordinary_work();
        let result = (|| {
            let target = self.open_macos_metadata_target_measured(path, &mut work)?;
            work.release(target.logical_owned_bytes())?;
            Ok(target)
        })();
        result.map_err(|error| match error {
            DemandError::MacMetadata(error) => error,
            error => MacMetadataError::Io(ordinary_error(error)),
        })
    }

    #[cfg(target_os = "macos")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_macos_metadata_target_measured<W: crate::demand::SourceDemandReadWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<MacMetadataTarget, DemandError> {
        open_macos_metadata_target_measured(&self.directory, path, work)
    }

    /// Binds metadata restoration to the current inode before deferred I/O.
    /// A later rename or path replacement cannot redirect the mutation.
    #[cfg(target_os = "linux")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_linux_metadata_target(
        &self,
        path: &Path,
    ) -> Result<LinuxMetadataTarget, LinuxMetadataError> {
        self.open_linux_metadata_target_measured(path, &mut ordinary_work()).map_err(|error| match error {
            DemandError::LinuxMetadata(error) => error,
            error => LinuxMetadataError::Io(ordinary_error(error)),
        })
    }

    #[cfg(target_os = "linux")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_linux_metadata_target_measured<W: crate::demand::SourceDemandReadWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<LinuxMetadataTarget, DemandError> {
        LinuxMetadataTarget::open_measured(&self.directory, path, work)
    }

    /// Pins the exact Windows leaf before metadata restoration is deferred.
    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target(
        &self,
        path: &Path,
    ) -> Result<WindowsMetadataTarget, WindowsMetadataError> {
        self.open_windows_metadata_target_measured(path, &mut ordinary_work()).map_err(|error| match error {
            DemandError::WindowsMetadata(error) => error,
            error => WindowsMetadataError::Io(ordinary_error(error)),
        })
    }

    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target_measured<W: crate::demand::SourceDemandReadWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<WindowsMetadataTarget, DemandError> {
        with_wide_path_with_work(path, work, |path, work| self.open_windows_metadata_target_wide_measured(path, work))
    }

    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target_wide_measured<W: crate::demand::SourceDemandReadWork>(
        &self, path: &[u16], work: &mut W,
    ) -> Result<WindowsMetadataTarget, DemandError> {
        WindowsMetadataTarget::open_wide_measured(&self.directory, path, work)
    }

    #[cfg(unix)]
    pub fn symlink(&self, target: &OsStr, destination: &Path) -> io::Result<()> {
        self.symlink_measured(target, destination, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[cfg(windows)]
    pub fn symlink_file(&self, target: &Path, destination: &Path) -> io::Result<()> {
        let mut work = ordinary_work();
        with_wide_path(target, &mut work, |target, work| {
            with_wide_path(destination, work, |destination, work| self.symlink_file_wide_measured(target, destination, work))
        }).map_err(ordinary_error)
    }

    /// Applies a permission mask without opening the target node.
    ///
    /// Special files must never be chmodded through an open file
    /// description: opening a FIFO blocks until a peer appears.
    #[cfg(unix)]
    pub fn set_permissions_without_open(&self, path: &Path, mode: u32) -> io::Result<()> {
        self.set_permissions_without_open_measured(path, mode, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn create_fifo_held(&self, path: &Path, mode: u32) -> io::Result<()> {
        self.create_fifo_held_measured(path, mode, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[cfg(unix)]
    #[cfg(any(feature = "native-mount", all(test, target_os = "macos")))]
    pub(crate) fn create_device_held(&self, path: &Path, mode: u32, device: libc::dev_t) -> io::Result<()> {
        self.create_device_held_measured(path, mode, device, &mut ordinary_work()).map_err(ordinary_error)
    }

    #[cfg(unix)]
    pub fn bind_unix_socket(&self, destination: &Path) -> io::Result<()> {
        self.bind_unix_socket_measured(destination, &mut ordinary_work()).map_err(ordinary_error)
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn open_macos_metadata_target_measured<W: crate::demand::SourceDemandReadWork>(
    root: &Dir, path: &Path, work: &mut W,
) -> Result<MacMetadataTarget, DemandError> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;

    with_unix_parent_leaf(root, path, work, |parent, leaf, work| {
        let before = macos_stat_at_measured(parent, leaf, work)?;
        let kind = before.st_mode & libc::S_IFMT;
        if !matches!(kind, libc::S_IFREG | libc::S_IFDIR | libc::S_IFIFO) {
            // Sockets and symlinks cannot safely be opened for mutation.
            let parent = clone_directory_with(parent, || native_read_with_work(work))?;
            let bytes = leaf.to_bytes();
            let copied = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
            work.allocate_for(bytes.len() as u64, copied)?;
            let leaf = Path::new(OsStr::from_bytes(bytes)).to_path_buf();
            let target = MacMetadataTarget::Noop {
                parent, leaf, device: before.st_dev as u64, inode: before.st_ino,
            };
            return Ok(target);
        }
        let file = open_unix_named_with(parent, leaf,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
                | if kind == libc::S_IFIFO { libc::O_EVTONLY } else { 0 },
            || native_read_with_work(work))?;
        let after = macos_fstat_measured(file.as_raw_fd(), work)?;
        if before.st_dev != after.st_dev || before.st_ino != after.st_ino {
            return Err(io::Error::other("metadata target changed during admission").into());
        }
        if !matches!(after.st_mode & libc::S_IFMT, libc::S_IFREG | libc::S_IFDIR | libc::S_IFIFO) {
            return Err(MacMetadataError::Unsupported("non-ordinary file kind").into());
        }
        Ok(MacMetadataTarget::Held(file))
    })
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_stat_at_measured<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, leaf: &std::ffi::CStr, work: &mut W,
) -> Result<libc::stat, DemandError> {
    use std::os::fd::AsRawFd as _;
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    native_read_with_work(work)?;
    // SAFETY: the parent, single-component name, and output remain live.
    if unsafe { libc::fstatat(parent.as_raw_fd(), leaf.as_ptr(),
        observed.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: successful fstatat initialized the complete structure.
    Ok(unsafe { observed.assume_init() })
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
impl MacMetadataTarget {
    /// Bytes retained by admission until this target is consumed or dropped.
    /// A measured caller dropping it must release these bytes on its ledger.
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Held(_) => 0,
            Self::Noop { leaf, .. } => leaf.capacity() as u64,
        }
    }

    /// Sets each extended attribute on the held inode by its exact raw
    /// name, and reads each back. A node no descriptor can hold (a socket,
    /// say) takes none: asking for any on one fails closed.
    #[cfg(feature = "native-mount")]
    pub(crate) fn set_extended_attributes(
        &self,
        attributes: &[(Vec<u8>, bytes::Bytes)],
    ) -> Result<(), MacMetadataError> {
        self.set_extended_attributes_measured(attributes, &mut ordinary_work())
            .map_err(|error| match error {
                DemandError::MacMetadata(error) => error,
                error => MacMetadataError::Io(ordinary_error(error)),
            })
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn set_extended_attributes_measured<W: crate::demand::SourceDemandReadWork>(
        &self, attributes: &[(Vec<u8>, bytes::Bytes)], work: &mut W,
    ) -> Result<(), DemandError> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::OsStrExt as _;
        work.check()?;
        if attributes.is_empty() { return Ok(()); }
        let Self::Held(file) = self else {
            return Err(MacMetadataError::Unsupported("named_attributes on a non-ordinary file kind").into());
        };
        let fd = file.as_raw_fd();
        for (name, value) in attributes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let mut entered = false;
            let result = unix_name_with_work(OsStr::from_bytes(name), work, |name, work| {
                entered = true;
                native_write(work)?;
                // SAFETY: the name and exact value slice remain live.
                if unsafe { libc::fsetxattr(fd, name.as_ptr(), value.as_ptr().cast(),
                    value.len(), 0, 0) } != 0 {
                    return Err(io::Error::last_os_error().into());
                }
                let length = value.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
                work.allocate(length as u64)?;
                let mut readback = vec![0_u8; length];
                let result = (|| {
                    work.check()?;
                    work.preflight(WorkCounters { source_bytes_read: length as u64, ..WorkCounters::UNCHARGED })?;
                    native_read_with_work(work)?;
                    // SAFETY: readback is writable for its whole length.
                    let read = unsafe { libc::fgetxattr(fd, name.as_ptr(),
                        readback.as_mut_ptr().cast(), readback.len(), 0, 0) };
                    if let Ok(read) = u64::try_from(read) {
                        work.record(WorkCounters { source_bytes_read: read, ..WorkCounters::UNCHARGED })?;
                    }
                    if usize::try_from(read).ok() != Some(value.len()) {
                        return Err(MacMetadataError::Unsupported("named_attributes readback mismatch").into());
                    }
                    for (actual, expected) in readback.iter().zip(value.iter()) {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        if actual != expected {
                            return Err(MacMetadataError::Unsupported("named_attributes readback mismatch").into());
                        }
                    }
                    Ok(())
                })();
                drop(readback);
                work.release(length as u64)?;
                result
            });
            match result {
                Err(DemandError::Io(error)) if !entered && error.kind() == io::ErrorKind::InvalidInput =>
                    return Err(MacMetadataError::Unsupported("named_attributes").into()),
                result => result?,
            }
        }
        Ok(())
    }

    /// Applies representable metadata to the held inode and reads it back.
    /// Canonical ctime requires a durable native-view baseline first.
    pub(crate) fn apply(
        self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), MacMetadataError> {
        let mut work = ordinary_work();
        work.adopt(self.logical_owned_bytes()).map_err(|error|
            MacMetadataError::Io(ordinary_error(error)))?;
        self.apply_measured(metadata, &mut work).map_err(|error| match error {
            DemandError::MacMetadata(error) => error,
            error => MacMetadataError::Io(ordinary_error(error)),
        })
    }

    /// Admits each held-inode primitive separately. A no-op target revalidates
    /// its identity without following the leaf or opening its payload.
    pub(crate) fn apply_measured<W: crate::demand::SourceDemandReadWork>(
        self, metadata: crate::kernel::FileMetadata, work: &mut W,
    ) -> Result<(), DemandError> {
        use crate::kernel::MetadataField;
        use std::os::fd::AsRawFd as _;

        let owned = self.logical_owned_bytes();
        let result = (|| {
        work.check()?;
        validate_macos_metadata_fields(metadata)?;
        let file = match self {
            Self::Held(file) => file,
            Self::Noop {
                parent,
                leaf,
                device,
                inode,
            } => {
                let result = (|| {
                    if metadata != crate::kernel::FileMetadata::default() {
                        return Err(MacMetadataError::Unsupported("non-ordinary file kind").into());
                    }
                    let observed = unix_name_with_work(leaf.as_os_str(), work, |leaf, work|
                        macos_stat_at_measured(&parent, leaf, work))?;
                    if observed.st_dev as u64 != device || observed.st_ino != inode {
                        return Err(io::Error::other("metadata target changed during no-op").into());
                    }
                    Ok(())
                })();
                drop(leaf);
                return result;
            }
        };
        let fd = file.as_raw_fd();
        let current = macos_fstat_measured(fd, work)?;
        if let MetadataField::Value(mode) = metadata.posix_mode
            && mode & u32::from(libc::S_IFMT) != 0
            && mode & u32::from(libc::S_IFMT) != u32::from(current.st_mode & libc::S_IFMT)
        {
            return Err(MacMetadataError::Unsupported("posix_mode file kind").into());
        }
        let uid = match metadata.posix_uid {
            MetadataField::Value(value) => value,
            MetadataField::Unavailable => current.st_uid,
        };
        let gid = match metadata.posix_gid {
            MetadataField::Value(value) => value,
            MetadataField::Unavailable => current.st_gid,
        };
        if uid != current.st_uid || gid != current.st_gid {
            native_write(work)?;
            // SAFETY: fd pins the admitted inode; no later path lookup occurs.
            if unsafe { libc::fchown(fd, uid, gid) } != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        if let MetadataField::Value(mode) = metadata.posix_mode {
            // Ownership changes may clear setuid/setgid, so mode is applied last.
            let mask = libc::mode_t::try_from(mode & 0o7777)
                .map_err(|_| MacMetadataError::Unsupported("posix_mode"))?;
            native_write(work)?;
            // SAFETY: fd pins the admitted inode; fchmod reads no memory.
            if unsafe { libc::fchmod(fd, mask) } != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        apply_macos_timestamp_metadata_measured(fd, metadata, work)?;
        let observed = macos_fstat_measured(fd, work)?;
        if macos_mismatch(metadata.posix_uid, observed.st_uid)
            || macos_mismatch(metadata.posix_gid, observed.st_gid)
            || matches!(metadata.posix_mode, MetadataField::Value(value) if u32::from(observed.st_mode) & 0o7777 != value & 0o7777)
            || matches!(metadata.posix_flags, MetadataField::Value(0) if observed.st_flags != 0)
            || matches!(metadata.created_ns, MetadataField::Value(value) if macos_nanos(observed.st_birthtime, observed.st_birthtime_nsec) != Some(value))
            || matches!(metadata.accessed_ns, MetadataField::Value(value) if macos_nanos(observed.st_atime, observed.st_atime_nsec) != Some(value))
            || matches!(metadata.modified_ns, MetadataField::Value(value) if macos_nanos(observed.st_mtime, observed.st_mtime_nsec) != Some(value))
        {
            return Err(MacMetadataError::Unsupported("metadata readback mismatch").into());
        }
        Ok(())
        })();
        work.release(owned)?;
        result
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn validate_macos_metadata_fields(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), MacMetadataError> {
    use crate::kernel::MetadataField;

    for (present, field) in [
        (
            matches!(metadata.posix_flags, MetadataField::Value(value) if value != 0),
            "posix_flags",
        ),
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.named_attributes, MetadataField::Value(_)),
            "named_attributes",
        ),
        (matches!(metadata.acl, MetadataField::Value(_)), "acl"),
        (
            matches!(metadata.security_descriptor, MetadataField::Value(_)),
            "security_descriptor",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns without native-view baseline",
        ),
    ] {
        if present {
            return Err(MacMetadataError::Unsupported(field));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn apply_macos_timestamp_metadata_measured<W: crate::demand::SourceDemandReadWork>(
    fd: libc::c_int, metadata: crate::kernel::FileMetadata, work: &mut W,
) -> Result<(), DemandError> {
    use crate::kernel::MetadataField;

    if let MetadataField::Value(value) = metadata.created_ns {
        let mut attributes = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: libc::ATTR_CMN_CRTIME,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        let mut created = macos_timespec(value);
        native_write(work)?;
        // SAFETY: both buffers remain live for the synchronous system call.
        if unsafe {
            libc::fsetattrlist(
                fd,
                (&raw mut attributes).cast(),
                (&raw mut created).cast(),
                std::mem::size_of::<libc::timespec>(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
    }
    if matches!(metadata.accessed_ns, MetadataField::Value(_))
        || matches!(metadata.modified_ns, MetadataField::Value(_))
    {
        let omitted = libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        };
        let times = [
            match metadata.accessed_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
            match metadata.modified_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
        ];
        native_write(work)?;
        // SAFETY: fd pins the inode and times is a live two-element array.
        if unsafe { libc::futimens(fd, times.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_mismatch<T: Copy + Eq>(field: crate::kernel::MetadataField<T>, observed: T) -> bool {
    matches!(field, crate::kernel::MetadataField::Value(expected) if expected != observed)
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_timespec(value: i64) -> libc::timespec {
    libc::timespec {
        tv_sec: value.div_euclid(1_000_000_000),
        tv_nsec: value.rem_euclid(1_000_000_000),
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_nanos(seconds: libc::time_t, nanoseconds: libc::c_long) -> Option<i64> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds);
    i64::try_from(nanos).ok()
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_fstat_measured<W: crate::demand::SourceDemandReadWork>(fd: std::os::fd::RawFd, work: &mut W) -> Result<libc::stat, DemandError> {
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    native_read_with_work(work)?;
    // SAFETY: fstat initializes the whole result on success.
    if unsafe { libc::fstat(fd, observed.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: fstat succeeded above, so it initialized `observed`.
    Ok(unsafe { observed.assume_init() })
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
impl WindowsMetadataTarget {
    fn open_wide_measured<W: crate::demand::SourceDemandReadWork>(directory: &Dir, path: &[u16], work: &mut W) -> Result<Self, DemandError> {
        use windows::Wdk::Storage::FileSystem::{FILE_DIRECTORY_FILE, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{
            FILE_GENERIC_READ, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE,
        };

        work.check()?;
        if path.is_empty() {
            let file = open_windows_wide_at_with_work(directory, &[u16::from(b'.')],
                FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, work, || DemandError::InvalidRequest)?
                .ok_or(DemandError::InvalidRequest)?;
            return Ok(Self { file });
        }
        let mut separator = None;
        for (index, unit) in path.iter().enumerate() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if *unit == u16::from(b'\\') { separator = Some(index); }
        }
        let (parent_path, leaf) = match separator {
            Some(index) => (&path[..index], &path[index + 1..]),
            None => (&[][..], path),
        };
        if separator.is_some() && parent_path.is_empty() {
            return Err(DemandError::InvalidRequest);
        }
        let parent = if parent_path.is_empty() { None } else {
            Some(Dir::from_std_file(open_windows_wide_at_with_work(directory, parent_path,
                FILE_GENERIC_READ, FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT, work, || DemandError::InvalidRequest)?
                .ok_or(DemandError::InvalidRequest)?.into_std()))
        };
        let parent = parent.as_ref().unwrap_or(directory);
        let file = cap_std::fs::File::from_std(open_windows_leaf_handle_with_work(parent, leaf,
            FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE, None, work)?);
        Ok(Self { file })
    }

    /// Applies representable metadata and verifies it through the pinned leaf.
    /// Fields which this provider cannot reproduce fail closed.
    pub(crate) fn apply(
        self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), WindowsMetadataError> {
        self.apply_measured(metadata, &mut ordinary_work()).map_err(|error| match error {
            DemandError::WindowsMetadata(error) => error,
            error => WindowsMetadataError::Io(ordinary_error(error)),
        })
    }

    /// Admits the held-handle query, update, and readback independently; a
    /// denied or cancelled boundary never begins the corresponding call.
    pub(crate) fn apply_measured<W: crate::demand::SourceDemandReadWork>(
        self, metadata: crate::kernel::FileMetadata, work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        reject_unsupported_windows_metadata(metadata)?;
        if metadata == crate::kernel::FileMetadata::default() {
            return Ok(());
        }
        let current = query_windows_basic_info_measured(&self.file, work)?;
        let desired = desired_windows_basic_info(metadata, current)?;
        set_windows_basic_info_measured(&self.file, desired, work)?;
        verify_windows_basic_info(metadata, desired, query_windows_basic_info_measured(&self.file, work)?)
            .map_err(Into::into)
    }
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn reject_unsupported_windows_metadata(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), WindowsMetadataError> {
    use crate::kernel::MetadataField;
    for (field, present) in [
        (
            "posix mode",
            !matches!(metadata.posix_mode, MetadataField::Unavailable),
        ),
        (
            "POSIX owner",
            !matches!(metadata.posix_uid, MetadataField::Unavailable),
        ),
        (
            "POSIX group",
            !matches!(metadata.posix_gid, MetadataField::Unavailable),
        ),
        (
            "POSIX flags",
            !matches!(metadata.posix_flags, MetadataField::Unavailable),
        ),
        (
            "named attributes",
            !matches!(metadata.named_attributes, MetadataField::Unavailable),
        ),
        ("ACL", !matches!(metadata.acl, MetadataField::Unavailable)),
        (
            "security descriptor",
            !matches!(metadata.security_descriptor, MetadataField::Unavailable),
        ),
    ] {
        if present {
            return Err(WindowsMetadataError::Unsupported(field));
        }
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn desired_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    current: FILE_BASIC_INFO,
) -> Result<FILE_BASIC_INFO, WindowsMetadataError> {
    use crate::kernel::MetadataField;
    const SETTABLE_ATTRIBUTES: u32 = 0x0000_0001 // readonly
        | 0x0000_0002 // hidden
        | 0x0000_0004 // system
        | 0x0000_0020 // archive
        | 0x0000_0080 // normal
        | 0x0000_0100 // temporary
        | 0x0000_2000; // not content indexed
    const STRUCTURAL_ATTRIBUTES: u32 = 0x0000_0010 // directory
        | 0x0000_0040 // device
        | 0x0000_0200 // sparse
        | 0x0000_0400 // reparse point
        | 0x0000_0800 // compressed
        | 0x0000_1000 // offline
        | 0x0000_4000 // encrypted
        | 0x0000_8000 // integrity stream
        | 0x0001_0000 // virtual
        | 0x0002_0000 // no scrub data
        | 0x0004_0000 // EA / recall on open
        | 0x0008_0000 // pinned
        | 0x0010_0000 // unpinned
        | 0x0040_0000; // recall on data access
    const KNOWN_ATTRIBUTES: u32 = SETTABLE_ATTRIBUTES | STRUCTURAL_ATTRIBUTES;
    let attributes = match metadata.windows_attributes {
        MetadataField::Unavailable => current.FileAttributes,
        MetadataField::Value(attributes) => {
            if attributes == 0
                || attributes & !KNOWN_ATTRIBUTES != 0
                || attributes & 0x80 != 0 && attributes != 0x80
                || attributes & STRUCTURAL_ATTRIBUTES
                    != current.FileAttributes & STRUCTURAL_ATTRIBUTES
            {
                return Err(WindowsMetadataError::Unsupported("Windows attributes"));
            }
            attributes
        }
    };
    Ok(FILE_BASIC_INFO {
        CreationTime: metadata_time(metadata.created_ns, current.CreationTime)?,
        LastAccessTime: metadata_time(metadata.accessed_ns, current.LastAccessTime)?,
        LastWriteTime: metadata_time(metadata.modified_ns, current.LastWriteTime)?,
        ChangeTime: metadata_time(metadata.changed_ns, current.ChangeTime)?,
        FileAttributes: attributes,
    })
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn verify_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    desired: FILE_BASIC_INFO,
    observed: FILE_BASIC_INFO,
) -> Result<(), WindowsMetadataError> {
    use crate::kernel::MetadataField;
    for (field, expected, actual, required) in [
        (
            "creation time",
            desired.CreationTime,
            observed.CreationTime,
            !matches!(metadata.created_ns, MetadataField::Unavailable),
        ),
        (
            "access time",
            desired.LastAccessTime,
            observed.LastAccessTime,
            !matches!(metadata.accessed_ns, MetadataField::Unavailable),
        ),
        (
            "modification time",
            desired.LastWriteTime,
            observed.LastWriteTime,
            !matches!(metadata.modified_ns, MetadataField::Unavailable),
        ),
        (
            "change time",
            desired.ChangeTime,
            observed.ChangeTime,
            !matches!(metadata.changed_ns, MetadataField::Unavailable),
        ),
    ] {
        if required && expected != actual {
            return Err(WindowsMetadataError::Unsupported(field));
        }
    }
    if matches!(metadata.windows_attributes, MetadataField::Value(_))
        && observed.FileAttributes != desired.FileAttributes
    {
        return Err(WindowsMetadataError::Unsupported("Windows attributes"));
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn query_windows_basic_info_measured<W: crate::demand::SourceDemandReadWork>(file: &cap_std::fs::File, work: &mut W) -> Result<FILE_BASIC_INFO, DemandError> {
    query_windows_basic_info_with_work(file, work)
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn set_windows_basic_info_measured<W: crate::demand::SourceDemandReadWork>(
    file: &cap_std::fs::File, information: FILE_BASIC_INFO, work: &mut W,
) -> Result<(), DemandError> {
    set_windows_basic_info_with_work(file, information, work)
}


/// Opens the directory holding `path`'s leaf without following any
/// intermediate link, and returns it with that leaf name.
#[cfg(windows)]
fn open_windows_parent<'a>(root: &Dir, path: &'a Path) -> io::Result<(Dir, &'a OsStr)> {
    let mut parent = root.try_clone()?;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid metadata path",
            ));
        };
        if components.peek().is_none() {
            return Ok((parent, name));
        }
        parent = parent.open_dir_nofollow(name)?;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "metadata path has no leaf",
    ))
}

/// A file under a private name beside its destination, for bytes not yet
/// published. The name is held by a delete-on-close handle, so the kernel
/// removes it when that handle closes for any reason, including the process
/// dying; no sweep ever looks for leftovers. [`Self::publish`] first links
/// the complete file to its final name, which then outlives the staged one.
#[cfg(windows)]
pub(crate) struct StagedWindowsFile {
    parent: Dir,
    name: std::path::PathBuf,
    /// Deletes the staged name when closed.
    guard: File,
}

#[cfg(windows)]
impl Drop for StagedWindowsFile {
    /// Unlinks the staged name at once. Delete-on-close removes a name only
    /// once every handle to the file closes, and another process (a virus
    /// scanner) may hold one for a while; a POSIX delete takes the name away
    /// now. Where the file system cannot, delete-on-close still removes it.
    fn drop(&mut self) {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{
            FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
            FILE_DISPOSITION_FLAG_ON_CLOSE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
            FILE_DISPOSITION_INFO_EX, FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfoEx,
            SetFileInformationByHandle,
        };

        let set = |flags: u32| {
            let disposition = FILE_DISPOSITION_INFO_EX {
                Flags: FILE_DISPOSITION_INFO_EX_FLAGS(flags),
            };
            // SAFETY: the guard's live handle, opened with delete access,
            // and a correctly sized disposition that outlives the call.
            unsafe {
                SetFileInformationByHandle(
                    HANDLE(self.guard.as_raw_handle()),
                    FileDispositionInfoEx,
                    (&raw const disposition).cast(),
                    u32::try_from(size_of::<FILE_DISPOSITION_INFO_EX>()).unwrap_or(u32::MAX),
                )
            }
        };
        // Delete-on-close would apply the ordinary delete at close, which
        // keeps the name while other handles last: it gives way to a POSIX
        // delete, and stays where that cannot be set.
        // A staged file may already carry its final read-only attribute.
        if set(FILE_DISPOSITION_FLAG_ON_CLOSE.0).is_ok()
            && set(FILE_DISPOSITION_FLAG_DELETE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0)
            .is_err()
        {
            let _ = set(FILE_DISPOSITION_FLAG_ON_CLOSE.0 | FILE_DISPOSITION_FLAG_DELETE.0);
        }
    }
}

#[cfg(windows)]
impl StagedWindowsFile {
    /// Creates an empty staged file under a name no other writer uses.
    pub(crate) fn create(parent: Dir) -> io::Result<Self> {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            DELETE, FILE_FLAG_DELETE_ON_CLOSE, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
        };

        let name =
            std::path::PathBuf::from(format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4().simple()));
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0)
            .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE.0);
        let guard = parent.open_with(&name, &options)?.into_std();
        Ok(Self {
            parent,
            name,
            guard,
        })
    }

    /// Opens the staged file for writing; the staged name still goes when
    /// the guard closes, however long this handle lives.
    pub(crate) fn open_writer(&self, overlapped: bool) -> io::Result<File> {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

        let mut options = OpenOptions::new();
        options.write(true);
        if overlapped {
            options.custom_flags(FILE_FLAG_OVERLAPPED.0);
        }
        self.parent
            .open_with(&self.name, &options)
            .map(cap_std::fs::File::into_std)
    }

    /// Gives the complete file its final `name` in the same directory,
    /// failing rather than replacing an existing entry. The final name is a
    /// second link made atomically, so no reader of it can observe the file
    /// before it is complete, and it is named relative to the held
    /// directory, never re-resolved by path. Closing the guard then removes
    /// only the staged name.
    pub(crate) fn publish(&self, name: &OsStr) -> io::Result<()> {
        self.link(&self.parent, name, false)
    }

    /// As [`Self::publish`], naming the file in `directory`, on the same
    /// volume, instead of beside the staged name.
    #[cfg(feature = "native-mount")]
    pub(crate) fn publish_in(&self, directory: &Dir, name: &OsStr) -> io::Result<()> {
        self.link(directory, name, false)
    }

    /// Gives the complete file its final `name` in `directory`, in place of
    /// the file there now, in one step no reader observes between: the
    /// replaced file goes with POSIX semantics, also while a handle to it
    /// stays open.
    #[cfg(feature = "native-mount")]
    pub(crate) fn publish_over_in(&self, directory: &Dir, name: &OsStr) -> io::Result<()> {
        self.link(directory, name, true)
    }

    fn link(&self, directory: &Dir, name: &OsStr, replace: bool) -> io::Result<()> {
        use std::mem::{offset_of, size_of};
        use std::os::windows::ffi::OsStrExt as _;
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Wdk::Storage::FileSystem::{
            FILE_INFORMATION_CLASS, FILE_LINK_INFORMATION, FileLinkInformation,
            NtSetInformationFile,
        };
        // `FileLinkInformationEx`, whose flags ask for replacement with POSIX
        // semantics, also of a read-only file: `FILE_LINK_REPLACE_IF_EXISTS |
        // FILE_LINK_POSIX_SEMANTICS | FILE_LINK_IGNORE_READONLY_ATTRIBUTE`.
        const FILE_LINK_INFORMATION_EX: FILE_INFORMATION_CLASS = FILE_INFORMATION_CLASS(72);
        const REPLACE_WITH_POSIX_SEMANTICS: u32 = 0x1 | 0x2 | 0x40;
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::System::IO::IO_STATUS_BLOCK;

        let name = name.encode_wide().collect::<Vec<_>>();
        let overflow = || io::Error::other("link information overflow");
        let name_bytes = name
            .len()
            .checked_mul(size_of::<u16>())
            .ok_or_else(overflow)?;
        let name_offset = offset_of!(FILE_LINK_INFORMATION, FileName);
        let total = name_offset
            .checked_add(name_bytes)
            .ok_or_else(overflow)?
            .max(size_of::<FILE_LINK_INFORMATION>());
        // u64 storage satisfies FILE_LINK_INFORMATION's alignment.
        let mut storage = vec![0_u64; total.div_ceil(size_of::<u64>())];
        let information = storage.as_mut_ptr().cast::<FILE_LINK_INFORMATION>();
        // SAFETY: `storage` spans `total` bytes, aligned for the structure, with
        // `name_bytes` after the name offset; nothing else aliases it.
        unsafe {
            if replace {
                (*information).Anonymous.Flags = REPLACE_WITH_POSIX_SEMANTICS;
            } else {
                (*information).Anonymous.ReplaceIfExists = false;
            }
            (*information).RootDirectory = HANDLE(directory.as_handle().as_raw_handle());
            (*information).FileNameLength = u32::try_from(name_bytes).map_err(|_| overflow())?;
            std::ptr::copy_nonoverlapping(
                name.as_ptr(),
                information.cast::<u8>().add(name_offset).cast::<u16>(),
                name.len(),
            );
        }
        let mut status_block = IO_STATUS_BLOCK::default();
        // SAFETY: both handles, the status block, and the initialized information
        // buffer outlive this synchronous call; the length is exactly the buffer's.
        let status = unsafe {
            NtSetInformationFile(
                HANDLE(self.guard.as_handle().as_raw_handle()),
                &raw mut status_block,
                information.cast(),
                u32::try_from(total).map_err(|_| overflow())?,
                if replace {
                    FILE_LINK_INFORMATION_EX
                } else {
                    FileLinkInformation
                },
            )
        };
        if status.is_ok() {
            return Ok(());
        }
        Err(status_error(status))
    }
}

#[cfg(windows)]
fn open_windows_regular_source(root: &Dir, path: &Path, overlapped: bool) -> io::Result<File> {
    let mut work = ordinary_work();
    let identity = directory_identity_with_work(root, &mut work).map_err(ordinary_error)?;
    let directory = clone_directory_with(root, || native_read_with_work(&mut work)).map_err(ordinary_error)?;
    let root = HostRoot::from_held_dir_with_identity(directory, identity);
    with_wide_path(path, &mut work, |path, work| {
        let reads = if overlapped { FileReads::Positional } else { FileReads::Cursor };
        let file = root.open_file_wide_for_with_work(path, reads, work)?.into_std();
        let stat = root.stat_file_with_work(&file, work)?;
        if !stat.file_type().is_file()
            || stat.file_attributes() & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(DemandError::InvalidRequest);
        }
        Ok(file)
    }).map_err(ordinary_error)
}


/// Opens a non-overlapped clone source, which must still be the `expected`
/// file when one is named.
#[cfg(windows)]
fn open_windows_clone_source(
    source_root: &HostRoot, source: &Path, expected: Option<crate::NativeRootIdentity>,
) -> io::Result<File> {
    let mut work = ordinary_work();
    with_wide_path(source, &mut work, |path, work| {
        let file = source_root.open_file_wide_measured(path, work)?.into_std();
        let stat = source_root.stat_file_measured(&file, work)?;
        if !stat.file_type().is_file() || stat.file_attributes() & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(DemandError::InvalidRequest);
        }
        if expected.is_some_and(|identity| stat.identity().ok() != Some(identity)) {
            return Err(io::Error::other("copy source identity changed").into());
        }
        Ok(file)
    }).map_err(ordinary_error)
}

/// Whether a file of `length` bytes can be block cloned at all.
#[cfg(windows)]
fn clonable_windows_length(length: u64) -> bool {
    length >= 4 * 1024 && i64::try_from(length).is_ok()
}

/// Block clones `source` into the empty `target`. `ReFS` volumes use either
/// 4-KiB or 64-KiB clusters: the common smaller unit is tried first for
/// maximum sharing, then 64 KiB when the volume requires it. `false` leaves
/// `target` empty for an ordinary copy.
#[cfg(windows)]
fn clone_windows_file_into(source: &mut File, target: &mut File, length: u64) -> io::Result<bool> {
    clone_windows_file_into_measured(source, target, length, &mut ordinary_work()).map_err(ordinary_error)
}

#[cfg(windows)]
fn clone_windows_file_into_measured<W: crate::demand::SourceDemandReadWork>(source: &File, target: &File, length: u64, work: &mut W) -> Result<bool, DemandError> {
    for alignment in [4 * 1024, 64 * 1024] {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        match clone_windows_file_measured(source, target, length, alignment, work) {
            Ok(()) => return Ok(true),
            Err(DemandError::Io(_)) => truncate_file_with_work(target, 0, work)?,
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(windows)]
fn windows_leaf_name_with_work<W: NativeHostWork>(path: &[u16], work: &mut W) -> Result<windows::Win32::Foundation::UNICODE_STRING, W::Error> {
    work.check()?;
    if path.is_empty() || path == [46] || path == [46, 46] {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into());
    }
    for unit in path {
        let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let invalid = matches!(*unit, 0 | 47 | 58 | 92);
        work.record(delta)?;
        if invalid { return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native leaf").into()); }
    }
    let bytes = path.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let length = u16::try_from(bytes).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "native leaf exceeds NT length"))?;
    Ok(windows::Win32::Foundation::UNICODE_STRING {
        Length: length, MaximumLength: length, Buffer: windows::core::PWSTR(path.as_ptr().cast_mut()),
    })
}

#[cfg(windows)]
fn create_windows_wide_at_measured<W: NativeHostWork>(
    directory: &Dir, path: &[u16], directory_file: bool, overlapped: bool,
    open_if: bool, delete_on_close: bool, work: &mut W,
) -> Result<File, W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        NtCreateFile, FILE_CREATE, FILE_OPEN_IF, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE,
        FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, FILE_DELETE_ON_CLOSE,
        NTCREATEFILE_CREATE_OPTIONS,
    };
    use windows::Win32::Foundation::{HANDLE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE};
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_ATTRIBUTE_NORMAL,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_SHARE_DELETE,
    };
    let name = windows_leaf_name_with_work(path, work)?;
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const name, Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let options = (if directory_file { FILE_DIRECTORY_FILE } else { FILE_NON_DIRECTORY_FILE })
        | FILE_OPEN_REPARSE_POINT
        | if overlapped { NTCREATEFILE_CREATE_OPTIONS(0) } else { FILE_SYNCHRONOUS_IO_NONALERT }
        | if delete_on_close { FILE_DELETE_ON_CLOSE } else { NTCREATEFILE_CREATE_OPTIONS(0) };
    let access = FILE_GENERIC_WRITE | FILE_GENERIC_READ
        | if delete_on_close { DELETE } else { windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS(0) };
    native_attempt(work, WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        // SAFETY: held parent, borrowed raw name and output storage outlive this call.
        let status = unsafe { NtCreateFile(&raw mut handle, access, &raw const attributes,
            &raw mut block, None, FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            if open_if { FILE_OPEN_IF } else { FILE_CREATE }, options, None, 0) };
        if status.is_err() { return Err(status_error(status)); }
        // SAFETY: successful creation transferred ownership of this new handle.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })
}

#[cfg(windows)]
pub(crate) fn create_windows_directory_with_work<W: NativeHostWork>(directory: &Dir, name: &[u16], work: &mut W) -> Result<File, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationFile};
    let file = create_windows_wide_at_measured(directory, name, true, false, true, false, work)?;
    let information = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut information = FILE_STAT_INFORMATION::default();
        // SAFETY: live held file and fixed-size initialized output query storage.
        let status = unsafe { NtQueryInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            &raw mut block, (&raw mut information).cast(), std::mem::size_of::<FILE_STAT_INFORMATION>() as u32, FileStatInformation) };
        if status.is_err() { return Err(status_error(status)); }
        Ok(information)
    })?;
    if information.FileAttributes & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(W::invalid_request());
    }
    Ok(file)
}

#[cfg(windows)]
fn windows_set_information_measured<T, W: NativeHostWork>(
    file: &File, information: &T,
    class: windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS, work: &mut W,
) -> Result<(), W::Error> {
    native_attempt(work, WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED },
        || windows_set_information_held(file, information, class))
}

#[cfg(windows)]
fn windows_set_information_held<T>(
    file: &File, information: &T,
    class: windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
    // SAFETY: correctly sized initialized information and held handle outlive the call.
    let status = unsafe { windows::Wdk::Storage::FileSystem::NtSetInformationFile(
        windows::Win32::Foundation::HANDLE(file.as_raw_handle()), &raw mut block,
        std::ptr::from_ref(information).cast(), std::mem::size_of::<T>() as u32, class) };
    if status.is_err() { return Err(status_error(status)); }
    Ok(())
}

#[cfg(windows)]
fn windows_keep_created_file_measured<W: crate::demand::SourceDemandReadWork>(file: &File, work: &mut W) -> Result<(), DemandError> {
    keep_windows_created_file_with_work(file, work)
}

#[cfg(windows)]
pub(crate) fn keep_windows_created_file_with_work<W: NativeHostWork>(file: &File, work: &mut W) -> Result<(), W::Error> {
    // FILE_DISPOSITION_ON_CLOSE without FILE_DISPOSITION_DELETE clears the
    // create-time FILE_DELETE_ON_CLOSE flag; ordinary DeleteFile=false does not.
    windows_set_information_measured(file, &8_u32,
        windows::Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS(64), work)
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_handle_measured<W: crate::demand::SourceDemandReadWork>(
    parent: &Dir, name: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    directory: Option<bool>, work: &mut W,
) -> Result<File, DemandError> {
    open_windows_leaf_handle_with_work(parent, name, access, directory, work)
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_handle_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16],
    access: windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS,
    directory: Option<bool>, work: &mut W,
) -> Result<File, W::Error> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{NtCreateFile, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
        FILE_SYNCHRONOUS_IO_NONALERT, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, NTCREATEFILE_CREATE_OPTIONS};
    use windows::Win32::Foundation::{HANDLE, OBJ_CASE_INSENSITIVE};
    use windows::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_SHARE_DELETE};
    let name = windows_leaf_name_with_work(name, work)?;
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<windows::Wdk::Foundation::OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(parent.as_handle().as_raw_handle()),
        ObjectName: &raw const name, Attributes: OBJ_CASE_INSENSITIVE, ..Default::default()
    };
    let options = FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT | match directory {
        Some(true) => FILE_DIRECTORY_FILE, Some(false) => FILE_NON_DIRECTORY_FILE,
        None => NTCREATEFILE_CREATE_OPTIONS(0),
    };
    native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        // SAFETY: a validated leaf is resolved, with final reparses opened rather than followed.
        let status = unsafe { NtCreateFile(&raw mut handle, access, &raw const attributes,
            &raw mut block, None, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN, options, None, 0) };
        if status.is_err() { return Err(status_error(status)); }
        // SAFETY: successful open transferred ownership of this handle.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })
}

/// Creates one new held directory and transfers its handle before any later
/// durability boundary, so a refused flush cannot lose recreation custody.
#[cfg(windows)]
pub(crate) fn create_windows_new_directory_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W,
) -> Result<File, W::Error> {
    create_windows_wide_at_measured(parent, name, true, false, false, false, work)
}

#[cfg(windows)]
pub(crate) fn create_windows_file_with_work<W: NativeHostWork>(parent: &Dir, name: &[u16], work: &mut W) -> Result<File, W::Error> {
    create_windows_wide_at_measured(parent, name, false, false, false, false, work)
}

#[cfg(windows)]
pub(crate) fn create_windows_provisional_file_with_work<W: NativeHostWork>(parent: &Dir, name: &[u16], work: &mut W) -> Result<File, W::Error> {
    create_windows_wide_at_measured(parent, name, false, false, false, true, work)
}

/// A caller-held provisional writer in the existing native creation engine.
/// Delete-on-close retains failed-start cleanup while positional I/O is pending.
#[cfg(windows)]
pub(crate) fn create_windows_provisional_overlapped_file_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], work: &mut W,
) -> Result<File, W::Error> {
    create_windows_wide_at_measured(parent, name, false, true, false, true, work)
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_with_options_measured<W: crate::demand::SourceDemandReadWork>(parent: &Dir, name: &[u16], options: &NativeOpenOptions, work: &mut W) -> Result<File, DemandError> {
    open_windows_leaf_with_options_with_work(parent, name, options, work)
}

#[cfg(windows)]
pub(crate) fn open_windows_leaf_with_options_with_work<W: NativeHostWork>(parent: &Dir, name: &[u16], options: &NativeOpenOptions, work: &mut W) -> Result<File, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationFile};
    use windows::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_ATTRIBUTE_REPARSE_POINT};
    let file = match options {
        NativeOpenOptions::Read => open_windows_leaf_handle_with_work(parent, name, FILE_GENERIC_READ, Some(false), work)?,
        NativeOpenOptions::ReadWrite => {
            use windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE;
            open_windows_leaf_handle_with_work(parent, name, FILE_GENERIC_READ | FILE_GENERIC_WRITE, Some(false), work)?
        }
        NativeOpenOptions::CreateNew => create_windows_wide_at_measured(parent, name, false, false, false, false, work)?,
        NativeOpenOptions::ReadWriteCreateNew => create_windows_wide_at_measured(parent, name, false, false, false, false, work)?,
        NativeOpenOptions::ReadWriteCreate => create_windows_wide_at_measured(parent, name, false, false, true, false, work)?,
    };
    let information = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        let mut information = FILE_STAT_INFORMATION::default();
        // SAFETY: fixed-size query output and live held regular-file handle.
        let status = unsafe { NtQueryInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            &raw mut block, (&raw mut information).cast(), std::mem::size_of::<FILE_STAT_INFORMATION>() as u32, FileStatInformation) };
        if status.is_err() { return Err(status_error(status)); }
        Ok(information)
    })?;
    if information.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(W::invalid_request());
    }
    Ok(file)
}

#[cfg(windows)]
pub(crate) fn remove_windows_leaf_measured<W: crate::demand::SourceDemandReadWork>(parent: &Dir, name: &[u16], directory: bool, work: &mut W) -> Result<(), DemandError> {
    remove_windows_leaf_with_work(parent, name, directory, work)
}

#[cfg(windows)]
pub(crate) fn remove_windows_leaf_with_work<W: NativeHostWork>(parent: &Dir, name: &[u16], directory: bool, work: &mut W) -> Result<(), W::Error> {
    remove_windows_leaf_with_identity_with_work(parent, name, directory, None, work)
}

#[cfg(windows)]
pub(crate) fn remove_windows_leaf_if_identity_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], directory: bool, expected: crate::NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error> {
    remove_windows_leaf_with_identity_with_work(parent, name, directory, Some(expected), work)
}

#[cfg(windows)]
fn remove_windows_leaf_with_identity_with_work<W: NativeHostWork>(
    parent: &Dir, name: &[u16], directory: bool, expected: Option<crate::NativeRootIdentity>, work: &mut W,
) -> Result<(), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE,
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
    use windows::Wdk::Storage::FileSystem::{FILE_DISPOSITION_INFORMATION, FileDispositionInformation};
    let file = open_windows_leaf_handle_with_work(parent, name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, Some(directory), work)?;
    if let Some(expected) = expected {
        let observed = native_attempt(work, WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }, || {
            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            // SAFETY: this is the same held DELETE handle that will receive the disposition.
            unsafe { GetFileInformationByHandle(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                &raw mut information).map_err(io::Error::from)?; }
            Ok(crate::NativeRootIdentity {
                device: u64::from(information.dwVolumeSerialNumber),
                object: u64::from(information.nFileIndexHigh) << 32 | u64::from(information.nFileIndexLow),
            })
        })?;
        if observed != expected {
            return Err(io::Error::other("held deletion target identity changed").into());
        }
    }
    windows_set_information_measured(&file, &FILE_DISPOSITION_INFORMATION { DeleteFile: true }, FileDispositionInformation, work)
}

#[cfg(windows)]
pub(crate) fn rename_windows_leaf_measured<W: crate::demand::SourceDemandReadWork>(from: &Dir, name: &[u16], to: &Dir, to_name: &[u16], work: &mut W) -> Result<(), DemandError> {
    use std::mem::{offset_of, size_of};
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{FILE_RENAME_INFORMATION, FileRenameInformation, NtSetInformationFile};
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let file = open_windows_leaf_handle_measured(from, name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, None, work)?;
    for unit in to_name {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *unit == 92 { return Err(DemandError::InvalidRequest); }
    }
    if to_name == [46] || to_name == [46, 46] { return Err(DemandError::InvalidRequest); }
    windows_wide_name_with_work(to_name, work, || DemandError::InvalidRequest, |_, work| {
        let name_bytes = to_name.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
        let offset = offset_of!(FILE_RENAME_INFORMATION, FileName);
        let total = offset.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?.max(size_of::<FILE_RENAME_INFORMATION>());
        let words = total.div_ceil(size_of::<u64>());
        let allocation = words.checked_mul(8).ok_or(crate::WorkError::Overflow)? as u64;
        native_buffer(work, allocation, name_bytes as u64, 0)?;
        let mut storage = vec![0_u64; words];
        let result = (|| {
            let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
            // SAFETY: aligned complete rename header and UTF-16 name storage.
            unsafe {
                (*information).Anonymous.ReplaceIfExists = true;
                (*information).RootDirectory = windows::Win32::Foundation::HANDLE(to.as_handle().as_raw_handle());
                (*information).FileNameLength = name_bytes as u32;
                std::ptr::copy_nonoverlapping(to_name.as_ptr(), information.cast::<u8>().add(offset).cast::<u16>(), to_name.len());
            }
            let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
            native_write(work)?;
            // SAFETY: held handles and initialized rename buffer outlive the call.
            let status = unsafe { NtSetInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                &raw mut block, information.cast(), total as u32, FileRenameInformation) };
            if status.is_err() { return Err(status_error(status).into()); }
            Ok(())
        })();
        drop(storage);
        work.release(allocation)?;
        result
    })?.ok_or(DemandError::InvalidRequest)
}

#[cfg(windows)]
impl HostRoot {
    pub(crate) fn create_overlapped_file_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &Path, work: &mut W) -> Result<File, DemandError> {
        with_wide_path_with_work(path, work, |path, work| self.create_overlapped_file_wide_measured(path, work))
    }

    pub(crate) fn create_file_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &[u16], work: &mut W) -> Result<File, DemandError> {
        self.create_windows_file_wide_measured(path, false, false, work)
    }

    pub(crate) fn create_overlapped_file_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &[u16], work: &mut W) -> Result<File, DemandError> {
        self.create_windows_file_wide_measured(path, true, false, work)
    }

    fn create_windows_file_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &[u16], overlapped: bool, temporary: bool, work: &mut W) -> Result<File, DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        create_windows_wide_at_measured(&parent, leaf, false, overlapped, false, temporary, work)
    }

    pub(crate) fn create_dir_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &[u16], work: &mut W) -> Result<(), DemandError> {
        let (parent, leaf) = wide_parent_leaf(path)?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        create_windows_wide_at_measured(&parent, leaf, true, false, false, false, work)?;
        Ok(())
    }

    pub(crate) fn create_dir_all_held_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, path: &[u16], work: &mut W) -> Result<HostDirectory, DemandError> {
        let mut current = clone_directory_with(&self.directory, || native_read_with_work(work))?;
        if path.is_empty() { return Ok(HostDirectory { directory: current }); }
        for component in path.split(|unit| *unit == 92) {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if component.is_empty() || component == [46] || component == [46, 46] {
                return Err(DemandError::InvalidRequest);
            }
            let file = create_windows_directory_with_work(&current, component, work)?;
            current = Dir::from_std_file(file);
        }
        Ok(HostDirectory { directory: current })
    }

    pub(crate) fn hard_link_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, source: &[u16], destination: &[u16], work: &mut W) -> Result<(), DemandError> {
        use windows::Wdk::Storage::FileSystem::{FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT};
        use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        let (source_parent, source_leaf) = wide_parent_leaf(source)?;
        let source_parent = self.open_dir_held_wide_with_work(source_parent, work)?;
        let source = open_windows_wide_at_with_work(&source_parent, source_leaf,
            DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, work, || DemandError::InvalidRequest)?
            .ok_or(DemandError::InvalidRequest)?.into_std();
        let (parent, leaf) = wide_parent_leaf(destination)?;
        let parent = self.open_dir_held_wide_with_work(parent, work)?;
        link_windows_wide_measured(&source, &parent, leaf, work)
    }

    pub(crate) fn clone_file_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, source: &[u16], destination: &[u16], work: &mut W) -> Result<bool, DemandError> {
        self.clone_file_from_wide_measured(self, source, destination, work)
    }

    pub(crate) fn clone_file_from_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, source_root: &HostRoot, source: &[u16], destination: &[u16], work: &mut W) -> Result<bool, DemandError> {
        self.clone_file_from_identity_wide_measured(source_root, source, destination, None, work)
    }

    fn clone_file_from_identity_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, source_root: &HostRoot, source: &[u16], destination: &[u16], expected: Option<crate::NativeRootIdentity>, work: &mut W) -> Result<bool, DemandError> {
        let source = source_root.open_file_wide_for_with_work(source, FileReads::Cursor, work)?.into_std();
        let stat = source_root.stat_file_with_work(&source, work)?;
        if !stat.file_type().is_file() || stat.file_attributes() & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(DemandError::InvalidRequest);
        }
        if expected.is_some_and(|identity| stat.identity().ok() != Some(identity)) {
            return Err(io::Error::other("copy source identity changed").into());
        }
        let length = stat.len();
        if !clonable_windows_length(length) { return Ok(false); }
        let target = self.create_windows_file_wide_measured(destination, false, true, work)?;
        let cloned = clone_windows_file_into_measured(&source, &target, length, work);
        if matches!(cloned, Ok(true)) { windows_keep_created_file_measured(&target, work)?; }
        // Failed or cancelled work closes the still-delete-on-close target; no
        // budget-exempt path lookup or explicit deletion can escape admission.
        drop(target);
        cloned
    }

    pub(crate) fn symlink_file_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, target: &[u16], destination: &[u16], work: &mut W) -> Result<(), DemandError> {
        let file = self.create_windows_file_wide_measured(destination, false, true, work)?;
        set_windows_symlink_wide_measured(&file, target, work)?;
        windows_keep_created_file_measured(&file, work)
    }
}

#[cfg(windows)]
fn link_windows_wide_measured<W: crate::demand::SourceDemandReadWork>(file: &File, parent: &Dir, name: &[u16], work: &mut W) -> Result<(), DemandError> {
    use std::mem::{offset_of, size_of};
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{FILE_LINK_INFORMATION, FileLinkInformation, NtSetInformationFile};
    windows_wide_name_with_work(name, work, || DemandError::InvalidRequest, |_, work| {
        let name_bytes = name.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
        let offset = offset_of!(FILE_LINK_INFORMATION, FileName);
        let total = offset.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?.max(size_of::<FILE_LINK_INFORMATION>());
        let words = total.div_ceil(size_of::<u64>());
        let allocation = words.checked_mul(8).ok_or(crate::WorkError::Overflow)? as u64;
        native_buffer(work, allocation, name_bytes as u64, 0)?;
        let mut storage = vec![0_u64; words];
        let result = (|| {
            let information = storage.as_mut_ptr().cast::<FILE_LINK_INFORMATION>();
            // SAFETY: aligned storage spans the header and complete raw UTF-16 name.
            unsafe {
                (*information).Anonymous.ReplaceIfExists = false;
                (*information).RootDirectory = windows::Win32::Foundation::HANDLE(parent.as_handle().as_raw_handle());
                (*information).FileNameLength = name_bytes as u32;
                std::ptr::copy_nonoverlapping(name.as_ptr(), information.cast::<u8>().add(offset).cast::<u16>(), name.len());
            }
            let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
            native_write(work)?;
            // SAFETY: held handles and the initialized link buffer outlive this synchronous call.
            let status = unsafe { NtSetInformationFile(windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
                &raw mut block, information.cast(), total as u32, FileLinkInformation) };
            if status.is_err() { return Err(status_error(status).into()); }
            Ok(())
        })();
        drop(storage);
        work.release(allocation)?;
        result
    })?.ok_or(DemandError::InvalidRequest)
}

#[cfg(windows)]
fn set_windows_symlink_wide_measured<W: crate::demand::SourceDemandReadWork>(file: &File, target: &[u16], work: &mut W) -> Result<(), DemandError> {
    use std::os::windows::io::AsRawHandle as _;
    for unit in target {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if *unit == 0 { return Err(DemandError::InvalidRequest); }
    }
    let drive = target.len() >= 3 && target[1] == 58 && target[2] == 92;
    let unc = target.starts_with(&[92, 92]);
    let extended = target.starts_with(&[92, 92, 63, 92]);
    let relative = !drive && !unc && !target.starts_with(&[92]);
    let (prefix, tail): (&[u16], &[u16]) = if extended { (&[92, 63, 63, 92], &target[4..]) }
        else if unc { (&[92, 63, 63, 92, 85, 78, 67, 92], &target[2..]) }
        else if drive { (&[92, 63, 63, 92], target) } else { (&[], target) };
    let substitute_units = prefix.len().checked_add(tail.len()).ok_or(crate::WorkError::Overflow)?;
    let substitute_bytes = substitute_units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let print_bytes = target.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let data_bytes = 12usize.checked_add(substitute_bytes).and_then(|n| n.checked_add(print_bytes)).ok_or(crate::WorkError::Overflow)?;
    let total = data_bytes.checked_add(8).ok_or(crate::WorkError::Overflow)?;
    if total > 16 * 1024 { return Err(DemandError::InvalidRequest); }
    native_buffer(work, total as u64, (substitute_bytes + print_bytes) as u64, 0)?;
    let mut storage = vec![0_u8; total];
    let result = (|| {
        storage[..4].copy_from_slice(&0xA000000Cu32.to_le_bytes());
        storage[4..6].copy_from_slice(&(data_bytes as u16).to_le_bytes());
        storage[10..12].copy_from_slice(&(substitute_bytes as u16).to_le_bytes());
        storage[12..14].copy_from_slice(&(substitute_bytes as u16).to_le_bytes());
        storage[14..16].copy_from_slice(&(print_bytes as u16).to_le_bytes());
        storage[16..20].copy_from_slice(&u32::from(relative).to_le_bytes());
        for (index, unit) in prefix.iter().chain(tail).chain(target).enumerate() {
            storage[20 + index * 2..22 + index * 2].copy_from_slice(&unit.to_le_bytes());
        }
        native_write(work)?;
        // SAFETY: live synchronous handle and complete validated symlink reparse buffer.
        unsafe { windows::Win32::System::IO::DeviceIoControl(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            windows::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT,
            Some(storage.as_ptr().cast()), total as u32, None, 0, None, None)
            .map_err(io::Error::from)?; }
        Ok(())
    })();
    drop(storage);
    work.release(total as u64)?;
    result
}

#[cfg(windows)]
const STAGING_PREFIX: &str = ".acyclic-copy-";

#[cfg(windows)]
async fn copy_windows_file_worker(
    source_root: &HostRoot,
    source_path: &Path,
    staged: &std::sync::Arc<StagedWindowsFile>,
    receiver: &tokio::sync::oneshot::Sender<io::Result<()>>,
) -> io::Result<()> {
    use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};

    const CHUNK: usize = 1024 * 1024;
    let source_directory = source_root.directory.try_clone()?;
    let source_path_owned = source_path.to_path_buf();
    let (source, identity, length) = acyclic_native_runtime::run_blocking_io(move || {
        let source = open_windows_regular_source(&source_directory, &source_path_owned, true)?;
        let identity = crate::NativeRootIdentity::from_file(&source)?;
        let length = source.metadata()?.len();
        Ok::<_, io::Error>((source, identity, length))
    })
    .await??;
    if receiver.is_closed() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
    }
    if clonable_windows_length(length) {
        let clone_source = HostRoot {
            directory: source_root.directory.try_clone()?,
            identity: source_root.identity,
        };
        let clone_source_path = source_path.to_path_buf();
        let clone_target = std::sync::Arc::clone(staged);
        if acyclic_native_runtime::run_blocking_io(move || {
            let mut source =
                open_windows_clone_source(&clone_source, &clone_source_path, Some(identity))?;
            let mut target = clone_target.open_writer(false)?;
            clone_windows_file_into(&mut source, &mut target, length)
        })
        .await??
        {
            return Ok(());
        }
        if receiver.is_closed() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
        }
    }
    let writer = std::sync::Arc::clone(staged);
    let destination =
        acyclic_native_runtime::run_blocking_io(move || writer.open_writer(true)).await??;
    // SAFETY: both handles were opened with FILE_FLAG_OVERLAPPED and are
    // transferred once; no independent I/O is performed after this point.
    let source = unsafe { NativeFile::from_overlapped_file_unchecked(source)? };
    // SAFETY: as above.
    let destination = unsafe { NativeFile::from_overlapped_file_unchecked(destination)? };
    let mut offset = 0_u64;
    while offset < length {
        if receiver.is_closed() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
        }
        let requested = usize::try_from((length - offset).min(CHUNK as u64))
            .map_err(|_| io::Error::other("copy chunk overflow"))?;
        let mut result = source
            .read_batch_async(vec![OwnedRead {
                offset,
                length: requested,
            }])
            .await?;
        let bytes = result
            .pop()
            .ok_or_else(|| io::Error::other("copy read missing"))?;
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "copy source became shorter",
            ));
        }
        let copied = bytes.len() as u64;
        destination
            .write_all_batch_async(vec![OwnedWrite { offset, bytes }])
            .await?;
        offset += copied;
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn metadata_time(
    field: crate::kernel::MetadataField<i64>,
    current: i64,
) -> Result<i64, WindowsMetadataError> {
    let crate::kernel::MetadataField::Value(nanoseconds) = field else {
        return Ok(current);
    };
    // Applying metadata must reproduce the captured instant, so an instant
    // between ticks is refused rather than silently moved.
    if nanoseconds % 100 != 0 {
        return Err(WindowsMetadataError::Unsupported("sub-100ns timestamp"));
    }
    Ok(unix_nanoseconds_to_filetime(nanoseconds))
}

impl HostDirectory {
    pub fn symlink_metadata(&self, name: &Path) -> io::Result<Metadata> {
        self.directory.symlink_metadata(name)
    }

    pub fn rename_to(
        &self, name: &Path, destination: &Self, destination_name: &Path,
    ) -> io::Result<()> {
        self.rename_to_measured(name, destination, destination_name, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Creates one child directory and retains a capability for it.
    pub fn create_dir_held(&self, name: &Path) -> io::Result<Self> {
        let mut work = ordinary_work();
        self.create_dir_measured(name, &mut work).map_err(ordinary_error)?;
        Ok(Self { directory: self.open_root_measured(name, &mut work).map_err(ordinary_error)?.directory })
    }

    /// Removes one empty child directory.
    pub fn remove_dir(&self, name: &Path) -> io::Result<()> {
        self.remove_empty_dir_measured(name, &mut ordinary_work()).map_err(ordinary_error)
    }

    /// Releases the held directory handle before callers remove its subtree.
    pub fn close(self) {
        drop(self);
    }

    pub fn remove(&self, name: &Path) -> io::Result<()> {
        self.remove_measured(name, &mut ordinary_work()).map_err(ordinary_error)
    }
}

impl HostDirectory {
    pub(crate) fn open_root_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, work: &mut W) -> Result<HostRoot, DemandError> {
        #[cfg(unix)]
        let directory = open_unix_directory_path_with_work(&self.directory, name, false, work)?;
        #[cfg(windows)]
        let directory = {
            let root = self.root_measured(work)?;
            root.open_dir_held_with_work(name, work)?
        };
        native_read_with_work(work)?;
        let identity = held_root_identity(&directory)?;
        Ok(HostRoot::from_held_dir_with_identity(directory, identity))
    }

    fn root_measured<W: crate::demand::SourceDemandReadWork>(&self, work: &mut W) -> Result<HostRoot, DemandError> {
        let directory = clone_directory_with(&self.directory, || native_read_with_work(work))?;
        native_read_with_work(work)?;
        let identity = held_root_identity(&directory)?;
        Ok(HostRoot::from_held_dir_with_identity(directory, identity))
    }

    pub(crate) fn create_dir_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, work: &mut W) -> Result<(), DemandError> {
        require_leaf(name)?;
        self.root_measured(work)?.create_dir_measured(name, work)
    }

    pub(crate) fn stat_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, work: &mut W) -> Result<HostStat, DemandError> {
        if !name.as_os_str().is_empty() { require_leaf(name)?; }
        #[cfg(unix)]
        { stat_unix_at(&self.directory, if name.as_os_str().is_empty() { OsStr::new(".") } else { name.as_os_str() }, work) }
        #[cfg(windows)]
        { self.root_measured(work)?.stat_with_work(name, work) }
    }

    pub(crate) fn open_file_with_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, options: &NativeOpenOptions, work: &mut W) -> Result<File, DemandError> {
        open_leaf_with_work(&self.directory, name, options, work)
    }

    pub(crate) fn rename_to_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, destination: &Self, destination_name: &Path, work: &mut W) -> Result<(), DemandError> {
        require_leaf(name)?;
        require_leaf(destination_name)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            unix_name_with_work(name.as_os_str(), work, |name, work| unix_name_with_work(destination_name.as_os_str(), work, |destination_name, work| {
                // SAFETY: held parent capabilities and terminated leaf names are live.
                unix_mutation(work, || unsafe { libc::renameat(self.directory.as_raw_fd(), name.as_ptr(),
                    destination.directory.as_raw_fd(), destination_name.as_ptr()) })
            }))
        }
        #[cfg(windows)]
        {
            with_wide_path_with_work(name, work, |name, work| with_wide_path_with_work(destination_name, work, |destination_name, work|
                rename_windows_leaf_measured(&self.directory, name, &destination.directory, destination_name, work)))
        }
    }

    pub(crate) fn sync_measured<W: crate::demand::SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        sync_directory_held_with_work(&self.directory, work)
    }

    fn remove_empty_dir_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, work: &mut W) -> Result<(), DemandError> {
        self.unlink_measured(name, true, work)
    }

    fn unlink_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, directory: bool, work: &mut W) -> Result<(), DemandError> {
        remove_leaf_kind_with_work(&self.directory, name, directory, work)
    }

    pub(crate) fn remove_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, work: &mut W) -> Result<(), DemandError> {
        let stat = match self.stat_measured(name, work) {
            Ok(stat) => stat,
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        self.remove_identity_measured(name, stat.identity()?, work)
    }

    pub(crate) fn remove_identity_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &Path, expected: crate::NativeRootIdentity, work: &mut W) -> Result<(), DemandError> {
        require_leaf(name)?;
        #[cfg(unix)]
        {
            let stat = self.stat_measured(name, work)?;
            if stat.identity()? != expected { return Err(DemandError::StaleVersion); }
            if stat.file_type().is_dir() && !stat.file_type().is_symlink() {
                let root = self.open_root_measured(name, work)?;
                if root.identity() != expected { return Err(DemandError::StaleVersion); }
                let child = HostDirectory { directory: root.directory };
                let root = child.root_measured(work)?;
                let (mut reader, buffer_bytes) = root.read_dir_stats_with_work(Path::new(""), work)?;
                let result: Result<(), DemandError> = (|| {
                    while let Some((entry, name_bytes)) = reader.next_with_work(work)? {
                        let result = child.remove_measured(Path::new(&entry.name), work);
                        drop(entry);
                        work.release(name_bytes)?;
                        result?;
                    }
                    Ok(())
                })();
                drop(reader);
                work.release(buffer_bytes)?;
                result?;
                drop(root);
                drop(child);
            }
            if self.stat_measured(name, work)?.identity()? != expected { return Err(DemandError::StaleVersion); }
            self.unlink_measured(name, stat.file_type().is_dir() && !stat.file_type().is_symlink(), work)
        }
        #[cfg(windows)]
        { with_wide_path_with_work(name, work, |name, work| self.remove_identity_wide_measured(name, expected, work)) }
    }

    #[cfg(windows)]
    fn remove_identity_wide_measured<W: crate::demand::SourceDemandReadWork>(&self, name: &[u16], expected: crate::NativeRootIdentity, work: &mut W) -> Result<(), DemandError> {
        let root = self.root_measured(work)?;
        let stat = root.stat_wide_with_work(name, work)?;
        if stat.identity()? != expected { return Err(DemandError::StaleVersion); }
        let real_directory = stat.is_dir() && stat.file_attributes() & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 == 0;
        if real_directory {
            let directory = root.open_dir_held_wide_with_work(name, work)?;
            let child = HostDirectory { directory };
            let child_root = child.root_measured(work)?;
            if child_root.identity() != expected { return Err(DemandError::StaleVersion); }
            let (mut reader, bytes) = child_root.read_dir_stats_wide_with_work(&[], work)?;
            let result: Result<(), DemandError> = (|| {
                while let Some((entry, bytes)) = reader.next_wide_with_work(work)? {
                    let result = (|| {
                        let stat = child_root.stat_wide_with_work(&entry.name, work)?;
                        child.remove_identity_wide_measured(&entry.name, stat.identity()?, work)
                    })();
                    drop(entry);
                    work.release(bytes)?;
                    result?;
                }
                Ok(())
            })();
            drop(reader);
            work.release(bytes)?;
            result?;
        }
        if root.stat_wide_with_work(name, work)?.identity()? != expected { return Err(DemandError::StaleVersion); }
        remove_windows_leaf_measured(&self.directory, name, stat.is_dir(), work)
    }
}


/// Network, clustered, and user-space filesystems, by `statfs(2)` magic.
#[cfg(target_os = "linux")]
const REMOTE_FILESYSTEMS: [u64; 13] = [
    0x6969,      // NFS
    0x517b,      // SMB
    0xff53_4d42, // CIFS
    0xfe53_4d42, // SMB2
    0x5346_414f, // AFS
    0x00c3_6400, // Ceph
    0x6573_5546, // FUSE
    0x6a65_6a63, // virtiofs, which a hypervisor's host changes unseen
    0x0102_1997, // 9P
    0x564c,      // NCP
    0x7375_7245, // Coda
    0x0bd0_0bd0, // Lustre
    0x4750_4653, // GPFS
];

fn filesystem_is_local(directory: &Dir) -> bool {
    filesystem_is_local_measured(directory, &mut ordinary_work()).unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn filesystem_is_local_measured<W: NativeHostWork>(directory: &Dir, work: &mut W) -> Result<bool, W::Error> {
    use std::os::fd::AsRawFd as _;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    native_read_with_work(work)?;
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Ok(false);
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let kind = unsafe { stats.assume_init() }.f_type;
    // libc models `f_type` as signed for glibc and unsigned for musl.
    Ok(u64::try_from(kind).is_ok_and(|kind| !REMOTE_FILESYSTEMS.contains(&kind)))
}

#[cfg(target_os = "macos")]
fn filesystem_is_local_measured<W: NativeHostWork>(directory: &Dir, work: &mut W) -> Result<bool, W::Error> {
    use std::os::fd::AsRawFd as _;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    native_read_with_work(work)?;
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Ok(false);
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let flags = unsafe { stats.assume_init() }.f_flags;
    Ok(u32::try_from(libc::MNT_LOCAL).is_ok_and(|local| flags & local != 0))
}

#[cfg(windows)]
fn filesystem_is_local_measured<W: NativeHostWork>(directory: &Dir, work: &mut W) -> Result<bool, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{
        FileFsDeviceInformation, NtQueryVolumeInformationFile,
    };
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    /// The characteristic of a volume on a network redirector.
    const FILE_REMOTE_DEVICE: u32 = 0x10;
    // `FILE_FS_DEVICE_INFORMATION`: the device type, then characteristics.
    let mut device = [0_u32; 2];
    let mut status = IO_STATUS_BLOCK::default();
    // SAFETY: the handle is live for the call, and the buffer is exactly one
    // `FILE_FS_DEVICE_INFORMATION` of the length passed.
    native_read_with_work(work)?;
    let queried = unsafe {
        NtQueryVolumeInformationFile(
            HANDLE(directory.as_raw_handle()),
            &raw mut status,
            device.as_mut_ptr().cast(),
            8,
            FileFsDeviceInformation,
        )
    };
    let [_, characteristics] = device;
    Ok(queried.is_ok() && characteristics & FILE_REMOTE_DEVICE == 0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn filesystem_is_local_measured<W: NativeHostWork>(_directory: &Dir, work: &mut W) -> Result<bool, W::Error> {
    work.check()?;
    Ok(false)
}

/// Deallocates an already-zero range of a host file.
///
/// APFS materializes the zero tail created by `ftruncate` as soon as any byte
/// of the file is written, so holes must be punched explicitly after the data
/// spans are in place. The range is shrunk inward to filesystem-block
/// alignment. Unsupported or failed deallocation is reported rather than
/// silently materializing a dense file.
#[cfg(target_os = "macos")]
pub fn punch_hole(file: &impl std::os::fd::AsRawFd, offset: u64, length: u64) -> io::Result<()> {
    let mut work = ordinary_work();
    punch_hole_with_admission(file, offset, length, |write| {
        if write { native_write(&mut work) } else { native_read(&mut work) }.map_err(ordinary_error)
    })
}

#[cfg(target_os = "macos")]
pub(crate) fn punch_hole_measured(
    file: &impl std::os::fd::AsRawFd, offset: u64, length: u64,
    meter: &dyn acyclic_native_runtime::NativeReadMeter,
) -> io::Result<()> {
    punch_hole_with_admission(file, offset, length, |write| {
        if write { meter.admit_write(0) } else { meter.admit_read(0) }
    })
}

#[cfg(target_os = "macos")]
fn punch_hole_with_admission(
    file: &impl std::os::fd::AsRawFd, offset: u64, length: u64,
    mut before: impl FnMut(bool) -> io::Result<()>,
) -> io::Result<()> {
    let fd = file.as_raw_fd();
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    loop {
        before(false)?;
        // SAFETY: fstatfs fills live, correctly sized storage for the held descriptor.
        if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } == 0 { break; }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let block = u64::from(unsafe { stats.assume_init() }.f_bsize);
    if block == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host filesystem reports a zero allocation unit",
        ));
    }
    let Some(start) = offset
        .checked_add(block - 1)
        .map(|edge| edge / block * block)
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole start overflow",
        ));
    };
    let Some(end) = offset.checked_add(length).map(|edge| edge / block * block) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole end overflow",
        ));
    };
    if end <= start {
        return Ok(());
    }
    let (Ok(fp_offset), Ok(fp_length)) = (i64::try_from(start), i64::try_from(end - start)) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole range exceeds host limits",
        ));
    };
    let arg = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset,
        fp_length,
    };
    loop {
        before(true)?;
        // SAFETY: the argument struct outlives this value-only fcntl call.
        if unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &arg) } == 0 { return Ok(()); }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
    }
}

#[cfg(windows)]
fn clone_windows_file_measured<W: crate::demand::SourceDemandReadWork>(source: &File, target: &File, length: u64, alignment: u64, work: &mut W) -> Result<(), DemandError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE, FSCTL_SET_SPARSE};
    const CLONE_CHUNK: u64 = 1024 * 1024 * 1024;
    let clone_length = length / alignment * alignment;
    if clone_length == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "file is smaller than clone alignment").into());
    }
    native_write(work)?;
    // SAFETY: live synchronous target handle; this control has no buffers.
    unsafe { DeviceIoControl(HANDLE(target.as_raw_handle()), FSCTL_SET_SPARSE,
        None, 0, None, 0, None, None).map_err(io::Error::from)?; }
    truncate_file_with_work(target, length, work)?;
    let mut offset = 0;
    while offset < clone_length {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let count = (clone_length - offset).min(CLONE_CHUNK);
        let duplicate = DUPLICATE_EXTENTS_DATA {
            FileHandle: HANDLE(source.as_raw_handle()),
            SourceFileOffset: i64::try_from(offset).map_err(|_| crate::WorkError::Overflow)?,
            TargetFileOffset: i64::try_from(offset).map_err(|_| crate::WorkError::Overflow)?,
            ByteCount: i64::try_from(count).map_err(|_| crate::WorkError::Overflow)?,
        };
        native_write(work)?;
        // SAFETY: both held handles and complete fixed-size input remain live.
        unsafe { DeviceIoControl(HANDLE(target.as_raw_handle()), FSCTL_DUPLICATE_EXTENTS_TO_FILE,
            Some((&raw const duplicate).cast()), std::mem::size_of::<DUPLICATE_EXTENTS_DATA>() as u32,
            None, 0, None, None).map_err(io::Error::from)?; }
        offset += count;
    }
    if clone_length < length {
        let ranges = query_allocated_data_ranges_measured(source, clone_length, length - clone_length, 64, work)?;
        let ranges_bytes = (ranges.capacity() * std::mem::size_of::<HostDataRange>()) as u64;
        let result: Result<(), DemandError> = (|| {
            let mut buffer = [0_u8; 64 * 1024];
            for range in &ranges {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                let size = usize::try_from(range.length).map_err(|_| crate::WorkError::Overflow)?;
                let buffer = buffer.get_mut(..size).ok_or(DemandError::InvalidRequest)?;
                windows_copy_tail_measured(source, target, range.offset, buffer, work)?;
            }
            Ok(())
        })();
        drop(ranges);
        work.release(ranges_bytes)?;
        result?;
    }
    Ok(())
}

pub(crate) fn truncate_file_with_work<W: NativeHostWork>(
    file: &File, length: u64, work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let length = libc::off_t::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
        loop {
            let delta = WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            work.record(delta)?;
            // SAFETY: ftruncate acts only on the live held descriptor.
            if unsafe { libc::ftruncate(file.as_raw_fd(), length) } == 0 { return Ok(()); }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted { return Err(error.into()); }
        }
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::FileEndOfFileInformation;
        use windows::Win32::Storage::FileSystem::FILE_END_OF_FILE_INFO;
        let information = FILE_END_OF_FILE_INFO { EndOfFile: i64::try_from(length).map_err(|_| crate::WorkError::Overflow)? };
        windows_set_information_measured(file, &information, FileEndOfFileInformation, work)
    }
}

#[cfg(windows)]
fn windows_copy_tail_measured<W: crate::demand::SourceDemandReadWork>(source: &File, target: &File, offset: u64, buffer: &mut [u8], work: &mut W) -> Result<(), DemandError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{NtReadFile, NtWriteFile};
    use windows::Win32::Foundation::HANDLE;
    let mut consumed = 0usize;
    while consumed < buffer.len() {
        let length = buffer.len() - consumed;
        work.preflight(WorkCounters { source_bytes_read: length as u64, ..WorkCounters::UNCHARGED })?;
        native_read_with_work(work)?;
        let position = i64::try_from(offset.checked_add(consumed as u64).ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?;
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: synchronous held handle and writable admitted tail slice.
        let status = unsafe { NtReadFile(HANDLE(source.as_raw_handle()), None, None, None,
            &raw mut block, buffer[consumed..].as_mut_ptr().cast(), length as u32, Some(&position), None) };
        if status.is_err() { return Err(status_error(status).into()); }
        let count = block.Information;
        if count == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "clone source tail shortened").into()); }
        if count > length { return Err(DemandError::InvalidRequest); }
        work.record(WorkCounters { source_bytes_read: count as u64, ..WorkCounters::UNCHARGED })?;
        consumed += count;
    }
    let mut consumed = 0usize;
    while consumed < buffer.len() {
        native_write(work)?;
        let position = i64::try_from(offset.checked_add(consumed as u64).ok_or(crate::WorkError::Overflow)?).map_err(|_| crate::WorkError::Overflow)?;
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: synchronous held handle and initialized tail slice remain live.
        let status = unsafe { NtWriteFile(HANDLE(target.as_raw_handle()), None, None, None,
            &raw mut block, buffer[consumed..].as_ptr().cast(), (buffer.len() - consumed) as u32, Some(&position), None) };
        if status.is_err() { return Err(status_error(status).into()); }
        let count = block.Information;
        if count == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "clone tail write made no progress").into()); }
        if count > buffer.len() - consumed { return Err(DemandError::InvalidRequest); }
        consumed += count;
    }
    Ok(())
}

/// Why a read of a [`YieldingFile`] failed: its handle gave way to a rename
/// or delete of the file, so what it named is gone or replaced.
#[cfg(windows)]
#[derive(Debug)]
pub struct Yielded;

#[cfg(windows)]
impl std::fmt::Display for Yielded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the file gave way to a rename or delete")
    }
}

#[cfg(windows)]
impl std::error::Error for Yielded {}

/// A file held for reading that gives way to whoever renames or deletes it.
///
/// Windows refuses to replace a file by rename, as editors and `git` save,
/// while any handle to it is open, however shared. So the file holds a
/// read-handle oplock: a rename or delete breaks it and waits, the handle
/// closes at once, and the rename proceeds; every read after fails with
/// [`Yielded`]. Where no oplock is granted it reads as a plain handle.
#[cfg(windows)]
pub struct YieldingFile {
    held: std::sync::Arc<YieldingHeld>,
    /// The thread-pool wait that closes the handle once the oplock breaks.
    wait: Option<windows::Win32::Foundation::HANDLE>,
}

#[cfg(windows)]
struct YieldingHeld {
    // ReadFile starts under this lock, but its completion is waited for
    // outside it. The break callback can therefore cancel stalled reads.
    file: std::sync::Mutex<Option<std::sync::Arc<File>>>,
    /// The oplock request, alive until the kernel completes it; null for a
    /// plain handle.
    request: *mut OplockRequest,
}

// SAFETY: `request` is written only by the kernel while the request is
// pending, and freed once, after it completes, by the sole owner.
#[cfg(windows)]
unsafe impl Send for YieldingHeld {}
// SAFETY: as for `Send`; the file is behind a lock.
#[cfg(windows)]
unsafe impl Sync for YieldingHeld {}

// SAFETY: the wait handle is only unregistered, once, by the owner.
#[cfg(windows)]
unsafe impl Send for YieldingFile {}
// SAFETY: shared access touches only the locked file.
#[cfg(windows)]
unsafe impl Sync for YieldingFile {}

#[cfg(windows)]
#[repr(C)]
struct OplockRequest {
    overlapped: windows::Win32::System::IO::OVERLAPPED,
    input: windows::Win32::System::Ioctl::REQUEST_OPLOCK_INPUT_BUFFER,
    output: windows::Win32::System::Ioctl::REQUEST_OPLOCK_OUTPUT_BUFFER,
}

#[cfg(windows)]
impl YieldingFile {
    /// Holds `file`, an overlapped handle, under a read-handle oplock.
    #[cfg(test)]
    fn new(file: File) -> io::Result<Self> {
        Self::new_measured(file, &mut ordinary_work()).map_err(ordinary_error)
    }

    fn new_measured(file: File, work: &mut DemandWork) -> Result<Self, DemandError> {
        Self::new_with_work(file, work)
    }

    fn new_with_work<W: crate::demand::SourceDemandReadWork>(file: File, work: &mut W) -> Result<Self, DemandError> {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::{ERROR_IO_PENDING, HANDLE, CloseHandle};
        use windows::Win32::System::IO::DeviceIoControl;
        use windows::Win32::System::Ioctl::{
            FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_HANDLE, OPLOCK_LEVEL_CACHE_READ,
            REQUEST_OPLOCK_CURRENT_VERSION, REQUEST_OPLOCK_INPUT_BUFFER,
            REQUEST_OPLOCK_INPUT_FLAG_REQUEST, REQUEST_OPLOCK_OUTPUT_BUFFER,
        };
        use windows::Win32::System::Threading::{
            CreateEventW, INFINITE, RegisterWaitForSingleObject, WT_EXECUTEONLYONCE,
        };
        let handle = HANDLE(file.as_raw_handle());
        let mut yielding = Self::plain_with_work(file, work)?;
        let result = (|| {
            // SAFETY: event creation has no borrowed pointer arguments.
            let event = unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::other)?;
            let bytes = std::mem::size_of::<OplockRequest>() as u64;
            if let Err(error) = work.allocate(bytes) {
                unsafe { let _ = CloseHandle(event); }
                return Err(error);
            }
            let request = Box::into_raw(Box::new(OplockRequest {
                overlapped: windows::Win32::System::IO::OVERLAPPED { hEvent: event, ..Default::default() },
                input: REQUEST_OPLOCK_INPUT_BUFFER {
                    StructureVersion: u16::try_from(REQUEST_OPLOCK_CURRENT_VERSION).unwrap_or(u16::MAX),
                    StructureLength: size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u16,
                    RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ | OPLOCK_LEVEL_CACHE_HANDLE,
                    Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
                },
                output: REQUEST_OPLOCK_OUTPUT_BUFFER::default(),
            }));
            if let Err(error) = native_read_with_work(work) {
                unsafe { let _ = CloseHandle(event); drop(Box::from_raw(request)); }
                work.release(bytes)?;
                return Err(error);
            }
            std::sync::Arc::get_mut(&mut yielding.held)
                .ok_or_else(|| io::Error::other("oplock state was unexpectedly shared"))?.request = request;
            // SAFETY: stable owned request and held handle remain alive until
            // the oplock completes; YieldingFile::drop cancels and waits.
            let requested = unsafe { DeviceIoControl(handle, FSCTL_REQUEST_OPLOCK,
                Some((&raw const (*request).input).cast()), size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u32,
                Some((&raw mut (*request).output).cast()), size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>() as u32,
                None, Some(&raw mut (*request).overlapped)) };
            if !requested.is_err_and(|error| error.code() == ERROR_IO_PENDING.to_hresult()) {
                std::sync::Arc::get_mut(&mut yielding.held)
                    .ok_or_else(|| io::Error::other("oplock state was unexpectedly shared"))?.request = std::ptr::null_mut();
                unsafe { let _ = CloseHandle(event); drop(Box::from_raw(request)); }
                work.release(bytes)?;
                return Ok(());
            }
            let mut wait = HANDLE::default();
            // SAFETY: the state outlives this registered callback, unregistered
            // before state destruction by YieldingFile::drop.
            unsafe { RegisterWaitForSingleObject(&raw mut wait, event, Some(yield_on_break),
                Some(std::sync::Arc::as_ptr(&yielding.held).cast()), INFINITE, WT_EXECUTEONLYONCE) }
                .map_err(io::Error::other)?;
            yielding.wait = Some(wait);
            Ok(())
        })();
        if let Err(error) = result {
            let bytes = yielding.logical_owned_bytes();
            drop(yielding);
            work.release(bytes)?;
            return Err(error);
        }
        Ok(yielding)
    }

    fn plain_measured(file: File, work: &mut DemandWork) -> Result<Self, DemandError> {
        Self::plain_with_work(file, work)
    }

    fn plain_with_work<W: crate::demand::SourceDemandReadWork>(file: File, work: &mut W) -> Result<Self, DemandError> {
        let file_bytes = std::mem::size_of::<File>() as u64;
        let held_bytes = std::mem::size_of::<YieldingHeld>() as u64;
        work.allocate(file_bytes)?;
        let file = std::sync::Arc::new(file);
        if let Err(error) = work.allocate(held_bytes) {
            drop(file);
            work.release(file_bytes)?;
            return Err(error);
        }
        Ok(Self {
            held: std::sync::Arc::new(YieldingHeld {
                file: std::sync::Mutex::new(Some(file)), request: std::ptr::null_mut(),
            }),
            wait: None,
        })
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        (std::mem::size_of::<YieldingHeld>() + std::mem::size_of::<File>()
            + if self.held.request.is_null() { 0 } else { std::mem::size_of::<OplockRequest>() }) as u64
    }

    pub(crate) fn with_measured<T>(&self, operation: impl FnOnce(&File) -> Result<T, DemandError>) -> Result<T, DemandError> {
        let file = self.held.file.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match file.as_ref() {
            Some(file) => operation(file),
            None => Err(DemandError::from(io::Error::other(Yielded))),
        }
    }

    /// Holds `file` without an oplock: it reads, and never gives way.
    #[cfg(test)]
    fn plain(file: File) -> Self {
        Self {
            held: std::sync::Arc::new(YieldingHeld {
                file: std::sync::Mutex::new(Some(std::sync::Arc::new(file))),
                request: std::ptr::null_mut(),
            }),
            wait: None,
        }
    }

    /// Whether an oplock break closed the held handle.
    pub(crate) fn was_yielded(&self) -> bool {
        self.held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }

    /// Runs `operation` on the handle unless it gave way.
    pub fn with<T>(&self, operation: impl FnOnce(&File) -> io::Result<T>) -> io::Result<T> {
        let file = self
            .held
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match file.as_ref() {
            Some(file) => operation(file),
            None => Err(io::Error::other(Yielded)),
        }
    }

    /// Reads at `offset` into `destination` through an overlapped request
    /// of its own, which the oplock's completion never answers for.
    pub fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
        self.read_at_measured(offset, destination, &mut ordinary_work()).map_err(ordinary_error)
    }

    pub(crate) fn read_at_measured(&self, offset: u64, destination: &mut [u8], work: &mut DemandWork) -> Result<usize, DemandError> {
        self.read_at_with_work(offset, destination, work)
    }

    pub(crate) fn read_at_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, offset: u64, destination: &mut [u8], work: &mut W,
    ) -> Result<usize, DemandError> {
        work.check()?;
        if destination.is_empty() { return Ok(0); }
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::{CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, HANDLE, WAIT_FAILED, WAIT_TIMEOUT};
        use windows::Win32::Storage::FileSystem::ReadFile;
        use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
        use windows::Win32::System::Threading::{CreateEventW, WaitForSingleObject};

        work.with_native_read_meter(|meter| {
            let mut operations = acyclic_native_runtime::NativeOperationReservation::new(
                meter, acyclic_native_runtime::NativeOperationKind::Read, 2,
            ).map_err(crate::demand::native_demand_error)?;
            let mut byte_reservation = acyclic_native_runtime::NativeByteReservation::new(
                meter, acyclic_native_runtime::NativeOperationKind::Read,
                destination.len().min(u32::MAX as usize),
            ).map_err(crate::demand::native_demand_error)?;
            let held = self
                .held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let file = held.as_ref().ok_or_else(|| io::Error::other(Yielded))?;
            let handle = HANDLE(file.as_raw_handle());
            // SAFETY: no pointers are passed.
            let event =
                unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::other)?;
            let mut overlapped = OVERLAPPED {
                hEvent: event,
                ..Default::default()
            };
            let (low, high) = (offset & u64::from(u32::MAX), offset >> 32);
            overlapped.Anonymous.Anonymous.Offset = u32::try_from(low).unwrap_or(u32::MAX);
            overlapped.Anonymous.Anonymous.OffsetHigh = u32::try_from(high).unwrap_or(u32::MAX);
            let length = destination.len().min(u32::MAX as usize);
            let buffer = destination.get_mut(..length).unwrap_or_default();
            let mut transferred = 0;
            // SAFETY: the buffer and `overlapped` outlive the request, which
            // is waited for before either goes.
            let mut pending = false;
            let (started, start_charge) = operations.attempt(|| {
                unsafe { ReadFile(handle, Some(buffer), None, Some(&raw mut overlapped)) }.or_else(
                    |error| {
                        if error.code() == ERROR_IO_PENDING.to_hresult() {
                            pending = true;
                            Ok(())
                        } else {
                            Err(error)
                        }
                    },
                )
            });
            // Keep the file alive after releasing the lock. A break cancels
            // this request; its completion is still awaited before freeing
            // the buffer and OVERLAPPED.
            let file = std::sync::Arc::clone(file);
            drop(held);
            // Completion cannot be abandoned once ReadFile owns the caller's
            // buffer. Poll only the event, observing cancellation/deadline and
            // bounded polling work before every wait. Cancellation still drains
            // the exact request before releasing its OVERLAPPED and buffer.
            let mut interrupted = None;
            if pending {
                loop {
                    if let Err(error) = meter.examine(1, 0).map_err(crate::demand::native_demand_error) {
                        interrupted = Some(error);
                        unsafe { let _ = CancelIoEx(HANDLE(file.as_raw_handle()), Some(&raw const overlapped)); }
                        break;
                    }
                    let status = unsafe { WaitForSingleObject(event, 10) };
                    if status == WAIT_TIMEOUT { continue; }
                    if status == WAIT_FAILED {
                        interrupted = Some(DemandError::Io(io::Error::last_os_error()));
                        unsafe { let _ = CancelIoEx(HANDLE(file.as_raw_handle()), Some(&raw const overlapped)); }
                    }
                    break;
                }
            }
            // This cleanup boundary was admitted before starting the request;
            // it must be charged even when cancellation arrived during the wait.
            let mut completion_charge = Ok(());
            let read = started.and_then(|()| {
                let (read, recorded) = operations.attempt(|| unsafe {
                    GetOverlappedResult(HANDLE(file.as_raw_handle()), &raw const overlapped, &raw mut transferred, true)
                });
                completion_charge = recorded;
                read
            });
            // SAFETY: the request is complete; nothing waits on the event.
            let _ = unsafe { CloseHandle(event) };
            let bytes_recorded = byte_reservation.record(transferred as usize);
            start_charge.map_err(crate::demand::native_demand_error)?;
            completion_charge.map_err(crate::demand::native_demand_error)?;
            bytes_recorded.map_err(crate::demand::native_demand_error)?;
            if let Some(error) = interrupted { return Err(error); }
            meter.check().map_err(crate::demand::native_demand_error)?;
            match read {
                Ok(()) => Ok(transferred as usize),
                Err(error) if error.code() == ERROR_HANDLE_EOF.to_hresult() => Ok(0),
                Err(_)
                    if self
                        .held
                        .file
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_none() =>
                {
                    Err(DemandError::from(io::Error::other(Yielded)))
                }
                Err(error) => Err(DemandError::from(io::Error::from_raw_os_error(error.code().0 & 0xFFFF))),
            }
        })
    }
}

/// Closes the handle whose oplock broke, which lets the rename or delete
/// that broke it proceed.
#[cfg(windows)]
unsafe extern "system" fn yield_on_break(context: *mut std::ffi::c_void, _timed_out: bool) {
    // SAFETY: the context is the held state, alive until this wait is
    // unregistered, which waits for this callback.
    let held = unsafe { &*context.cast::<YieldingHeld>() };
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::CancelIoEx;
    let file = held
        .file
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(file) = file {
        // SAFETY: the handle is live; reads were started before this lock
        // was acquired, and their waiters keep the handle alive.
        let _ = unsafe { CancelIoEx(HANDLE(file.as_raw_handle()), None) };
    }
}

#[cfg(windows)]
impl Drop for YieldingFile {
    fn drop(&mut self) {
        use windows::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows::Win32::System::Threading::{INFINITE, UnregisterWaitEx, WaitForSingleObject};

        if let Some(wait) = self.wait.take() {
            // SAFETY: blocks until a running callback returns; none runs
            // after.
            let _ = unsafe { UnregisterWaitEx(wait, Some(INVALID_HANDLE_VALUE)) };
        }
        // Closing the handle completes a request still pending.
        drop(
            self.held
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        let request = self.held.request;
        if !request.is_null() {
            // SAFETY: the request completed, or completes as its handle
            // closed; its event says when, and nothing writes it after.
            unsafe {
                let event = (*request).overlapped.hEvent;
                WaitForSingleObject(event, INFINITE);
                let _ = CloseHandle(event);
                drop(Box::from_raw(request));
            }
        }
    }
}

pub(crate) fn allocated_data_ranges_measured<W: crate::demand::SourceDemandReadWork>(
    file: &File,
    logical_bytes: u64,
    maximum_ranges: u32,
    work: &mut W,
) -> Result<Vec<HostDataRange>, DemandError> {
    query_allocated_data_ranges_measured(file, 0, logical_bytes, maximum_ranges, work)
}

pub(crate) fn allocated_data_ranges_with_work<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    file: &File, logical_bytes: u64, maximum_ranges: u32, work: &mut W,
    remaining: impl FnMut(&mut W) -> Result<crate::WorkBudget, DemandError>,
    #[cfg(windows)]
    device_control: impl FnMut(&File, u32, &[u8], &mut [u8], &mut W) -> Result<usize, DemandError>,
) -> Result<Vec<HostDataRange>, DemandError> {
    query_allocated_data_ranges_with_work(
        file, 0, logical_bytes, maximum_ranges, work, remaining,
        #[cfg(windows)]
        device_control,
    )
}

/// Shares the existing native-runtime held request/completion engine without
/// depending on the optional snapshot-capture module. The original typed meter
/// cause wins over its allocation-free IO marker; ordinary OS misses stay IO.
#[cfg(windows)]
fn native_device_control_measured<W: crate::demand::SourceDemandReadWork>(
    file: &File, code: u32, input: &[u8], output: &mut [u8], work: &mut W,
) -> Result<usize, DemandError> {
    work.with_native_read_meter(|meter| {
        acyclic_native_runtime::device_control_in_place_measured(file, code, input, output, meter)
    }).map_err(|error| {
        work.with_original_leaf(|original| original.take_native_io_failure())
            .unwrap_or_else(|| crate::demand::native_demand_error(error))
    })
}

pub(crate) fn query_allocated_data_ranges_measured<W: crate::demand::SourceDemandReadWork>(
    file: &File,
    offset: u64,
    length: u64,
    maximum_ranges: u32,
    work: &mut W,
) -> Result<Vec<HostDataRange>, DemandError> {
    query_allocated_data_ranges_with_work(
        file, offset, length, maximum_ranges, work, |work| work.remaining(),
        #[cfg(windows)]
        |file, code, input, output, work| {
            native_device_control_measured::<W>(file, code, input, output, work)
        },
    )
}

pub(crate) fn query_allocated_data_ranges_with_work<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    file: &File, offset: u64, length: u64, maximum_ranges: u32, work: &mut W,
    _remaining: impl FnMut(&mut W) -> Result<crate::WorkBudget, DemandError>,
    #[cfg(windows)]
    mut device_control: impl FnMut(&File, u32, &[u8], &mut [u8], &mut W) -> Result<usize, DemandError>,
) -> Result<Vec<HostDataRange>, DemandError> {
    work.check()?;
    if length == 0 { return Ok(Vec::new()); }
    let logical_bytes = offset.checked_add(length).ok_or(crate::WorkError::Overflow)?;
    if maximum_ranges == 0 { return Err(DemandError::InvalidRequest); }
    #[cfg(unix)]
    {
        let mut remaining = _remaining;
        use std::os::fd::AsRawFd as _;
        let mut ranges = Vec::new();
        let mut offset = offset;
        let seek = |offset: i64, kind: i32, work: &mut W| {
            loop {
                native_read_with_work(work)?;
                // SAFETY: the held regular-file descriptor remains live for
                // the call; lseek changes only its offset, never its contents.
                let result = unsafe { libc::lseek(file.as_raw_fd(), offset, kind) };
                if result >= 0 { return Ok(result); }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(DemandError::Io(error));
                }
            }
        };
        while offset < logical_bytes {
            let start = i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?;
            let data_offset = match seek(start, libc::SEEK_DATA, work) {
                Err(DemandError::Io(error)) if error.raw_os_error() == Some(libc::ENXIO) => break,
                result => result?,
            };
            let data = u64::try_from(data_offset).map_err(|_| DemandError::InvalidRequest)?;
            if data >= logical_bytes { break; }
            let hole = seek(data_offset, libc::SEEK_HOLE, work)?;
            let hole = u64::try_from(hole).map_err(|_| DemandError::InvalidRequest)?.min(logical_bytes);
            if hole <= data || data < offset {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "host sparse-range query made no progress").into());
            }
            if ranges.len() >= maximum_ranges as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "host file exceeds admitted sparse range count").into());
            }
            if ranges.len() == ranges.capacity() {
                let element = std::mem::size_of::<HostDataRange>();
                let old = ranges.capacity() * element;
                let wanted = ranges.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
                let grown = ranges.capacity().saturating_mul(2).max(4)
                    .min(maximum_ranges as usize).max(wanted);
                let capacity = match grown.checked_mul(element) {
                    Some(bytes) if bytes as u64 <= remaining(work)?.peak_allocation_bytes => grown,
                    _ => wanted,
                };
                let bytes = capacity.checked_mul(element).ok_or(crate::WorkError::Overflow)?;
                work.claim(bytes as u64)?;
                let mut replacement = Vec::new();
                replacement.try_reserve_exact(capacity).map_err(|_| DemandError::InvalidRequest)?;
                let copied = WorkCounters { bytes_copied: (ranges.len() * element) as u64,
                    ..WorkCounters::UNCHARGED };
                work.admit(copied)?;
                work.record(copied)?;
                replacement.append(&mut ranges);
                let previous = std::mem::replace(&mut ranges, replacement);
                drop(previous);
                work.release(old as u64)?;
            }
            let copied = WorkCounters { bytes_copied: std::mem::size_of::<HostDataRange>() as u64,
                items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(copied)?;
            work.record(copied)?;
            ranges.push(HostDataRange { offset: data, length: hole - data });
            offset = hole;
        }
        Ok(ranges)
    }
    #[cfg(windows)]
    {
        use windows::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_MORE_DATA};
        use windows::Win32::System::Ioctl::{FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES};
        let element = std::mem::size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        let maximum = usize::try_from(u64::from(maximum_ranges) + 1).map_err(|_| DemandError::InvalidRequest)?
            .min(u32::MAX as usize / element);
        let mut count = maximum.min(8);
        work.claim((count * element) as u64)?;
        let mut output = Vec::new();
        output.try_reserve_exact(count).map_err(|_| DemandError::InvalidRequest)?;
        output.resize(count, FILE_ALLOCATED_RANGE_BUFFER::default());
        let query = FILE_ALLOCATED_RANGE_BUFFER {
            FileOffset: i64::try_from(offset).map_err(|_| DemandError::InvalidRequest)?,
            Length: i64::try_from(length).map_err(|_| DemandError::InvalidRequest)?,
        };
        // SAFETY: FILE_ALLOCATED_RANGE_BUFFER is initialized plain native data.
        let input = unsafe { std::slice::from_raw_parts(std::ptr::from_ref(&query).cast::<u8>(), element) };
        let returned = loop {
            work.check()?;
            // SAFETY: the vector's initialized records stay alive, exclusive,
            // and correctly aligned through synchronous ioctl completion.
            let bytes = unsafe { std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), output.len() * element) };
            match device_control(file, FSCTL_QUERY_ALLOCATED_RANGES, input, bytes, work) {
                Ok(returned) => break returned,
                Err(DemandError::Io(error)) if [ERROR_MORE_DATA, ERROR_INSUFFICIENT_BUFFER].iter()
                    .any(|code| error.raw_os_error() == i32::try_from(code.0).ok()) => {
                    if count == maximum {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "host file exceeds admitted sparse range count").into());
                    }
                    let next = count.saturating_mul(2).min(maximum);
                    work.claim((next * element) as u64)?;
                    let mut replacement = Vec::new();
                    replacement.try_reserve_exact(next).map_err(|_| DemandError::InvalidRequest)?;
                    replacement.resize(next, FILE_ALLOCATED_RANGE_BUFFER::default());
                    drop(output);
                    work.release((count * element) as u64)?;
                    output = replacement;
                    count = next;
                }
                Err(error) => return Err(error),
            }
        };
        if returned > output.len() * element || returned % element != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid range response").into());
        }
        let returned_count = returned / element;
        if returned_count > maximum_ranges as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "host file exceeds admitted sparse range count").into());
        }
        let bytes = returned_count.checked_mul(std::mem::size_of::<HostDataRange>()).ok_or(crate::WorkError::Overflow)?;
        work.claim(bytes as u64)?;
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(returned_count).map_err(|_| DemandError::InvalidRequest)?;
        let mut prior_end = offset;
        for range in output.iter().take(returned_count) {
            let copied = WorkCounters { items_examined: 1, bytes_copied: std::mem::size_of::<HostDataRange>() as u64,
                ..WorkCounters::UNCHARGED };
            work.admit(copied)?;
            work.record(copied)?;
            let native_start = u64::try_from(range.FileOffset).map_err(|_| DemandError::InvalidRequest)?;
            let start = native_start.max(offset);
            let length = u64::try_from(range.Length).map_err(|_| DemandError::InvalidRequest)?;
            let end = native_start.checked_add(length).ok_or(crate::WorkError::Overflow)?.min(logical_bytes);
            if start < prior_end { return Err(DemandError::InvalidRequest); }
            if start < end {
                ranges.push(HostDataRange { offset: start, length: end - start });
                prior_end = end;
            }
        }
        drop(output);
        work.release((count * element) as u64)?;
        Ok(ranges)
    }
}

pub fn allocated_data_ranges(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    allocated_data_ranges_platform(file, logical_bytes, maximum_ranges)
}

#[cfg(unix)]
fn allocated_data_ranges_platform(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    use std::os::fd::AsRawFd;

    let mut ranges = Vec::new();
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let offset_i64 = i64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset exceeds i64"))?;
        // SAFETY: the descriptor is borrowed from a live regular file and
        // `lseek` only updates that descriptor's current offset.
        let data = unsafe { libc::lseek(file.as_raw_fd(), offset_i64, libc::SEEK_DATA) };
        if data < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return Err(error);
        }
        // SAFETY: same live descriptor and bounded non-negative offset.
        let hole = unsafe { libc::lseek(file.as_raw_fd(), data, libc::SEEK_HOLE) };
        if hole < 0 {
            return Err(io::Error::last_os_error());
        }
        let data = u64::try_from(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative data offset"))?;
        let hole = u64::try_from(hole)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative hole offset"))?
            .min(logical_bytes);
        if hole <= data {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host sparse-range query made no progress",
            ));
        }
        if ranges.len() >= usize::try_from(maximum_ranges).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host file exceeds admitted sparse range count",
            ));
        }
        ranges.push(HostDataRange {
            offset: data,
            length: hole - data,
        });
        offset = hole;
    }
    Ok(ranges)
}

#[cfg(windows)]
fn allocated_data_ranges_platform(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    query_allocated_data_ranges(file, 0, logical_bytes, maximum_ranges)
}

#[cfg(windows)]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded native range query and validation"
)]
fn query_allocated_data_ranges(
    file: &cap_std::fs::File,
    offset: u64,
    length: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    use std::mem::size_of;
    use windows::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_MORE_DATA};
    use windows::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
    };

    if length == 0 {
        return Ok(Vec::new());
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range end overflow"))?;
    let max_capacity = usize::try_from(u64::from(maximum_ranges) + 1)
        .unwrap_or(usize::MAX)
        .min(u32::MAX as usize / size_of::<FILE_ALLOCATED_RANGE_BUFFER>());
    let mut output = vec![FILE_ALLOCATED_RANGE_BUFFER::default(); max_capacity.min(8)];
    let query = FILE_ALLOCATED_RANGE_BUFFER {
        FileOffset: i64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range offset exceeds i64"))?,
        Length: i64::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds i64"))?,
    };
    // SAFETY: the query is plain data, viewed as exactly its own bytes.
    let input = unsafe {
        std::slice::from_raw_parts(
            std::ptr::from_ref(&query).cast::<u8>(),
            size_of::<FILE_ALLOCATED_RANGE_BUFFER>(),
        )
    };
    let returned = loop {
        let output_bytes = output.len() * size_of::<FILE_ALLOCATED_RANGE_BUFFER>();
        // SAFETY: the ranges are plain data; the view spans exactly the
        // vector's initialized elements and is the only borrow of them.
        let output_view = unsafe {
            std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), output_bytes)
        };
        let result = acyclic_native_runtime::device_control_in_place(
            file,
            FSCTL_QUERY_ALLOCATED_RANGES,
            input,
            output_view,
        );
        match result {
            Ok(returned) => break returned,
            Err(error)
                if [ERROR_MORE_DATA, ERROR_INSUFFICIENT_BUFFER]
                    .iter()
                    .any(|code| error.raw_os_error() == i32::try_from(code.0).ok()) =>
            {
                if output.len() == max_capacity {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "host file exceeds admitted sparse range count",
                    ));
                }
                output.resize(
                    output.len().saturating_mul(2).min(max_capacity),
                    FILE_ALLOCATED_RANGE_BUFFER::default(),
                );
            }
            Err(error) => return Err(error),
        }
    };
    let count = Some(returned)
        .filter(|bytes| bytes % size_of::<FILE_ALLOCATED_RANGE_BUFFER>() == 0)
        .map(|bytes| bytes / size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid range response"))?;
    if count > usize::try_from(maximum_ranges).unwrap_or(usize::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host file exceeds admitted sparse range count",
        ));
    }
    output
        .into_iter()
        .take(count)
        .filter_map(|range| {
            let start = match u64::try_from(range.FileOffset) {
                Ok(start) => start.max(offset),
                Err(_) => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "negative range offset",
                    )));
                }
            };
            let Ok(size) = u64::try_from(range.Length) else {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "negative range length",
                )));
            };
            let range_end = match u64::try_from(range.FileOffset)
                .ok()
                .and_then(|base| base.checked_add(size))
            {
                Some(range_end) => range_end.min(end),
                None => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "range end overflow",
                    )));
                }
            };
            (start < range_end).then_some(Ok(HostDataRange {
                offset: start,
                length: range_end - start,
            }))
        })
        .collect()
}

#[cfg(not(any(unix, windows)))]
fn allocated_data_ranges_platform(
    _: &cap_std::fs::File,
    _: u64,
    _: u32,
) -> io::Result<Vec<HostDataRange>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "host sparse range discovery is unavailable",
    ))
}


#[cfg(windows)]
fn root_is_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
}

#[cfg(not(windows))]
fn root_is_reparse_point(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(all(test, target_os = "macos"))]
mod macos_metadata_tests {
    use super::{HostRoot, MacMetadataError};
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Path;
    use std::time::UNIX_EPOCH;

    #[test]
    fn clone_from_a_separate_held_source_is_cow_and_cannot_escape()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let view_path = temporary.path().join("view");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&view_path)?;
        std::fs::write(source_path.join("file"), b"original")?;
        let source = HostRoot::open(&source_path)?;
        let view = HostRoot::open(&view_path)?;
        if !view.clone_file_from(&source, Path::new("file"), Path::new("clone"))? {
            return Ok(()); // This volume does not support APFS cloning.
        }
        std::fs::write(source_path.join("file"), b"changed")?;
        assert_eq!(std::fs::read(view_path.join("clone"))?, b"original");
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("../view/clone"), Path::new("escape")),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        std::os::unix::fs::symlink(&view_path, source_path.join("pivot"))?;
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("pivot/clone"), Path::new("escape")),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        Ok(())
    }

    #[test]
    fn held_metadata_restores_fields_without_retargeting_a_replaced_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let original = root_path.join("file");
        std::fs::write(&original, b"old")?;
        let owner = std::fs::metadata(&original)?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_macos_metadata_target(Path::new("file"))?;
        let moved = root_path.join("moved");
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"replacement")?;

        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o4751),
            posix_uid: MetadataField::Value(owner.uid()),
            posix_gid: MetadataField::Value(owner.gid()),
            created_ns: MetadataField::Value(1_234_567_890_123_000_000),
            modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
            accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
            ..FileMetadata::default()
        };
        target.apply(metadata)?;
        let observed = std::fs::metadata(&moved)?;
        assert_eq!(observed.mode() & 0o7777, 0o4751);
        assert_eq!(observed.uid(), owner.uid());
        assert_eq!(observed.gid(), owner.gid());
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        assert_eq!(std::fs::read(&original)?, b"replacement");
        assert_ne!(std::fs::metadata(&original)?.mode() & 0o7777, 0o4751);
        assert!(
            root.open_macos_metadata_target(Path::new("../outside"))
                .is_err()
        );
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"untouched")?;
        std::os::unix::fs::symlink(temporary.path(), root_path.join("pivot"))?;
        assert!(
            root.open_macos_metadata_target(Path::new("pivot/outside"))
                .is_err()
        );
        assert_eq!(std::fs::read(outside)?, b"untouched");
        root.open_macos_metadata_target(Path::new("file"))?
            .apply(FileMetadata {
                posix_flags: MetadataField::Value(0),
                ..FileMetadata::default()
            })?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"))?
                .apply(FileMetadata {
                    posix_flags: MetadataField::Value(1),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported("posix_flags"))
        ));
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"))?
                .apply(FileMetadata {
                    changed_ns: MetadataField::Value(1),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported(
                "changed_ns without native-view baseline"
            ))
        ));
        Ok(())
    }

    #[test]
    fn fifo_metadata_uses_a_nonblocking_held_handle_and_socket_metadata_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;

        let temporary = tempfile::tempdir()?;
        let pipe = temporary.path().join("pipe");
        let name = std::ffi::CString::new(pipe.as_os_str().as_bytes())?;
        // SAFETY: `name` is a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let socket = temporary.path().join("socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
        let root = HostRoot::open(temporary.path())?;
        root.open_macos_metadata_target(Path::new("pipe"))?
            .apply(FileMetadata {
                posix_mode: MetadataField::Value(0o640),
                created_ns: MetadataField::Value(1_234_567_890_123_000_000),
                modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
                accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
                ..FileMetadata::default()
            })?;
        let observed = std::fs::symlink_metadata(&pipe)?;
        assert_eq!(observed.mode() & 0o7777, 0o640);
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        root.open_macos_metadata_target(Path::new("socket"))?
            .apply(FileMetadata::default())?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("socket"))?
                .apply(FileMetadata {
                    posix_mode: MetadataField::Value(0o640),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported("non-ordinary file kind"))
        ));
        let old_socket = temporary.path().join("old-socket");
        let pending = root.open_macos_metadata_target(Path::new("socket"))?;
        std::fs::rename(&socket, old_socket)?;
        let _replacement = std::os::unix::net::UnixListener::bind(&socket)?;
        assert!(matches!(
            pending.apply(FileMetadata::default()),
            Err(MacMetadataError::Io(_))
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn held_metadata_offload_does_not_stall_the_service_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::mpsc;
        use std::time::Duration;

        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let target = root.open_macos_metadata_target(Path::new("file"))?;
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = tokio::spawn(async move {
            acyclic_native_runtime::run_blocking_io(move || {
                assert!(started_tx.send(()).is_ok(), "test receiver remains live");
                let executor_progressed = release_rx.recv_timeout(Duration::from_secs(2)).is_ok();
                target.apply(FileMetadata::default())?;
                Ok::<_, MacMetadataError>(executor_progressed)
            })
            .await
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)))
            .await??;
        release_tx.send(())?;
        assert!(worker.await???);
        Ok(())
    }

    #[test]
    fn special_node_creation_stays_inside_the_held_root() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::FileTypeExt as _;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::os::unix::fs::symlink(&outside, root_path.join("pivot"))?;
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.create_fifo_held(Path::new("pivot/pipe"), 0o600)
                .is_err()
        );
        assert!(root.bind_unix_socket(Path::new("pivot/socket")).is_err());
        assert!(
            root.create_device_held(
                Path::new("pivot/device"),
                u32::from(libc::S_IFCHR) | 0o600,
                0,
            )
            .is_err()
        );
        assert!(std::fs::read_dir(&outside)?.next().is_none());

        root.create_fifo_held(Path::new("pipe"), 0o600)?;
        root.bind_unix_socket(Path::new("socket"))?;
        assert!(
            std::fs::symlink_metadata(root_path.join("pipe"))?
                .file_type()
                .is_fifo()
        );
        assert!(
            std::fs::symlink_metadata(root_path.join("socket"))?
                .file_type()
                .is_socket()
        );
        Ok(())
    }
}

#[cfg(all(test, windows))]
mod yielding_tests {
    #[test]
    fn a_rejected_oplock_request_drops_without_waiting() -> std::io::Result<()> {
        // NUL accepts a file handle but rejects FSCTL_REQUEST_OPLOCK. Its
        // request event remains unsignaled, exposing accidental waits on a
        // request that was never pending.
        let file = super::YieldingFile::new(std::fs::File::open("NUL")?)?;
        assert!(file.held.request.is_null());
        drop(file);
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::HostRoot;
    use std::io::Read;
    use std::path::Path;

    /// Only a local root may serve host I/O inline on a native callback
    /// thread; every other stays bounded by the callback's timeout.
    #[test]
    fn a_local_root_is_classified_local() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        assert!(HostRoot::open(temporary.path())?.is_local());
        #[cfg(target_os = "linux")]
        assert!(super::REMOTE_FILESYSTEMS.contains(&0x6969));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn capability_reads_do_not_change_owned_access_times() -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        use std::time::{Duration, UNIX_EPOCH};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().to_path_buf();
        let directory = temporary.path().join("directory");
        std::fs::create_dir(&directory)?;
        let file = directory.join("file");
        std::fs::write(&file, b"body")?;
        let old = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for path in [&file, &directory, &root_path] {
            std::fs::File::open(path)?.set_times(std::fs::FileTimes::new().set_accessed(old))?;
        }

        let root = HostRoot::open(temporary.path())?;
        let mut body = String::new();
        root.open_file(Path::new("directory/file"))?
            .read_to_string(&mut body)?;
        assert_eq!(body, "body");
        let held = root.open_dir_held(Path::new("directory"))?;
        assert_eq!(HostRoot::scan_held_dir(&held)?.count(), 1);
        let root_directory = root.open_dir_held(Path::new(""))?;
        assert_eq!(HostRoot::scan_held_dir(&root_directory)?.count(), 1);
        for path in [&file, &directory, &root_path] {
            assert_eq!(std::fs::metadata(path)?.atime(), 1_700_000_000);
        }
        Ok(())
    }

    #[test]
    fn held_root_rejects_intermediate_symlink_escape_for_reads_and_writes() -> std::io::Result<()> {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside_path)?;
        std::fs::write(outside_path.join("secret"), b"outside")?;
        symlink(&outside_path, root_path.join("pivot"))?;

        let root = HostRoot::open(&root_path)?;
        assert!(root.open_file(Path::new("pivot/secret")).is_err());
        assert!(
            root.symlink_metadata_held(Path::new("pivot/secret"))
                .is_err()
        );
        assert!(root.create_file(Path::new("pivot/created")).is_err());
        assert!(!outside_path.join("created").exists());

        let mut secret = String::new();
        std::fs::File::open(outside_path.join("secret"))?.read_to_string(&mut secret)?;
        assert_eq!(secret, "outside");
        Ok(())
    }

    #[test]
    fn held_special_metadata_rejects_intermediate_symlink_escape() -> std::io::Result<()> {
        use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _, symlink};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside_path)?;
        let target = outside_path.join("target");
        std::fs::write(&target, b"outside")?;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))?;
        symlink(&outside_path, root_path.join("pivot"))?;

        let root = HostRoot::open(&root_path)?;
        assert!(
            root.set_permissions_without_open(Path::new("pivot/target"), 0o600)
                .is_err()
        );
        assert!(
            root.create_fifo_held(Path::new("pivot/created"), 0o600)
                .is_err()
        );
        assert!(!outside_path.join("created").exists());
        root.create_fifo_held(Path::new("fifo"), 0o600)?;
        assert!(
            std::fs::symlink_metadata(root_path.join("fifo"))?
                .file_type()
                .is_fifo()
        );
        assert_eq!(
            std::fs::metadata(&target)?.permissions().mode() & 0o777,
            0o644
        );
        assert_eq!(std::fs::read(target)?, b"outside");
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn held_linux_metadata_targets_inode_across_rename_and_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let original = root_path.join("original");
        let moved = root_path.join("moved");
        std::fs::write(&original, b"old")?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_linux_metadata_target(Path::new("original"))?;
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"new")?;
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o644))?;
        let owner = std::fs::metadata(&moved)?;
        let original_mode = owner.permissions().mode() & 0o7777;
        let atime_ns = 1_700_000_000_123_456_789_i64;
        let mtime_ns = 1_700_000_001_987_654_321_i64;
        let representable = FileMetadata {
            posix_mode: MetadataField::Value(0o600),
            posix_uid: MetadataField::Value(owner.uid()),
            posix_gid: MetadataField::Value(owner.gid()),
            accessed_ns: MetadataField::Value(atime_ns),
            modified_ns: MetadataField::Value(mtime_ns),
            ..FileMetadata::default()
        };
        assert!(matches!(
            target.apply(FileMetadata {
                created_ns: MetadataField::Value(12),
                ..representable
            }),
            Err(super::LinuxMetadataError::Unsupported("created_ns"))
        ));
        assert_eq!(
            std::fs::metadata(&moved)?.permissions().mode() & 0o7777,
            original_mode
        );
        target.apply(representable)?;
        let old = std::fs::metadata(&moved)?;
        assert_eq!(old.permissions().mode() & 0o777, 0o600);
        assert_eq!(old.atime() * 1_000_000_000 + old.atime_nsec(), atime_ns);
        assert_eq!(old.mtime() * 1_000_000_000 + old.mtime_nsec(), mtime_ns);
        assert_eq!(std::fs::read(&moved)?, b"old");
        assert_eq!(std::fs::read(&original)?, b"new");
        assert_eq!(
            std::fs::metadata(&original)?.permissions().mode() & 0o777,
            0o644
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn held_linux_symlink_metadata_does_not_mutate_target() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::unix::fs::{MetadataExt as _, symlink};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"outside")?;
        let link = root_path.join("link");
        symlink(&outside, &link)?;
        let outside_before = std::fs::metadata(&outside)?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_linux_metadata_target(Path::new("link"))?;
        let desired = 1_700_000_000_456_789_123_i64;
        target.apply(FileMetadata {
            modified_ns: MetadataField::Value(desired),
            ..FileMetadata::default()
        })?;
        let link_after = std::fs::symlink_metadata(&link)?;
        let outside_after = std::fs::metadata(&outside)?;
        assert_eq!(
            link_after.mtime() * 1_000_000_000 + link_after.mtime_nsec(),
            desired
        );
        assert_eq!(outside_after.mtime(), outside_before.mtime());
        assert_eq!(outside_after.mtime_nsec(), outside_before.mtime_nsec());
        assert_eq!(std::fs::read(outside)?, b"outside");
        Ok(())
    }

    #[test]
    fn root_symbolic_link_is_never_admitted() -> std::io::Result<()> {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir()?;
        let real = temporary.path().join("real");
        let link = temporary.path().join("link");
        std::fs::create_dir(&real)?;
        symlink(&real, &link)?;
        assert!(HostRoot::open(&link).is_err());
        Ok(())
    }

    #[test]
    fn bound_unix_socket_lands_inside_the_held_root() -> std::io::Result<()> {
        use std::os::unix::fs::FileTypeExt;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("nested"))?;
        let root = HostRoot::open(temporary.path())?;
        root.bind_unix_socket(Path::new("root.sock"))?;
        root.bind_unix_socket(Path::new("nested/child.sock"))?;
        for bound in ["root.sock", "nested/child.sock"] {
            let file_type = std::fs::symlink_metadata(temporary.path().join(bound))?.file_type();
            assert!(file_type.is_socket(), "{bound} is not a socket");
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn bound_unix_socket_rejects_paths_beyond_sun_path() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = HostRoot::open(temporary.path())?;
        let name = format!("{}.sock", "n".repeat(128));
        match root.bind_unix_socket(Path::new(&name)) {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput),
            Ok(()) => return Err(std::io::Error::other("over-long socket path was admitted")),
        }
        Ok(())
    }
}

#[cfg(all(test, windows))]
mod windows_clone_tests {
    use super::{
        HostRoot, UNIX_EPOCH_FILETIME, WindowsMetadataError, allocated_data_ranges,
        filetime_to_unix_nanoseconds, unix_nanoseconds_to_filetime,
    };
    use acyclic_native_runtime::{Durability, NativeFile, OwnedWrite};
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn filetime_conversions_round_trip(nanoseconds in any::<i64>(), ticks in any::<i64>()) {
            let tick = unix_nanoseconds_to_filetime(nanoseconds);
            prop_assert_eq!(
                filetime_to_unix_nanoseconds(tick),
                nanoseconds.checked_sub(nanoseconds.rem_euclid(100))
            );
            match filetime_to_unix_nanoseconds(ticks) {
                Some(exact) => prop_assert_eq!(unix_nanoseconds_to_filetime(exact), ticks),
                None => prop_assert!(
                    i128::from(ticks) - i128::from(UNIX_EPOCH_FILETIME) > i128::from(i64::MAX / 100)
                        || i128::from(ticks) - i128::from(UNIX_EPOCH_FILETIME)
                            < i128::from(i64::MIN / 100)
                ),
            }
        }
    }

    #[test]
    fn filetime_conversions_cover_their_extremes() {
        assert_eq!(unix_nanoseconds_to_filetime(0), UNIX_EPOCH_FILETIME);
        assert_eq!(unix_nanoseconds_to_filetime(-1), UNIX_EPOCH_FILETIME - 1);
        assert_eq!(unix_nanoseconds_to_filetime(99), UNIX_EPOCH_FILETIME);
        // Rounding the earliest nanosecond down to a tick leaves the range.
        assert_eq!(
            filetime_to_unix_nanoseconds(unix_nanoseconds_to_filetime(i64::MIN)),
            None
        );
        assert_eq!(filetime_to_unix_nanoseconds(i64::MAX), None);
        assert_eq!(filetime_to_unix_nanoseconds(i64::MIN), None);
        // 1601 itself precedes the earliest `i64` nanosecond (1677).
        assert_eq!(filetime_to_unix_nanoseconds(0), None);
        assert!(
            super::metadata_time(crate::kernel::MetadataField::Value(150), 7).is_err(),
            "exact writers refuse instants between ticks"
        );
    }
    use bytes::Bytes;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    #[tokio::test]
    async fn held_async_copy_preserves_source_hardlinks_and_rejects_escape() -> std::io::Result<()>
    {
        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let destination_path = temporary.path().join("destination");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&destination_path)?;
        std::fs::create_dir(&outside_path)?;
        std::fs::write(source_path.join("file"), b"source contents")?;
        std::fs::hard_link(source_path.join("file"), source_path.join("alias"))?;
        std::fs::write(outside_path.join("untouched"), b"outside")?;
        let source = HostRoot::open(&source_path)?;
        let destination = HostRoot::open(&destination_path)?;
        destination
            .copy_file_from(&source, Path::new("alias"), Path::new("copy"))
            .await?;
        assert_eq!(
            std::fs::read(destination_path.join("copy"))?,
            b"source contents"
        );
        std::fs::write(destination_path.join("copy"), b"changed")?;
        assert_eq!(std::fs::read(source_path.join("file"))?, b"source contents");
        assert_eq!(
            std::fs::read(source_path.join("alias"))?,
            b"source contents"
        );

        assert!(
            destination
                .copy_file_from(
                    &source,
                    Path::new("../outside/untouched"),
                    Path::new("escape")
                )
                .await
                .is_err()
        );
        match std::os::windows::fs::symlink_dir(&outside_path, source_path.join("pivot")) {
            Ok(()) => assert!(
                destination
                    .copy_file_from(&source, Path::new("pivot/untouched"), Path::new("escape"))
                    .await
                    .is_err()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        assert!(!destination_path.join("escape").exists());
        assert_eq!(std::fs::read(outside_path.join("untouched"))?, b"outside");
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_async_copy_leaves_no_partial_output() -> std::io::Result<()> {
        use std::time::Duration;

        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let destination_path = temporary.path().join("destination");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&destination_path)?;
        let source_file = std::fs::File::create(source_path.join("large"))?;
        let length = 128 * 1024 * 1024;
        source_file.set_len(length)?;
        let source = HostRoot::open(&source_path)?;
        let destination = HostRoot::open(&destination_path)?;
        let task = tokio::spawn(async move {
            destination
                .copy_file_from(&source, Path::new("large"), Path::new("copy"))
                .await
        });
        let copy_path = destination_path.join("copy");
        let staged = || -> std::io::Result<bool> {
            for entry in std::fs::read_dir(&destination_path)? {
                if entry?
                    .file_name()
                    .to_string_lossy()
                    .starts_with(super::STAGING_PREFIX)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        // Every observation of the destination name, before and after the
        // cancellation, finds it absent or complete.
        let absent_or_complete = || match std::fs::metadata(&copy_path) {
            Ok(metadata) => assert_eq!(metadata.len(), length, "partial destination"),
            // Windows briefly denies metadata access while the copier's
            // handle on the just-renamed file closes; that observes nothing.
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ),
                "{error}"
            ),
        };
        while !staged()? && !task.is_finished() {
            absent_or_complete();
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        // The detached owner always finishes: it publishes the complete copy
        // or removes the staged bytes, and then no staged name remains.
        while staged()? {
            absent_or_complete();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        absent_or_complete();
        Ok(())
    }

    #[test]
    fn by_name_and_listed_stats_report_exactly_what_a_handle_query_does() -> std::io::Result<()> {
        use super::HostStat;
        use std::io::Write as _;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        std::fs::write(temporary.path().join("directory").join("file"), b"payload")?;
        std::fs::hard_link(
            temporary.path().join("directory").join("file"),
            temporary.path().join("alias"),
        )?;
        // Growing the file through one link leaves the other link's index
        // record stale, and an open writer's record is stale until it closes.
        std::fs::OpenOptions::new()
            .append(true)
            .open(temporary.path().join("directory").join("file"))?
            .write_all(b" grown through one link")?;
        let mut writer = std::fs::File::create(temporary.path().join("open"))?;
        writer.write_all(b"written, never closed")?;
        let root = HostRoot::open(temporary.path())?;
        let same = |fast: &HostStat, held: &HostStat| {
            assert_eq!(fast.file_type(), held.file_type());
            assert_eq!(fast.len(), held.len());
            assert_eq!(fast.file_attributes(), held.file_attributes());
            assert_eq!(fast.creation_time(), held.creation_time());
            assert_eq!(fast.last_write_time(), held.last_write_time());
            assert_eq!(fast.volume_serial_number(), held.volume_serial_number());
            assert_eq!(fast.file_index(), held.file_index());
            assert_eq!(fast.number_of_links(), held.number_of_links());
            assert!(fast.number_of_links().is_some());
            assert_eq!(fast.created()?, held.created()?);
            assert_eq!(fast.modified()?, held.modified()?);
            assert_eq!(fast.accessed()?, held.accessed()?);
            Ok::<(), std::io::Error>(())
        };
        for path in [
            Path::new("directory"),
            Path::new("directory/file"),
            Path::new("DIRECTORY/FILE"),
        ] {
            let fast = root
                .stat_by_name(path)?
                .ok_or_else(|| std::io::Error::other("a plain path is answered by name"))?;
            let held = HostStat::from_metadata(&root.symlink_metadata(path)?)?;
            same(&fast, &held)?;
            // The held walk's by-name open reads exactly what the walk does.
            same(
                &fast,
                &HostStat::from_metadata(&root.symlink_metadata_held(path)?)?,
            )?;
        }
        // Every enumerated name carries exactly the facts its own stat
        // reports, except a reparse point, which only its own stat resolves.
        for directory in [Path::new(""), Path::new("directory")] {
            let mut names = Vec::new();
            for entry in root.read_dir_stats(directory)? {
                let entry = entry?;
                let path = directory.join(&entry.name);
                let listed = entry.stat.ok_or_else(|| {
                    std::io::Error::other("a plain entry is listed with its facts")
                })?;
                same(&listed, &root.stat(&path)?)?;
                if listed.file_type().is_file() {
                    same(
                        &listed,
                        &root.stat_file(&root.open_file(&path)?.into_std())?,
                    )?;
                }
                names.push(entry.name);
            }
            names.sort();
            let expected: &[&str] = if directory.as_os_str().is_empty() {
                &["alias", "directory", "open"]
            } else {
                &["file"]
            };
            assert_eq!(names, expected);
        }
        assert_eq!(
            root.stat(Path::new("directory/missing"))
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound)
        );
        // Only the held walk may resolve a reparse point, final or not.
        assert!(
            root.stat_by_name(Path::new(""))
                .map(|stat| stat.is_none())?
        );
        match std::os::windows::fs::symlink_dir(
            temporary.path().join("directory"),
            temporary.path().join("link"),
        ) {
            Ok(()) => {
                assert!(root.stat_by_name(Path::new("link"))?.is_none());
                assert!(root.stat_by_name(Path::new("link/file"))?.is_none());
                assert!(root.stat(Path::new("link"))?.file_type().is_symlink());
                let listed = root
                    .read_dir_stats(Path::new(""))?
                    .find(|entry| entry.as_ref().is_ok_and(|entry| entry.name == "link"))
                    .ok_or_else(|| std::io::Error::other("link is listed"))??;
                assert!(
                    listed.stat.is_none(),
                    "a reparse point is stat'ed on its own"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    #[test]
    fn a_file_opened_by_name_is_the_one_the_held_walk_opens() -> std::io::Result<()> {
        use std::io::Read as _;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        std::fs::write(temporary.path().join("directory").join("file"), b"payload")?;
        let root = HostRoot::open(temporary.path())?;
        for path in [Path::new("directory/file"), Path::new("DIRECTORY/FILE")] {
            let mut opened = root
                .open_file_by_name(path, super::FileReads::Cursor)?
                .ok_or_else(|| std::io::Error::other("a plain path is opened by name"))?;
            let held = root.symlink_metadata(path)?;
            let metadata = opened.metadata()?;
            assert_eq!(
                cap_primitives::fs::_WindowsByHandle::file_index(&metadata),
                cap_primitives::fs::_WindowsByHandle::file_index(&held)
            );
            let mut read = Vec::new();
            opened.read_to_end(&mut read)?;
            assert_eq!(read, b"payload");
        }
        assert!(
            root.open_file_by_name(Path::new(""), super::FileReads::Cursor)?
                .is_none()
        );
        assert!(
            root.open_file_by_name(Path::new("directory"), super::FileReads::Cursor)
                .is_err()
        );
        assert_eq!(
            root.open_file(Path::new("directory/missing"))
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound)
        );
        // Only the held walk may resolve a reparse point on the way.
        match std::os::windows::fs::symlink_dir(
            temporary.path().join("directory"),
            temporary.path().join("link"),
        ) {
            Ok(()) => assert!(
                root.open_file_by_name(Path::new("link/file"), super::FileReads::Cursor)?
                    .is_none()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// Names the directory [`staged_copy_crash_child`] stages into.
    const CRASH_DIRECTORY: &str = "ACYCLIC_STAGED_COPY_CRASH_DIRECTORY";

    /// Stages written bytes, then dies without unwinding, as a process
    /// killed mid-copy does. Runs only as [`a_process_dying_mid_copy_leaves_nothing`]'s
    /// child.
    #[test]
    #[ignore = "runs only as a child of a_process_dying_mid_copy_leaves_nothing"]
    fn staged_copy_crash_child() -> std::io::Result<()> {
        let Some(directory) = std::env::var_os(CRASH_DIRECTORY) else {
            return Ok(());
        };
        let staged = super::StagedWindowsFile::create(cap_std::fs::Dir::open_ambient_dir(
            directory,
            cap_std::ambient_authority(),
        )?)?;
        staged.open_writer(false)?.write_all(b"partial copy")?;
        std::process::abort();
    }

    #[test]
    fn a_process_dying_mid_copy_leaves_nothing() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let status = acyclic_native_runtime::process_status(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "native_host::windows_clone_tests::staged_copy_crash_child",
                    "--test-threads=1",
                ])
                .env(CRASH_DIRECTORY, temporary.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null()),
            std::time::Duration::from_secs(120),
        )?;
        assert!(!status.success(), "the child must die mid-copy");
        let names = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert!(names.is_empty(), "a crashed copy left {names:?}");
        Ok(())
    }

    #[test]
    fn a_published_copy_keeps_only_its_final_name() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let parent =
            cap_std::fs::Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let staged = super::StagedWindowsFile::create(parent)?;
        staged.open_writer(false)?.write_all(b"complete copy")?;
        staged.publish(std::ffi::OsStr::new("copy"))?;
        assert_eq!(
            staged
                .publish(std::ffi::OsStr::new("copy"))
                .map_err(|error| error.kind()),
            Err(std::io::ErrorKind::AlreadyExists),
            "publishing never replaces an existing name"
        );
        drop(staged);
        let names = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(names, ["copy"]);
        assert_eq!(
            std::fs::read(temporary.path().join("copy"))?,
            b"complete copy"
        );
        Ok(())
    }

    #[test]
    fn a_staged_name_goes_while_another_process_holds_the_file() -> std::io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };

        let temporary = tempfile::tempdir()?;
        let parent =
            cap_std::fs::Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let staged = super::StagedWindowsFile::create(parent)?;
        // A scanner opening the staged file shares everything, as this does.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
            .open(temporary.path().join(&staged.name))?;
        drop(staged);
        let names = std::fs::read_dir(temporary.path())?.count();
        drop(held);
        assert_eq!(names, 0, "the staged name outlived its guard");
        Ok(())
    }

    #[test]
    fn hydrating_a_placeholder_leaves_its_attributes_the_same() {
        use super::same_windows_attributes;
        // Recall on data access, then archive once hydrated.
        assert!(same_windows_attributes(0x0040_0000, 0x0000_0020));
        assert!(same_windows_attributes(0x0040_0001, 0x0000_0021));
        // Read-only set meanwhile is a change.
        assert!(!same_windows_attributes(0x0040_0000, 0x0000_0021));
        // Archive alone changing on a hydrated file is a change.
        assert!(!same_windows_attributes(0x0000_0000, 0x0000_0020));
        // Made ordinary: reparse point and sparse (ReFS) marks drop.
        assert!(same_windows_attributes(0x0000_0420, 0x0000_0020));
        assert!(same_windows_attributes(0x0000_0220, 0x0000_0020));
        // Read-only set meanwhile is still a change.
        assert!(!same_windows_attributes(0x0000_0420, 0x0000_0021));
        // An ordinary sparse file's archive bit is its own.
        assert!(!same_windows_attributes(0x0000_0220, 0x0000_0200));
        // Becoming sparse is a change.
        assert!(!same_windows_attributes(0x0000_0020, 0x0000_0220));
    }

    #[test]
    fn a_name_deleted_while_held_open_is_not_found() -> std::io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Storage::FileSystem::{
            DELETE, FILE_DISPOSITION_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            FileDispositionInfo, SetFileInformationByHandle,
        };

        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("pending");
        std::fs::write(&path, b"held")?;
        let share = (FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0;
        // A scanner holding the file keeps its deleted name listed.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(share)
            .open(&path)?;
        let deleting = std::fs::OpenOptions::new()
            .access_mode(DELETE.0)
            .share_mode(share)
            .open(&path)?;
        let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
        // SAFETY: a live handle and a correctly sized input structure.
        unsafe {
            SetFileInformationByHandle(
                windows::Win32::Foundation::HANDLE(deleting.as_raw_handle()),
                FileDispositionInfo,
                (&raw const disposition).cast(),
                u32::try_from(std::mem::size_of::<FILE_DISPOSITION_INFO>()).unwrap_or(u32::MAX),
            )
        }?;
        drop(deleting);
        assert_eq!(std::fs::read_dir(temporary.path())?.count(), 1);
        let root = HostRoot::open(temporary.path())?;
        let stat = root.stat(Path::new("pending"));
        drop(held);
        assert_eq!(
            stat.map(|_| ()).map_err(|error| error.kind()),
            Err(std::io::ErrorKind::NotFound)
        );
        Ok(())
    }

    #[tokio::test]
    async fn copy_never_replaces_an_existing_destination() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("source"), b"new")?;
        std::fs::write(temporary.path().join("existing"), b"old")?;
        let root = HostRoot::open(temporary.path())?;
        let copied = root
            .copy_file_from(&root, Path::new("source"), Path::new("existing"))
            .await;
        assert_eq!(
            copied.map_err(|error| error.kind()),
            Err(std::io::ErrorKind::AlreadyExists),
            "an existing destination is never replaced"
        );
        assert_eq!(std::fs::read(temporary.path().join("existing"))?, b"old");
        let mut names = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.sort();
        assert_eq!(
            names,
            ["existing", "source"],
            "a staged copy was left behind"
        );
        Ok(())
    }

    #[test]
    fn held_windows_metadata_applies_and_verifies_file_and_directory_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const FIXED_NS: i64 = 1_700_000_000_000_000_000;
        const ARCHIVE: u32 = 0x20;
        const DIRECTORY: u32 = 0x10;
        const HIDDEN: u32 = 0x2;
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"payload")?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        let root = HostRoot::open(temporary.path())?;
        for (path, attributes) in [
            (Path::new("file"), ARCHIVE | HIDDEN),
            (Path::new("directory"), DIRECTORY | HIDDEN),
        ] {
            root.open_windows_metadata_target(path)?
                .apply(FileMetadata {
                    windows_attributes: MetadataField::Value(attributes),
                    created_ns: MetadataField::Value(FIXED_NS),
                    modified_ns: MetadataField::Value(FIXED_NS + 100),
                    accessed_ns: MetadataField::Value(FIXED_NS + 200),
                    changed_ns: MetadataField::Value(FIXED_NS + 300),
                    ..FileMetadata::default()
                })?;
            assert_eq!(
                std::fs::symlink_metadata(temporary.path().join(path))?.file_attributes(),
                attributes
            );
        }
        let file = temporary.path().join("file");
        let mut permissions = std::fs::metadata(&file)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&file, permissions.clone())?;
        root.open_windows_metadata_target(Path::new("file"))?
            .apply(FileMetadata::default())?;
        root.open_windows_metadata_target(Path::new("file"))?
            .apply(FileMetadata {
                windows_attributes: MetadataField::Value(ARCHIVE),
                ..FileMetadata::default()
            })?;
        Ok(())
    }

    #[test]
    fn held_windows_metadata_rejects_unrepresentable_fields_and_path_escape() -> std::io::Result<()>
    {
        use crate::kernel::{FileMetadata, MetadataField};

        const REPARSE_POINT: u32 = 0x400;
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        std::fs::write(root_path.join("file"), b"inside")?;
        std::fs::write(temporary.path().join("outside"), b"outside")?;
        let root = HostRoot::open(&root_path)?;
        assert!(matches!(
            root.open_windows_metadata_target(Path::new("file"))
                .map_err(std::io::Error::other)?
                .apply(FileMetadata {
                    windows_attributes: MetadataField::Value(REPARSE_POINT),
                    ..FileMetadata::default()
                }),
            Err(WindowsMetadataError::Unsupported("Windows attributes"))
        ));
        assert!(matches!(
            root.open_windows_metadata_target(Path::new("file"))
                .map_err(std::io::Error::other)?
                .apply(FileMetadata {
                    named_attributes: MetadataField::Value(crate::ObjectId {
                        kind: crate::ObjectKind::AttributePage,
                        digest: crate::Digest::from_bytes([7; 32]),
                    }),
                    ..FileMetadata::default()
                }),
            Err(WindowsMetadataError::Unsupported("named attributes"))
        ));
        assert!(
            root.open_windows_metadata_target(Path::new("../outside"))
                .is_err()
        );
        assert_eq!(std::fs::read(temporary.path().join("outside"))?, b"outside");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
        use std::os::windows::fs::MetadataExt as _;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("target"), b"outside")?;
        match std::os::windows::fs::symlink_dir(&outside, root_path.join("pivot")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error),
        }
        let before = std::fs::metadata(outside.join("target"))?.file_attributes();
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.open_windows_metadata_target(Path::new("pivot/target"))
                .is_err()
        );
        assert_eq!(
            std::fs::metadata(outside.join("target"))?.file_attributes(),
            before
        );
        assert_eq!(std::fs::read(outside.join("target"))?, b"outside");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_updates_final_reparse_point_without_touching_target()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const FIXED_NS: i64 = 1_700_000_000_000_000_000;
        const HIDDEN: u32 = 0x2;
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let target = root_path.join("target");
        let link = root_path.join("link");
        std::fs::write(&target, b"target")?;
        match std::os::windows::fs::symlink_file(&target, &link) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let target_attributes = std::fs::symlink_metadata(&target)?.file_attributes();
        let link_attributes = std::fs::symlink_metadata(&link)?.file_attributes();
        let root = HostRoot::open(&root_path)?;
        root.open_windows_metadata_target(Path::new("link"))?
            .apply(FileMetadata {
                windows_attributes: MetadataField::Value(link_attributes | HIDDEN),
                created_ns: MetadataField::Value(FIXED_NS),
                modified_ns: MetadataField::Value(FIXED_NS + 100),
                accessed_ns: MetadataField::Value(FIXED_NS + 200),
                changed_ns: MetadataField::Value(FIXED_NS + 300),
                ..FileMetadata::default()
            })?;
        assert_eq!(
            std::fs::symlink_metadata(&link)?.file_attributes(),
            link_attributes | HIDDEN
        );
        assert_eq!(
            std::fs::symlink_metadata(&target)?.file_attributes(),
            target_attributes
        );
        assert_eq!(std::fs::read(&target)?, b"target");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_targets_leaf_across_rename_and_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const HIDDEN: u32 = 0x2;
        const MODIFIED_NS: i64 = 1_700_000_000_000_000_000;
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        let moved = temporary.path().join("moved");
        std::fs::write(&original, b"old")?;
        let root = HostRoot::open(temporary.path())?;
        let target = root.open_windows_metadata_target(Path::new("original"))?;
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"new")?;
        let replacement_metadata = std::fs::metadata(&original)?;
        let replacement_attributes = replacement_metadata.file_attributes();
        let replacement_modified = replacement_metadata.last_write_time();
        let moved_attributes = std::fs::metadata(&moved)?.file_attributes();
        target.apply(FileMetadata {
            windows_attributes: MetadataField::Value(moved_attributes | HIDDEN),
            modified_ns: MetadataField::Value(MODIFIED_NS),
            ..FileMetadata::default()
        })?;
        assert_eq!(
            std::fs::metadata(&moved)?.file_attributes(),
            moved_attributes | HIDDEN
        );
        assert_eq!(
            std::fs::metadata(&original)?.file_attributes(),
            replacement_attributes
        );
        assert_eq!(
            std::fs::metadata(&moved)?.last_write_time(),
            u64::try_from(unix_nanoseconds_to_filetime(MODIFIED_NS))?
        );
        assert_eq!(
            std::fs::metadata(&original)?.last_write_time(),
            replacement_modified
        );
        assert_eq!(std::fs::read(&moved)?, b"old");
        assert_eq!(std::fs::read(&original)?, b"new");
        Ok(())
    }

    #[tokio::test]
    async fn capability_opened_overlapped_file_runs_native_io_without_reopen() -> std::io::Result<()>
    {
        let directory = tempfile::tempdir()?;
        let root = HostRoot::open(directory.path())?;
        let path = Path::new("overlapped.bin");
        let file = root.create_overlapped_file(path)?;
        // SAFETY: HostRoot used FILE_FLAG_OVERLAPPED and this new handle is
        // transferred directly, with no other I/O or completion-port owner.
        let native = unsafe { NativeFile::from_overlapped_file_unchecked(file)? };
        native.set_len_async(8).await?;
        native
            .write_all_batch_async(vec![OwnedWrite {
                offset: 2,
                bytes: Bytes::from_static(b"abc"),
            }])
            .await?;
        native.sync_async(Durability::Full).await?;
        assert_eq!(
            std::fs::read(directory.path().join(path))?,
            b"\0\0abc\0\0\0"
        );
        Ok(())
    }

    #[test]
    fn held_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("secret"), b"outside")?;
        match std::os::windows::fs::symlink_dir(&outside, root_path.join("pivot")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error),
        }
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.symlink_metadata_held(Path::new("pivot/secret"))
                .is_err()
        );
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn block_clone_preserves_cow_or_leaves_no_destination() -> std::io::Result<()> {
        let parent = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT");
        let require_clone = parent.is_some();
        let directory = if let Some(parent) = parent {
            tempfile::Builder::new()
                .prefix("acyclic-clone-")
                .tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: this synchronous control takes no buffers and the source
        // handle remains live for the entire call.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        source.set_len(1024 * 1024)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(1024 * 1024 - 4096))?;
        source.write_all(&[0xA5; 4096])?;
        source.sync_all()?;
        let cloned = root.clone_file(source_path, target_path)?;
        let capabilities = crate::probe_native_storage_capabilities(directory.path())
            .map_err(std::io::Error::other)?;
        if capabilities.block_cloning || require_clone {
            assert!(cloned, "advertised block cloning rejected an aligned file");
        }
        if cloned {
            let mut target = root.open_file(target_path)?;
            let mut byte = [0];
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
            target.seek(SeekFrom::Start(512 * 1024))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0]);
            target.seek(SeekFrom::Start(1024 * 1024 - 1))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
            source.seek(SeekFrom::Start(0))?;
            source.write_all(&[0x5A])?;
            source.sync_all()?;
            target.seek(SeekFrom::Start(0))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
        } else {
            assert!(root.symlink_metadata(target_path).is_err());
        }
        Ok(())
    }

    #[test]
    fn block_clone_accepts_unaligned_file_lengths() -> std::io::Result<()> {
        let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
            return Ok(());
        };
        let directory = tempfile::Builder::new()
            .prefix("acyclic-unaligned-clone-")
            .tempdir_in(parent)?;
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: no buffers are passed and the source handle stays live.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        source.set_len(1024 * 1024 + 17)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(1024 * 1024))?;
        source.write_all(&[0x5A; 17])?;
        source.sync_all()?;
        let cloned = root.clone_file(source_path, target_path)?;
        assert!(
            cloned,
            "advertised block cloning rejected an unaligned file"
        );
        let mut target = root.open_file(target_path)?;
        let ranges = allocated_data_ranges(&target, 1024 * 1024 + 17, 3)?;
        assert_eq!(ranges.len(), 2, "clone must preserve the sparse middle");
        target.seek(SeekFrom::Start(1024 * 1024))?;
        let mut tail = [0; 17];
        target.read_exact(&mut tail)?;
        assert_eq!(tail, [0x5A; 17]);
        source.seek(SeekFrom::Start(1024 * 1024))?;
        source.write_all(&[0x11; 17])?;
        source.sync_all()?;
        target.seek(SeekFrom::Start(1024 * 1024))?;
        target.read_exact(&mut tail)?;
        assert_eq!(tail, [0x5A; 17]);
        Ok(())
    }

    #[test]
    fn block_clone_spans_multiple_native_requests() -> std::io::Result<()> {
        let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
            return Ok(());
        };
        let directory = tempfile::Builder::new()
            .prefix("acyclic-large-clone-")
            .tempdir_in(parent)?;
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: no buffers are passed and the source handle stays live.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        let second_chunk = 1024_u64 * 1024 * 1024;
        let length = second_chunk + 64 * 1024 + 17;
        source.set_len(length)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(second_chunk))?;
        source.write_all(&[0x5A; 4096])?;
        source.seek(SeekFrom::Start(length - 17))?;
        source.write_all(&[0x11; 17])?;
        source.sync_all()?;
        assert!(root.clone_file(source_path, target_path)?);
        let mut target = root.open_file(target_path)?;
        target.seek(SeekFrom::Start(second_chunk))?;
        let mut byte = [0];
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0x5A]);
        target.seek(SeekFrom::Start(length - 1))?;
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0x11]);
        let ranges = allocated_data_ranges(&target, length, 4)?;
        assert_eq!(ranges.len(), 3, "multi-request clone must preserve holes");
        Ok(())
    }

    #[test]
    fn sparse_range_query_grows_only_when_ranges_require_it() -> std::io::Result<()> {
        let directory = tempfile::tempdir()?;
        let root = HostRoot::open(directory.path())?;
        let path = Path::new("ranges.bin");
        let mut file = root.create_file(path)?;
        // SAFETY: the control has no buffers and the file handle remains live.
        unsafe {
            DeviceIoControl(
                HANDLE(file.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        file.set_len(20 * 1024 * 1024)?;
        for index in 0..10_u64 {
            file.seek(SeekFrom::Start(index * 2 * 1024 * 1024))?;
            file.write_all(&[0xA5; 4096])?;
        }
        file.sync_all()?;
        let source = root.open_file(path)?;
        assert!(allocated_data_ranges(&source, 20 * 1024 * 1024, 9).is_err());
        let ranges = allocated_data_ranges(&source, 20 * 1024 * 1024, 10)?;
        assert_eq!(ranges.len(), 10);
        assert_eq!(ranges[0].offset, 0);
        assert_eq!(ranges[9].offset, 18 * 1024 * 1024);
        Ok(())
    }
}

/// Whether the file system holding the open file or directory reports every
/// change to a change notification, or that it lost some: NTFS does. `ReFS`
/// (a Dev Drive) was seen to drop a removal without a trace, and other file
/// systems, network shares among them, promise less.
///
/// # Errors
///
/// Returns the host failure to name the volume's file system.
#[cfg(windows)]
pub(crate) fn reports_every_change(
    file: &impl std::os::windows::io::AsRawHandle,
) -> io::Result<bool> {
    reports_every_change_measured(file, &mut ordinary_work()).map_err(ordinary_error)
}

#[cfg(windows)]
pub(crate) fn reports_every_change_measured(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut DemandWork,
) -> Result<bool, DemandError> {
    reports_every_change_with_work(file, work)
}

#[cfg(windows)]
pub(crate) fn reports_every_change_with_work<W: NativeHostWork<Error = DemandError>>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
) -> Result<bool, DemandError> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
    let mut name = [0_u16; 64];
    native_read_with_work(work)?;
    // SAFETY: a live handle and an output buffer that outlives the call.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(file.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(&mut name),
        )
    }
    .map_err(io::Error::from)?;
    let mut length = 0;
    for unit in &name {
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
        work.admit(examined)?;
        let terminator = *unit == 0;
        work.record(examined)?;
        if terminator { break; }
        length += 1;
    }
    Ok(name.get(..length) == Some(&[78, 84, 70, 83][..]))
}

#[cfg(all(test, windows))]
#[test]
fn directory_record_decoder_checks_each_flexible_record_boundary() {
    use std::mem::{offset_of, size_of};
    use windows::Win32::Storage::FileSystem::FILE_ID_EXTD_DIR_INFO;

    let header = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
    let stride = (header + size_of::<u16>()).next_multiple_of(size_of::<u64>());
    let mut buffer = vec![0_u64; stride * 2 / size_of::<u64>()];
    let write_record = |buffer: &mut [u64], offset: usize, next: u32, name_bytes: u32| {
        // SAFETY: these tests allocate both complete fixed headers; only the
        // fixed fields and one UTF-16 name unit are written, without a struct
        // reference or an unchecked flexible-tail access.
        unsafe {
            let bytes = buffer.as_mut_ptr().cast::<u8>().add(offset);
            let record = bytes.cast::<FILE_ID_EXTD_DIR_INFO>();
            std::ptr::addr_of_mut!((*record).NextEntryOffset).write_unaligned(next);
            std::ptr::addr_of_mut!((*record).FileNameLength).write_unaligned(name_bytes);
            bytes.add(header).cast::<u16>().write_unaligned(u16::from(b'a'));
        }
    };
    write_record(&mut buffer, 0, u32::try_from(stride).expect("small record"), 2);
    write_record(&mut buffer, stride, 0, 2);
    let (next, name) = decode_host_directory_record(&buffer, 0).expect("first record");
    assert_eq!(next, Some(stride));
    assert_eq!(name, [u16::from(b'a')]);
    assert_eq!(decode_host_directory_record(&buffer, stride).expect("last record").0, None);

    for (next, name_bytes) in [
        (8, 2), (u32::MAX, 2), (u32::try_from(stride * 2).expect("small buffer"), 2),
        (u32::try_from(stride).expect("small record"), 0),
        (u32::try_from(stride).expect("small record"), 3),
        (u32::try_from(stride).expect("small record"), u32::MAX),
        // The name still fits the complete buffer, but crosses this record's
        // boundary into the next header. Whole-buffer validation is insufficient.
        (u32::try_from(stride).expect("small record"), u32::try_from(stride - header + 2).expect("short name")),
    ] {
        write_record(&mut buffer, 0, next, name_bytes);
        assert!(decode_host_directory_record(&buffer, 0).is_err(), "{next}/{name_bytes}");
    }
    assert!(decode_host_directory_record(&buffer, usize::MAX).is_err());
    assert!(decode_host_directory_record(&buffer, 1).is_err());
    write_record(&mut buffer, 0, 0, 2);
    assert!(decode_host_directory_record(&buffer[..header / size_of::<u64>()], 0).is_err());
}

#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
mod bounded_source_tests {
    use super::*;
    use crate::CancellationToken;

    #[cfg(unix)]
    #[test]
    fn concurrent_held_reads_share_one_actual_started_request_allowance()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("held");
        std::fs::write(&path, b"actual held bytes")?;
        let file = std::fs::File::open(path)?;
        let cancellation = CancellationToken::new();
        let budget = crate::WorkBudget {
            backend_read_operations: 1, ..crate::WorkBudget::UNBOUNDED
        };
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let read = || {
                barrier.wait();
                file_identity_and_length_with_work(&file, &mut &work)
            };
            let first = scope.spawn(read);
            let second = scope.spawn(read);
            let first = first.join().expect("first held reader did not panic");
            let second = second.join().expect("second held reader did not panic");
            assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
            let refused = if first.is_err() { first } else { second };
            assert!(matches!(refused, Err(crate::NativeTreeMaterializationError::Work(_))));
        });
        assert_eq!(work.receipt().backend_read_operations, 1);
        work.receipt().verify(budget)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_reservations_restore_only_their_own_non_lifo_allowance() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd as _;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("leaf"), b"actual cleanup")?;
        let parent = Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let cancellation = CancellationToken::new();
        let budget = crate::WorkBudget {
            backend_write_operations: 2, ..crate::WorkBudget::default()
        };
        let mut work = DemandWork::new(budget, &cancellation);
        let earlier = work.reserve_cleanup_write()?;
        let later = work.reserve_cleanup_write()?;
        assert_eq!(work.budget.backend_write_operations, 0);
        cancellation.cancel();
        let (deleted, recorded) = work.attempt_cleanup_write(earlier, || {
            // SAFETY: held parent and static terminated leaf; exactly one
            // native cleanup attempt consumes the earlier reserved slot.
            unsafe { libc::unlinkat(parent.as_raw_fd(), c"leaf".as_ptr(), 0) }
        });
        assert_eq!(deleted, 0);
        recorded?;
        assert_eq!(work.budget.backend_write_operations, 1);
        assert_eq!(work.work.backend_write_operations, 1);
        assert!(work.work.admit(&WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        }, &work.budget).is_err(), "the later reservation remains withheld");
        work.cancel_cleanup_write(later);
        assert_eq!(work.budget.backend_write_operations, 2);
        work.work.verify(budget)?;
        assert!(!root.path().join("leaf").exists());

        let mut work = DemandWork::new(budget, &CancellationToken::new());
        let earlier = work.reserve_cleanup_write()?;
        let later = work.reserve_cleanup_write()?;
        work.cancel_cleanup_write(earlier);
        assert_eq!(work.budget.backend_write_operations, 1);
        work.cancel_cleanup_write(later);
        assert_eq!(work.budget.backend_write_operations, 2);
        assert_eq!(work.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn borrowed_root_clone_admits_copy_and_capacity_before_ownership() -> Result<(), Box<dyn std::error::Error>> {
        let path = Path::new("borrowed-root");
        let bytes = path.as_os_str().len() as u64;
        for deny_copy in [false, true] {
            let mut budget = WorkBudget::UNBOUNDED;
            if deny_copy { budget.bytes_copied = bytes - 1; }
            else { budget.peak_allocation_bytes = bytes - 1; }
            let mut work = DemandWork::new(budget, &CancellationToken::new());
            assert!(matches!(clone_path_measured(path, &mut work), Err(DemandError::Work(_))));
            assert_eq!(work.work.bytes_copied, 0);
            assert_eq!(work.work.allocation_operations, 0);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let owned = clone_path_measured(path, &mut work)?;
        assert_eq!(owned, path);
        assert_eq!(owned.capacity() as u64, bytes);
        assert_eq!(work.live_allocation_bytes(), bytes);
        assert_eq!(work.work.bytes_copied, bytes);
        assert_eq!(work.work.allocation_operations, 1);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_stat_preserves_full_file_id_and_opaque_reparse_kind() -> io::Result<()> {
        use windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION;
        use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        let mut information = FILE_STAT_INFORMATION::default();
        information.FileId = -1;
        information.FileAttributes = FILE_ATTRIBUTE_REPARSE_POINT.0;
        information.ReparseTag = 0x9000_001A;
        let opaque = HostStat::from_stat_information(&information, 17)?;
        assert_eq!(opaque.file_index(), u64::MAX);
        assert_eq!(opaque.volume_serial_number(), 17);
        assert!(opaque.file_type().is_file());
        assert!(!opaque.file_type().is_symlink());
        for tag in [0xA000_0003, 0xA000_000C] {
            information.ReparseTag = tag;
            assert!(HostStat::from_stat_information(&information, 17)?.file_type().is_symlink());
        }
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_root_spelling_preserves_opaque_units_without_reallocating() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::ffi::OsStringExt as _;
        let units = [67, 58, 92, 0xD800, 120];
        // The externally owned input is outside the finite producer; the
        // producer only encodes these opaque units into one exact raw buffer.
        let input = std::path::PathBuf::from(OsString::from_wide(&units));
        let bytes = (units.len() + 1) as u64 * 2;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = bytes;
        budget.backend_read_operations = 0;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        let normalized = normalize_root_wide_measured(&input, &mut work)?;
        assert_eq!(&normalized[..units.len()], &units);
        assert_eq!(normalized.last(), Some(&0));
        assert_eq!(normalized.len(), normalized.capacity());
        assert_eq!(work.work.allocation_operations, 1);
        assert_eq!(work.work.bytes_encoded, units.len() as u64 * 2);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.live_allocation_bytes(), bytes);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn absolute_root_clones_raw_bytes_without_cwd_io() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;
        let root = Path::new(OsStr::from_bytes(b"/raw-\xff/../root"));
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        let normalized = normalize_root_path_measured(root, &mut work)?;
        assert_eq!(normalized, root);
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.bytes_copied, root.as_os_str().len() as u64);
        assert_eq!(work.live_allocation_bytes(), normalized.capacity() as u64);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn relative_root_preserves_raw_join_semantics_and_retained_capacity() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;
        let cwd = std::env::current_dir()?;
        for relative in [
            Path::new(""),
            Path::new("."),
            Path::new(OsStr::from_bytes(b"./raw-\xff/../root/")),
        ] {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            let normalized = normalize_root_path_measured(relative, &mut work)?;
            assert_eq!(normalized, cwd.join(relative));
            assert_eq!(normalized.capacity(), normalized.as_os_str().len());
            assert_eq!(work.live_allocation_bytes(), normalized.capacity() as u64);
            assert!(work.work.backend_read_operations > 0);
            assert_eq!(work.work.source_bytes_read, 0);
            assert_eq!(work.work.source_entries_visited, 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn cwd_denial_releases_scratch_without_recording_an_attempt() {
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(normalize_root_path_measured(Path::new("relative"), &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.work.source_bytes_read, 0);
    }

    #[cfg(unix)]
    #[test]
    fn denied_root_join_keeps_cwd_attempts_but_releases_its_buffer() {
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(normalize_root_path_measured(Path::new("relative"), &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "bytes_copied", .. }))));
        assert!(work.work.backend_read_operations > 0);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_root_normalization_never_allocates_or_enters_cwd() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        for path in [Path::new("relative"), Path::new("/absolute")] {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            assert!(matches!(normalize_root_path_measured(path, &mut work), Err(DemandError::Cancelled)));
            assert_eq!(work.work, WorkCounters::UNCHARGED);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn held_cwd_ancestry_reconstructs_paths_beyond_native_path_max() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStringExt as _;
        let temporary = tempfile::tempdir()?;
        let mut expected = temporary.path().canonicalize()?;
        let mut directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut raw_name = vec![b'a'; 220];
        raw_name[0] = 0xff;
        let name = OsString::from_vec(raw_name);
        for _ in 0..24 {
            directory.create_dir(&name)?;
            directory = directory.open_dir(&name)?;
            expected.push(&name);
        }
        assert!(expected.as_os_str().len() > libc::PATH_MAX as usize);
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let reconstructed = cwd_ancestry_measured(directory, &mut work)?;
        assert_eq!(reconstructed, expected);
        assert_eq!(reconstructed.capacity(), reconstructed.as_os_str().len());
        assert_eq!(work.live_allocation_bytes(), reconstructed.capacity() as u64);
        assert!(work.work.items_examined >= 24);
        assert!(work.work.backend_read_operations > 24);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn ancestry_reader_denial_releases_every_temporary_claim() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        for allocation_denial in [true, false] {
            let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
            let mut budget = WorkBudget::UNBOUNDED;
            let counter = if allocation_denial {
                budget.peak_allocation_bytes = HostStatReader::BUFFER_BYTES as u64 - 1;
                "peak_allocation_bytes"
            } else {
                budget.items_examined = 0;
                "items_examined"
            };
            let mut work = DemandWork::new(budget, &CancellationToken::new());
            assert!(matches!(cwd_ancestry_measured(directory, &mut work),
                Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: denied, .. })) if denied == counter));
            assert_eq!(work.live_allocation_bytes(), 0);
            assert_eq!(work.work.source_bytes_read, 0);
            assert_eq!(work.work.source_entries_visited, 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn ancestry_denial_releases_previously_retained_names_and_slots() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        parent.create_dir("leaf")?;
        let directory = parent.open_dir("leaf")?;
        let mut budget = WorkBudget::UNBOUNDED;
        // At most three raw records plus four name-validation bytes admit
        // the first ancestor, but cannot admit the tempfile name above it.
        budget.items_examined = 7;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(cwd_ancestry_measured(directory, &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert!(work.work.peak_allocation_bytes > HostStatReader::BUFFER_BYTES as u64);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        Ok(())
    }

    #[test]
    fn cancellation_stops_native_source_boundaries_before_io() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"payload")?;
        let root = HostRoot::open(temporary.path())?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(root.stat_measured(Path::new("file"), &mut work), Err(DemandError::Cancelled)));
        assert!(matches!(root.open_file_measured(Path::new("file"), &mut work), Err(DemandError::Cancelled)));
        assert!(matches!(root.read_dir_stats_measured(Path::new(""), &mut work), Err(DemandError::Cancelled)));
        assert_eq!(work.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn root_open_retains_the_actual_open_when_identity_query_is_denied() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 1;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(HostRoot::open_measured(temporary.path(), &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(work.work.backend_read_operations, 1);
        Ok(())
    }

    #[test]
    fn a_directory_buffer_is_admitted_before_it_is_created() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = HostRoot::open(temporary.path())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = HostStatReader::BUFFER_BYTES as u64 - 1;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(root.read_dir_stats_measured(Path::new(""), &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }))));
        assert!(work.work.peak_allocation_bytes < HostStatReader::BUFFER_BYTES as u64);
        Ok(())
    }

    #[test]
    fn refill_cannot_run_after_its_io_allowance_is_exhausted() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("entry"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let mut work = ordinary_work();
        let mut reader = root.read_dir_stats_measured(Path::new(""), &mut work)?;
        assert_eq!(reader.logical_owned_bytes(), HostStatReader::BUFFER_BYTES as u64);
        let before = work.work;
        work.budget.backend_read_operations = before.backend_read_operations;
        #[cfg(unix)]
        let result = reader.next_measured(&mut work);
        #[cfg(windows)]
        let result = reader.next_wide_measured(&mut work);
        assert!(matches!(result,
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(work.work.backend_read_operations, before.backend_read_operations);
        assert_eq!(work.work.source_entries_visited, 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn skipped_dot_records_consume_the_native_item_allowance() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("entry"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let mut work = ordinary_work();
        let mut reader = root.read_dir_stats_measured(Path::new(""), &mut work)?;
        let before_items = work.work.items_examined;
        work.budget.items_examined = before_items + 1;
        let before_reads = work.work.backend_read_operations;
        assert!(matches!(reader.next_measured(&mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(work.work.items_examined, before_items + 1);
        assert_eq!(work.work.source_entries_visited, 0);
        assert_eq!(work.work.backend_read_operations, before_reads + 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn link_reads_are_capped_and_each_growth_read_is_retained() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let target = "t".repeat(600);
        std::os::unix::fs::symlink(&target, temporary.path().join("link"))?;
        let root = HostRoot::open(temporary.path())?;
        let mut limited = ordinary_work();
        assert!(matches!(root.read_link_measured(Path::new("link"), 129, &mut limited), Err(DemandError::InvalidRequest)));
        assert_eq!(limited.work.source_bytes_read, 130);
        assert!(limited.work.peak_allocation_bytes <= 135);
        let mut complete = ordinary_work();
        assert_eq!(root.read_link_measured(Path::new("link"), u64::MAX, &mut complete)?, std::path::PathBuf::from(&target));
        assert_eq!(complete.work.source_bytes_read, 256 + 512 + 600);
        assert_eq!(complete.work.backend_read_operations, 4);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn link_buffer_denial_precedes_the_first_target_read() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::os::unix::fs::symlink("target", temporary.path().join("link"))?;
        let root = HostRoot::open(temporary.path())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = 4;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(root.read_link_measured(Path::new("link"), 32, &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }))));
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.backend_read_operations, 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn measured_source_walks_refuse_intermediate_links() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("secret"), b"outside")?;
        std::os::unix::fs::symlink(&outside, root_path.join("pivot"))?;
        let root = HostRoot::open(&root_path)?;
        let mut work = ordinary_work();
        assert!(root.stat_measured(Path::new("pivot/secret"), &mut work).is_err());
        assert!(root.open_file_measured(Path::new("pivot/secret"), &mut work).is_err());
        assert!(root.read_dir_stats_measured(Path::new("pivot"), &mut work).is_err());
        Ok(())
    }

    #[test]
    fn held_rename_never_replaces_an_existing_destination() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("source"), b"source")?;
        std::fs::write(temporary.path().join("destination"), b"destination")?;
        let root = HostRoot::open(temporary.path())?;
        let directory = root.open_dir_held(Path::new(""))?;
        assert!(rename_no_replace_held(&directory, Path::new("source"), &directory, Path::new("destination")).is_err());
        assert_eq!(std::fs::read(temporary.path().join("source"))?, b"source");
        assert_eq!(std::fs::read(temporary.path().join("destination"))?, b"destination");
        std::fs::remove_file(temporary.path().join("destination"))?;
        rename_no_replace_held(&directory, Path::new("source"), &directory, Path::new("destination"))?;
        assert_eq!(std::fs::read(temporary.path().join("destination"))?, b"source");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn held_rename_uses_the_original_parent_after_its_path_is_replaced() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("parent");
        let moved = temporary.path().join("moved");
        std::fs::create_dir(&original)?;
        std::fs::write(original.join("source"), b"held")?;
        let root = HostRoot::open(&original)?;
        let directory = root.open_dir_held(Path::new(""))?;
        std::fs::rename(&original, &moved)?;
        std::fs::create_dir(&original)?;
        std::fs::write(original.join("source"), b"replacement")?;
        durable_rename_held(&directory, Path::new("source"), &directory, Path::new("destination"))?;
        assert_eq!(std::fs::read(moved.join("destination"))?, b"held");
        assert_eq!(std::fs::read(original.join("source"))?, b"replacement");
        assert!(!original.join("destination").exists());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn pending_read_completion_is_admitted_before_the_read_starts() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"data")?;
        let file = YieldingFile::plain(File::open(temporary.path().join("file"))?);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 1;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        work.adopt(file.logical_owned_bytes())?;
        let mut buffer = [0xa5; 4];
        assert!(matches!(file.read_at_measured(0, &mut buffer, &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(buffer, [0xa5; 4]);
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.source_bytes_read, 0);
        Ok(())
    }
}

#[cfg(all(test, any(windows, target_os = "linux", target_os = "macos")))]
mod bounded_record_tests {
    use super::*;

    #[test]
    fn exclusive_fence_cookie_reserves_removal_before_create() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = HostRoot::open(temporary.path())?;
        for writes in [0, 1] {
            let mut budget = WorkBudget::UNBOUNDED;
            budget.backend_write_operations = writes;
            let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
            assert!(matches!(root.fence_cookie_measured(Path::new("cookie"), &mut work),
                Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_write_operations", .. }))));
            assert_eq!(work.work.backend_write_operations, 0);
            assert_eq!(work.budget.backend_write_operations, writes);
            assert!(!temporary.path().join("cookie").exists());
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 2;
        let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
        root.fence_cookie_measured(Path::new("cookie"), &mut work)?;
        assert_eq!(work.work.backend_write_operations, 2);
        assert_eq!(work.budget.backend_write_operations, 2);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert!(!temporary.path().join("cookie").exists());
        Ok(())
    }

    #[test]
    fn exclusive_fence_cookie_never_removes_an_existing_name() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("cookie"), b"keep")?;
        let root = HostRoot::open(temporary.path())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 2;
        let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
        let error = root.fence_cookie_measured(Path::new("cookie"), &mut work).expect_err("exclusive collision");
        assert!(matches!(error, DemandError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists));
        assert_eq!(std::fs::read(temporary.path().join("cookie"))?, b"keep");
        assert_eq!(work.work.backend_write_operations, 1);
        assert_eq!(work.budget.backend_write_operations, 2);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn committed_fence_cookie_is_removed_and_charged_after_cancellation() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_write_operations = 2;
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut boundary = CancelAfterName { work: &work, cancellation: &cancellation, cancel_on_write: true };
        #[cfg(unix)]
        let name = c"cookie";
        #[cfg(windows)]
        let name = &[99_u16, 111, 111, 107, 105, 101][..];
        assert!(matches!(fence_cookie_held_with_work(&parent, name, &mut boundary),
            Err(crate::materializer::NativeTreeMaterializationError::Cancelled)));
        assert_eq!(work.receipt().backend_write_operations, 2);
        assert_eq!(work.live_allocation_bytes(), ACCOUNTING_STATE_BYTES);
        assert!(!temporary.path().join("cookie").exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn held_canonical_directory_observes_renamed_object_not_replaced_spelling() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        let moved = temporary.path().join("moved");
        std::fs::create_dir(&original)?;
        let directory = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
        std::fs::rename(&original, &moved)?;
        std::fs::create_dir(&original)?;
        let expected = std::fs::canonicalize(&moved)?;
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let (actual, allocation) = canonical_directory_path_held_with_work(directory, &mut work)?;
        assert_eq!(actual, expected);
        assert_ne!(actual, std::fs::canonicalize(&original)?);
        assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        drop(actual);
        work.release(allocation)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn canonical_directory_observes_physical_alias_and_exact_output_custody() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;
        let temporary = tempfile::tempdir()?;
        let physical = temporary.path().join("physical");
        std::fs::create_dir(&physical)?;
        let leaf = OsStr::from_bytes(b"child-\xff");
        std::fs::create_dir(physical.join(leaf))?;
        std::os::unix::fs::symlink(&physical, temporary.path().join("alias"))?;
        let input = temporary.path().join("alias").join(leaf);
        let expected = std::fs::canonicalize(&input)?;
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let actual = canonical_directory_path_with_work(&input, &mut work)?;
        assert_eq!(actual, expected);
        assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        assert_eq!(work.work.source_path_components, 0);
        assert!(work.work.backend_read_operations > 2);
        let bytes = actual.capacity() as u64;
        drop(actual);
        work.release(bytes)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn canonical_directory_denial_releases_ancestry_buffers() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 2;
        let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(canonical_directory_path_with_work(temporary.path(), &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(work.work.backend_read_operations, 2);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn canonical_wide_directory_retains_native_capacity_without_scalar_conversion() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
        let temporary = tempfile::tempdir()?;
        let input = temporary.path().join(OsString::from_wide(&[0xD800, 120]));
        std::fs::create_dir(&input)?;
        let expected = std::fs::canonicalize(&input)?;
        let expected: Vec<u16> = expected.as_os_str().encode_wide().collect();
        let directory = Dir::open_ambient_dir(&input, cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let (actual, allocation) = canonical_directory_wide_with_work(&directory, &mut work)?;
        assert_eq!(actual, expected);
        assert_ne!(actual.last(), Some(&0));
        assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64 * 2);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.work.bytes_encoded, 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.allocation_operations, 1);
        assert_eq!(work.work.backend_read_operations, 2);
        drop(actual);
        work.release(allocation)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn canonical_wide_directory_admits_query_and_buffer_separately() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        for deny_buffer in [false, true] {
            let mut budget = WorkBudget::UNBOUNDED;
            if deny_buffer { budget.peak_allocation_bytes = 0; }
            else { budget.backend_read_operations = 0; }
            let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
            assert!(matches!(canonical_directory_wide_with_work(&directory, &mut work), Err(DemandError::Work(_))));
            assert_eq!(work.work.backend_read_operations, u64::from(deny_buffer));
            assert_eq!(work.work.allocation_operations, 0);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 1;
        let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(canonical_directory_wide_with_work(&directory, &mut work),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.work.allocation_operations, 1);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn all_changes_volume_probe_admits_native_query_and_name_scan() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let expected = reports_every_change(&directory)?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let mut denied = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(reports_every_change_measured(&directory, &mut denied),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(denied.work, WorkCounters::UNCHARGED);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = 0;
        let mut scan_denied = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(reports_every_change_measured(&directory, &mut scan_denied),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(scan_denied.work.backend_read_operations, 1);
        assert_eq!(scan_denied.work.items_examined, 0);
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        assert_eq!(reports_every_change_measured(&directory, &mut work)?, expected);
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert!(work.work.items_examined > 0);
        Ok(())
    }

    #[test]
    fn owned_child_denies_native_boundary_and_cancelled_work_before_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        #[cfg(unix)]
        let name = OsStr::new("child");
        #[cfg(windows)]
        let name = &[99_u16, 104, 105, 108, 100][..];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let mut denied = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(open_owned_child_directory_with_work(&parent, name, &mut denied),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(denied.work.backend_read_operations, 0);
        assert_eq!(denied.live_allocation_bytes(), 0);
        let mut denied_file = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(verify_owned_child_no_mount_with_work(&parent, name, &mut denied_file),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(denied_file.work.backend_read_operations, 0);
        assert_eq!(denied_file.live_allocation_bytes(), 0);
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(open_owned_child_directory_with_work(&parent, name, &mut cancelled), Err(DemandError::Cancelled)));
        assert_eq!(cancelled.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn owned_child_uses_held_parent_after_lexical_replacement() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        std::fs::create_dir(&original)?;
        std::fs::create_dir(original.join("child"))?;
        let parent = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
        let child = Dir::open_ambient_dir(original.join("child"), cap_std::ambient_authority())?;
        let expected = held_root_identity(&child)?;
        std::fs::rename(&original, temporary.path().join("moved"))?;
        std::fs::create_dir(&original)?;
        std::fs::create_dir(original.join("child"))?;
        #[cfg(unix)]
        let name = OsStr::new("child");
        #[cfg(windows)]
        let name = &[99_u16, 104, 105, 108, 100][..];
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        let actual = open_owned_child_directory_with_work(&parent, name, &mut work)?;
        assert_eq!(held_root_identity(&actual)?, expected);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn owned_child_refuses_symlinks_and_nonleaf_names() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("child"))?;
        std::os::unix::fs::symlink("child", temporary.path().join("link"))?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        for name in ["link", "..", "child/../child"] {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
            assert!(open_owned_child_directory_with_work(&parent, OsStr::new(name), &mut work).is_err());
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        assert!(verify_owned_child_no_mount_with_work(&parent, OsStr::new("link"), &mut work).is_err());
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.source_bytes_read, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn owned_nondirectory_mount_probe_uses_metadata_only_fifo_handle() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd as _;
        let temporary = tempfile::tempdir()?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        // SAFETY: held parent and static terminated single leaf are live.
        if unsafe { libc::mkfifoat(parent.as_raw_fd(), c"fifo".as_ptr(), 0o600) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new());
        verify_owned_child_no_mount_with_work(&parent, OsStr::new("fifo"), &mut work)?;
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(work.work.backend_read_operations >= 2);
        Ok(())
    }

    const ACCOUNTING_STATE_BYTES: u64 = crate::materializer::NativeMaterializationWork::accounting_state_bytes();

    #[cfg(unix)]
    #[test]
    fn held_ancestry_survives_aliases_and_lexical_parent_replacement() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let parent_path = temporary.path().join("parent");
        std::fs::create_dir(&parent_path)?;
        std::fs::create_dir(parent_path.join("child"))?;
        std::os::unix::fs::symlink(&parent_path, temporary.path().join("alias"))?;
        let parent = Dir::open_ambient_dir(&parent_path, cap_std::ambient_authority())?;
        let child = Dir::open_ambient_dir(temporary.path().join("alias/child"), cap_std::ambient_authority())?;
        let parent_identity = held_root_identity(&parent)?;
        let child_identity = held_root_identity(&child)?;
        std::fs::rename(&parent_path, temporary.path().join("moved"))?;
        std::fs::create_dir(&parent_path)?;
        let replacement = Dir::open_ambient_dir(&parent_path, cap_std::ambient_authority())?;
        let replacement_identity = held_root_identity(&replacement)?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = 0;
        let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(directory_has_ancestor_with_work(&child, child_identity, &mut work)?);
        assert!(directory_has_ancestor_with_work(&child, parent_identity, &mut work)?);
        assert!(!directory_has_ancestor_with_work(&child, replacement_identity, &mut work)?);
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn held_ancestry_admits_parent_open_and_cancellation_before_attempts() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("child"))?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let child = Dir::open_ambient_dir(temporary.path().join("child"), cap_std::ambient_authority())?;
        let ancestor = held_root_identity(&parent)?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 1;
        let mut denied = DemandWork::new(budget, &crate::CancellationToken::new());
        assert!(matches!(directory_has_ancestor_with_work(&child, ancestor, &mut denied),
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(denied.work.backend_read_operations, 1);
        assert_eq!(denied.work.allocation_operations, 0);
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(directory_has_ancestor_with_work(&child, ancestor, &mut cancelled), Err(DemandError::Cancelled)));
        assert_eq!(cancelled.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn generic_link_returns_only_exact_owned_target_and_releases_scratch() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let target = vec![0xFF; 600];
        use std::os::unix::ffi::OsStrExt as _;
        std::os::unix::fs::symlink(OsStr::from_bytes(&target), temporary.path().join("link"))?;
        let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let work = crate::materializer::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(cause, _)| cause)?;
        let (actual, allocation) = read_link_held_with_work(&parent, Path::new("link"), 600, &mut &work)?;
        assert_eq!(actual, target);
        assert_eq!(actual.capacity(), target.len());
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + target.len() as u64);
        assert_eq!(work.receipt().backend_read_operations, 3);
        assert_eq!(work.receipt().source_bytes_read, 256 + 512 + 600);
        drop((actual, allocation));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);

        let denied = crate::materializer::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(cause, _)| cause)?;
        assert!(read_link_held_with_work(&parent, Path::new("link"), 129, &mut &denied).is_err());
        assert_eq!(denied.receipt().backend_read_operations, 1);
        assert_eq!(denied.receipt().source_bytes_read, 130);
        assert_eq!(materialization_live_bytes(&denied)?, ACCOUNTING_STATE_BYTES);
        Ok(())
    }

    struct CancelAfterName<'a> {
        work: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
        cancel_on_write: bool,
    }

    impl NativeHostWork for CancelAfterName<'_> {
        type Error = crate::materializer::NativeTreeMaterializationError;
        type Allocation = crate::materializer::NativeAllocation;
        type CleanupWrite = crate::materializer::NativeCleanupWrite;
        fn io_error_kind(error: &Self::Error) -> Option<io::ErrorKind> {
            <&crate::materializer::NativeMaterializationWork as NativeHostWork>::io_error_kind(error)
        }
        fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
            crate::materializer::NativeTreeMaterializationError::AllocationFailed(error)
        }
        fn check(&self) -> Result<(), Self::Error> { self.work.admit(WorkCounters::UNCHARGED) }
        fn admit(&mut self, delta: WorkCounters) -> Result<(), Self::Error> { self.work.admit(delta) }
        fn record(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
            self.work.record_admitted(delta)?;
            if self.cancel_on_write && delta.backend_write_operations != 0
                || !self.cancel_on_write && delta.bytes_copied != 0 {
                self.cancellation.cancel();
            }
            Ok(())
        }
        fn started_io(&mut self, delta: WorkCounters) -> Result<(), Self::Error> {
            self.work.charge(delta)?;
            if self.cancel_on_write && delta.backend_write_operations != 0 {
                self.cancellation.cancel();
            }
            Ok(())
        }
        fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> { self.work.claim(bytes) }
        fn reserve_opaque_decode(&mut self, logical_max: u64) -> Result<Self::Allocation, Self::Error> {
            self.work.retain_owned(logical_max)
        }
        fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64) -> Result<(), Self::Error> {
            allocation.reconcile_capacity(actual)
        }
        fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> { drop(allocation); Ok(()) }
        fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
            self.work.reserve_cleanup_write()
        }
        fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) { drop(reservation); }
        fn attempt_cleanup_write<T>(&mut self, reservation: Self::CleanupWrite, operation: impl FnOnce() -> T)
            -> (T, Result<(), Self::Error>) {
            reservation.attempt_recorded(operation)
        }
    }

    #[cfg(windows)]
    #[test]
    fn current_directory_late_decode_cancellation_retires_actual_output_and_scratch() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = ACCOUNTING_STATE_BYTES + 1024 * 1024;
        budget.backend_read_operations = 16;
        budget.allocation_operations = 16;
        budget.bytes_copied = 1024 * 1024;
        budget.source_bytes_read = 0;
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut boundary = CancelAfterName { work: &work, cancellation: &cancellation, cancel_on_write: false };
        assert!(matches!(current_directory_with_work(&mut boundary),
            Err(crate::materializer::NativeTreeMaterializationError::Cancelled)));
        assert!(work.receipt().backend_read_operations >= 2);
        assert!(work.receipt().bytes_copied > 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
        work.receipt().verify(budget)?;
        Ok(())
    }

    #[test]
    fn cancellation_after_native_name_copy_releases_it_without_consuming_the_record() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let cancellation = crate::CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64 + 32;
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
        let mut boundary = CancelAfterName { work: &work, cancellation: &cancellation, cancel_on_write: false };
        let (mut reader, allocation) = read_dir_held_with_work(&directory, &mut boundary)?;
        #[cfg(unix)]
        let failure = reader.next_with_work(&mut boundary);
        #[cfg(windows)]
        let failure = reader.next_wide_with_work(&mut boundary);
        assert!(matches!(failure, Err(crate::materializer::NativeTreeMaterializationError::Cancelled)));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64);
        let offset = reader.next;
        #[cfg(unix)]
        let repeated = reader.next_with_work(&mut boundary);
        #[cfg(windows)]
        let repeated = reader.next_wide_with_work(&mut boundary);
        assert!(matches!(repeated, Err(crate::materializer::NativeTreeMaterializationError::Cancelled)));
        assert_eq!(reader.next, offset);
        #[cfg(unix)]
        assert_eq!(work.receipt().backend_read_operations, 2);
        #[cfg(windows)]
        assert_eq!(work.receipt().backend_read_operations, 3);
        assert_eq!(work.receipt().source_bytes_read, 0);
        drop((reader, allocation));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
        Ok(())
    }

    #[test]
    fn generic_file_stat_pins_the_opened_object_across_name_replacement() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let file = File::open(temporary.path().join("kept"))?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let work = crate::materializer::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(cause, _)| cause)?;
        let before = stat_file_with_work(&file, &mut &work)?;
        std::fs::rename(temporary.path().join("kept"), temporary.path().join("original"))?;
        std::fs::write(temporary.path().join("kept"), b"replacement")?;
        let after = stat_file_with_work(&file, &mut &work)?;
        #[cfg(unix)]
        let replacement = stat_leaf_with_work(&directory, OsStr::new("kept"), &mut &work)?;
        #[cfg(windows)]
        let replacement = stat_windows_leaf_with_work(&directory, &[107, 101, 112, 116], &mut &work)?;
        assert_eq!(before.identity()?, after.identity()?);
        assert_ne!(after.identity()?, replacement.identity()?);
        assert_eq!(after.len(), 4);
        assert_eq!(replacement.len(), 11);
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
        Ok(())
    }

    fn materialization_live_bytes(
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<u64, crate::materializer::NativeTreeMaterializationError> {
        let (bytes, _empty_allocation) = work.with_owned_allocations(|ledger, _, _, _| {
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(ledger.live_bytes())
        })?;
        Ok(bytes)
    }

    #[test]
    fn held_scanners_keep_independent_cursors_and_scoped_native_allocations() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64 + 64;
        let work = crate::materializer::NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).map_err(|(cause, _)| cause)?;
        let (mut first, first_allocation) = read_dir_held_with_work(&directory, &mut &work)?;
        let (mut second, second_allocation) = read_dir_held_with_work(&directory, &mut &work)?;
        assert_eq!(first_allocation.bytes(), HostStatReader::BUFFER_BYTES as u64);
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64);
        #[cfg(unix)]
        let (first_entry, first_name) = first.next_with_work(&mut &work)?
            .ok_or_else(|| io::Error::other("first cursor lost its entry"))?;
        #[cfg(windows)]
        let (first_entry, first_name) = first.next_wide_with_work(&mut &work)?
            .ok_or_else(|| io::Error::other("first cursor lost its entry"))?;
        #[cfg(unix)]
        let (second_entry, second_name) = second.next_with_work(&mut &work)?
            .ok_or_else(|| io::Error::other("second cursor shared the first cursor's position"))?;
        #[cfg(windows)]
        let (second_entry, second_name) = second.next_wide_with_work(&mut &work)?
            .ok_or_else(|| io::Error::other("second cursor shared the first cursor's position"))?;
        assert_eq!(first_entry.name, second_entry.name);
        assert_eq!(first_name.bytes(), first_entry.logical_owned_bytes());
        assert_eq!(second_name.bytes(), second_entry.logical_owned_bytes());
        assert!(first_entry.stat.is_some());
        assert!(second_entry.stat.is_some());
        assert_eq!(materialization_live_bytes(&work)?,
            ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64 + first_name.bytes() + second_name.bytes());
        drop((first_entry, first_name, second_entry, second_name));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + 2 * HostStatReader::BUFFER_BYTES as u64);
        drop((first, first_allocation, second, second_allocation));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
        assert_eq!(work.receipt().source_entries_visited, 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.receipt().bytes_encoded, 0);
        Ok(())
    }

    #[test]
    fn generic_metadata_denial_releases_only_the_owned_name() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64 + 32;
        #[cfg(unix)]
        { budget.backend_read_operations = 2; }
        #[cfg(windows)]
        { budget.backend_read_operations = 3; }
        let work = crate::materializer::NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).map_err(|(cause, _)| cause)?;
        let (mut reader, allocation) = read_dir_held_with_work(&directory, &mut &work)?;
        #[cfg(unix)]
        let failure = reader.next_with_work(&mut &work);
        #[cfg(windows)]
        let failure = reader.next_wide_with_work(&mut &work);
        assert!(matches!(failure,
            Err(crate::materializer::NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64);
        let offset = reader.next;
        #[cfg(unix)]
        let repeated = reader.next_with_work(&mut &work);
        #[cfg(windows)]
        let repeated = reader.next_wide_with_work(&mut &work);
        assert!(matches!(repeated,
            Err(crate::materializer::NativeTreeMaterializationError::Work(
                crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        assert_eq!(reader.next, offset);
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES + HostStatReader::BUFFER_BYTES as u64);
        assert_eq!(work.receipt().backend_read_operations, budget.backend_read_operations);
        assert_eq!(work.receipt().source_entries_visited, 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        drop((reader, allocation));
        assert_eq!(materialization_live_bytes(&work)?, ACCOUNTING_STATE_BYTES);
        Ok(())
    }

    #[test]
    fn a_record_survives_metadata_admission_failure_without_another_refill() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let mut first = ordinary_work();
        let mut reader = root.read_dir_stats_measured(Path::new(""), &mut first)?;
        first.budget.backend_read_operations = first.work.backend_read_operations + 1;
        #[cfg(unix)]
        let failure = reader.next_measured(&mut first);
        #[cfg(windows)]
        let failure = reader.next_wide_measured(&mut first);
        assert!(matches!(failure,
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
        let mut retry = ordinary_work();
        retry.adopt(reader.logical_owned_bytes())?;
        #[cfg(unix)]
        let entry = reader.next_measured(&mut retry)?;
        #[cfg(windows)]
        let entry = reader.next_wide_measured(&mut retry)?;
        let entry = entry.ok_or_else(|| io::Error::other("denied native record was lost"))?;
        #[cfg(unix)]
        assert_eq!(entry.name, OsStr::new("kept"));
        #[cfg(windows)]
        assert_eq!(entry.name, [107, 101, 112, 116]);
        assert_eq!(retry.work.source_entries_visited, 0);
        assert!(retry.work.items_examined > 0);
        // One file-record stat, or Linux's separately measured statx fallback.
        // Another refill would also return skipped dot records.
        assert!(retry.work.backend_read_operations <= 2);
        assert!(entry.stat.is_some());
        Ok(())
    }

    #[test]
    fn a_record_survives_name_allocation_denial() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("kept"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let mut first = ordinary_work();
        let mut reader = root.read_dir_stats_measured(Path::new(""), &mut first)?;
        first.budget.peak_allocation_bytes = reader.logical_owned_bytes();
        #[cfg(unix)]
        let failure = reader.next_measured(&mut first);
        #[cfg(windows)]
        let failure = reader.next_wide_measured(&mut first);
        assert!(matches!(failure,
            Err(DemandError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }))));
        let mut retry = ordinary_work();
        retry.adopt(reader.logical_owned_bytes())?;
        #[cfg(unix)]
        let entry = reader.next_measured(&mut retry)?;
        #[cfg(windows)]
        let entry = reader.next_wide_measured(&mut retry)?;
        let entry = entry.ok_or_else(|| io::Error::other("denied native record was lost"))?;
        #[cfg(unix)]
        assert_eq!(entry.name, OsStr::new("kept"));
        #[cfg(windows)]
        assert_eq!(entry.name, [107, 101, 112, 116]);
        assert_eq!(retry.work.source_entries_visited, 0);
        assert!(retry.work.items_examined > 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn opaque_utf16_names_never_enter_an_unmetered_os_string_conversion() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::ffi::OsStringExt as _;
        let temporary = tempfile::tempdir()?;
        let name = [0xd800, 120];
        std::fs::write(temporary.path().join(OsString::from_wide(&name)), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let mut lookup = DemandWork::new(budget, &crate::CancellationToken::new());
        assert_eq!(root.stat_wide_measured(&name, &mut lookup)?.len(), 4);
        assert_eq!(lookup.work.allocation_operations, 0);
        assert_eq!(lookup.work.backend_read_operations, 1);

        let mut listing = ordinary_work();
        let mut reader = root.read_dir_stats_wide_measured(&[], &mut listing)?;
        let entry = reader.next_wide_measured(&mut listing)?
            .ok_or_else(|| io::Error::other("opaque native name was not returned"))?;
        assert_eq!(entry.name, name);
        assert_eq!(entry.name.capacity(), name.len());
        assert_eq!(entry.logical_owned_bytes(), 4);
        assert_eq!(entry.stat.ok_or_else(|| io::Error::other("opaque name lacks facts"))?.len(), 4);
        assert_eq!(listing.work.bytes_encoded, 0);
        Ok(())
    }
}
