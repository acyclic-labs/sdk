use super::*;

#[cfg(feature = "local")]
mod prepared;
#[cfg(feature = "local")]
pub(crate) use prepared::PreparedMaterializationRecord;

#[cfg(windows)]
pub(super) fn windows_name_packet<W: NativeHostWork>(
    parent: &impl std::os::windows::io::AsRawHandle,
    name: &[u16],
    flags: u32,
    work: &mut W,
) -> Result<(Vec<u64>, W::Allocation, u32), W::Error> {
    use nt::FILE_RENAME_INFORMATION;
    let name_bytes = name
        .len()
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)?;
    let offset = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
    let length = offset
        .checked_add(name_bytes)
        .ok_or(crate::WorkError::Overflow)?;
    let length_u32 = u32::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
    let words = length
        .max(std::mem::size_of::<FILE_RENAME_INFORMATION>())
        .div_ceil(8);
    let (mut data, allocation) = buffer::<u64, _>(words, work)?;
    data.resize(words, 0);
    let information = data.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: aligned owned storage covers the native header and the bounded UTF-16 tail.
    let tail = unsafe {
        (*information).Anonymous.Flags = flags;
        (*information).RootDirectory = win::HANDLE(parent.as_raw_handle());
        (*information).FileNameLength =
            u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?;
        std::slice::from_raw_parts_mut(
            information.cast::<u8>().add(offset).cast::<u16>(),
            name.len(),
        )
    };
    tail.copy_from_slice(name);
    Ok((data, allocation, length_u32))
}

#[cfg(unix)]
pub(super) fn rename_unix_names_once(
    from: &Dir,
    source: &std::ffi::CStr,
    to: &Dir,
    destination: &std::ffi::CStr,
    mode: acyclic_native_runtime::RenameMode,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let flags = if mode == acyclic_native_runtime::RenameMode::Replace {
            rustix::fs::RenameFlags::empty()
        } else {
            rustix::fs::RenameFlags::NOREPLACE
        };
        rustix::fs::renameat_with(from, source, to, destination, flags).map_err(Into::into)
    }
    #[cfg(target_os = "macos")]
    {
        let flags = if mode == acyclic_native_runtime::RenameMode::NoReplace {
            libc::RENAME_EXCL
        } else {
            0
        };
        // SAFETY: both held directory descriptors and C names outlive this atomic rename.
        if unsafe {
            libc::renameatx_np(
                from.as_raw_fd(),
                source.as_ptr(),
                to.as_raw_fd(),
                destination.as_ptr(),
                flags,
            )
        } < 0
        {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(unix)]
pub(super) fn sync_held_descriptor_once(file: &impl std::os::fd::AsFd) -> io::Result<()> {
    // One native full flush per metered attempt; EINTR returns to `retry_io`.
    acyclic_native_runtime::sync_descriptor_once(file.as_fd())
}

#[cfg(windows)]
pub(super) fn sync_held_descriptor_once(
    file: &impl std::os::windows::io::AsRawHandle,
) -> io::Result<()> {
    // SAFETY: this held file/directory handle has genuine write access.
    unsafe { fs::FlushFileBuffers(win::HANDLE(file.as_raw_handle())) }
        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
}
