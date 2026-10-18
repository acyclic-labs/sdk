// The one positional read into uninitialized capacity on a held descriptor.

/// Appends at most `length` bytes read at `offset` to `data`'s spare capacity
/// without initializing it first; zero means end of file.
pub(crate) fn read_spare_at(
    file: &File,
    offset: u64,
    data: &mut Vec<u8>,
    length: usize,
) -> io::Result<usize> {
    let spare = data
        .spare_capacity_mut()
        .get_mut(..length)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: pread writes at most `length` bytes into this live spare slice of the held descriptor.
    let count = unsafe { libc::pread(file.as_raw_fd(), spare.as_mut_ptr().cast(), length, offset) };
    let count = usize::try_from(count).map_err(|_| io::Error::last_os_error())?;
    if count > length {
        return Err(io::ErrorKind::InvalidData.into());
    }
    // SAFETY: pread initialized exactly `count` bytes at the start of the spare capacity.
    unsafe { data.set_len(data.len() + count) };
    Ok(count)
}
