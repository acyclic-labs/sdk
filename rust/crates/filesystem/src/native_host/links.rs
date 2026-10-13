use super::*;

fn read_link_buffer<W: NativeHostWork>(
    parent: &Dir,
    #[cfg(unix)] name: &std::ffi::CStr,
    #[cfg(windows)] name: &[u16],
    maximum: u64,
    work: &mut W,
) -> Result<(Vec<u8>, std::ops::Range<usize>, W::Allocation), W::Error> {
    #[cfg(unix)]
    let limit = maximum.saturating_add(1).min(isize::MAX as u64);
    #[cfg(unix)]
    let mut size = limit.min(256) as usize;
    #[cfg(windows)]
    let size = maximum.saturating_mul(4).saturating_add(32).min(16 * 1024) as usize;
    #[cfg(windows)]
    let file = open_windows_at(
        parent,
        name,
        fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
        nt::FILE_OPEN,
        nt::FILE_SYNCHRONOUS_IO_NONALERT,
        work,
    )?;
    loop {
        work.admit(WorkCounters {
            source_bytes_read: size as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let (mut bytes, allocation) = buffer(size, work)?;
        let result = (|| {
            work.started_io(copy_work(bytes.capacity() as u64))?;
            bytes.resize(bytes.capacity(), 0);
            let count = loop {
                work.admit(WorkCounters {
                    source_bytes_read: size as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                work.started_io(READ)?;
                #[cfg(unix)]
                let read = {
                    // SAFETY: held parent, terminated name and admitted writable output remain live.
                    let count = unsafe {
                        libc::readlinkat(
                            parent.as_raw_fd(),
                            name.as_ptr(),
                            bytes.as_mut_ptr().cast(),
                            size,
                        )
                    };
                    if count < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(count as usize)
                    }
                };
                #[cfg(windows)]
                let read = {
                    use std::os::windows::io::AsRawHandle as _;
                    let mut count = 0;
                    // SAFETY: held no-follow file and initialized admitted output remain live.
                    let result = unsafe {
                        windows::Win32::System::IO::DeviceIoControl(
                            win::HANDLE(file.as_raw_handle()),
                            windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT,
                            None,
                            0,
                            Some(bytes.as_mut_ptr().cast()),
                            size as u32,
                            Some(&raw mut count),
                            None,
                        )
                    };
                    work.record(WorkCounters {
                        source_bytes_read: u64::from(count),
                        ..WorkCounters::UNCHARGED
                    })?;
                    if result
                        .as_ref()
                        .is_err_and(|error| matches!(error.code().0 & 0xffff, 122 | 234))
                    {
                        return Err(W::invalid_request());
                    }
                    result
                        .map(|()| count as usize)
                        .map_err(|error| io::Error::from_raw_os_error(error.code().0 & 0xffff))
                };
                match read {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result?,
                }
            };
            #[cfg(unix)]
            {
                work.record(WorkCounters {
                    source_bytes_read: count as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                if count == size {
                    if size as u64 == limit {
                        return Err(W::invalid_request());
                    }
                    return Ok(None);
                }
                work.check()?;
                Ok(Some(0..count))
            }
            #[cfg(windows)]
            {
                let target = link_target::<W>(
                    bytes
                        .get(..count)
                        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
                    maximum,
                )?;
                work.check()?;
                Ok(Some(target))
            }
        })();
        match result {
            Ok(Some(target)) => return Ok((bytes, target, allocation)),
            result => {
                drop(bytes);
                work.release(allocation)?;
                result?;
            }
        }
        #[cfg(unix)]
        {
            size = (size as u64).saturating_mul(2).min(limit) as usize;
        }
    }
}

#[cfg(windows)]
fn link_target<W: NativeHostWork>(
    bytes: &[u8],
    maximum: u64,
) -> Result<std::ops::Range<usize>, W::Error> {
    let malformed = || W::Error::from(io::Error::from(io::ErrorKind::InvalidData));
    let word = |offset| {
        let data = bytes.get(offset..offset + 2).ok_or_else(&malformed)?;
        Ok::<_, W::Error>(u16::from_le_bytes([data[0], data[1]]))
    };
    let tag = u32::from_le_bytes(
        bytes
            .get(..4)
            .ok_or_else(&malformed)?
            .try_into()
            .expect("four-byte tag"),
    );
    let header = match tag {
        0xA000_000C => 20,
        0xA000_0003 => 16,
        _ => return Err(W::invalid_request()),
    };
    let (offset, length) = (usize::from(word(8)?), usize::from(word(10)?));
    if !offset.is_multiple_of(2) || !length.is_multiple_of(2) {
        return Err(malformed());
    }
    let mut start = header + offset;
    let end = start + length;
    let target = bytes.get(start..end).ok_or_else(&malformed)?;
    let relative = tag == 0xA000_000C && word(16)? & 1 != 0;
    if !relative && target.starts_with(&[92, 0, 63, 0, 63, 0, 92, 0]) {
        start += 8;
    }
    if (end - start) as u64 > maximum {
        return Err(W::invalid_request());
    }
    Ok(start..end)
}

#[cfg(windows)]
pub(crate) fn read_link_held<W: NativeHostWork>(
    parent: &Dir,
    name: &[u16],
    maximum: u64,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    let (bytes, target, allocation) = read_link_buffer(parent, name, maximum, work)?;
    let result = (|| {
        let (mut wide, allocation) = buffer(target.len() / 2, work)?;
        if let Err(error) = work.started_io(copy_work(target.len() as u64)) {
            drop(wide);
            work.release(allocation)?;
            return Err(error);
        }
        wide.extend(
            bytes[target]
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]])),
        );
        if let Err(error) = work.check() {
            drop(wide);
            work.release(allocation)?;
            return Err(error);
        }
        Ok((wide, allocation))
    })();
    drop(bytes);
    work.release(allocation)?;
    result
}

impl HostRoot {
    #[cfg(unix)]
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &Path,
        maximum: u64,
        work: &mut W,
    ) -> Result<(Vec<u8>, W::Allocation), W::Error> {
        let (mut bytes, target, allocation) =
            with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                read_link_buffer(parent, leaf, maximum, work)
            })?;
        bytes.truncate(target.end);
        Ok((bytes, allocation))
    }
    #[cfg(windows)]
    pub(crate) fn read_link<W: NativeHostWork>(
        &self,
        path: &[u16],
        maximum: u64,
        work: &mut W,
    ) -> Result<(Vec<u16>, W::Allocation), W::Error> {
        let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
        read_link_held(&self.open_dir_held(parent, work)?, leaf, maximum, work)
    }
    pub(crate) fn read_source_link<W: SourceDemandReadWork>(
        &self,
        #[cfg(unix)] path: &Path,
        #[cfg(windows)] path: &[u16],
        maximum: u64,
        work: &mut W,
    ) -> Result<acyclic_stream::OwnedBytes, DemandError> {
        let original = work
            .native_source_work()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        NativeHostWork::check(&original)?;
        let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
        let (bytes, target, allocation) =
            with_parent_leaf(&self.directory, path, work, |parent, leaf, work| {
                read_link_buffer(parent, leaf, maximum, work)
            })?;
        if let Err(error) = ownership.retain(allocation, work) {
            drop(bytes);
            work.release(allocation)?;
            return Err(error);
        }
        let prepared =
            acyclic_stream::PreparedOwnedBytes::new(bytes, ownership, original.runtime_meter())
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        crate::native_name::original_name_error(original.runtime_error(error))
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        DemandError::InvalidRequest
                    }
                })?;
        work.check()?;
        let bytes = prepared.freeze(target.end).map_err(|failed| {
            drop(failed);
            DemandError::InvalidRequest
        })?;
        Ok(bytes.slice(target))
    }
}
