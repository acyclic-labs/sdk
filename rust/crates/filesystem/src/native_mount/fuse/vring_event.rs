//! Kernel identity of an owned eventfd; an fd number is not its identity.

use super::{NativeMountError, fuse_reserve, fuse_vec};
use crate::{NativeMaterializationWork, materializer::NativeResourceCredit::Copy};
use std::fs::File;
use std::io::{Read, Write};
use std::os::{fd::AsRawFd, unix::ffi::OsStrExt};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct CounterId(u64);

impl CounterId {
    pub(super) fn of(
        file: &File,
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let mut path = fuse_vec::<u8>(b"/proc/self/fdinfo/".len() + 10, work)?;
        path.extend_from_slice(b"/proc/self/fdinfo/");
        write!(&mut *path, "{}", file.as_raw_fd())?;
        let mut info = File::open(std::ffi::OsStr::from_bytes(&path))?;
        let mut contents = fuse_vec::<u8>(256, work)?;
        let mut chunk = [0_u8; 256];
        loop {
            work.admit(Copy.delta(chunk.len() as u64))?;
            let length = match info.read(&mut chunk) {
                Ok(0) => break,
                Ok(length) => length,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            work.record_admitted(Copy.delta(length as u64))?;
            fuse_reserve(&mut contents, length, work)?;
            contents.extend_from_slice(chunk.get(..length).ok_or(libc::EIO)?);
        }
        let mut id = None;
        let mut semaphore = None;
        // Linux fs/eventfd.c::eventfd_show_fdinfo supplies these fields while
        // the owned original event holds its kernel ID. No inode-number guess,
        // privileged kcmp, probe write or consumed kick establishes identity.
        for line in contents.split(|byte| *byte == b'\n') {
            if let Some(value) = line.strip_prefix(b"eventfd-id:") {
                if id.is_some() {
                    return Err(libc::EPROTO.into());
                }
                id = Some(decimal(value)?);
            }
            if let Some(value) = line.strip_prefix(b"eventfd-semaphore:") {
                if semaphore.is_some() {
                    return Err(libc::EPROTO.into());
                }
                semaphore = Some(decimal(value)?);
            }
        }
        if semaphore != Some(0) {
            return Err(libc::ENOTSUP.into());
        }
        id.map(Self).ok_or_else(|| libc::ENOTSUP.into())
    }
}

fn decimal(bytes: &[u8]) -> Result<u64, NativeMountError> {
    let bytes = bytes.trim_ascii();
    if bytes.is_empty() {
        return Err(libc::EPROTO.into());
    }
    bytes.iter().try_fold(0_u64, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err(libc::EPROTO.into());
        }
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(*byte - b'0')))
            .ok_or_else(|| libc::EOVERFLOW.into())
    })
}
