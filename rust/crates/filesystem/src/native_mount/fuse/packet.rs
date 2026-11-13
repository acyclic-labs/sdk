//! Bounded encoding of the standard Linux FUSE ABI, independent of its transport.
//!
//! Layout reference: Linux v6.12, include/uapi/linux/fuse.h (protocol 7.31).
//! Fields are decoded without casting caller storage to aligned Rust structs.

use super::*;
use acyclic_native_runtime::fuse_protocol;

pub(super) use fuse_protocol::OUTPUT_HEADER;

pub(super) struct PacketRequest<'a> {
    pub(super) opcode: u32,
    pub(super) unique: u64,
    pub(super) inode: u64,
    pub(super) owner: (u32, u32),
    pub(super) body: &'a [u8],
    pub(super) minimum_payload: Result<usize, i32>,
}

impl<'a> PacketRequest<'a> {
    pub(super) fn parse(packet: &'a [u8]) -> Result<Self, i32> {
        let fuse_protocol::Request {
            opcode,
            unique,
            inode,
            owner,
            body,
        } = fuse_protocol::Request::parse(packet)?;
        Ok(Self {
            opcode,
            unique,
            inode,
            owner,
            body,
            minimum_payload: minimum_payload(opcode, body),
        })
    }
}

fn minimum_payload(opcode: u32, body: &[u8]) -> Result<usize, i32> {
    Ok(match opcode {
        1 | 6 | 8 | 9 | 13 => 128,
        3 | 4 => 104,
        14 | 27 => 16,
        35 => 144,
        16 | 46 | 47 => 8,
        17 => 80,
        26 => 64,
        15 | 28 | 44 => {
            let mut input = PacketInput::new(body);
            input.take(16)?;
            usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?
        }
        22 | 23 => {
            let mut input = PacketInput::new(body);
            match input.u32()? {
                0 => 8,
                size => usize::try_from(size).map_err(|_| libc::EOVERFLOW)?,
            }
        }
        _ => 0,
    })
}

pub(super) struct PacketInput<'a>(fuse_protocol::Input<'a>);

impl<'a> PacketInput<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self(fuse_protocol::Input::new(bytes))
    }
    pub(super) fn take(&mut self, len: usize) -> Result<&'a [u8], i32> {
        self.0.take(len)
    }
    pub(super) fn u32(&mut self) -> Result<u32, i32> {
        self.0.u32()
    }
    pub(super) fn u64(&mut self) -> Result<u64, i32> {
        self.0.u64()
    }
    pub(super) fn name(&mut self) -> Result<&'a OsStr, i32> {
        self.0.name().map(OsStr::from_bytes)
    }
    pub(super) fn remaining(&self) -> &'a [u8] {
        self.0.remaining()
    }
    pub(super) fn end(self) -> Result<(), i32> {
        self.0.end()
    }
}

pub(super) struct PacketOutput<'a> {
    bytes: &'a mut [u8],
    length: usize,
    capacity: usize,
    pub(super) payload: Option<FuseOwned<crate::kernel::OwnedSourceChunk>>,
    pub(super) complete: bool,
    pub(super) plus: bool,
}

impl<'a> PacketOutput<'a> {
    /// Reserve before dispatching a mutation, so its successful result always fits.
    pub(super) fn new(bytes: &'a mut [u8], minimum_payload: usize) -> Result<Self, i32> {
        if bytes.len()
            < OUTPUT_HEADER
                .checked_add(minimum_payload)
                .ok_or(libc::EOVERFLOW)?
        {
            return Err(libc::ENOBUFS);
        }
        let capacity = bytes.len();
        bytes.get_mut(..OUTPUT_HEADER).ok_or(libc::ENOBUFS)?.fill(0);
        Ok(Self {
            bytes,
            length: OUTPUT_HEADER,
            capacity,
            payload: None,
            complete: false,
            plus: false,
        })
    }
    pub(super) fn remaining(&self) -> usize {
        self.capacity - self.length
    }
    pub(super) fn limit_payload(&mut self, size: usize) -> Result<(), i32> {
        let capacity = OUTPUT_HEADER.checked_add(size).ok_or(libc::EOVERFLOW)?;
        if capacity > self.capacity || capacity < self.length {
            return Err(libc::ENOBUFS);
        }
        self.capacity = capacity;
        Ok(())
    }
    pub(super) fn bytes(&mut self, value: &[u8]) -> Result<(), i32> {
        if self.payload.is_some() {
            return Err(libc::EPROTO);
        }
        let end = self
            .length
            .checked_add(value.len())
            .ok_or(libc::EOVERFLOW)?;
        if end > self.capacity {
            return Err(libc::ENOBUFS);
        }
        self.bytes
            .get_mut(self.length..end)
            .ok_or(libc::ENOBUFS)?
            .copy_from_slice(value);
        self.length = end;
        Ok(())
    }
    pub(super) fn retain(
        &mut self,
        payload: FuseOwned<crate::kernel::OwnedSourceChunk>,
    ) -> Result<(), i32> {
        if self.length != OUTPUT_HEADER || self.payload.is_some() {
            return Err(libc::EPROTO);
        }
        let length = OUTPUT_HEADER
            .checked_add(payload.bytes.len())
            .ok_or(libc::EOVERFLOW)?;
        if length > self.capacity {
            return Err(libc::ENOBUFS);
        }
        self.length = length;
        self.payload = Some(payload);
        Ok(())
    }
    pub(super) fn u32(&mut self, value: u32) -> Result<(), i32> {
        self.bytes(&value.to_ne_bytes())
    }
    pub(super) fn u64(&mut self, value: u64) -> Result<(), i32> {
        self.bytes(&value.to_ne_bytes())
    }
    pub(super) fn finish(&mut self, unique: u64, errno: Option<i32>) -> Result<usize, i32> {
        self.length = fuse_protocol::reply_header(self.bytes, self.length, unique, errno)?;
        if errno.is_some() {
            self.payload = None;
        }
        Ok(self.length)
    }
    fn timestamp(value: SystemTime) -> Result<(u64, u32), i32> {
        match value.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(value) => Ok((value.as_secs(), value.subsec_nanos())),
            Err(error) => {
                let value = error.duration();
                let secs = i64::try_from(value.as_secs()).map_err(|_| libc::EOVERFLOW)?;
                let secs = secs
                    .checked_neg()
                    .and_then(|secs| secs.checked_sub(i64::from(value.subsec_nanos() != 0)))
                    .ok_or(libc::EOVERFLOW)?;
                let nanos = if value.subsec_nanos() == 0 {
                    0
                } else {
                    1_000_000_000 - value.subsec_nanos()
                };
                Ok((secs as u64, nanos))
            }
        }
    }
    pub(super) fn attr(&mut self, attr: &FileAttr) -> Result<(), i32> {
        let (atime, atimensec) = Self::timestamp(attr.atime)?;
        let (mtime, mtimensec) = Self::timestamp(attr.mtime)?;
        let (ctime, ctimensec) = Self::timestamp(attr.ctime)?;
        for value in [attr.ino.0, attr.size, attr.blocks, atime, mtime, ctime] {
            self.u64(value)?;
        }
        for value in [
            atimensec,
            mtimensec,
            ctimensec,
            file_mode(attr.kind) | u32::from(attr.perm),
            attr.nlink,
            attr.uid,
            attr.gid,
            attr.rdev,
            attr.blksize,
            0,
        ] {
            self.u32(value)?;
        }
        Ok(())
    }
    pub(super) fn entry(&mut self, attr: &FileAttr, ttl: &Duration) -> Result<(), i32> {
        self.u64(attr.ino.0)?;
        self.u64(0)?;
        self.u64(ttl.as_secs())?;
        self.u64(ttl.as_secs())?;
        self.u32(ttl.subsec_nanos())?;
        self.u32(ttl.subsec_nanos())?;
        self.attr(attr)
    }
    pub(super) fn attributes(&mut self, attr: &FileAttr, ttl: &Duration) -> Result<(), i32> {
        self.u64(ttl.as_secs())?;
        self.u32(ttl.subsec_nanos())?;
        self.u32(0)?;
        self.attr(attr)
    }
    pub(super) fn unknown_capacity(&mut self) -> Result<(), i32> {
        // Existing fuser default: unknown capacity, 512-byte blocks, NAME_MAX 255.
        for _ in 0..5 {
            self.u64(0)?;
        }
        for value in [512, 255, 0, 0, 0, 0, 0, 0, 0, 0] {
            self.u32(value)?;
        }
        Ok(())
    }
    pub(super) fn opened(&mut self, handle: u64, flags: FopenFlags) -> Result<(), i32> {
        self.u64(handle)?;
        self.u32(flags.bits())?;
        self.u32(0)
    }
    pub(super) fn directory_entry(
        &mut self,
        offset: u64,
        name: &OsStr,
        attr: &FileAttr,
        ttl: &Duration,
    ) -> Result<bool, i32> {
        let name = name.as_bytes();
        let record = 24usize
            .checked_add(name.len())
            .and_then(|len| len.checked_add(7))
            .map(|len| len & !7)
            .ok_or(libc::EOVERFLOW)?;
        let needed = record
            .checked_add(if self.plus { 128 } else { 0 })
            .ok_or(libc::EOVERFLOW)?;
        if self.remaining() < needed {
            return Ok(true);
        }
        if self.plus {
            // Validate fallible fields before touching the record or its lookup reference.
            Self::timestamp(attr.atime)?;
            Self::timestamp(attr.mtime)?;
            Self::timestamp(attr.ctime)?;
            self.entry(attr, ttl)?;
        }
        self.u64(attr.ino.0)?;
        self.u64(offset)?;
        self.u32(u32::try_from(name.len()).map_err(|_| libc::EOVERFLOW)?)?;
        self.u32(file_mode(attr.kind) >> 12)?;
        self.bytes(name)?;
        let padding = record - 24 - name.len();
        self.bytes([0; 7].get(..padding).ok_or(libc::EOVERFLOW)?)?;
        Ok(false)
    }
}

impl DirectoryOutput for &mut PacketOutput<'_> {
    fn counts_lookups(&self) -> bool {
        self.plus
    }
    fn push(
        &mut self,
        offset: u64,
        name: &OsStr,
        attr: &FileAttr,
        ttl: &Duration,
    ) -> Result<bool, NativeMountError> {
        self.directory_entry(offset, name, attr, ttl)
            .map_err(Into::into)
    }
    fn ok(self) {
        self.complete = true;
    }
}

fn file_mode(kind: FileType) -> u32 {
    match kind {
        FileType::RegularFile => S_IFREG,
        FileType::Directory => S_IFDIR,
        FileType::Symlink => S_IFLNK,
        FileType::NamedPipe => S_IFIFO,
        FileType::Socket => S_IFSOCK,
        FileType::CharDevice => S_IFCHR,
        FileType::BlockDevice => S_IFBLK,
    }
}
