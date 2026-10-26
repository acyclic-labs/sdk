//! Bounded encoding of the standard Linux FUSE ABI, independent of its transport.
//!
//! Layout reference: Linux v6.12, include/uapi/linux/fuse.h (protocol 7.31).
//! Fields are decoded without casting caller storage to aligned Rust structs.

use super::*;

pub(super) const INPUT_HEADER: usize = 40;
pub(super) const OUTPUT_HEADER: usize = 16;

pub(super) struct PacketRequest<'a> {
    pub(super) opcode: u32,
    pub(super) unique: u64,
    pub(super) inode: u64,
    pub(super) owner: (u32, u32),
    pub(super) body: &'a [u8],
}

impl<'a> PacketRequest<'a> {
    pub(super) fn parse(packet: &'a [u8]) -> Result<Self, i32> {
        if packet.len() < INPUT_HEADER {
            return Err(libc::EPROTO);
        }
        let mut input = PacketInput::new(packet);
        let len = usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?;
        let opcode = input.u32()?;
        let unique = input.u64()?;
        let inode = input.u64()?;
        let owner = (input.u32()?, input.u32()?);
        let _pid = input.u32()?;
        let padding = input.u32()?;
        if len != packet.len() || padding != 0 || (unique == 0 && !matches!(opcode, 2 | 42)) {
            return Err(libc::EPROTO);
        }
        Ok(Self {
            opcode,
            unique,
            inode,
            owner,
            body: input.remaining(),
        })
    }
}

pub(super) struct PacketInput<'a> {
    remaining: &'a [u8],
}

impl<'a> PacketInput<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    pub(super) fn take(&mut self, len: usize) -> Result<&'a [u8], i32> {
        let (value, tail) = self.remaining.split_at_checked(len).ok_or(libc::EPROTO)?;
        self.remaining = tail;
        Ok(value)
    }
    pub(super) fn u32(&mut self) -> Result<u32, i32> {
        Ok(u32::from_ne_bytes(
            self.take(4)?.try_into().map_err(|_| libc::EPROTO)?,
        ))
    }
    pub(super) fn u64(&mut self) -> Result<u64, i32> {
        Ok(u64::from_ne_bytes(
            self.take(8)?.try_into().map_err(|_| libc::EPROTO)?,
        ))
    }
    pub(super) fn name(&mut self) -> Result<&'a OsStr, i32> {
        let end = self
            .remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(libc::EPROTO)?;
        let value = self.take(end + 1)?;
        Ok(OsStr::from_bytes(
            value.strip_suffix(&[0]).ok_or(libc::EPROTO)?,
        ))
    }
    pub(super) fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    pub(super) fn end(self) -> Result<(), i32> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(libc::EPROTO)
        }
    }
}

pub(super) struct PacketOutput<'a> {
    bytes: &'a mut [u8],
    length: usize,
    capacity: usize,
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
    pub(super) fn u32(&mut self, value: u32) -> Result<(), i32> {
        self.bytes(&value.to_ne_bytes())
    }
    pub(super) fn u64(&mut self, value: u64) -> Result<(), i32> {
        self.bytes(&value.to_ne_bytes())
    }
    pub(super) fn finish(&mut self, unique: u64, errno: Option<i32>) -> Result<usize, i32> {
        if let Some(errno) = errno {
            if errno <= 0 {
                return Err(libc::EINVAL);
            }
            self.length = OUTPUT_HEADER;
            self.bytes
                .get_mut(4..8)
                .ok_or(libc::ENOBUFS)?
                .copy_from_slice(&(-errno).to_ne_bytes());
        }
        let length = u32::try_from(self.length).map_err(|_| libc::EOVERFLOW)?;
        self.bytes
            .get_mut(..4)
            .ok_or(libc::ENOBUFS)?
            .copy_from_slice(&length.to_ne_bytes());
        self.bytes
            .get_mut(8..16)
            .ok_or(libc::ENOBUFS)?
            .copy_from_slice(&unique.to_ne_bytes());
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
