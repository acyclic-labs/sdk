//! The Linux FUSE session ABI, shared by the external host and guest transports.
//!
//! This core owns negotiation and kernel session state. Filesystem operations
//! are delegated only after negotiation has physically completed.

use core::sync::atomic::{AtomicU8, Ordering};

/// Length of the Linux fuse_in_header.
pub const INPUT_HEADER: usize = 40;
/// Length of the Linux fuse_out_header.
pub const OUTPUT_HEADER: usize = 16;
/// The existing protective packet envelope ceiling.
pub const MAXIMUM_WRITE: u32 = 16 * 1024 * 1024;
/// Kernel capability permitting the negotiated page bound.
pub const MAX_PAGES: u64 = 1 << 22;

const COLD: u8 = 0;
const NEGOTIATING: u8 = 1;
const READY: u8 = 2;
const CLOSED: u8 = 3;
const PROTOCOL_MAJOR: u32 = 7;
const PROTOCOL_MINOR: u32 = 31;
const ASYNC_READ: u64 = 1;
const BIG_WRITES: u64 = 1 << 5;
const INIT_EXT: u64 = 1 << 30;
const EPROTO: i32 = 71;
const EINVAL: i32 = 22;
const EOVERFLOW: i32 = 75;
const ENOBUFS: i32 = 105;

/// An exact borrowed kernel request, independent of a filesystem backend.
pub struct Request<'a> {
    /// FUSE operation number.
    pub opcode: u32,
    /// Kernel request identity; reference-release messages may use zero.
    pub unique: u64,
    /// Kernel inode identity.
    pub inode: u64,
    /// Original POSIX user and group identities.
    pub owner: (u32, u32),
    /// Original operation body.
    pub body: &'a [u8],
}

impl<'a> Request<'a> {
    /// Validates the complete header without aligned casts or allocation.
    pub fn parse(packet: &'a [u8]) -> Result<Self, i32> {
        if packet.len() < INPUT_HEADER {
            return Err(EPROTO);
        }
        let mut input = Input::new(packet);
        let length = input.u32()? as usize;
        let opcode = input.u32()?;
        let unique = input.u64()?;
        let inode = input.u64()?;
        let owner = (input.u32()?, input.u32()?);
        let _pid = input.u32()?;
        let padding = input.u32()?;
        if length != packet.len() || padding != 0 || (unique == 0 && !matches!(opcode, 2 | 42)) {
            return Err(EPROTO);
        }
        Ok(Self {
            opcode,
            unique,
            inode,
            owner,
            body: input.remaining(),
        })
    }

    /// Kernel identity rule for a native mount owned by one POSIX user.
    /// Handle operations retain the authority established by their open.
    pub fn owner_allowed(&self, owner_uid: u32) -> bool {
        self.owner.0 == owner_uid
            || matches!(
                self.opcode,
                2 | 15 | 16 | 18 | 20 | 25 | 26 | 28 | 29 | 30 | 38 | 42 | 44
            )
    }
}

/// Checked reads from an operation body; lengths are always original slices.
pub struct Input<'a> {
    remaining: &'a [u8],
}

impl<'a> Input<'a> {
    /// Borrows the original packet bytes.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    /// Consumes one exact field.
    pub fn take(&mut self, length: usize) -> Result<&'a [u8], i32> {
        let (value, tail) = self.remaining.split_at_checked(length).ok_or(EPROTO)?;
        self.remaining = tail;
        Ok(value)
    }
    /// Reads one native-endian ABI word.
    pub fn u32(&mut self) -> Result<u32, i32> {
        Ok(u32::from_ne_bytes(
            self.take(4)?.try_into().map_err(|_| EPROTO)?,
        ))
    }
    /// Reads one native-endian ABI double word.
    pub fn u64(&mut self) -> Result<u64, i32> {
        Ok(u64::from_ne_bytes(
            self.take(8)?.try_into().map_err(|_| EPROTO)?,
        ))
    }
    /// Consumes one NUL-terminated name, returning its bytes without the NUL.
    pub fn name(&mut self) -> Result<&'a [u8], i32> {
        let end = self
            .remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(EPROTO)?;
        self.take(end + 1)?.strip_suffix(&[0]).ok_or(EPROTO)
    }
    /// Borrows all remaining original bytes.
    pub fn remaining(&self) -> &'a [u8] {
        self.remaining
    }
    /// Refuses trailing bytes after an exact ABI body.
    pub fn end(self) -> Result<(), i32> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(EPROTO)
        }
    }
}

/// Original negotiated buffer funding and immutable backend cache policy.
#[derive(Clone, Copy)]
pub struct Configuration {
    /// Maximum admitted read or write body.
    pub maximum_write: u32,
    /// Native host page size.
    pub page_size: u32,
    /// Capabilities admitted by the original filesystem projection.
    pub cache_capabilities: u64,
}

impl Configuration {
    /// Checks the existing page-aligned funded packet envelope.
    pub fn validate(self) -> Result<Self, i32> {
        if !(4096..=MAXIMUM_WRITE).contains(&self.maximum_write)
            || !self.page_size.is_power_of_two()
            || self.page_size < 4096
            || self.maximum_write % self.page_size != 0
        {
            return Err(EINVAL);
        }
        Ok(self)
    }
}

/// Typed refusal retained across the physical host boundary.
#[derive(Clone, Copy, Debug)]
pub enum Error {
    /// A Linux ABI or transport refusal.
    Errno(i32),
    /// A capability needed to fit the originally funded buffers is absent.
    UnsupportedCapabilities(u64),
    /// This session has already closed.
    Closed,
}

impl Error {
    /// Original Linux response status for a negative reply.
    pub fn errno(self) -> i32 {
        match self {
            Self::Errno(errno) => errno,
            Self::UnsupportedCapabilities(_) => 95,
            Self::Closed => 19,
        }
    }
}

impl From<i32> for Error {
    fn from(errno: i32) -> Self {
        Self::Errno(errno)
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Errno(errno) => write!(formatter, "FUSE protocol errno {errno}"),
            Self::UnsupportedCapabilities(flags) => {
                write!(formatter, "FUSE capabilities {flags:#x} are required")
            }
            Self::Closed => formatter.write_str("FUSE session is closed"),
        }
    }
}

impl core::error::Error for Error {}

/// A phase change that is applied only after actual physical publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transition {
    /// A newer protocol major must negotiate the supported major again.
    Renegotiate,
    /// The initialization response has completed.
    Ready,
}

/// A kernel session message or an admitted filesystem operation.
pub enum Prepared {
    /// Invoke the original filesystem backend, retaining its own custody.
    Backend,
    /// Publish these encoded response bytes before applying the transition.
    Reply {
        /// Number of initialized bytes in the original response buffer.
        length: usize,
        /// A successful INIT transition, if any.
        transition: Option<Transition>,
    },
}

/// The single negotiation state machine for physical and guest FUSE transports.
pub struct Session {
    configuration: Configuration,
    phase: AtomicU8,
}

impl Session {
    /// Starts cold with the original admitted configuration.
    pub fn new(configuration: Configuration) -> Result<Self, i32> {
        Ok(Self {
            configuration: configuration.validate()?,
            phase: AtomicU8::new(COLD),
        })
    }
    /// Whether the initialization response has physically completed.
    pub fn ready(&self) -> bool {
        self.phase.load(Ordering::Acquire) == READY
    }
    /// Whether irreversible session retirement has begun.
    pub fn closed(&self) -> bool {
        self.phase.load(Ordering::Acquire) == CLOSED
    }
    /// Prevents any later publication from reopening a closed session.
    pub fn close(&self) {
        self.phase.store(CLOSED, Ordering::Release);
    }
    /// Admits one request or prepares a kernel session response.
    pub fn prepare(&self, request: &Request<'_>, response: &mut [u8]) -> Result<Prepared, Error> {
        if request.opcode == 26 {
            self.phase
                .compare_exchange(COLD, NEGOTIATING, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|phase| {
                    if phase == CLOSED {
                        Error::Closed
                    } else {
                        EPROTO.into()
                    }
                })?;
            return self.initialize(request, response);
        }
        match self.phase.load(Ordering::Acquire) {
            READY => {}
            CLOSED => return Err(Error::Closed),
            _ => return Err(EPROTO.into()),
        }
        if request.opcode == 38 {
            Input::new(request.body).end()?;
            self.close();
            return Ok(Prepared::Reply {
                length: reply(response, request.unique, &[], None)?,
                transition: None,
            });
        }
        if matches!(request.opcode, 15 | 16 | 28 | 44) {
            let mut input = Input::new(request.body);
            input.take(16)?;
            if input.u32()? > self.configuration.maximum_write {
                return Err(Error::Errno(90));
            }
        }
        Ok(Prepared::Backend)
    }
    /// Completes a successful INIT reply, without undoing concurrent retirement.
    pub fn delivered(&self, transition: Transition) {
        let next = match transition {
            Transition::Renegotiate => COLD,
            Transition::Ready => READY,
        };
        let _ = self
            .phase
            .compare_exchange(NEGOTIATING, next, Ordering::AcqRel, Ordering::Acquire);
    }
    fn initialize(&self, request: &Request<'_>, response: &mut [u8]) -> Result<Prepared, Error> {
        let mut input = Input::new(request.body);
        let major = input.u32()?;
        let minor = input.u32()?;
        if major > PROTOCOL_MAJOR {
            let mut body = [0; 8];
            body[..4].copy_from_slice(&PROTOCOL_MAJOR.to_ne_bytes());
            body[4..].copy_from_slice(&PROTOCOL_MINOR.to_ne_bytes());
            return Ok(Prepared::Reply {
                length: reply(response, request.unique, &body, None)?,
                transition: Some(Transition::Renegotiate),
            });
        }
        if major != PROTOCOL_MAJOR || minor < PROTOCOL_MINOR {
            return Err(EPROTO.into());
        }
        let maximum = self.configuration.maximum_write;
        let readahead = input.u32()?.min(maximum);
        let capabilities = u64::from(input.u32()?);
        if capabilities & MAX_PAGES == 0
            && u64::from(maximum) < 32 * u64::from(self.configuration.page_size)
        {
            return Err(Error::UnsupportedCapabilities(MAX_PAGES));
        }
        if capabilities & INIT_EXT != 0 {
            input.take(48)?;
        }
        input.end()?;
        let flags = capabilities
            & (self.configuration.cache_capabilities | ASYNC_READ | BIG_WRITES | MAX_PAGES);
        let flags = u32::try_from(flags).map_err(|_| EOVERFLOW)?;
        let pages = if u64::from(flags) & MAX_PAGES != 0 {
            maximum
                .max(readahead)
                .div_ceil(self.configuration.page_size)
        } else {
            0
        };
        let pages = u16::try_from(pages).map_err(|_| EOVERFLOW)?;
        let mut body = [0; 64];
        for (offset, value) in [
            (0, PROTOCOL_MAJOR),
            (4, PROTOCOL_MINOR),
            (8, readahead),
            (12, flags),
            (20, maximum),
            (24, 1),
        ] {
            body[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
        }
        body[16..18].copy_from_slice(&16_u16.to_ne_bytes());
        body[18..20].copy_from_slice(&12_u16.to_ne_bytes());
        body[28..30].copy_from_slice(&pages.to_ne_bytes());
        Ok(Prepared::Reply {
            length: reply(response, request.unique, &body, None)?,
            transition: Some(Transition::Ready),
        })
    }
}

/// Writes the common response header, including exact negative-reply length.
pub fn reply_header(
    bytes: &mut [u8],
    length: usize,
    unique: u64,
    errno: Option<i32>,
) -> Result<usize, i32> {
    let length = if errno.is_some() {
        OUTPUT_HEADER
    } else {
        length
    };
    if length < OUTPUT_HEADER || bytes.len() < length {
        return Err(ENOBUFS);
    }
    let status = match errno {
        Some(errno) if errno > 0 => -errno,
        Some(_) => return Err(EINVAL),
        None => 0,
    };
    let length_word = u32::try_from(length).map_err(|_| EOVERFLOW)?;
    bytes[..4].copy_from_slice(&length_word.to_ne_bytes());
    bytes[4..8].copy_from_slice(&status.to_ne_bytes());
    bytes[8..16].copy_from_slice(&unique.to_ne_bytes());
    Ok(length)
}

/// Encodes a complete inline response without allocating another envelope.
pub fn reply(bytes: &mut [u8], unique: u64, body: &[u8], errno: Option<i32>) -> Result<usize, i32> {
    let length = OUTPUT_HEADER.checked_add(body.len()).ok_or(EOVERFLOW)?;
    if errno.is_none() {
        bytes
            .get_mut(OUTPUT_HEADER..length)
            .ok_or(ENOBUFS)?
            .copy_from_slice(body);
    }
    reply_header(bytes, length, unique, errno)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_length_requires_backing_storage_but_errors_discard_payload() {
        let mut bytes = [0xa5; OUTPUT_HEADER];
        assert_eq!(
            reply_header(&mut bytes, OUTPUT_HEADER + 1, 7, None),
            Err(ENOBUFS)
        );
        assert_eq!(
            bytes, [0xa5; OUTPUT_HEADER],
            "a refused frame must not publish a header"
        );
        assert_eq!(
            reply_header(&mut bytes, usize::MAX, 7, Some(EINVAL)),
            Ok(OUTPUT_HEADER)
        );
        assert_eq!(
            u32::from_ne_bytes(bytes[..4].try_into().unwrap()) as usize,
            OUTPUT_HEADER
        );
    }
}
