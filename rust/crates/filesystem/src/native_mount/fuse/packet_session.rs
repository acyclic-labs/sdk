//! FUSE negotiation and physical reply completion, shared by every transport.

use super::packet::{PacketInput, PacketOutput, PacketRequest};
use super::packet_dispatch::{PacketCompletion, PacketFailure};
use super::packet_router::PacketRouter;
use super::*;
use std::sync::atomic::AtomicU8;

const COLD: u8 = 0;
const NEGOTIATING: u8 = 1;
const READY: u8 = 2;
const CLOSED: u8 = 3;
const PROTOCOL_MAJOR: u32 = 7;
const PROTOCOL_MINOR: u32 = 31;
// The same maximum as the host session replaced by this entry.
pub(super) const MAXIMUM_WRITE: u32 = 16 * 1024 * 1024;

pub(super) struct PacketSession {
    router: PacketRouter,
    phase: AtomicU8,
    maximum_write: u32,
    page_size: std::num::NonZeroU32,
}

/// A reply retains projection leases and original work until publication.
/// Abandoning it closes the session: a completed mutation cannot be replayed
/// merely because its transport failed to establish delivery.
#[must_use]
pub(super) struct PacketReply<'a> {
    session: &'a PacketSession,
    completion: PacketCompletion,
    next_phase: Option<u8>,
    pending: bool,
}

impl PacketReply<'_> {
    pub(super) fn length(&self) -> Option<usize> {
        self.completion.length
    }

    pub(super) fn delivered(mut self) {
        if let Some(next) = self.next_phase {
            // A concurrent close must never be undone by late publication.
            let _ = self.session.phase.compare_exchange(
                NEGOTIATING,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        self.pending = false;
    }
}

impl Drop for PacketReply<'_> {
    fn drop(&mut self) {
        if self.pending {
            self.session.close();
        }
    }
}

impl PacketSession {
    pub(super) fn new(
        router: PacketRouter,
        maximum_write: u32,
        page_size: std::num::NonZeroU32,
    ) -> Result<Self, NativeMountError> {
        if !(4096..=MAXIMUM_WRITE).contains(&maximum_write)
            || !page_size.get().is_power_of_two()
            || page_size.get() < 4096
            || maximum_write % page_size.get() != 0
        {
            return Err(libc::EINVAL.into());
        }
        Ok(Self {
            router,
            phase: AtomicU8::new(COLD),
            maximum_write,
            page_size,
        })
    }

    pub(super) fn ready(&self) -> bool {
        self.phase.load(Ordering::Acquire) == READY
    }

    pub(super) fn packet_capacity(&self) -> usize {
        self.maximum_write as usize + 4096
    }

    pub(super) fn single_core(&self) -> Option<&Arc<ProjectionCore>> {
        match &self.router {
            PacketRouter::Single(projection) => Some(&projection.core),
            PacketRouter::Shared(_) => None,
        }
    }

    pub(super) fn close(&self) {
        self.phase.store(CLOSED, Ordering::Release);
        self.router.close();
    }

    pub(super) fn dispatch<'a>(
        &'a self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
    ) -> Result<PacketReply<'a>, PacketFailure> {
        let span = callback_span!(TRACE, packet, opcode = request.opcode, ino = request.inode);
        let result = self.dispatch_inner(request, response);
        let errno = match &result {
            Ok(reply) if reply.length().is_some() => {
                let mut input = PacketInput::new(response);
                input
                    .u32()
                    .ok()
                    .and_then(|_| input.u32().ok())
                    .map(|bits| i32::from_ne_bytes(bits.to_ne_bytes()))
                    .filter(|value| *value < 0)
                    .and_then(i32::checked_neg)
            }
            Ok(_) => None,
            Err(error) => Some(error.errno()),
        };
        if let Some(errno) = errno {
            errno_on(&span, errno);
        }
        result
    }

    fn dispatch_inner<'a>(
        &'a self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
    ) -> Result<PacketReply<'a>, PacketFailure> {
        let (completion, next_phase) = if request.opcode == 26 {
            self.phase
                .compare_exchange(COLD, NEGOTIATING, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| libc::EPROTO)?;
            match self.initialize(request, response) {
                Ok(result) => result,
                Err(error) => {
                    self.close();
                    return Err(error);
                }
            }
        } else {
            if !self.ready() {
                return Err(libc::EPROTO.into());
            }
            if request.opcode == 38 {
                PacketInput::new(request.body).end()?;
                let mut output = PacketOutput::new(response, 0)?;
                let completion = PacketCompletion::reply(output.finish(request.unique, None)?);
                self.close();
                (completion, None)
            } else {
                if matches!(request.opcode, 15 | 16 | 28 | 44) {
                    let mut input = PacketInput::new(request.body);
                    input.take(16)?;
                    if input.u32()? > self.maximum_write {
                        return Err(libc::EMSGSIZE.into());
                    }
                }
                match self.router.dispatch(request, response) {
                    Ok(completion) => (completion, None),
                    Err(error) => {
                        if error.is_terminal() {
                            self.close();
                        }
                        return Err(error);
                    }
                }
            }
        };
        Ok(PacketReply {
            session: self,
            completion,
            next_phase,
            pending: true,
        })
    }

    fn initialize(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
    ) -> Result<(PacketCompletion, Option<u8>), PacketFailure> {
        let mut input = PacketInput::new(request.body);
        let major = input.u32()?;
        let minor = input.u32()?;
        if major > PROTOCOL_MAJOR {
            let mut output = PacketOutput::new(response, 8)?;
            output.u32(PROTOCOL_MAJOR)?;
            output.u32(PROTOCOL_MINOR)?;
            return Ok((
                PacketCompletion::reply(output.finish(request.unique, None)?),
                Some(COLD),
            ));
        }
        // This entry uses the current fixed layouts, without a second legacy
        // decoder. All virtio-fs kernels support at least this protocol.
        if major != PROTOCOL_MAJOR || minor < PROTOCOL_MINOR {
            return Err(libc::EPROTO.into());
        }
        let readahead = input.u32()?.min(self.maximum_write);
        let capabilities = InitFlags::from_bits_retain(u64::from(input.u32()?));
        // Without MAX_PAGES the kernel keeps its default 32-page read bound,
        // regardless of max_write. Do not admit a bound the transport cannot
        // fund. With MAX_PAGES, both reads and writes fit the same page-aligned
        // admitted capacity (Linux fs/fuse/{inode,file}.c).
        if !capabilities.contains(InitFlags::FUSE_MAX_PAGES)
            && u64::from(self.maximum_write) < 32 * u64::from(self.page_size.get())
        {
            return Err(
                NativeMountError::FuseUnsupportedCapabilities(InitFlags::FUSE_MAX_PAGES).into(),
            );
        }
        if capabilities.contains(InitFlags::FUSE_INIT_EXT) {
            input.take(48)?;
        }
        input.end()?;
        let mut output = PacketOutput::new(response, 64)?;
        let flags = capabilities
            & (REQUESTED_CAPABILITIES
                | InitFlags::FUSE_ASYNC_READ
                | InitFlags::FUSE_BIG_WRITES
                | InitFlags::FUSE_MAX_PAGES);
        let work = self.router.initialize()?;
        output.u32(PROTOCOL_MAJOR)?;
        output.u32(PROTOCOL_MINOR)?;
        output.u32(readahead)?;
        output.u32(u32::try_from(flags.bits()).map_err(|_| libc::EOVERFLOW)?)?;
        output.bytes(&16_u16.to_ne_bytes())?;
        output.bytes(&12_u16.to_ne_bytes())?;
        output.u32(self.maximum_write)?;
        output.u32(1)?; // nanosecond timestamp granularity
        let pages = if flags.contains(InitFlags::FUSE_MAX_PAGES) {
            self.maximum_write
                .max(readahead)
                .div_ceil(self.page_size.get())
        } else {
            0
        };
        output.bytes(
            &u16::try_from(pages)
                .map_err(|_| libc::EOVERFLOW)?
                .to_ne_bytes(),
        )?;
        output.bytes(&0_u16.to_ne_bytes())?;
        output.bytes(&[0; 32])?; // flags2 and reserved fields
        let mut completion = PacketCompletion::reply(output.finish(request.unique, None)?);
        if let Some(work) = work {
            completion = completion.retaining_work(work);
        }
        Ok((completion, Some(READY)))
    }
}
