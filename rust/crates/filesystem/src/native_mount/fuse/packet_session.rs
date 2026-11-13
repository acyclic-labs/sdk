//! FUSE negotiation and physical reply completion, shared by every transport.

use super::packet::{PacketInput, PacketRequest};
use super::packet_dispatch::{PacketCompletion, PacketFailure};
use super::packet_router::PacketRouter;
use super::*;
use acyclic_native_runtime::fuse_protocol::{self, Prepared, Transition};
use std::sync::atomic::AtomicBool;

#[cfg(test)]
thread_local! {
    static BEFORE_CLOSE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {
        std::cell::RefCell::new(None)
    };
}
// The same maximum as the host session replaced by this entry.
pub(super) use fuse_protocol::MAXIMUM_WRITE;

pub(super) struct PacketSession {
    pub(super) guest: Option<Arc<dyn super::super::GuestSource>>,
    router: PacketRouter,
    protocol: Protocol,
    maximum_write: u32,
}

enum Protocol {
    Kernel(fuse_protocol::Session),
    // Logical backend retirement is independent of the external kernel session.
    Backend(AtomicBool),
}

/// A reply retains projection leases and original work until publication.
/// Abandoning it closes the session: a completed mutation cannot be replayed
/// merely because its transport failed to establish delivery.
#[must_use]
pub(super) struct PacketReply<'a> {
    session: &'a PacketSession,
    completion: PacketCompletion,
    next_phase: Option<Transition>,
    pending: bool,
}

impl PacketReply<'_> {
    pub(super) fn length(&self) -> Option<usize> {
        self.completion.length
    }

    pub(super) fn payload(&self) -> Option<&[u8]> {
        self.completion
            .payload
            .as_ref()
            .map(|payload| payload.bytes.as_ref())
    }

    pub(super) fn delivered(mut self) {
        if let Some(next) = self.next_phase
            && let Protocol::Kernel(protocol) = &self.session.protocol
        {
            protocol.delivered(next);
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
    #[cfg(test)]
    pub(super) fn probe_close(probe: impl FnOnce() + 'static) {
        BEFORE_CLOSE.set(Some(Box::new(probe)));
    }

    pub(super) fn new(
        router: PacketRouter,
        maximum_write: u32,
        page_size: std::num::NonZeroU32,
    ) -> Result<Self, NativeMountError> {
        let protocol = fuse_protocol::Session::new(fuse_protocol::Configuration {
            maximum_write,
            page_size: page_size.get(),
            cache_capabilities: router.cache_capabilities()?.bits(),
        })?;
        Ok(Self {
            guest: None,
            router,
            protocol: Protocol::Kernel(protocol),
            maximum_write,
        })
    }

    /// The external host already owns the native kernel session. This endpoint
    /// serves only logical filesystem requests under the original source work.
    pub(super) fn backend(
        router: PacketRouter,
        maximum_write: u32,
        page_size: std::num::NonZeroU32,
    ) -> Result<Self, NativeMountError> {
        fuse_protocol::Configuration {
            maximum_write,
            page_size: page_size.get(),
            cache_capabilities: 0,
        }
        .validate()?;
        Ok(Self {
            guest: None,
            router,
            protocol: Protocol::Backend(AtomicBool::new(false)),
            maximum_write,
        })
    }

    pub(super) fn ready(&self) -> bool {
        match &self.protocol {
            Protocol::Kernel(protocol) => protocol.ready(),
            Protocol::Backend(closed) => !closed.load(Ordering::Acquire),
        }
    }

    pub(super) fn closed(&self) -> bool {
        match &self.protocol {
            Protocol::Kernel(protocol) => protocol.closed(),
            Protocol::Backend(closed) => closed.load(Ordering::Acquire),
        }
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
        match &self.protocol {
            Protocol::Kernel(protocol) => protocol.close(),
            Protocol::Backend(closed) => closed.store(true, Ordering::Release),
        }
        #[cfg(test)]
        if let Some(probe) = BEFORE_CLOSE.take() {
            probe();
        }
        self.router.close();
    }

    pub(super) fn retire_failure(&self, error: &PacketFailure) {
        // The original source retains its terminal cause. Host ingress must
        // keep answering that original refusal until its retirement owner
        // unmounts, including when a no-reply FORGET reaches it first. Closing
        // here would substitute ENODEV for cancellation on the next lookup.
        // The socket owner separately stops on the returned source failure.
        if !matches!(error, PacketFailure::Source { .. }) {
            self.close();
        }
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
        // These are kernel session/reference messages, not filesystem actions
        // by a POSIX caller. Linux may tag them with root even when only a
        // non-root identity is enrolled. The phase machine and original source
        // guard still govern negotiation, retirement and reference release.
        if let Some(guest) = &self.guest
            && !matches!(request.opcode, 2 | 26 | 38 | 42)
        {
            guest.posix().require_identity(request.owner)?;
            super::guest::require_metadata(guest.posix(), request)?;
        }
        let prepared = match &self.protocol {
            Protocol::Kernel(protocol) => protocol
                .prepare(
                    &fuse_protocol::Request {
                        opcode: request.opcode,
                        unique: request.unique,
                        inode: request.inode,
                        owner: request.owner,
                        body: request.body,
                    },
                    response,
                )
                .map_err(PacketFailure::from)?,
            Protocol::Backend(closed) => {
                if closed.load(Ordering::Acquire) {
                    return Err(PacketFailure::TransportClosed);
                }
                if matches!(request.opcode, 26 | 38) {
                    return Err(libc::EPROTO.into());
                }
                Prepared::Backend
            }
        };
        let (completion, next_phase) = match prepared {
            Prepared::Backend => (self.router.dispatch(request, response)?, None),
            Prepared::Reply { length, transition } => {
                let mut completion = PacketCompletion::reply(length);
                if transition == Some(Transition::Ready)
                    && let Some(work) = self.router.initialize()?
                {
                    completion = completion.retaining_work(work);
                }
                if request.opcode == 38 {
                    self.close();
                }
                (completion, transition)
            }
        };
        Ok(PacketReply {
            session: self,
            completion,
            next_phase,
            pending: true,
        })
    }
}
