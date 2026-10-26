//! Physical split-ring completion over the one FUSE packet entry.
//!
//! rust-vmm owns descriptor traversal, memory mappings and ring publication.
//! Snapshot each descriptor once before dispatch: constructing separate Readers
//! and Writers would walk mutable guest descriptor storage twice.

use super::packet::PacketRequest;
use super::packet_buffers::PacketBuffers;
use super::packet_session::PacketSession;
use super::vring::PacketVring;
use super::*;
use std::io::IoSlice;
use std::ops::Deref;
use virtio_queue::{DescriptorChain, QueueOwnedT, QueueT};
use vm_memory::{
    GuestAddressSpace, GuestMemory, GuestMemoryAtomic, GuestMemoryMmap, VolatileSlice,
};

pub(super) type GuestRam = GuestMemoryAtomic<GuestMemoryMmap<()>>;

pub(super) enum QueueProgress {
    Empty,
    Completed,
    Stopped,
}

struct Segment<'a> {
    bytes: VolatileSlice<'a>,
    writable: bool,
}

/// The actual queue head and guest ranges stay affine through used publication.
struct VringPacket<'a> {
    head: u16,
    segments: FuseOwned<Vec<Segment<'a>>>,
    readable: usize,
    writable: usize,
}

impl<'a> VringPacket<'a> {
    fn snapshot<M>(
        memory: &'a GuestMemoryMmap<()>,
        chain: DescriptorChain<M>,
        capacity: usize,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError>
    where
        M: Deref<Target = GuestMemoryMmap<()>>,
    {
        let mut packet = Self {
            head: chain.head_index(),
            segments: fuse_vec(capacity, work)?,
            readable: 0,
            writable: 0,
        };
        let mut count = 0_usize;
        let mut ended = false;
        let mut writable_seen = false;
        for descriptor in chain {
            work.charge(Items.delta(1))?;
            count = count.checked_add(1).ok_or(libc::EOVERFLOW)?;
            if count > capacity || ended || descriptor.flags() & !3 != 0 {
                return Err(libc::EPROTO.into());
            }
            let writable = descriptor.is_write_only();
            if writable_seen && !writable {
                return Err(libc::EPROTO.into());
            }
            writable_seen |= writable;
            let size = descriptor.len() as usize;
            let length = if writable {
                &mut packet.writable
            } else {
                &mut packet.readable
            };
            *length = length.checked_add(size).ok_or(libc::EOVERFLOW)?;
            for slice in memory.get_slices(descriptor.addr(), size) {
                work.charge(Items.delta(1))?;
                // A descriptor can cross several admitted RAM regions. Queue
                // size bounds descriptors, not the number of mapped slices.
                fuse_reserve(&mut packet.segments, 1, work)?;
                packet.segments.push(Segment {
                    bytes: slice.map_err(transport_error)?,
                    writable,
                });
            }
            ended = !descriptor.has_next();
        }
        // The maintained iterator stops on malformed pointers and cycles too.
        // Those are failures, never a successful truncated request.
        if count == 0 || !ended {
            return Err(libc::EPROTO.into());
        }
        Ok(packet)
    }

    fn gather(
        &self,
        input: &mut [u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if input.len() != self.readable {
            return Err(libc::EMSGSIZE.into());
        }
        work.charge(Copy.delta(self.readable as u64))?;
        let mut remaining = input;
        for segment in self.segments.iter().filter(|segment| !segment.writable) {
            let (target, tail) = remaining
                .split_at_mut_checked(segment.bytes.len())
                .ok_or(libc::EPROTO)?;
            if segment.bytes.copy_to(target) != target.len() {
                return Err(libc::EIO.into());
            }
            remaining = tail;
        }
        if !remaining.is_empty() {
            return Err(libc::EPROTO.into());
        }
        Ok(())
    }

    fn scatter(
        &self,
        output: &[IoSlice<'_>],
        work: &crate::NativeMaterializationWork,
    ) -> Result<u32, NativeMountError> {
        let length = output.iter().try_fold(0_usize, |length, part| {
            length.checked_add(part.len()).ok_or(libc::EOVERFLOW)
        })?;
        if length > self.writable {
            return Err(libc::ENOBUFS.into());
        }
        let length = u32::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        work.charge(Copy.delta(u64::from(length)))?;
        let mut segments = self.segments.iter().filter(|segment| segment.writable);
        let mut current = segments.next();
        let mut offset = 0;
        for part in output {
            let mut remaining: &[u8] = part;
            while !remaining.is_empty() {
                let segment = current.ok_or(libc::EPROTO)?;
                let size = remaining.len().min(segment.bytes.len() - offset);
                let (source, tail) = remaining.split_at_checked(size).ok_or(libc::EPROTO)?;
                segment
                    .bytes
                    .subslice(offset, size)
                    .map_err(transport_error)?
                    .copy_from(source);
                remaining = tail;
                offset += size;
                if offset == segment.bytes.len() {
                    current = segments.next();
                    offset = 0;
                }
            }
        }
        Ok(length)
    }

    fn complete(
        &self,
        vring: &mut PacketVring,
        memory: &GuestMemoryMmap<()>,
        output: &[IoSlice<'_>],
        work: &crate::NativeMaterializationWork,
        event_idx: bool,
    ) -> Result<(), NativeMountError> {
        let length = self.scatter(output, work)?;
        // Use the actual original mapping for the used ring and event index.
        let queue = &mut vring.queue;
        queue
            .add_used(memory, self.head, length)
            .map_err(transport_error)?;
        if !event_idx || queue.needs_notification(memory).map_err(transport_error)? {
            if let Some(call) = &vring.call {
                call.notify()?;
            }
        }
        Ok(())
    }
}

fn pop_packet<'a>(
    vring: &mut PacketVring,
    memory: &'a Arc<GuestMemoryMmap<()>>,
    work: &crate::NativeMaterializationWork,
) -> Result<Option<VringPacket<'a>>, NativeMountError> {
    work.admit(crate::WorkCounters::UNCHARGED)?;
    // A control operation may have disabled this ring since the kick was read.
    if !vring.running() {
        return Ok(None);
    }
    let queue = &mut vring.queue;
    let capacity = usize::from(queue.size());
    let available = queue
        .avail_idx(memory.as_ref(), Ordering::Acquire)
        .map_err(transport_error)?
        .0;
    let pending = available.wrapping_sub(queue.next_avail());
    if pending == 0 {
        return Ok(None);
    }
    if usize::from(pending) > capacity {
        return Err(libc::EPROTO.into());
    }
    let chain = queue
        .iter(memory.clone())
        .map_err(transport_error)?
        .next()
        .ok_or(libc::EPROTO)?;
    VringPacket::snapshot(memory.as_ref(), chain, capacity, work).map(Some)
}

/// Caller holds this exact vring's lock throughout callback and publication.
/// Thus SET_VRING_ENABLE/GET_VRING_BASE cannot report a drained queue while its
/// original request is still executing. The original memory Arc stays live too.
/// On error the queue is marked failed before that lock can be released. The
/// control owner must retire the connection and retain this actual error.
pub(super) fn serve_vring_packet(
    session: &PacketSession,
    vring: &mut PacketVring,
    memory: &GuestRam,
    buffers: &mut PacketBuffers,
    work: &crate::NativeMaterializationWork,
    hiprio: bool,
    event_idx: bool,
) -> Result<QueueProgress, NativeMountError> {
    let result = (|| {
        let memory = memory.memory().into_inner();
        let Some(packet) = pop_packet(vring, &memory, work)? else {
            return Ok(QueueProgress::Empty);
        };
        let input = buffers
            .input
            .get_mut(..packet.readable)
            .ok_or(libc::EMSGSIZE)?;
        packet.gather(input, work)?;
        let request = PacketRequest::parse(input)?;
        if hiprio != matches!(request.opcode, 2 | 36 | 42) {
            return Err(libc::EPROTO.into());
        }
        // Bound the packet entry before dispatching any filesystem effect.
        let capacity = buffers.output.len().min(packet.writable);
        let response = buffers.output.get_mut(..capacity).ok_or(libc::ENOBUFS)?;
        let running = session
            .publish(&request, response, |reply| {
                let reply = IoSlice::new(reply.unwrap_or_default());
                packet.complete(vring, memory.as_ref(), &[reply], work, event_idx)
            })
            .map_err(super::packet_dispatch::PacketFailure::into_native_error)?;
        Ok(if running {
            QueueProgress::Completed
        } else {
            QueueProgress::Stopped
        })
    })();
    if result.is_err() {
        // Neither a malformed chain nor uncertain used publication authorizes
        // replay, descriptor substitution, or a manufactured completion.
        vring.fail();
        session.close();
    }
    result
}

/// Attempt physical delivery from the SAME canonical invalidation pass.
/// `false` means no guest buffer was consumed: the caller retains its original
/// PendingInvalidation without acknowledging or moving it into another queue.
/// An error is terminal transport uncertainty, never permission to retry a
/// consumed descriptor or acknowledge the notification.
/// This is a physical primitive, not notification-feature admission. The
/// eventual frontend profile must support these canonical FUSE message types;
/// Virtio 1.2's NOTIFICATION bit alone does not establish invalidation support.
pub(super) fn serve_vring_notification(
    vring: &mut PacketVring,
    memory: &GuestRam,
    message: &[IoSlice<'_>],
    work: &crate::NativeMaterializationWork,
    event_idx: bool,
) -> Result<bool, NativeMountError> {
    let result = (|| {
        let memory = memory.memory().into_inner();
        let Some(packet) = pop_packet(vring, &memory, work)? else {
            return Ok(false);
        };
        if packet.readable != 0 {
            return Err(libc::EPROTO.into());
        }
        packet.complete(vring, memory.as_ref(), message, work, event_idx)?;
        Ok(true)
    })();
    if result.is_err() {
        vring.fail();
    }
    result
}

fn transport_error(error: impl std::error::Error + Send + Sync + 'static) -> NativeMountError {
    std::io::Error::other(error).into()
}
