//! Physical split-ring completion over the one FUSE packet entry.
//!
//! rust-vmm owns descriptor layout, memory mappings and ring publication.
//! Snapshot each descriptor once before dispatch: constructing separate Readers
//! and Writers would walk mutable guest descriptor storage twice.

use super::packet::PacketRequest;
use super::packet_buffers::PacketBuffers;
use super::packet_session::PacketSession;
use super::vring::{PacketVring, RingFeatures};
use super::vring_memory::GuestRam;
use super::*;
use std::io::IoSlice;
use virtio_queue::{QueueOwnedT, QueueT, desc::split::Descriptor};
use vm_memory::{Address, Bytes, GuestAddress, GuestMemoryBackend, GuestMemoryMmap, VolatileSlice};

pub(super) enum QueueProgress {
    Empty,
    Completed,
    Stopped,
}

struct Segment<'a> {
    bytes: VolatileSlice<'a>,
    writable: bool,
}

/// Reused only while the caller pins this exact guest-memory generation.
pub(super) struct VringScratch<'a> {
    memory: &'a GuestRam,
    segments: FuseOwned<Vec<Segment<'a>>>,
}

impl<'a> VringScratch<'a> {
    pub(super) fn new(
        memory: &'a GuestRam,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        Ok(Self {
            memory,
            segments: fuse_vec(0, work)?,
        })
    }
}

/// The actual queue head and guest ranges stay affine through used publication.
struct VringPacket<'a, 'scratch> {
    head: u16,
    segments: &'scratch mut FuseOwned<Vec<Segment<'a>>>,
    readable: usize,
    writable: usize,
}

impl<'a, 'scratch> VringPacket<'a, 'scratch> {
    fn snapshot(
        memory: &'a GuestMemoryMmap<()>,
        head: u16,
        mut table: GuestAddress,
        capacity: usize,
        work: &crate::NativeMaterializationWork,
        indirect_enabled: bool,
        segments: &'scratch mut FuseOwned<Vec<Segment<'a>>>,
    ) -> Result<Self, NativeMountError> {
        segments.clear();
        let mut packet = Self {
            head,
            segments,
            readable: 0,
            writable: 0,
        };
        let mut count = 0_usize;
        let mut table_size = capacity;
        let mut index = head;
        let mut indirect = false;
        let mut total_bytes = 0_u32;
        let mut writable_seen = false;
        loop {
            work.charge(Items.delta(1))?;
            if usize::from(index) >= table_size || count >= capacity {
                return Err(libc::EPROTO.into());
            }
            let address = table
                .checked_add(u64::from(index) * 16)
                .ok_or(libc::EOVERFLOW)?;
            let descriptor = memory
                .read_obj::<Descriptor>(address)
                .map_err(transport_error)?;
            if descriptor.flags() & !7 != 0 {
                return Err(libc::EPROTO.into());
            }
            // DescriptorChain::next hides the indirect wrapper. Read each
            // maintained POD descriptor once so unnegotiated/nested indirect
            // tables cannot be accepted or re-read under guest mutation.
            if descriptor.refers_to_indirect_table() {
                let length = descriptor.len() as usize;
                if !indirect_enabled
                    || indirect
                    || descriptor.has_next()
                    || length == 0
                    || length % 16 != 0
                    || length / 16 > usize::from(u16::MAX) + 1
                    || !memory.check_range(descriptor.addr(), length)
                {
                    return Err(libc::EPROTO.into());
                }
                table = descriptor.addr();
                table_size = length / 16;
                index = 0;
                indirect = true;
                continue;
            }
            count += 1;
            total_bytes = total_bytes
                .checked_add(descriptor.len())
                .ok_or(libc::EOVERFLOW)?;
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
                fuse_reserve(packet.segments, 1, work)?;
                packet.segments.push(Segment {
                    bytes: slice.map_err(transport_error)?,
                    writable,
                });
            }
            if !descriptor.has_next() {
                break;
            }
            index = descriptor.next();
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
                call.event.notify()?;
            }
        }
        Ok(())
    }
}

fn pop_packet<'a, 'scratch>(
    vring: &mut PacketVring,
    memory: &'a Arc<GuestMemoryMmap<()>>,
    work: &crate::NativeMaterializationWork,
    indirect: bool,
    segments: &'scratch mut FuseOwned<Vec<Segment<'a>>>,
) -> Result<Option<VringPacket<'a, 'scratch>>, NativeMountError> {
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
    let table = GuestAddress(queue.desc_table());
    let chain = queue
        .iter(memory.clone())
        .map_err(transport_error)?
        .next()
        .ok_or(libc::EPROTO)?;
    VringPacket::snapshot(
        memory.as_ref(),
        chain.head_index(),
        table,
        capacity,
        work,
        indirect,
        segments,
    )
    .map(Some)
}

/// Caller holds this exact vring's lock throughout callback and publication.
/// Thus SET_VRING_ENABLE/GET_VRING_BASE cannot report a drained queue while its
/// original request is still executing. The original memory Arc stays live too.
/// On error the queue is marked failed before that lock can be released. The
/// control owner must retire the connection and retain this actual error.
pub(super) fn serve_vring_packet(
    session: &PacketSession,
    vring: &mut PacketVring,
    scratch: &mut VringScratch<'_>,
    buffers: &mut PacketBuffers,
    work: &crate::NativeMaterializationWork,
    hiprio: bool,
    features: RingFeatures,
) -> Result<QueueProgress, NativeMountError> {
    let result = (|| {
        let memory = scratch.memory.memory();
        let Some(packet) = pop_packet(
            vring,
            memory,
            work,
            features.indirect,
            &mut scratch.segments,
        )?
        else {
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
                let slices = reply.slices();
                packet.complete(
                    vring,
                    memory.as_ref(),
                    slices.as_ref().map_or(&[], |slices| slices.as_slice()),
                    work,
                    features.event_idx,
                )
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

fn transport_error(error: impl std::error::Error + Send + Sync + 'static) -> NativeMountError {
    std::io::Error::other(error).into()
}
