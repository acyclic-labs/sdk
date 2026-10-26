//! One offered memory table, retained through its original used publication.

use super::{FuseOwned, NativeMountError, fuse_vec};
use crate::{NativeMaterializationWork, materializer::NativeResourceCredit::Items};
use std::{alloc::Layout, fs::File, sync::Arc};
use vhost::vhost_user::message::{MAX_ATTACHED_FD_ENTRIES, VhostUserMemoryRegion};
use vm_memory::{GuestAddress, GuestMemoryMmap, GuestMemoryRegion, GuestRegionMmap};

struct Window {
    user: u64,
    guest: u64,
    length: u64,
}

pub(super) struct GuestRam {
    // This custody includes the region vector and each map's Arc<File>,
    // Arc<GuestRegionMmap> and outer Arc. No unfunded from_regions collection.
    memory: FuseOwned<Arc<GuestMemoryMmap<()>>>,
    windows: FuseOwned<Vec<Window>>,
}

impl GuestRam {
    pub(super) fn new(
        offers: &[VhostUserMemoryRegion],
        files: Vec<File>,
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        if offers.is_empty()
            || offers.len() > MAX_ATTACHED_FD_ENTRIES
            || offers.len() != files.len()
        {
            return Err(libc::EINVAL.into());
        }
        let mut windows = fuse_vec::<Window>(offers.len(), work)?;
        // Validate every range before creating any mapping. The descriptor
        // received by SCM_RIGHTS remains the actual mapping's file owner.
        for (offer, file) in offers.iter().zip(&files) {
            work.charge(Items.delta(1))?;
            let length = offer.memory_size;
            let guest = offer.guest_phys_addr;
            let user = offer.user_addr;
            let offset = offer.mmap_offset;
            if length == 0
                || usize::try_from(length).is_err()
                || guest.checked_add(length).is_none()
                || user.checked_add(length).is_none()
            {
                return Err(libc::EINVAL.into());
            }
            let end = offset.checked_add(length).ok_or(libc::EOVERFLOW)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() || end > metadata.len() {
                return Err(libc::EINVAL.into());
            }
            for previous in windows.iter() {
                work.charge(Items.delta(1))?;
                if overlaps(user, length, previous.user, previous.length)
                    || overlaps(guest, length, previous.guest, previous.length)
                {
                    return Err(libc::EINVAL.into());
                }
            }
            windows.push(Window {
                user,
                guest,
                length,
            });
        }

        let mut regions = fuse_vec::<Arc<GuestRegionMmap<()>>>(offers.len(), work)?;
        let per_region = arc_bytes::<File>()?
            .checked_add(arc_bytes::<GuestRegionMmap<()>>()?)
            .ok_or(libc::EOVERFLOW)?;
        let outer = arc_bytes::<GuestMemoryMmap<()>>()?;
        let bytes = per_region
            .checked_mul(offers.len() as u64)
            .and_then(|bytes| bytes.checked_add(outer))
            .ok_or(libc::EOVERFLOW)?;
        regions.claim(bytes, work)?;
        for (offer, file) in offers.iter().zip(files) {
            work.charge(Items.delta(1))?;
            let mapping = offer
                .mmap_region::<()>(file)
                .map_err(std::io::Error::other)?;
            let region = GuestRegionMmap::new(mapping, GuestAddress(offer.guest_phys_addr))
                .ok_or(libc::EINVAL)?;
            regions.push(Arc::new(region));
        }
        // The protocol permits arbitrary ordering of the offered regions.
        // Unstable sorting needs no additional allocation.
        regions.sort_unstable_by_key(|region| region.start_addr());
        let memory = regions
            .try_map(GuestMemoryMmap::from_arc_regions)
            .map_err(std::io::Error::other)?
            .map(Arc::new);
        Ok(Self { memory, windows })
    }

    pub(super) fn memory(&self) -> &Arc<GuestMemoryMmap<()>> {
        &self.memory
    }

    /// Translate the complete ring range, not just its first byte. A ring
    /// crossing region boundaries requires contiguous userspace AND GPA ranges.
    pub(super) fn translate(
        &self,
        user: u64,
        length: u64,
        work: &NativeMaterializationWork,
    ) -> Result<GuestAddress, NativeMountError> {
        let end = user.checked_add(length).ok_or(libc::EOVERFLOW)?;
        if length == 0 {
            return Err(libc::EINVAL.into());
        }
        let mut cursor = user;
        let mut base = None;
        while cursor < end {
            let mut found = None;
            for window in self.windows.iter() {
                work.charge(Items.delta(1))?;
                let window_end = window.user + window.length;
                if cursor >= window.user && cursor < window_end {
                    found = Some((window, window_end));
                    break;
                }
            }
            let (window, window_end) = found.ok_or(libc::EFAULT)?;
            let guest = window.guest + (cursor - window.user);
            let initial = *base.get_or_insert(guest);
            if initial.checked_add(cursor - user) != Some(guest) {
                return Err(libc::EFAULT.into());
            }
            cursor = end.min(window_end);
        }
        base.map(GuestAddress).ok_or_else(|| libc::EFAULT.into())
    }
}

fn overlaps(a: u64, a_length: u64, b: u64, b_length: u64) -> bool {
    // All four endpoints were checked before inserting a window.
    a < b + b_length && b < a + a_length
}

fn arc_bytes<T>() -> Result<u64, NativeMountError> {
    crate::arc_layout::allocation_bytes(Layout::new::<T>())
        .map(|bytes| bytes as u64)
        .map_err(|_| libc::EOVERFLOW.into())
}
