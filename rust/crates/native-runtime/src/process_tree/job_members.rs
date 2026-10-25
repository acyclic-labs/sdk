//! Complete process membership from the kernel's variable-length Job page.
use std::io;

pub(super) const HEADER: usize = size_of::<u64>() / size_of::<usize>();

#[derive(Clone, Copy)]
pub(super) struct Page {
    pub assigned: usize,
    pub listed: usize,
    pub complete: bool,
}

pub(super) fn collect_members(
    mut query: impl FnMut(&mut [usize]) -> io::Result<Page>,
) -> io::Result<Vec<u32>> {
    // The kernel owns the required length. A prefix is never a membership
    // witness: obtain one complete page before any caller can observe it.
    let mut capacity = 1_usize;
    let mut storage = Vec::new();
    loop {
        let slots = HEADER
            .checked_add(capacity)
            .ok_or(io::ErrorKind::InvalidData)?;
        storage
            .try_reserve_exact(slots.saturating_sub(storage.len()))
            .map_err(io::Error::other)?;
        storage.resize(slots, 0_usize);
        let page = query(&mut storage)?;
        if page.listed > capacity || page.listed > page.assigned {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if !page.complete || page.listed < page.assigned {
            if page.assigned <= capacity {
                return Err(io::ErrorKind::InvalidData.into());
            }
            capacity = page.assigned;
            continue;
        }
        let mut members = Vec::new();
        members
            .try_reserve_exact(page.listed)
            .map_err(io::Error::other)?;
        for pid in storage.iter().skip(HEADER).take(page.listed) {
            members.push(u32::try_from(*pid).map_err(|_| io::ErrorKind::InvalidData)?);
        }
        return Ok(members);
    }
}

/// A termination waiter needs one pinned target, but only a consistent,
/// complete empty page can witness that every member has gone.
pub(super) fn first_reported_member(page: Page, pid: usize) -> io::Result<Option<u32>> {
    if page.listed > 1 || page.listed > page.assigned || (!page.complete && page.assigned <= 1) {
        return Err(io::ErrorKind::InvalidData.into());
    }
    if page.assigned == 0 {
        return Ok(None);
    }
    if page.listed != 1 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let pid = u32::try_from(pid).map_err(|_| io::ErrorKind::InvalidData)?;
    if pid == 0 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(Some(pid))
}

#[cfg(test)]
mod tests;
