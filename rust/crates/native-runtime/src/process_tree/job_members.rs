//! One pinned member or a complete empty Job page from the kernel.
use std::io;

#[derive(Clone, Copy)]
pub(super) struct Page {
    pub assigned: usize,
    pub listed: usize,
    pub complete: bool,
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
