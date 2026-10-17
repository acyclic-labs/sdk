use super::*;
use std::cell::{Cell, RefCell};

type Edit = (u64, Box<dyn FnOnce()>);
thread_local! {
    static STATE: Cell<Option<(bool, u64, u64)>> = const { Cell::new(None) };
    static EDIT: RefCell<Option<Edit>> = const { RefCell::new(None) };
}

pub(crate) struct Probe;
impl Probe {
    pub(crate) fn new(old: bool, edit: Option<Edit>) -> Self {
        assert!(STATE.replace(Some((old, 1, 0))).is_none());
        EDIT.set(edit);
        Self
    }
    pub(crate) fn trace(&self) -> u64 {
        STATE.get().unwrap().1
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        STATE.set(None);
        EDIT.set(None);
    }
}
pub(crate) fn old() -> bool {
    STATE.get().is_some_and(|(old, _, _)| old)
}
pub(super) fn query(handle: bool) {
    if let Some((old, bits, count)) = STATE.get() {
        let count = count + u64::from(!handle);
        STATE.set(Some((
            old,
            bits.checked_mul(2).unwrap() + u64::from(handle),
            count,
        )));
        if !handle {
            edit_at(count);
        }
    }
}
fn edit_at(point: u64) {
    let edit = EDIT.with_borrow_mut(|edit| {
        if edit.as_ref().is_some_and(|(at, _)| *at == point) {
            edit.take()
        } else {
            None
        }
    });
    if let Some((_, edit)) = edit {
        edit();
    }
}
pub(crate) fn after_ranges() {
    edit_at(0);
}

// Frozen descriptor query before metadata-only capture; never calls statx_at.
pub(crate) fn baseline<W: NativeHostWork>(file: &File, work: &mut W) -> Result<HostStat, W::Error> {
    baseline_at(file.as_raw_fd(), c"", work)
}

pub(crate) fn baseline_at<W: NativeHostWork>(
    fd: i32,
    name: &std::ffi::CStr,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(target_os = "linux")]
    {
        let directory = if fd == libc::AT_FDCWD {
            rustix::fs::CWD
        } else {
            // SAFETY: the caller holds this descriptor for the complete query.
            unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) }
        };
        let flags = rustix::fs::AtFlags::SYMLINK_NOFOLLOW
            | if name.is_empty() && fd != libc::AT_FDCWD {
                rustix::fs::AtFlags::EMPTY_PATH
            } else {
                rustix::fs::AtFlags::empty()
            };
        loop {
            work.started_io(READ)?;
            query(false);
            match rustix::fs::statx(
                directory,
                name,
                flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
            ) {
                Ok(stat) => {
                    let handle = file_handle_at(fd, name, flags);
                    return Ok(HostStat::from_statx(stat).with_handle(handle));
                }
                Err(rustix::io::Errno::INTR) => continue,
                Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM) => break,
                Err(error) => return Err(io::Error::from(error).into()),
            }
        }
    }
    Ok(HostStat::from_stat(fstat(fd, name, work)?)?)
}
