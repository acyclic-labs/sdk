#[cfg(target_os = "linux")]
use super::*;
use std::cell::{Cell, RefCell};

type Edit = (u64, Box<dyn FnOnce()>);
thread_local! {
    static STATE: Cell<Option<(bool, u64, u64)>> = const { Cell::new(None) };
    static EDIT: RefCell<Option<Edit>> = const { RefCell::new(None) };
    static EVENTS: RefCell<Option<(usize, [&'static str; 512])>> = const { RefCell::new(None) };
    static DUPLICATE: Cell<bool> = const { Cell::new(false) };
    static RESTORE_WALK: Cell<bool> = const { Cell::new(false) };
    static EXAMINATIONS: Cell<Option<u64>> = const { Cell::new(None) };
    #[cfg(target_os = "linux")]
    static FALLBACK: Cell<bool> = const { Cell::new(false) };
}

#[cfg(target_os = "linux")]
pub(crate) struct Fallback(bool);
#[cfg(target_os = "linux")]
impl Fallback {
    pub(crate) fn enter(force: bool) -> Self {
        Self(FALLBACK.replace(force))
    }
}
#[cfg(target_os = "linux")]
impl Drop for Fallback {
    fn drop(&mut self) {
        FALLBACK.set(self.0);
    }
}
#[cfg(target_os = "linux")]
pub(crate) fn statx_result<T>(
    result: Result<T, rustix::io::Errno>,
) -> Result<T, rustix::io::Errno> {
    if FALLBACK.get() {
        Err(rustix::io::Errno::NOSYS)
    } else {
        result
    }
}
#[cfg(target_os = "linux")]
pub(crate) fn uses_fallback(file: &File) -> bool {
    matches!(
        statx_result(rustix::fs::statx(
            file,
            c"",
            rustix::fs::AtFlags::EMPTY_PATH | rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
        )),
        Err(rustix::io::Errno::NOSYS | rustix::io::Errno::PERM)
    )
}

#[cfg(target_os = "linux")]
pub(crate) fn fstat_query() {
    event("fstat");
    if EVENTS.with_borrow(Option::is_none) {
        if let Some((old, bits, count)) = STATE.get() {
            STATE.set(Some((old, bits.checked_mul(2).unwrap(), count)));
        }
    }
}

pub(crate) struct Probe;
impl Probe {
    pub(crate) fn rescan(old: bool) -> Self {
        let probe = Self::new(old, None);
        EVENTS.set(Some((0, [""; 512])));
        EXAMINATIONS.set(Some(0));
        probe
    }
    pub(crate) fn publish(&self) {
        EVENTS.with_borrow(|events| {
            let (length, events) = events.as_ref().unwrap();
            for event in &events[..*length] {
                acyclic_test_support::differential::record(*event);
            }
        });
    }
    pub(crate) fn examinations(&self) -> u64 {
        EXAMINATIONS.get().unwrap()
    }
    pub(crate) fn duplicate_queries(&self) {
        DUPLICATE.set(true);
    }
    pub(crate) fn restore_enumeration(&self) {
        RESTORE_WALK.set(true);
    }
    pub(crate) fn new(old: bool, edit: Option<Edit>) -> Self {
        assert!(STATE.replace(Some((old, 1, 0))).is_none());
        EDIT.set(edit);
        Self
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn trace(&self) -> u64 {
        STATE.get().unwrap().1
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        STATE.set(None);
        EDIT.set(None);
        EVENTS.set(None);
        DUPLICATE.set(false);
        RESTORE_WALK.set(false);
        EXAMINATIONS.set(None);
    }
}
pub(crate) fn old() -> bool {
    STATE.get().is_some_and(|(old, _, _)| old)
}
pub(crate) fn duplicate() -> bool {
    DUPLICATE.get()
}
pub(crate) fn restore_walk() -> bool {
    RESTORE_WALK.get()
}
#[cfg(target_os = "linux")]
pub(super) fn query(handle: bool) {
    event(if handle { "handle" } else { "statx" });
    if EVENTS.with_borrow(Option::is_some) {
        return;
    }
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
pub(crate) fn event(event: &'static str) {
    EVENTS.with_borrow_mut(|events| {
        if let Some((length, events)) = events {
            events[*length] = event;
            *length += 1;
        }
    });
}
pub(crate) fn examined(items: u64) {
    if let Some(examined) = EXAMINATIONS.get() {
        EXAMINATIONS.set(Some(examined.checked_add(items).unwrap()));
    }
}
#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
pub(crate) fn after_ranges() {
    edit_at(0);
}

// Frozen descriptor query before metadata-only capture; never calls statx_at.
#[cfg(target_os = "linux")]
pub(crate) fn baseline<W: NativeHostWork>(file: &File, work: &mut W) -> Result<HostStat, W::Error> {
    baseline_at(file.as_raw_fd(), c"", work)
}

#[cfg(target_os = "linux")]
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
            match statx_result(rustix::fs::statx(
                directory,
                name,
                flags,
                rustix::fs::StatxFlags::BASIC_STATS | rustix::fs::StatxFlags::BTIME,
            )) {
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
