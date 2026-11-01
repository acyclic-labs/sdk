//! One process-wide alarm on the wall clock.
//!
//! Signed scopes expire at an absolute Unix time, so a monotonic sleep derived
//! once from that time misses a forward clock change. Every registration here
//! shares one index and one thread, which waits on the platform's own
//! wall-clock primitive: a `CLOCK_REALTIME` absolute timerfd that is cancelled
//! on clock changes on Linux, a `NOTE_ABSOLUTE` kqueue timer plus the
//! `com.apple.system.clock_set` notification on Apple platforms, and an
//! absolute waitable timer, which follows system time changes, on Windows.
//! Each wake re-reads the clock and fires every elapsed registration, so a
//! clock change is observed without polling.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

/// Called once on expiry or loss of the original alarm producer. It must
/// only signal its owner and return. Failure retains the original native cause.
pub type AlarmCallback = Arc<dyn Fn(Result<(), Arc<io::Error>>) + Send + Sync>;

/// A pending alarm. Dropping it cancels the alarm.
#[must_use = "dropping the registration cancels the alarm"]
pub struct WallAlarm {
    key: (u64, u64),
}

impl WallAlarm {
    /// Calls `fire` once the wall clock reaches `deadline_unix_ms`, at once if
    /// it already has.
    pub fn register(deadline_unix_ms: u64, fire: AlarmCallback) -> io::Result<Self> {
        let alarms = alarms()?;
        let key = locked(&alarms.index).insert(deadline_unix_ms, fire)?;
        let registration = Self { key };
        if let Err(error) = alarms.waiter.rearm() {
            return Err(retained_error(&fail(&alarms.index, error)));
        }
        Ok(registration)
    }
}

impl Drop for WallAlarm {
    fn drop(&mut self) {
        // The thread may still wake for this deadline; it finds nothing due
        // and waits for the next one.
        if let Ok(alarms) = alarms() {
            locked(&alarms.index).entries.remove(&self.key);
        }
    }
}

#[derive(Default)]
struct Index {
    next: u64,
    entries: BTreeMap<(u64, u64), AlarmCallback>,
    failure: Option<Arc<io::Error>>,
}

impl Index {
    fn insert(&mut self, deadline: u64, fire: AlarmCallback) -> io::Result<(u64, u64)> {
        if let Some(error) = &self.failure {
            return Err(retained_error(error));
        }
        let key = (deadline, self.next);
        self.next = self.next.checked_add(1).ok_or_else(|| {
            io::Error::other(
                "step=wall-alarm-registration; recovery=restart the exhausted alarm owner",
            )
        })?;
        self.entries.insert(key, fire);
        Ok(key)
    }

    /// Removes and returns the callbacks due at `now`, and the next deadline.
    fn take_due(&mut self, now: u64) -> (Vec<AlarmCallback>, Option<u64>) {
        let pending = now
            .checked_add(1)
            .map_or_else(BTreeMap::new, |after| self.entries.split_off(&(after, 0)));
        let due = std::mem::replace(&mut self.entries, pending);
        (
            due.into_values().collect(),
            self.entries.keys().next().map(|(deadline, _)| *deadline),
        )
    }
}

struct Alarms {
    index: Arc<Mutex<Index>>,
    waiter: Arc<platform::Waiter>,
}

fn retained_error(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), Arc::clone(error))
}

fn fail(index: &Mutex<Index>, error: io::Error) -> Arc<io::Error> {
    let (error, pending) = {
        let mut index = locked(index);
        let error = Arc::clone(index.failure.get_or_insert_with(|| Arc::new(error)));
        (error, std::mem::take(&mut index.entries))
    };
    for fire in pending.into_values() {
        fire(Err(Arc::clone(&error)));
    }
    error
}

fn locked(index: &Mutex<Index>) -> MutexGuard<'_, Index> {
    index.lock().unwrap_or_else(PoisonError::into_inner)
}

fn alarms() -> io::Result<&'static Alarms> {
    static ALARMS: OnceLock<Result<Alarms, Arc<io::Error>>> = OnceLock::new();
    ALARMS
        .get_or_init(|| start().map_err(Arc::new))
        .as_ref()
        .map_err(retained_error)
}

fn start() -> io::Result<Alarms> {
    let index = Arc::new(Mutex::new(Index::default()));
    let waiter = Arc::new(platform::Waiter::new()?);
    let (driven, waits) = (Arc::clone(&index), Arc::clone(&waiter));
    std::thread::Builder::new()
        .name("acyclic-wall-alarm".into())
        .spawn(move || drive(&driven, &*waits, now_unix_ms))?;
    Ok(Alarms { index, waiter })
}

/// What one wait of the alarm thread does; the platform waiter and the test
/// model implement it.
trait Wait {
    /// Returns once `deadline` has passed on the wall clock, the clock was
    /// changed, or a registration was added.
    fn wait(&self, deadline: Option<u64>) -> io::Result<()>;
}

impl Wait for platform::Waiter {
    fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
        platform::Waiter::wait(self, deadline)
    }
}

struct Driver<'a> {
    index: &'a Mutex<Index>,
    due: std::vec::IntoIter<AlarmCallback>,
}

impl Drop for Driver<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let error = fail(
            self.index,
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "step=wall-alarm-producer; recovery=restart the lost alarm owner",
            ),
        );
        for fire in self.due.by_ref() {
            fire(Err(Arc::clone(&error)));
        }
    }
}

fn drive(index: &Mutex<Index>, waiter: &dyn Wait, now: impl Fn() -> u64) {
    let mut driver = Driver {
        index,
        due: Vec::new().into_iter(),
    };
    loop {
        let (due, next) = {
            let mut index = locked(index);
            if index.failure.is_some() {
                return;
            }
            index.take_due(now())
        };
        driver.due = due.into_iter();
        for fire in driver.due.by_ref() {
            fire(Ok(()));
        }
        if let Err(error) = waiter.wait(next) {
            fail(index, error);
            return;
        }
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(target_os = "linux")]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

    pub(super) struct Waiter {
        timer: OwnedFd,
        rearm: OwnedFd,
    }

    fn owned(fd: libc::c_int) -> io::Result<OwnedFd> {
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call returned this fresh descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    impl Waiter {
        #[cfg(test)]
        pub(super) fn refused_timer() -> io::Result<Self> {
            let mut waiter = Self::new()?;
            // A real regular-file descriptor is refused by the original
            // timerfd_settime syscall before the OS wait can begin.
            waiter.timer = std::fs::File::open(std::env::current_exe()?)?.into();
            Ok(waiter)
        }

        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: plain descriptor-creating calls without pointers.
            let timer = owned(unsafe {
                libc::timerfd_create(libc::CLOCK_REALTIME, libc::TFD_CLOEXEC | libc::TFD_NONBLOCK)
            })?;
            // SAFETY: as above.
            let rearm = owned(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
            Ok(Self { timer, rearm })
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            let one = 1_u64.to_ne_bytes();
            // SAFETY: writes eight bytes from a live buffer to an owned eventfd.
            let written = unsafe { libc::write(self.rearm.as_raw_fd(), one.as_ptr().cast(), 8) };
            if written < 0 {
                let error = io::Error::last_os_error();
                // A saturated counter is already readable.
                if error.kind() != io::ErrorKind::WouldBlock {
                    return Err(error);
                }
            }
            Ok(())
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            let value = deadline.map_or(
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                |deadline| libc::timespec {
                    tv_sec: libc::time_t::try_from(deadline / 1000).unwrap_or(libc::time_t::MAX),
                    // Below one second, so it fits every `c_long`.
                    tv_nsec: libc::c_long::try_from((deadline % 1000) * 1_000_000).unwrap_or(0),
                },
            );
            let setting = libc::itimerspec {
                it_interval: libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: value,
            };
            // An absolute realtime timer fires when the wall clock reaches
            // the deadline; cancel-on-set also wakes on any clock change.
            let flags = if deadline.is_some() {
                libc::TFD_TIMER_ABSTIME | libc::TFD_TIMER_CANCEL_ON_SET
            } else {
                0
            };
            // SAFETY: an owned timerfd and a live setting; no old value is read.
            if unsafe {
                libc::timerfd_settime(
                    self.timer.as_raw_fd(),
                    flags,
                    &raw const setting,
                    std::ptr::null_mut(),
                )
            } < 0
            {
                let error = io::Error::last_os_error();
                // The clock changed while arming: wake and re-read it.
                if error.raw_os_error() == Some(libc::ECANCELED) {
                    return Ok(());
                }
                return Err(error);
            }
            let mut ready = [
                libc::pollfd {
                    fd: self.timer.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.rearm.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: two live pollfd entries for owned descriptors.
            if unsafe { libc::poll(ready.as_mut_ptr(), 2, -1) } < 0 {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::Interrupted {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            for fd in [&self.timer, &self.rearm] {
                drain(fd)?;
            }
            Ok(())
        }
    }

    fn drain(fd: &OwnedFd) -> io::Result<()> {
        let mut count = [0_u8; 8];
        // SAFETY: reads at most eight bytes into a live buffer.
        if unsafe { libc::read(fd.as_raw_fd(), count.as_mut_ptr().cast(), 8) } < 0 {
            let error = io::Error::last_os_error();
            // Not ready, or a timer cancelled by a clock change.
            if error.kind() != io::ErrorKind::WouldBlock
                && error.raw_os_error() != Some(libc::ECANCELED)
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

    unsafe extern "C" {
        fn notify_register_file_descriptor(
            name: *const libc::c_char,
            notify_fd: *mut libc::c_int,
            flags: libc::c_int,
            out_token: *mut libc::c_int,
        ) -> u32;
        fn notify_cancel(token: libc::c_int) -> u32;
    }

    const REARM: usize = 1;
    const TIMER: usize = 2;

    pub(super) struct Waiter {
        queue: OwnedFd,
        clock_set: OwnedFd,
        token: libc::c_int,
    }

    fn event(ident: usize, filter: i16, flags: u16, fflags: u32, data: isize) -> libc::kevent {
        libc::kevent {
            ident,
            filter,
            flags,
            fflags,
            data,
            udata: std::ptr::null_mut(),
        }
    }

    impl Waiter {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: creates one fresh kqueue descriptor.
            let queue = unsafe { libc::kqueue() };
            if queue < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: a successful call returned this fresh descriptor.
            let queue = unsafe { OwnedFd::from_raw_fd(queue) };
            let (mut fd, mut token) = (-1, 0);
            // SAFETY: a NUL-terminated name and two live out-parameters.
            let status = unsafe {
                notify_register_file_descriptor(
                    c"com.apple.system.clock_set".as_ptr(),
                    &raw mut fd,
                    0,
                    &raw mut token,
                )
            };
            if status != 0 || fd < 0 {
                return Err(io::Error::other(format!(
                    "clock-change notification registration failed: {status}"
                )));
            }
            // SAFETY: the notification system transferred this descriptor.
            let clock_set = unsafe { OwnedFd::from_raw_fd(fd) };
            let waiter = Self {
                queue,
                clock_set,
                token,
            };
            // SAFETY: an owned descriptor; only its status flags change.
            if unsafe {
                libc::fcntl(
                    waiter.clock_set.as_raw_fd(),
                    libc::F_SETFL,
                    libc::O_NONBLOCK,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            waiter.change(&[
                event(
                    REARM,
                    libc::EVFILT_USER,
                    libc::EV_ADD | libc::EV_CLEAR,
                    0,
                    0,
                ),
                event(
                    usize::try_from(waiter.clock_set.as_raw_fd()).unwrap_or(usize::MAX),
                    libc::EVFILT_READ,
                    libc::EV_ADD,
                    0,
                    0,
                ),
            ])?;
            Ok(waiter)
        }

        fn change(&self, changes: &[libc::kevent]) -> io::Result<()> {
            let count = libc::c_int::try_from(changes.len()).unwrap_or(libc::c_int::MAX);
            // SAFETY: a live change list and no event list on an owned kqueue.
            if unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    changes.as_ptr(),
                    count,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            self.change(&[event(REARM, libc::EVFILT_USER, 0, libc::NOTE_TRIGGER, 0)])
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            match deadline {
                // An absolute calendar timer; the clock-set notification
                // covers a clock change made after it was armed.
                Some(deadline) => self.change(&[event(
                    TIMER,
                    libc::EVFILT_TIMER,
                    libc::EV_ADD | libc::EV_ONESHOT,
                    libc::NOTE_ABSOLUTE | libc::NOTE_USECONDS,
                    isize::try_from(deadline.saturating_mul(1000)).unwrap_or(isize::MAX),
                )])?,
                None => {
                    // Absent when it already fired or was never armed.
                    let _ = self.change(&[event(TIMER, libc::EVFILT_TIMER, libc::EV_DELETE, 0, 0)]);
                }
            }
            let mut ready = [event(0, 0, 0, 0, 0)];
            // SAFETY: one live event slot on an owned kqueue; no timeout.
            if unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    ready.as_mut_ptr(),
                    1,
                    std::ptr::null(),
                )
            } < 0
            {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::Interrupted {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            let mut tokens = [0_u8; 64];
            // SAFETY: drains posted tokens into a live buffer.
            while unsafe {
                libc::read(
                    self.clock_set.as_raw_fd(),
                    tokens.as_mut_ptr().cast(),
                    tokens.len(),
                )
            } > 0
            {}
            Ok(())
        }
    }

    impl Drop for Waiter {
        fn drop(&mut self) {
            // SAFETY: cancels this waiter's own registration once.
            unsafe { notify_cancel(self.token) };
        }
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "the wall-clock timer and its wake handles are platform OS calls"
)]
mod platform {
    use std::io;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED};
    use windows_sys::Win32::System::Threading::{
        CancelWaitableTimer, CreateEventW, CreateWaitableTimerExW, INFINITE,
        SYNCHRONIZATION_SYNCHRONIZE, SetEvent, SetWaitableTimerEx, TIMER_MODIFY_STATE,
        WaitForMultipleObjects,
    };

    /// 100-nanosecond intervals from 1601 to the Unix epoch.
    const UNIX_EPOCH_TICKS: i64 = 116_444_736_000_000_000;

    pub(super) struct Waiter {
        timer: OwnedHandle,
        rearm: OwnedHandle,
    }

    fn owned(handle: HANDLE) -> io::Result<OwnedHandle> {
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful creation transfers one fresh owned handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
    }

    impl Waiter {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: an unnamed timer with default security.
            let timer = owned(unsafe {
                CreateWaitableTimerExW(
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    SYNCHRONIZATION_SYNCHRONIZE | TIMER_MODIFY_STATE,
                )
            })?;
            // SAFETY: an unnamed, initially clear, auto-reset event.
            let rearm = owned(unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) })?;
            Ok(Self { timer, rearm })
        }

        pub(super) fn rearm(&self) -> io::Result<()> {
            // SAFETY: signals an owned event.
            if unsafe { SetEvent(self.rearm.as_raw_handle()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            if let Some(deadline) = deadline {
                // A positive due time is absolute UTC; the system adjusts
                // absolute timers when its time changes.
                let due = i64::try_from(deadline)
                    .ok()
                    .and_then(|deadline| deadline.checked_mul(10_000))
                    .and_then(|ticks| ticks.checked_add(UNIX_EPOCH_TICKS))
                    .unwrap_or(i64::MAX);
                // SAFETY: an owned timer and a live due time, without a callback.
                if unsafe {
                    SetWaitableTimerEx(
                        self.timer.as_raw_handle(),
                        &raw const due,
                        0,
                        None,
                        std::ptr::null(),
                        std::ptr::null(),
                        0,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
            } else {
                // SAFETY: an owned timer; cancelling an unarmed timer is a no-op.
                unsafe { CancelWaitableTimer(self.timer.as_raw_handle()) };
            }
            let handles = [self.timer.as_raw_handle(), self.rearm.as_raw_handle()];
            // SAFETY: two owned handles that outlive the wait.
            if unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } == WAIT_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    fn counter() -> (Arc<AtomicU64>, AlarmCallback) {
        let fired = Arc::new(AtomicU64::new(0));
        let count = Arc::clone(&fired);
        (
            fired,
            Arc::new(move |_outcome| {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    #[test]
    fn signed_scope_expiry_wakes_a_quiet_owner_without_a_task_deadline() -> io::Result<()> {
        // Nothing else registers or changes after this alarm: only the
        // platform's wall-clock wait can deliver it.
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        let _alarm = WallAlarm::register(
            now_unix_ms() + 50,
            Arc::new(move |outcome| {
                let _ = locked_sender(&sender).send(outcome);
            }),
        )?;
        received
            .recv()
            .map_err(|_| io::Error::other("quiet alarm never fired"))?
            .map_err(|error| retained_error(&error))
    }

    fn locked_sender<T>(sender: &Mutex<T>) -> MutexGuard<'_, T> {
        sender.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[test]
    fn an_elapsed_deadline_fires_at_registration_and_a_dropped_one_never() -> io::Result<()> {
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        let _elapsed = WallAlarm::register(
            0,
            Arc::new(move |outcome| {
                let _ = locked_sender(&sender).send(outcome);
            }),
        )?;
        received
            .recv()
            .map_err(|_| io::Error::other("elapsed alarm never fired"))?
            .map_err(|error| retained_error(&error))?;
        let (fired, fire) = counter();
        let pending = WallAlarm::register(u64::MAX, fire)?;
        let key = pending.key;
        assert!(locked(&alarms()?.index).entries.contains_key(&key));
        drop(pending);
        assert!(!locked(&alarms()?.index).entries.contains_key(&key));
        assert_eq!(fired.load(Ordering::SeqCst), 0, "a cancelled alarm fired");
        Ok(())
    }

    /// The thread's own loop over a scripted clock: each wait advances the
    /// clock as the platform's wake would report it.
    struct Script {
        clock: Arc<AtomicU64>,
        steps: Mutex<Vec<(Option<u64>, u64)>>,
    }

    impl Wait for Script {
        fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
            let mut steps = locked_sender(&self.steps);
            if steps.is_empty() {
                return Err(io::Error::other("script finished"));
            }
            let (armed, now) = steps.remove(0);
            assert_eq!(deadline, armed, "the alarm armed another deadline");
            self.clock.store(now, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn alarm_producer_loss_retains_native_failure_and_refuses_late_registration() {
        let index = Mutex::new(Index::default());
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        let fire: AlarmCallback = Arc::new(move |outcome| {
            locked_sender(&sender).send(outcome).unwrap();
        });
        for deadline in [1, u64::MAX] {
            locked(&index).insert(deadline, Arc::clone(&fire)).unwrap();
        }
        let waiter = platform::Waiter::refused_timer().unwrap();
        let original = waiter.wait(None).unwrap_err();
        drive(&index, &waiter, || 0);
        let first = received.try_recv().unwrap().unwrap_err();
        assert_eq!(first.kind(), original.kind());
        assert_eq!(first.raw_os_error(), original.raw_os_error());
        let second = received.try_recv().unwrap().unwrap_err();
        assert!(Arc::ptr_eq(&first, &second));
        let late = locked(&index).insert(1, fire).unwrap_err();
        assert!(Arc::ptr_eq(
            &first,
            late.get_ref()
                .unwrap()
                .downcast_ref::<Arc<io::Error>>()
                .unwrap()
        ));
        let retained = fail(
            &index,
            io::Error::new(io::ErrorKind::BrokenPipe, "later producer loss"),
        );
        assert!(Arc::ptr_eq(&first, &retained));
        assert!(locked(&index).entries.is_empty());

        struct Lost;
        impl Wait for Lost {
            fn wait(&self, _: Option<u64>) -> io::Result<()> {
                panic!("original producer unwound");
            }
        }
        let index = Mutex::new(Index::default());
        let (sender, received) = mpsc::channel();
        let sender = Mutex::new(sender);
        locked(&index)
            .insert(
                1,
                Arc::new(move |outcome| {
                    locked_sender(&sender).send(outcome).unwrap();
                }),
            )
            .unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drive(&index, &Lost, || 0);
            }))
            .is_err()
        );
        assert_eq!(
            received.try_recv().unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn deadline_registration_rearms_after_a_clock_jump() {
        let index = Mutex::new(Index::default());
        let (fired, fire) = counter();
        locked(&index).insert(10_000, Arc::clone(&fire)).unwrap();
        locked(&index).insert(60_000, fire).unwrap();
        let clock = Arc::new(AtomicU64::new(0));
        // The first wait arms 10s; the clock is then set forward past it
        // before that monotonic span could elapse. The wake re-reads the
        // clock, fires the elapsed alarm and re-arms the later one, which a
        // second forward change also fires.
        let script = Script {
            clock: Arc::clone(&clock),
            steps: Mutex::new(vec![
                (Some(10_000), 30_000),
                (Some(60_000), 90_000),
                (None, 90_000),
            ]),
        };
        let now = Arc::clone(&clock);
        // The finished script ends the loop through its failure path.
        drive(&index, &script, move || now.load(Ordering::SeqCst));
        assert_eq!(fired.load(Ordering::SeqCst), 2);
        assert!(locked(&index).entries.is_empty());
    }
}
