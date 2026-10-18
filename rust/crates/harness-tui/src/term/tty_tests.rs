use super::*;
use std::collections::VecDeque;
use std::os::unix::fs::MetadataExt;
use std::time::Duration;

#[path = "../../tests/support/status_flags.rs"]
mod status_flags;

#[test]
fn empty_input_returns_would_block_without_waiting_under_state() -> io::Result<()> {
    let pty = rustix_openpty::openpty(None, None)?;
    let _master = File::from(pty.controller);
    let mut tty = RawTty::from_file(File::from(pty.user))?;
    let result = tty.receive(&mut [0; 8], Instant::now() + Duration::from_millis(1));
    let restored = tty.restore();
    // The old implementation slept until the deadline and returned zero,
    // keeping lifecycle::active locked for the entire wait.
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    restored
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Wrote(usize),
    Again,
    Interrupted,
    Wait,
}

fn record(trace: &mut Vec<Op>, result: &io::Result<usize>) {
    trace.push(match result {
        Ok(count) => Op::Wrote(*count),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Op::Again,
        Err(_) => Op::Interrupted,
    });
}

/// Scripted syscall results: the native error paths, deterministically.
struct Scripted {
    script: VecDeque<io::Result<usize>>,
    trace: Vec<Op>,
}
impl NonblockingWrite for Scripted {
    fn try_write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        let result = self.script.pop_front().unwrap_or(Ok(0));
        record(&mut self.trace, &result);
        result
    }
    fn wait_writable(&mut self, _deadline: Instant) -> io::Result<()> {
        self.trace.push(Op::Wait);
        Ok(())
    }
}

type Loop<D> = fn(&mut D, &[u8], Instant) -> io::Result<usize>;

fn write_all<D: NonblockingWrite>(
    device: &mut D,
    mut bytes: &[u8],
    each: Loop<D>,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !bytes.is_empty() {
        let count = each(device, bytes, deadline)?;
        bytes = bytes
            .get(count..)
            .ok_or_else(|| io::Error::other("wrote more bytes than offered"))?;
    }
    Ok(())
}

/// The removed loop: a fixed 1 ms sleep after EAGAIN, no readiness wait.
fn sleeping<D: NonblockingWrite>(
    device: &mut D,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<usize> {
    loop {
        match device.try_write(bytes) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                std::thread::sleep(Duration::from_millis(1).min(remaining));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

fn scripted_trace(each: Loop<Scripted>) -> Vec<Op> {
    let again = || Err(io::ErrorKind::WouldBlock.into());
    let interrupted = Err(io::ErrorKind::Interrupted.into());
    let mut device = Scripted {
        script: VecDeque::from([again(), interrupted, Ok(3), again(), again(), Ok(5)]),
        trace: Vec::new(),
    };
    write_all(&mut device, &[0; 8], each).unwrap_or_else(|error| panic!("{error}"));
    device.trace
}

const EXPECTED: [Op; 9] = [
    Op::Again,
    Op::Wait,
    Op::Interrupted,
    Op::Wrote(3),
    Op::Again,
    Op::Wait,
    Op::Again,
    Op::Wait,
    Op::Wrote(5),
];

// Property: each EAGAIN is followed by exactly one readiness wait, EINTR is
// retried at once, and nothing else runs: the exact work trace.
#[test]
fn eagain_waits_for_readiness_and_eintr_retries_at_once() {
    assert_eq!(scripted_trace(write_until), EXPECTED, "readiness trace");
}

#[test]
#[should_panic(expected = "readiness trace")]
fn negative_control_sleeping_loop_is_detected() {
    assert_eq!(scripted_trace(sleeping), EXPECTED, "readiness trace");
}

#[test]
fn an_elapsed_deadline_times_out_without_a_syscall() {
    let mut device = Scripted {
        script: VecDeque::new(),
        trace: Vec::new(),
    };
    let result = write_until(&mut device, b"x", Instant::now());
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(device.trace.is_empty());
}

/// The real device's primitives, traced.
struct Traced<'a> {
    tty: &'a mut RawTty,
    trace: Vec<Op>,
    spin: bool,
}
impl NonblockingWrite for Traced<'_> {
    fn try_write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let result = self.tty.try_write(bytes);
        record(&mut self.trace, &result);
        result
    }
    fn wait_writable(&mut self, deadline: Instant) -> io::Result<()> {
        self.trace.push(Op::Wait);
        if self.spin {
            // The removed macOS path for descriptors beyond FD_SETSIZE.
            std::thread::yield_now();
            Ok(())
        } else {
            self.tty.wait_writable(deadline)
        }
    }
}

const FRAME: usize = 256 * 1024;

/// Write a frame far larger than the kernel PTY queue (1 KiB on macOS, 4 KiB
/// on Linux) while the emulator side starts reading late.
/// `select` forces Darwin's /dev/tty-alias wait (unlimited select) on any
/// terminal; elsewhere every terminal has the one poll(2) wait.
fn real_trace(user: File, controller: File, spin: bool, select: bool) -> io::Result<Vec<Op>> {
    let controller_device = controller.metadata()?.rdev();
    let drain = std::thread::spawn(move || {
        let mut controller = controller;
        std::thread::sleep(Duration::from_millis(20));
        let mut total = 0;
        let mut buffer = [0; 65536];
        while total < FRAME {
            match controller.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => total += count,
            }
        }
        // Keep the controller in the join result until restoration is acknowledged.
        (controller, total)
    });
    let original_flags = status_flags::settable_status_flags(&user)?;
    let mut tty = RawTty::from_file(user)?;
    #[cfg(target_vendor = "apple")]
    if select {
        tty.readiness = Some(super::darwin::Readiness::select(&tty.file)?);
    }
    #[cfg(not(target_vendor = "apple"))]
    let _ = select;
    let mut traced = Traced {
        tty: &mut tty,
        trace: Vec::new(),
        spin,
    };
    let written = write_all(&mut traced, &vec![b'x'; FRAME], write_until);
    let trace = traced.trace;
    if let Err(error) = written {
        tty.restore()?;
        return Err(error);
    }
    // Joining transfers controller custody; worker completion cannot hang up
    // the slave while its owner restores termios and descriptor flags.
    let (controller, drained) = drain
        .join()
        .map_err(|_| io::Error::other("drain panicked"))?;
    assert_eq!(
        controller.metadata()?.rdev(),
        controller_device,
        "controller custody must survive until restoration acknowledgement"
    );
    let restored = tty.restore();
    restored.expect("restoration acknowledged while controller is retained");
    let restored = tcgetattr(&tty.file)?;
    assert_eq!(restored.input_modes, tty.original.input_modes);
    assert_eq!(restored.output_modes, tty.original.output_modes);
    assert_eq!(restored.control_modes, tty.original.control_modes);
    assert_eq!(restored.local_modes, tty.original.local_modes);
    assert_eq!(restored.input_speed(), tty.original.input_speed());
    assert_eq!(restored.output_speed(), tty.original.output_speed());
    for index in [
        rustix::termios::SpecialCodeIndex::VMIN,
        rustix::termios::SpecialCodeIndex::VTIME,
    ] {
        assert_eq!(
            restored.special_codes[index],
            tty.original.special_codes[index]
        );
    }
    assert_eq!(
        status_flags::settable_status_flags(&tty.file)?,
        original_flags
    );
    assert_eq!(drained, FRAME);
    drop(controller);
    Ok(trace)
}

/// Exact readiness property: the queue filled, and every wait returned with
/// room for the next write (no spinning, no fixed sleeps).
fn assert_waits_make_progress(trace: &[Op]) {
    assert!(trace.contains(&Op::Wait), "the PTY queue never filled");
    for pair in trace.windows(2) {
        if let [Op::Wait, next] = pair {
            assert!(
                matches!(next, Op::Wrote(count) if *count > 0),
                "a readiness wait returned without room: {pair:?}"
            );
        }
    }
}

#[test]
fn real_pty_waits_only_until_writable() -> io::Result<()> {
    for select in [false, true] {
        let pty = rustix_openpty::openpty(None, None)?;
        let trace = real_trace(
            File::from(pty.user),
            File::from(pty.controller),
            false,
            select,
        )?;
        assert_waits_make_progress(&trace);
    }
    Ok(())
}

/// Descriptors beyond select(2)'s `FD_SETSIZE` (1024) wait like any other, on
/// every wait the platform has (Darwin: kqueue and the unlimited select).
/// Skipped, and reported, when `RLIMIT_NOFILE` cannot reach fd 1500.
#[test]
fn high_descriptors_wait_only_until_writable() -> io::Result<()> {
    use rustix::process::{Resource, getrlimit, setrlimit};
    const HIGH: u64 = 1500;
    let mut limit = getrlimit(Resource::Nofile);
    if limit.maximum.is_some_and(|maximum| maximum <= HIGH) {
        eprintln!("skipped: RLIMIT_NOFILE maximum {limit:?} cannot reach fd {HIGH}");
        return Ok(());
    }
    if limit.current.is_some_and(|current| current <= HIGH) {
        limit.current = Some(limit.maximum.map_or(2048, |maximum| maximum.min(2048)));
        if let Err(error) = setrlimit(Resource::Nofile, limit) {
            eprintln!("skipped: RLIMIT_NOFILE cannot be raised to reach fd {HIGH}: {error}");
            return Ok(());
        }
    }
    for select in [false, true] {
        let pty = rustix_openpty::openpty(None, None)?;
        let high = rustix::io::fcntl_dupfd_cloexec(&pty.user, 1500)?;
        drop(pty.user);
        let trace = real_trace(File::from(high), File::from(pty.controller), false, select)?;
        assert_waits_make_progress(&trace);
    }
    Ok(())
}

#[test]
#[should_panic(expected = "without room")]
fn negative_control_spinning_wait_is_detected() {
    let pty = rustix_openpty::openpty(None, None).unwrap_or_else(|error| panic!("{error}"));
    let trace = real_trace(
        File::from(pty.user),
        File::from(pty.controller),
        true,
        false,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_waits_make_progress(&trace);
}
