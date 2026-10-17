//! The production input stream on a real PTY: reactor readiness, no threads.
#![cfg(all(unix, feature = "tui", not(acyclic_harness_tui_loom)))]

use std::fs::File;
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{Events, ProbeIo, Tty};
use futures_core::Stream;
use tokio::runtime::Runtime;

// A process owns one terminal lifecycle; these tests take turns.
static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|error| error.into_inner())
}

fn pty() -> io::Result<(File, File)> {
    let pty = rustix_openpty::openpty(None, None)?;
    Ok((File::from(pty.controller), File::from(pty.user)))
}

fn runtime(name: &str) -> io::Result<Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name(name)
        .build()
}

/// Threads of this process carrying a runtime's (blocking pool) thread name.
#[cfg(target_os = "linux")]
fn threads_named(name: &str) -> io::Result<usize> {
    let mut count = 0;
    for task in std::fs::read_dir("/proc/self/task")? {
        if std::fs::read_to_string(task?.path().join("comm"))?.trim_end() == name {
            count += 1;
        }
    }
    Ok(count)
}

fn poll_pending(runtime: &Runtime, future: &mut (impl std::future::Future + Unpin)) {
    runtime.block_on(std::future::poll_fn(|cx| {
        assert!(Pin::new(&mut *future).poll(cx).is_pending());
        Poll::Ready(())
    }));
}

fn next(runtime: &Runtime, events: &mut Events) -> io::Result<InputEvent> {
    runtime
        .block_on(std::future::poll_fn(|cx| {
            Pin::new(&mut *events).poll_next(cx)
        }))
        .ok_or(io::ErrorKind::UnexpectedEof)?
}

fn key(code: KeyCode, modifiers: KeyModifiers) -> InputEvent {
    InputEvent::Key(KeyEvent::new(code, modifiers))
}

struct NextEvent<'a>(&'a mut Events);
impl std::future::Future for NextEvent<'_> {
    type Output = Option<io::Result<InputEvent>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.0).poll_next(cx)
    }
}

#[test]
fn dropping_a_waiting_stream_releases_the_terminal_without_a_thread() -> io::Result<()> {
    let _serial = serial();
    let (_master, user) = pty()?;
    let runtime = runtime("tui-input-prod")?;
    let mut events = runtime.block_on(async { Tty::from_file(user.try_clone()?)?.events() })?;
    poll_pending(&runtime, &mut NextEvent(&mut events));
    #[cfg(target_os = "linux")]
    assert_eq!(
        threads_named("tui-input-prod")?,
        0,
        "input waits on a thread"
    );
    drop(events);
    // Nothing still holds the terminal: the owner slot is free immediately.
    drop(Tty::from_file(user)?);
    Ok(())
}

#[test]
fn negative_control_detects_spawn_blocking_reader_left_blocked() -> io::Result<()> {
    let _serial = serial();
    let (master, user) = pty()?;
    let runtime = runtime("tui-input-neg")?;
    let tty = Tty::from_file(user.try_clone()?)?;
    // The removed path: the terminal moves into a blocking task per batch,
    // waiting on a 24-hour deadline.
    let (started, entered) = std::sync::mpsc::sync_channel(0);
    let mut read = runtime.spawn_blocking(move || {
        let mut tty = tty;
        let _ = started.send(());
        tty.receive(&mut [0; 1024], Instant::now() + Duration::from_secs(86_400))
    });
    // Release the blocked read on assertion unwind before the runtime joins it.
    let mut master = master;
    entered.recv().map_err(|_| io::ErrorKind::BrokenPipe)?;
    poll_pending(&runtime, &mut read);
    drop(read);
    #[cfg(target_os = "linux")]
    assert_eq!(threads_named("tui-input-neg")?, 1);
    assert!(
        matches!(Tty::from_file(user.try_clone()?), Err(error) if error.kind() == io::ErrorKind::AlreadyExists),
        "dropped reader released the terminal"
    );
    master.write_all(b"x")?; // Release the blocked thread.
    runtime.shutdown_timeout(Duration::from_secs(5));
    drop(Tty::from_file(user)?);
    Ok(())
}

struct Flag(AtomicBool);
impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn closing_output_wakes_and_ends_a_waiting_stream() -> io::Result<()> {
    let _serial = serial();
    let (_master, user) = pty()?;
    let runtime = runtime("tui-input-close")?;
    let (tty, output) = Tty::from_file(user)?.split();
    let mut events = runtime.block_on(async { tty.events() })?;
    let flag = Arc::new(Flag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut events).poll_next(&mut cx).is_pending());
    drop(output);
    assert!(flag.0.load(Ordering::SeqCst), "close did not wake input");
    assert!(matches!(
        Pin::new(&mut events).poll_next(&mut cx),
        Poll::Ready(Some(Err(error))) if error.kind() == io::ErrorKind::NotConnected
    ));
    Ok(())
}

fn batch(runtime: &Runtime, events: &mut Events, master: &mut File, broken: bool) -> u64 {
    assert!(master.write_all(b"x").is_ok());
    let mut event = None;
    let allocations = acyclic_test_support::measure(|| {
        if broken {
            // Negative control: the removed per-batch blocking-thread handoff.
            assert!(runtime.block_on(runtime.spawn_blocking(|| ())).is_ok());
        }
        event = Some(next(runtime, events));
    });
    assert!(
        matches!(event, Some(Ok(event)) if event == key(KeyCode::Char('x'), KeyModifiers::NONE))
    );
    allocations.count_total
}

#[test]
fn pty_keys_decode_in_order_without_per_batch_work() -> io::Result<()> {
    let _serial = serial();
    let (mut master, user) = pty()?;
    let runtime = runtime("tui-input-keys")?;
    let mut events = runtime.block_on(async { Tty::from_file(user)?.events() })?;
    master.write_all(b"ab\x1b[13;6u")?;
    master.write_all(b"\x1b[200~p\x1b[201~")?;
    let ctrl_shift = KeyModifiers::SHIFT | KeyModifiers::CONTROL;
    for expected in [
        key(KeyCode::Char('a'), KeyModifiers::NONE),
        key(KeyCode::Char('b'), KeyModifiers::NONE),
        key(KeyCode::Enter, ctrl_shift),
        InputEvent::Paste("p".into()),
    ] {
        assert_eq!(next(&runtime, &mut events)?, expected);
    }
    // A lone Escape resolves on the reactor's timer, before the next key.
    master.write_all(b"\x1b")?;
    assert_eq!(
        next(&runtime, &mut events)?,
        key(KeyCode::Esc, KeyModifiers::NONE)
    );
    master.write_all(b"q")?;
    assert_eq!(
        next(&runtime, &mut events)?,
        key(KeyCode::Char('q'), KeyModifiers::NONE)
    );
    for _ in 0..4 {
        batch(&runtime, &mut events, &mut master, false);
    }
    // Warm steady state: readiness, one read, decode, no allocation at all.
    assert_eq!(batch(&runtime, &mut events, &mut master, false), 0);
    assert!(batch(&runtime, &mut events, &mut master, true) > 0);
    Ok(())
}

struct Wakes(std::sync::atomic::AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// CPU time of the calling thread, which runs the whole current-thread runtime.
#[cfg(target_os = "linux")]
fn cpu_time() -> io::Result<Duration> {
    let schedstat = std::fs::read_to_string("/proc/thread-self/schedstat")?;
    let nanos = schedstat
        .split_whitespace()
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or(io::ErrorKind::InvalidData)?;
    Ok(Duration::from_nanos(nanos))
}

/// Idle wakeups of a registered input waiter while the reactor runs 200 ms.
/// `polling` adds the removed 1 ms poll loop as the negative control.
fn idle_wakeups(runtime: &Runtime, events: &mut Events, polling: bool) -> io::Result<usize> {
    let wakes = Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    runtime.block_on(std::future::poll_fn(|_| {
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut *events).poll_next(&mut cx).is_pending());
        Poll::Ready(())
    }));
    runtime.block_on(async {
        let poll = async {
            while polling {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waker.wake_by_ref();
            }
            std::future::pending::<()>().await;
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), poll).await;
    });
    Ok(wakes.0.load(Ordering::SeqCst))
}

#[test]
fn idle_input_has_zero_wakeups_and_reports_latency() -> io::Result<()> {
    let _serial = serial();
    let (mut master, user) = pty()?;
    let runtime = runtime("tui-input-idle")?;
    let mut events = runtime.block_on(async { Tty::from_file(user)?.events() })?;
    #[cfg(target_os = "linux")]
    let before = cpu_time()?;
    let idle = idle_wakeups(&runtime, &mut events, false)?;
    #[cfg(target_os = "linux")]
    eprintln!("idle 200 ms cpu: {:?}", cpu_time()?.saturating_sub(before));
    assert_eq!(idle, 0, "idle terminal input woke its task");
    assert!(idle_wakeups(&runtime, &mut events, true)? > 0);
    // Measurement only (no wall-clock assertion): key write to decoded event.
    let mut samples = Vec::new();
    for _ in 0..200 {
        let start = Instant::now();
        master.write_all(b"x")?;
        assert_eq!(
            next(&runtime, &mut events)?,
            key(KeyCode::Char('x'), KeyModifiers::NONE)
        );
        samples.push(start.elapsed());
    }
    samples.sort();
    eprintln!(
        "input latency p50 {:?} p99 {:?} max {:?}; idle 200 ms: {idle} wakeups",
        samples[samples.len() / 2],
        samples[samples.len() * 99 / 100],
        samples[samples.len() - 1],
    );
    Ok(())
}

#[test]
fn a_completed_escape_sequence_leaves_no_timer_wakeup() -> io::Result<()> {
    let _serial = serial();
    let (mut master, user) = pty()?;
    let runtime = runtime("tui-input-escape")?;
    let mut events = runtime.block_on(async { Tty::from_file(user)?.events() })?;
    // Split sequence: the Escape arms the timer, the rest completes it.
    master.write_all(b"\x1b")?;
    let early = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(10),
            std::future::poll_fn(|cx| Pin::new(&mut events).poll_next(cx)),
        )
        .await
    });
    assert!(
        early.is_err(),
        "the lone Escape resolved before its timeout"
    );
    master.write_all(b"[A")?;
    assert_eq!(
        next(&runtime, &mut events)?,
        key(KeyCode::Up, KeyModifiers::NONE)
    );
    assert_eq!(idle_wakeups(&runtime, &mut events, false)?, 0);
    // Negative control: an Escape left pending does take its timer wakeup.
    master.write_all(b"\x1b")?;
    assert!(idle_wakeups(&runtime, &mut events, false)? > 0);
    assert_eq!(
        next(&runtime, &mut events)?,
        key(KeyCode::Esc, KeyModifiers::NONE)
    );
    Ok(())
}

/// Opens the controlling terminal with every standard stream redirected and
/// registers its input with the reactor; run by the test below.
#[cfg(target_os = "macos")]
#[test]
fn redirected_child() -> io::Result<()> {
    let Ok(report) = std::env::var("ACYCLIC_TERMINAL_REDIRECTED_REPORT") else {
        return Ok(());
    };
    let runtime = runtime("tui-input-redirected")?;
    let outcome = if std::env::var_os("ACYCLIC_TERMINAL_REDIRECTED_ALIAS").is_some() {
        // The removed fallback: the /dev/tty alias, which kqueue rejects.
        let alias = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")?;
        match runtime.block_on(async { tokio::io::unix::AsyncFd::new(alias) }) {
            Ok(_) => "registered".to_owned(),
            Err(error) => format!("rejected {error}"),
        }
    } else {
        match runtime.block_on(async { Tty::open()?.events() }) {
            Ok(_) => "registered".to_owned(),
            Err(error) => format!("failed {error}"),
        }
    };
    std::fs::write(report, outcome)
}

#[cfg(target_os = "macos")]
fn redirected(alias: bool) -> io::Result<String> {
    let directory = tempfile::tempdir()?;
    let report = directory.path().join("report");
    // script(1) gives the child a new session whose controlling terminal is
    // a fresh PTY; the shell then redirects all three standard streams.
    let mut command = std::process::Command::new("/usr/bin/script");
    command
        .args(["-q", "/dev/null", "/bin/sh", "-c"])
        .arg(r#"exec "$0" --exact redirected_child --nocapture </dev/null >/dev/null 2>&1"#)
        .arg(std::env::current_exe()?)
        .env("ACYCLIC_TERMINAL_REDIRECTED_REPORT", &report)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    if alias {
        command.env("ACYCLIC_TERMINAL_REDIRECTED_ALIAS", "1");
    }
    command.status()?;
    std::fs::read_to_string(report)
}

#[cfg(target_os = "macos")]
#[test]
fn controlling_terminal_input_survives_redirecting_every_standard_stream() -> io::Result<()> {
    assert_eq!(redirected(false)?, "registered");
    assert!(redirected(true)?.starts_with("rejected"));
    Ok(())
}

#[test]
fn window_changes_arrive_without_a_key_or_polling_timer() -> io::Result<()> {
    let _serial = serial();
    let (master, user) = pty()?;
    let runtime = runtime("tui-resize")?;
    let mut events = runtime.block_on(async { Tty::from_file(user)?.events() })?;
    poll_pending(&runtime, &mut NextEvent(&mut events));
    let old = events.size();
    let changed = (old.0.saturating_add(7), old.1.saturating_add(5));
    rustix::termios::tcsetwinsize(
        &master,
        rustix::termios::Winsize {
            ws_col: changed.0,
            ws_row: changed.1,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )?;
    // This PTY is not the process's controlling terminal: changing its size
    // alone supplies no signal, so the production stream stays parked.
    poll_pending(&runtime, &mut NextEvent(&mut events));
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::WINCH)?;
    let event = runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), NextEvent(&mut events)).await
        })
        .map_err(|_| io::ErrorKind::TimedOut)?
        .ok_or(io::ErrorKind::UnexpectedEof)??;
    assert_eq!(event, InputEvent::Resize(changed.0, changed.1));
    assert_eq!(events.size(), changed);
    // A duplicate native signal must not create a second dimension change.
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::WINCH)?;
    master.try_clone()?.write_all(b"x")?;
    assert_eq!(
        next(&runtime, &mut events)?,
        key(KeyCode::Char('x'), KeyModifiers::NONE)
    );
    Ok(())
}
