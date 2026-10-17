//! Real subprocesses and PTYs exercise the production signal lifecycle.
#![cfg(all(unix, feature = "tui", not(acyclic_harness_tui_loom)))]

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_core::Stream;

use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{
    ColourDepth, Environment, ImageProtocol, KeyboardProtocol, Multiplexer, ScreenMode, Tty,
};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::process::{Pid, Signal, WaitOptions, kill_process, waitpid};
use rustix::termios::{LocalModes, Termios, tcgetattr};

#[test]
fn lifecycle_child() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mode) = std::env::var("ACYCLIC_TERMINAL_SIGNAL_CHILD") else {
        return Ok(());
    };
    let file = File::from(rustix::io::dup(std::io::stdin())?);
    let mut tty = Tty::from_file(file)?;
    let env = Environment([("TERM_PROGRAM".into(), "kitty".into())].into());
    let caps = tty.negotiate(&env)?;
    assert_eq!(caps.keyboard, KeyboardProtocol::Legacy);
    assert_eq!(caps.images, ImageProtocol::Placeholder);
    assert_eq!(caps.colour, ColourDepth::Ansi16);
    tty.setup(ScreenMode::Alternate)?;
    if mode == "dropped" {
        drop(tty);
        std::io::stdout().write_all(b"READY\n")?;
        loop {
            std::thread::park();
        }
    }
    if mode == "reopen" {
        drop(tty);
        tty = Tty::from_file(File::from(rustix::io::dup(std::io::stdin())?))?;
    }
    if mode == "outside" {
        tty.restore()?;
        std::io::stdout().write_all(b"READY\n")?;
    } else {
        tty.write_all(b"READY\n")?;
    }
    if !matches!(
        mode.as_str(),
        "read" | "handoff" | "escape" | "rejected-paste" | "resize"
    ) {
        loop {
            std::thread::park();
        }
    }
    let (tty, mut output) = tty.split();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut events = runtime.block_on(async { tty.events() })?;
    if mode == "resize" {
        output.write_all(b"INPUT_READY\n")?;
    }
    let mut received = Vec::new();
    let mut rejected = false;
    loop {
        let next = std::future::poll_fn(|cx| std::pin::Pin::new(&mut events).poll_next(cx));
        let event = match runtime
            .block_on(next)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?
        {
            Err(error)
                if mode == "rejected-paste" && error.kind() == io::ErrorKind::InvalidData =>
            {
                assert!(!rejected);
                rejected = true;
                output.write_all(b"REJECTED\n")?;
                continue;
            }
            event => event?,
        };
        if mode == "resize" {
            if let InputEvent::Resize(width, height) = event {
                writeln!(output, "RESIZED {width} {height}")?;
            }
        }
        if mode == "handoff" || mode == "escape" || mode == "rejected-paste" {
            received.push(event.clone());
        }
        if mode == "escape" && matches!(event, InputEvent::Key(key) if key.code == KeyCode::Esc) {
            output.write_all(b"ESC\n")?;
        }
        if matches!(event, InputEvent::Key(key) if key.code == KeyCode::Char('q')) {
            if mode == "rejected-paste" {
                assert!(rejected);
                assert_eq!(
                    received,
                    vec![
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::F(5), KeyModifiers::CONTROL)),
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE)),
                    ]
                );
            }
            if mode == "handoff" {
                assert_eq!(
                    received,
                    vec![
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(
                            KeyCode::Enter,
                            KeyModifiers::SHIFT | KeyModifiers::CONTROL
                        )),
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE)),
                    ]
                );
            }
            if mode == "escape" {
                assert_eq!(
                    received,
                    vec![
                        InputEvent::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)),
                        InputEvent::Key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE)),
                    ]
                );
            }
            return Ok(());
        }
    }
}

#[test]
fn oversized_paste_rejection_preserves_following_numbered_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("rejected-paste")?;
    let mut bytes = b"\x1b[200~".to_vec();
    bytes.resize(bytes.len() + 17 * 1024, b'p');
    bytes.extend_from_slice(b"\x1b[201~x\x1b[1~\x1b[4~\x1b[15;5~q");
    let mut offset = 0;
    process.until(|process| {
        if offset == bytes.len() {
            return Ok(true);
        }
        let end = (offset + 1024).min(bytes.len());
        match process
            .master
            .write(bytes.get(offset..end).unwrap_or_default())
        {
            Ok(count) => offset += count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        Ok(offset == bytes.len())
    })?;
    let mut status = None;
    process.until(|process| {
        status = process.child.try_wait()?;
        Ok(status.is_some())
    })?;
    assert!(status.is_some_and(|status| status.success()));
    assert!(process.bytes.windows(8).any(|bytes| bytes == b"REJECTED"));
    process.restored()?;
    Ok(())
}

#[test]
fn dropping_and_reopening_tty_preserves_signal_dispositions()
-> Result<(), Box<dyn std::error::Error>> {
    for (mode, signal) in [
        ("dropped", Signal::TERM),
        ("dropped", Signal::HUP),
        ("dropped", Signal::INT),
        ("reopen", Signal::TERM),
    ] {
        let mut process = Process::start(mode)?;
        kill_process(process.pid()?, signal)?;
        let mut status = None;
        process.until(|process| {
            status = process.child.try_wait()?;
            Ok(status.is_some())
        })?;
        assert_eq!(
            status.and_then(|status| status.signal()),
            Some(signal.as_raw())
        );
        process.restored()?;
    }
    Ok(())
}

#[test]
fn isolated_escape_resolves_before_the_next_key() -> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("escape")?;
    process.master.write_all(b"\x1b")?;
    process.until(|process| Ok(process.bytes.windows(4).any(|bytes| bytes == b"ESC\n")))?;
    process.master.write_all(b"xq")?;
    let mut status = None;
    process.until(|process| {
        status = process.child.try_wait()?;
        Ok(status.is_some())
    })?;
    assert!(status.is_some_and(|status| status.success()));
    process.restored()?;
    Ok(())
}

#[test]
fn startup_handoff_filters_delayed_partial_and_multiplexer_replies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("handoff")?;
    process.master.write_all(b"a\x1b]11;rgb:ffff/")?;
    // Cross an input-read deadline with an incomplete reply still in flight.
    std::thread::sleep(Duration::from_millis(30));
    process.master.write_all(b"0000/0000\x07")?;
    process
        .master
        .write_all(&Multiplexer::Tmux.wrap(b"\x1b]11;rgb:0000/ffff/0000\x07"))?;
    process.master.write_all(b"b\x1b[13;6u")?;
    process.master.write_all(b"q")?;
    let mut status = None;
    process.until(|process| {
        status = process.child.try_wait()?;
        Ok(status.is_some())
    })?;
    assert!(status.is_some_and(|status| status.success()));
    process.restored()?;
    Ok(())
}

struct Process {
    child: Child,
    master: File,
    observer: File,
    original: Termios,
    bytes: Vec<u8>,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Process {
    fn start(mode: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pty = rustix_openpty::openpty(None, None)?;
        let master = File::from(pty.controller);
        fcntl_setfl(&master, fcntl_getfl(&master)? | OFlags::NONBLOCK)?;
        let observer = File::from(pty.user);
        let original = tcgetattr(&observer)?;
        let child = Command::new(std::env::current_exe()?)
            .args(["--exact", "lifecycle_child", "--nocapture"])
            .env("ACYCLIC_TERMINAL_SIGNAL_CHILD", mode)
            .stdin(Stdio::from(observer.try_clone()?))
            .stdout(Stdio::from(observer.try_clone()?))
            .stderr(Stdio::piped())
            .spawn()?;
        let mut process = Self {
            child,
            master,
            observer,
            original,
            bytes: Vec::new(),
        };
        process.until(|process| Ok(process.bytes.windows(3).any(|b| b == b"\x1b[c")))?;
        process
            .master
            .write_all(b"\x1b[?1004;2$y\x1b[?2004;2$y\x1b[?2027;0$y\x1b[?1049;2$y\x1b[?62;1c")?;
        process.until(|process| Ok(process.bytes.windows(5).any(|b| b == b"READY")))?;
        Ok(process)
    }

    fn pid(&self) -> io::Result<Pid> {
        Pid::from_raw(i32::try_from(self.child.id()).map_err(|_| io::ErrorKind::InvalidData)?)
            .ok_or(io::ErrorKind::InvalidData.into())
    }

    fn drain(&mut self) -> io::Result<()> {
        let mut bytes = [0; 4096];
        loop {
            match self.master.read(&mut bytes) {
                Ok(0) => return Ok(()),
                Ok(count) => self
                    .bytes
                    .extend_from_slice(bytes.get(..count).ok_or(io::ErrorKind::InvalidData)?),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.raw_os_error() == Some(5) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn until(
        &mut self,
        mut condition: impl FnMut(&mut Self) -> io::Result<bool>,
    ) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_millis(900);
        while Instant::now() < deadline {
            self.drain()?;
            if condition(self)? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            String::from_utf8_lossy(&self.bytes).into_owned(),
        ))
    }

    fn restored(&mut self) -> io::Result<()> {
        self.drain()?;
        let current = tcgetattr(&self.observer)?;
        assert_eq!(current.input_modes, self.original.input_modes);
        assert_eq!(current.output_modes, self.original.output_modes);
        assert_eq!(current.control_modes, self.original.control_modes);
        assert_eq!(current.local_modes, self.original.local_modes);
        assert_eq!(
            format!("{:?}", current.special_codes),
            format!("{:?}", self.original.special_codes)
        );
        struct Size;
        impl Dimensions for Size {
            fn total_lines(&self) -> usize {
                10
            }
            fn screen_lines(&self) -> usize {
                10
            }
            fn columns(&self) -> usize {
                40
            }
        }
        struct Events;
        impl EventListener for Events {}
        let mut term = Term::new(Config::default(), &Size, Events);
        let mut parser: Processor = Processor::new();
        parser.advance(&mut term, &self.bytes);
        assert!(
            !term.mode().intersects(
                TermMode::ALT_SCREEN | TermMode::BRACKETED_PASTE | TermMode::FOCUS_IN_OUT
            )
        );
        Ok(())
    }
}

#[test]
fn terminate_restores_even_when_the_application_is_blocked()
-> Result<(), Box<dyn std::error::Error>> {
    for (signal, mode) in [
        (Signal::TERM, "blocked"),
        (Signal::HUP, "blocked"),
        (Signal::INT, "blocked"),
        (Signal::INT, "outside"),
    ] {
        let mut process = Process::start(mode)?;
        kill_process(process.pid()?, signal)?;
        let mut status = None;
        process.until(|process| {
            status = process.child.try_wait()?;
            Ok(status.is_some())
        })?;
        assert_eq!(
            status.and_then(|status| status.signal()),
            Some(signal.as_raw())
        );
        process.restored()?;
    }
    Ok(())
}

#[test]
fn suspend_and_raw_ctrl_z_restore_then_resume_reapplies() -> Result<(), Box<dyn std::error::Error>>
{
    for raw_key in [false, true] {
        let mut process = Process::start(if raw_key { "read" } else { "blocked" })?;
        if raw_key {
            process.master.write_all(b"\x1a")?;
        } else {
            kill_process(process.pid()?, Signal::TSTP)?;
        }
        let pid = process.pid()?;
        process.until(|_| {
            Ok(
                waitpid(Some(pid), WaitOptions::UNTRACED | WaitOptions::NOHANG)?
                    .is_some_and(|(_, status)| status.stopped()),
            )
        })?;
        process.restored()?;
        kill_process(pid, Signal::CONT)?;
        process.until(|process| {
            Ok(!tcgetattr(&process.observer)?
                .local_modes
                .contains(LocalModes::ICANON))
        })?;
        if raw_key {
            process.master.write_all(b"q")?;
            process.until(|process| Ok(process.child.try_wait()?.is_some()))?;
        } else {
            kill_process(pid, Signal::TERM)?;
            process.until(|process| Ok(process.child.try_wait()?.is_some()))?;
        }
        process.restored()?;
    }
    Ok(())
}

#[test]
fn resume_observes_a_resize_without_sigwinch_or_a_key() -> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("resize")?;
    process.until(|process| Ok(process.bytes.windows(11).any(|b| b == b"INPUT_READY")))?;
    let pid = process.pid()?;
    kill_process(pid, Signal::TSTP)?;
    process.until(|_| {
        Ok(
            waitpid(Some(pid), WaitOptions::UNTRACED | WaitOptions::NOHANG)?
                .is_some_and(|(_, status)| status.stopped()),
        )
    })?;
    process.restored()?;
    // This PTY is not a controlling terminal, so the kernel sends no WINCH.
    rustix::termios::tcsetwinsize(
        &process.master,
        rustix::termios::Winsize {
            ws_col: 101,
            ws_row: 37,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )?;
    kill_process(pid, Signal::CONT)?;
    let marker = b"RESIZED 101 37";
    process.until(|process| Ok(process.bytes.windows(marker.len()).any(|b| b == marker)))?;
    process.master.write_all(b"q")?;
    process.until(|process| Ok(process.child.try_wait()?.is_some()))?;
    assert_eq!(
        process
            .bytes
            .windows(marker.len())
            .filter(|b| *b == marker)
            .count(),
        1
    );
    process.restored()?;
    Ok(())
}
