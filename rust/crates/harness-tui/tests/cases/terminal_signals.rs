//! Real subprocesses and PTYs exercise the production signal lifecycle.
#![cfg(all(unix, feature = "tui", not(loom)))]

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{
    ColourDepth, Environment, ImageProtocol, KeyboardProtocol, Multiplexer, ScreenMode, Tty,
};
use alacritty_terminal::event::EventListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use rustix::event::{PollFd, PollFlags, poll};
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::process::{Pid, Signal, WaitOptions, kill_process, waitpid};
use rustix::termios::{LocalModes, Termios, tcgetattr};

#[test]
fn lifecycle_child() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(mode) = std::env::var("ACYCLIC_TERMINAL_SIGNAL_CHILD") else {
        return Ok(());
    };
    if mode == "early-exit" {
        return Ok(());
    }
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
    if mode == "ready-exit" {
        return Ok(());
    }
    if mode != "read" && mode != "handoff" && mode != "escape" && mode != "rejected-paste" {
        loop {
            std::thread::park();
        }
    }
    let mut received = Vec::new();
    let mut rejected = false;
    let mut handoff_deadline = false;
    loop {
        let deadline = Instant::now() + Duration::from_millis(20);
        let events = match tty.read_events(deadline) {
            Err(error)
                if mode == "rejected-paste" && error.kind() == io::ErrorKind::InvalidData =>
            {
                assert!(!rejected);
                rejected = true;
                tty.write_all(b"REJECTED\n")?;
                continue;
            }
            events => events?,
        };
        if mode == "handoff"
            && !handoff_deadline
            && !received.is_empty()
            && events.is_empty()
            && Instant::now() >= deadline
        {
            tty.write_all(b"HANDOFF-DEADLINE\n")?;
            handoff_deadline = true;
        }
        for event in events {
            if mode == "handoff" || mode == "escape" || mode == "rejected-paste" {
                received.push(event.clone());
            }
            if mode == "escape" && matches!(event, InputEvent::Key(key) if key.code == KeyCode::Esc)
            {
                tty.write_all(b"ESC\n")?;
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
}

#[test]
fn oversized_paste_rejection_preserves_following_numbered_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("rejected-paste")?;
    let mut bytes = b"\x1b[200~".to_vec();
    bytes.resize(bytes.len() + 17 * 1024, b'p');
    bytes.extend_from_slice(b"\x1b[201~x\x1b[1~\x1b[4~\x1b[15;5~q");
    process.send(&bytes)?;
    assert!(process.wait()?.success());
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
        assert_eq!(process.wait()?.signal(), Some(signal.as_raw()));
        process.restored()?;
    }
    Ok(())
}

#[test]
fn isolated_escape_resolves_before_the_next_key() -> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("escape")?;
    process.send(b"\x1b")?;
    process.until(|process| Ok(process.bytes.windows(4).any(|bytes| bytes == b"ESC\n")))?;
    process.send(b"xq")?;
    assert!(process.wait()?.success());
    process.restored()?;
    Ok(())
}

#[test]
fn startup_handoff_filters_delayed_partial_and_multiplexer_replies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut process = Process::start("handoff")?;
    process.send(b"a\x1b]11;rgb:ffff/")?;
    // Cross an input-read deadline with an incomplete reply still in flight.
    process.until(|process| {
        Ok(process
            .bytes
            .windows(b"HANDOFF-DEADLINE\n".len())
            .any(|bytes| bytes == b"HANDOFF-DEADLINE\n"))
    })?;
    process.send(b"0000/0000\x07")?;
    process.send(&Multiplexer::Tmux.wrap(b"\x1b]11;rgb:0000/ffff/0000\x07"))?;
    process.send(b"b\x1b[13;6u")?;
    process.send(b"q")?;
    assert!(process.wait()?.success());
    process.restored()?;
    Ok(())
}

struct Process {
    child: Child,
    master: File,
    observer: File,
    original: Termios,
    bytes: Vec<u8>,
    errors: Vec<u8>,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Process {
    fn start(mode: &str) -> io::Result<Self> {
        let pty = rustix_openpty::openpty(None, None)?;
        let master = File::from(pty.controller);
        fcntl_setfl(&master, fcntl_getfl(&master)? | OFlags::NONBLOCK)?;
        let observer = File::from(pty.user);
        let original = tcgetattr(&observer)?;
        let child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "terminal_signals::lifecycle_child",
                "--nocapture",
            ])
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
            errors: Vec::new(),
        };
        let stderr = process
            .child
            .stderr
            .as_ref()
            .ok_or(io::ErrorKind::BrokenPipe)?;
        fcntl_setfl(stderr, fcntl_getfl(stderr)? | OFlags::NONBLOCK)?;
        process.until(|process| Ok(process.bytes.windows(3).any(|b| b == b"\x1b[c")))?;
        process.send(b"\x1b[?1004;2$y\x1b[?2004;2$y\x1b[?2027;0$y\x1b[?1049;2$y\x1b[?62;1c")?;
        process.until(|process| Ok(process.bytes.windows(5).any(|b| b == b"READY")))?;
        Ok(process)
    }

    fn pid(&self) -> io::Result<Pid> {
        Pid::from_raw(i32::try_from(self.child.id()).map_err(|_| io::ErrorKind::InvalidData)?)
            .ok_or(io::ErrorKind::InvalidData.into())
    }

    fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.child.wait()?;
        self.drain()?;
        Ok(status)
    }

    fn drain(&mut self) -> io::Result<bool> {
        // The observer keeps the slave open. Only the child's original stderr
        // pipe can report its exit while we retain that terminal-restoration view.
        let exited = read_available(
            self.child
                .stderr
                .as_mut()
                .ok_or(io::ErrorKind::BrokenPipe)?,
            &mut self.errors,
        )?;
        // Read stdout after observing stderr EOF so an already published marker
        // wins over the terminal event, including ready-before-observation.
        let closed = match read_available(&mut self.master, &mut self.bytes) {
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()) => {
                true
            }
            result => result?,
        };
        Ok(exited || closed)
    }

    fn source_closed(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "terminal child closed before the expected event: stdout={}; stderr={}",
                String::from_utf8_lossy(&self.bytes),
                String::from_utf8_lossy(&self.errors),
            ),
        )
    }

    fn wait_io(&self, events: PollFlags) -> io::Result<()> {
        let stderr = self
            .child
            .stderr
            .as_ref()
            .ok_or(io::ErrorKind::BrokenPipe)?;
        let mut fds = [
            PollFd::new(&self.master, events),
            PollFd::new(stderr, PollFlags::IN),
        ];
        loop {
            match poll(&mut fds, None) {
                Err(rustix::io::Errno::INTR) => continue,
                result => {
                    result?;
                }
            }
            for fd in &fds {
                if fd.revents().intersects(PollFlags::ERR | PollFlags::NVAL) {
                    return Err(io::Error::other("terminal child event source failed"));
                }
            }
            return Ok(());
        }
    }

    fn send(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            if self.drain()? {
                return Err(self.source_closed());
            }
            match self.master.write(bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_io(PollFlags::IN | PollFlags::OUT)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn until(&mut self, mut condition: impl FnMut(&Self) -> io::Result<bool>) -> io::Result<()> {
        loop {
            let closed = self.drain()?;
            if condition(self)? {
                return Ok(());
            }
            if closed {
                return Err(self.source_closed());
            }
            self.wait_io(PollFlags::IN)?;
        }
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

fn read_available(reader: &mut impl Read, output: &mut Vec<u8>) -> io::Result<bool> {
    let mut bytes = [0; 4096];
    loop {
        match reader.read(&mut bytes) {
            Ok(0) => return Ok(true),
            Ok(count) => {
                output.extend_from_slice(bytes.get(..count).ok_or(io::ErrorKind::InvalidData)?)
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn readiness_retains_output_and_reports_original_child_exit()
-> Result<(), Box<dyn std::error::Error>> {
    let error = match Process::start("early-exit") {
        Ok(_) => return Err("child without a readiness producer was accepted".into()),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    let mut process = Process::start("ready-exit")?;
    assert!(process.wait()?.success());
    process.until(|process| Ok(process.bytes.windows(5).any(|bytes| bytes == b"READY")))?;
    process.restored()?;
    Ok(())
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
        assert_eq!(process.wait()?.signal(), Some(signal.as_raw()));
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
            process.send(b"\x1a")?;
        } else {
            kill_process(process.pid()?, Signal::TSTP)?;
        }
        let pid = process.pid()?;
        let (_, status) = waitpid(Some(pid), WaitOptions::UNTRACED)?
            .ok_or("terminal child produced no stop status")?;
        assert!(status.stopped(), "terminal child exited before suspension");
        process.restored()?;
        kill_process(pid, Signal::CONT)?;
        process.until(|process| {
            Ok(!tcgetattr(&process.observer)?
                .local_modes
                .contains(LocalModes::ICANON))
        })?;
        if raw_key {
            process.send(b"q")?;
            process.wait()?;
        } else {
            kill_process(pid, Signal::TERM)?;
            process.wait()?;
        }
        process.restored()?;
    }
    Ok(())
}
