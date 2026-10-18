use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::time::{Duration, Instant};

use signal_hook::consts::signal::SIGTSTP;

use super::lifecycle::{self, Arc, Mutex, Owner, State};
use super::tty::RawTty;
use super::{Capabilities, Decoder, Environment, ProbeIo, ScreenMode, detect};
use crate::key_event::{InputEvent, KeyCode, KeyEventKind, KeyModifiers};

/// Process-owned terminal lifecycle. The signal worker restores even while the
/// application is computing or blocked outside terminal I/O. The one state lock
/// serializes every output mutation with restore/close, including on resume.
pub struct Tty {
    state: Arc<Mutex<State<RawTty>>>,
    owner: &'static Owner<RawTty>,
    decoder: Decoder,
    capabilities: Option<Capabilities>,
}

/// Output half retaining the same terminal lifecycle as its decoded input.
pub struct Output {
    state: Arc<Mutex<State<RawTty>>>,
    owner: &'static Owner<RawTty>,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        lifecycle::active(&self.state, |guard| guard.writer().write(bytes))
    }
    fn flush(&mut self) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.writer().flush())
    }
}
impl super::Present for Output {
    fn supports_scroll_regions(&self) -> bool {
        lifecycle::active(&self.state, |guard| Ok(guard.supports_scroll_regions())).unwrap_or(false)
    }
    fn present(&mut self, frame: &mut Vec<u8>, effects: super::FrameEffects) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.present(frame, effects))
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        effects: super::FrameEffects,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        lifecycle::present_if_current(&self.state, frame, effects, expected)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        lifecycle::active(&self.state, |guard| Ok(guard.outside_writes()))
            .ok()
            .flatten()
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let _ = lifecycle::detach(self.owner, &self.state, RawTty::restore);
    }
}

impl Tty {
    /// Separate output from the existing decoder without reopening the terminal.
    /// Dropping either half restores the owner and wakes any outstanding read.
    pub fn split(self) -> (Self, Output) {
        let output = Output {
            state: Arc::clone(&self.state),
            owner: self.owner,
        };
        (self, output)
    }
    /// Open the controlling terminal, including when stdout is redirected.
    pub fn open() -> io::Result<Self> {
        Self::from_file(OpenOptions::new().read(true).write(true).open("/dev/tty")?)
    }

    /// Install signal handling BEFORE making the terminal raw. A process owns
    /// one terminal lifecycle; callers must not open competing sessions.
    pub fn from_file(file: File) -> io::Result<Self> {
        let owner = super::signals::owner()?;
        let state = lifecycle::attach(owner, || RawTty::from_file(file))?;
        Ok(Self {
            state,
            owner,
            decoder: Decoder::default(),
            capabilities: None,
        })
    }

    /// Probe once and retain the same whole-stream decoder for subsequent input.
    pub fn negotiate(&mut self, env: &Environment) -> io::Result<&Capabilities> {
        if self.capabilities.is_none() {
            let (caps, decoder) = detect(self, env)?;
            self.capabilities = Some(caps);
            self.decoder = decoder;
        }
        self.capabilities
            .as_ref()
            .ok_or(io::ErrorKind::NotConnected.into())
    }

    /// Enable modes under the signal worker's restoration journal.
    pub fn setup(&mut self, screen: ScreenMode) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.setup(screen))
    }

    /// Enable an additional queried mode under the same journal.
    pub fn enable_mode(&mut self, mode: u16) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.enable_mode(mode))
    }

    /// Restore DEC modes before termios; retain the desired modes for resume.
    pub fn restore(&mut self) -> io::Result<()> {
        lifecycle::restore(&self.state, false, RawTty::restore)
    }

    /// Restore, suspend with the default SIGTSTP disposition, and reapply on
    /// continuation. Pasted Ctrl+Z never enters this path.
    pub fn suspend(&mut self) -> io::Result<()> {
        self.restore()?;
        signal_hook::low_level::emulate_default_handler(SIGTSTP)?;
        lifecycle::resume(&self.state, RawTty::reapply)
    }

    /// Read only classified events. Late probe replies remain in this decoder.
    pub fn read_events(&mut self, deadline: Instant) -> io::Result<Vec<InputEvent>> {
        let mut events = self.decoder.take_events();
        while events.is_empty() {
            let mut bytes = [0; 1024];
            let count = self.receive(&mut bytes, self.decoder.input_deadline(deadline))?;
            self.decoder
                .feed(bytes.get(..count).ok_or(io::ErrorKind::InvalidData)?)?;
            if count == 0 || Instant::now() >= deadline {
                self.decoder.finish_input()?;
                events = self.decoder.take_events();
                break;
            }
            events = self.decoder.take_events();
        }
        let mut output = Vec::new();
        for event in events {
            if matches!(&event, InputEvent::Key(key) if key.code == KeyCode::Char('z')
                && key.modifiers == KeyModifiers::CONTROL && key.kind == KeyEventKind::Press)
            {
                self.suspend()?;
            } else {
                output.push(event);
            }
        }
        Ok(output)
    }
}

impl ProbeIo for Tty {
    fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.writer().send(bytes, deadline))
    }

    fn receive(&mut self, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(0);
            }
            // Only the nonblocking syscall holds state. Both output and the
            // signal worker remain runnable while input waits for another byte.
            let result = lifecycle::active(&self.state, |guard| {
                guard.receive(bytes, deadline.min(now + Duration::from_millis(1)))
            });
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(
                        Duration::from_millis(1)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }

    fn detected(&mut self, caps: &Capabilities) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.detected(caps))
    }
}

impl Write for Tty {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        lifecycle::active(&self.state, |guard| guard.writer().write(bytes))
    }
    fn flush(&mut self) -> io::Result<()> {
        lifecycle::active(&self.state, |guard| guard.writer().flush())
    }
}

impl Drop for Tty {
    fn drop(&mut self) {
        if lifecycle::detach(self.owner, &self.state, RawTty::restore).is_err() {
            let _ = lifecycle::detach(self.owner, &self.state, RawTty::restore);
        }
    }
}

/// Restoration tests compare descriptor flags only through this helper.
#[cfg(test)]
#[path = "../../tests/support/status_flags.rs"]
mod status_flags;

#[cfg(test)]
mod tests {
    use super::status_flags::settable_status_flags as status_flags;
    use super::*;
    use rustix::fs::OFlags;
    use rustix::termios::{LocalModes, tcgetattr};

    fn read_redraw_model(tracked_read: bool) -> io::Result<usize> {
        use crate::ratatui::layout::Size;
        use crate::term::FrameTerminal;
        use std::io::Read;
        let pty = rustix_openpty::openpty(None, None)?;
        let mut master = File::from(pty.controller);
        rustix::fs::fcntl_setfl(&master, OFlags::NONBLOCK)?;
        let (mut tty, output) = Tty::from_file(File::from(pty.user))?.split();
        let size = Size::new(20, 4);
        let mut terminal = FrameTerminal::new(output, size)?;
        let mut draw = || terminal.draw(size, |frame| frame.set_cursor_position((3, 2)));
        draw()?;
        let drain = |master: &mut File| -> io::Result<Vec<u8>> {
            let mut bytes = Vec::new();
            match master.read_to_end(&mut bytes) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
            Ok(bytes)
        };
        assert!(!drain(&mut master)?.is_empty());
        let mut redundant = 0;
        for n in [8, 16] {
            for _ in 0..n {
                master.write_all(b"x")?;
                let deadline = Instant::now() + Duration::from_millis(100);
                if tracked_read {
                    // Negative control: the former polling path classified a
                    // read as outside output through the generic writer.
                    let count = lifecycle::active(&tty.state, |guard| {
                        guard.writer().receive(&mut [0; 8], deadline)
                    })?;
                    assert_eq!(count, 1);
                } else {
                    let events = tty.read_events(deadline)?;
                    assert_eq!(events.len(), 1);
                }
                draw()?;
                redundant += drain(&mut master)?.len();
            }
        }
        Ok(redundant)
    }

    fn input_reads_leave_identical_real_pty_frames_without_output() -> io::Result<()> {
        assert_eq!(read_redraw_model(false)?, 0);
        assert!(read_redraw_model(true)? > 0);
        Ok(())
    }

    #[test]
    fn delayed_old_state_drop_preserves_reopened_pty() -> io::Result<()> {
        // The signal dispatcher owns one terminal per process. Run all real
        // owner scenarios in this test so parallel tests cannot race attach.
        input_reads_leave_identical_real_pty_frames_without_output()?;
        let pty = rustix_openpty::openpty(None, None)?;
        let observer = File::from(pty.user);
        let original = tcgetattr(&observer)?;
        let flags = status_flags(&observer)?;
        let mut old = Tty::from_file(observer.try_clone()?)?;
        let mut master = File::from(pty.controller);
        master.write_all(b"/delegate say hello\r\n\x1b[?1;2c")?;
        old.negotiate(&Environment([("TERM".into(), "xterm".into())].into()))?;
        // Keep the old descriptor alive past detach, exactly as a concurrent
        // signal callback can. No scheduling or timing assumptions are needed.
        let retained = Arc::clone(&old.state);
        let (mut old, output) = old.split();
        let events = old.read_events(Instant::now())?;
        let typed: String = events
            .iter()
            .filter_map(|event| match event {
                InputEvent::Key(key) => match key.code {
                    KeyCode::Char(character) => Some(character),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(typed, "/delegate say hello");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event,
            InputEvent::Key(key) if key.code == KeyCode::Enter))
                .count(),
            1
        );
        drop(output);
        let new = Tty::from_file(observer.try_clone()?)?;
        drop(old);
        drop(retained);
        assert!(
            matches!(Tty::from_file(observer.try_clone()?), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
        );
        lifecycle::active(&new.state, |_| Ok(()))?;
        assert!(
            !tcgetattr(&observer)?
                .local_modes
                .contains(LocalModes::ICANON)
        );
        assert_eq!(status_flags(&observer)?, flags | OFlags::NONBLOCK);
        drop(new);
        let restored = tcgetattr(&observer)?;
        assert_eq!(restored.input_modes, original.input_modes);
        assert_eq!(restored.output_modes, original.output_modes);
        assert_eq!(restored.control_modes, original.control_modes);
        assert_eq!(restored.local_modes, original.local_modes);
        assert_eq!(
            format!("{:?}", restored.special_codes),
            format!("{:?}", original.special_codes)
        );
        assert_eq!(restored.input_speed(), original.input_speed());
        assert_eq!(restored.output_speed(), original.output_speed());
        assert_eq!(status_flags(&observer)?, flags);
        Ok(())
    }
}
