use std::borrow::Cow;
use std::io::{self, Write};

use super::{Capabilities, ModeState};

/// Screen selection made by the application/configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScreenMode {
    /// Preserve scrollback on the primary screen.
    Inline,
    /// Use the alternate screen if its original state is known.
    Alternate,
}

/// Owns the writer and restores only modes changed by this session, in reverse
/// order. Dropping during unwinding runs the same restoration as normal exit.
pub struct RestoreGuard<W: Write> {
    writer: W,
    undo: Vec<(u16, Cow<'static, [u8]>)>,
    modes: std::collections::BTreeMap<u16, ModeState>,
    desired: Vec<u16>,
    /// Device accesses other than presented frames (see `Present`).
    outside: u64,
}

#[cfg(all(unix, not(acyclic_harness_tui_loom)))]
impl RestoreGuard<super::tty::RawTty> {
    /// Native input cannot change the screen or invalidate a presented frame.
    pub(crate) fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.writer.read(bytes)
    }
}

impl<W: Write> RestoreGuard<W> {
    /// No terminal changes occur until setup or `enable_mode` is called.
    pub fn new(writer: W, caps: &Capabilities) -> Self {
        Self {
            writer,
            undo: Vec::new(),
            modes: caps.modes.clone(),
            desired: Vec::new(),
            outside: 0,
        }
    }

    /// Enable focus, paste, grapheme clustering, and the requested screen.
    /// Unknown/permanent states are preserved; no speculative reset on exit.
    pub fn setup(&mut self, screen: ScreenMode) -> io::Result<()> {
        self.outside = self.outside.wrapping_add(1);
        for mode in [1004, 2004, 2027] {
            self.enable_mode(mode)?;
        }
        if screen == ScreenMode::Alternate {
            self.enable_mode(1049)?;
        }
        if let Err(error) = self.writer.flush() {
            let _ = self.restore();
            return Err(error);
        }
        Ok(())
    }

    /// Enable a queried DEC private mode, remembering its exact prior state.
    pub fn enable_mode(&mut self, mode: u16) -> io::Result<()> {
        self.outside = self.outside.wrapping_add(1);
        if self.modes.get(&mode) != Some(&ModeState::Reset) {
            return Ok(());
        }
        let undo = Cow::Owned(format!("\x1b[?{mode}l").into_bytes());
        // Journal before writing: write_all can fail after emitting a prefix.
        self.undo.push((mode, undo));
        if !self.desired.contains(&mode) {
            self.desired.push(mode);
        }
        self.modes.insert(mode, ModeState::Set);
        if let Err(error) = self.writer.write_all(format!("\x1b[?{mode}h").as_bytes()) {
            let _ = self.restore();
            return Err(error);
        }
        Ok(())
    }

    /// Write application output using this guard's terminal connection.
    pub fn writer(&mut self) -> &mut W {
        self.outside = self.outside.wrapping_add(1);
        &mut self.writer
    }

    /// Input cannot mutate the displayed state. Keep this operation narrow:
    /// exposing the writer here would also permit untracked output changes.
    #[cfg(all(unix, not(acyclic_harness_tui_loom)))]
    pub(crate) fn receive(
        &mut self,
        bytes: &mut [u8],
        deadline: std::time::Instant,
    ) -> io::Result<usize>
    where
        W: super::ProbeIo,
    {
        self.writer.receive(bytes, deadline)
    }

    pub(crate) fn detected(&mut self, caps: &Capabilities) -> io::Result<()> {
        if !self.undo.is_empty() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        self.modes.clone_from(&caps.modes);
        Ok(())
    }

    #[cfg(any(unix, acyclic_harness_tui_loom))]
    pub(crate) fn reapply(&mut self) -> io::Result<()> {
        for mode in self.desired.clone() {
            self.enable_mode(mode)?;
        }
        self.writer.flush()
    }

    /// Attempt every undo even if one fails. Failed undo records remain for a
    /// subsequent retry (including Drop), and a second successful call is inert.
    pub fn restore(&mut self) -> io::Result<()> {
        self.outside = self.outside.wrapping_add(1);
        let mut first_error = None;
        let mut failed = Vec::new();
        while let Some((mode, undo)) = self.undo.pop() {
            // CAN cancels any partial escape/control string from a failed write.
            if let Err(error) = self
                .writer
                .write_all(b"\x18")
                .and_then(|()| self.writer.write_all(&undo))
            {
                first_error.get_or_insert(error);
                failed.push((mode, undo));
            } else {
                self.modes.insert(mode, ModeState::Reset);
            }
        }
        failed.reverse();
        self.undo = failed;
        if let Err(error) = self.writer.flush() {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }
}

const SHOW_CURSOR: (u16, &[u8]) = (25, b"[?25h");
const END_SYNCHRONIZED: (u16, &[u8]) = (2026, b"[?2026l");
const BEGIN_SYNCHRONIZED: &[u8] = b"[?2026h";

/// Frame presentation under the restoration journal: a synchronized update's
/// close and a hidden cursor's show are journaled before the first byte, so
/// an error, timeout or unwind at any byte is undone by restoration.
impl<W: Write> RestoreGuard<W> {
    fn journal(&mut self, (mode, undo): (u16, &'static [u8])) {
        if !self.undo.iter().any(|(journaled, _)| *journaled == mode) {
            self.undo.push((mode, Cow::Borrowed(undo)));
        }
    }

    /// Journal a frame before writing it; returns whether to bracket it as a
    /// synchronized update (DECSET 2026, when the probe found the mode).
    pub(crate) fn begin_frame(&mut self, cursor_hidden: bool) -> bool {
        if cursor_hidden {
            self.journal(SHOW_CURSOR);
        }
        let synchronized = self
            .modes
            .get(&END_SYNCHRONIZED.0)
            .is_some_and(|state| state.supported());
        if synchronized {
            self.journal(END_SYNCHRONIZED);
        }
        synchronized
    }

    /// Write part of a frame as one contiguous buffer, opening and/or closing
    /// its synchronized update. The buffer's contents are consumed.
    pub(crate) fn write_frame(
        &mut self,
        part: &mut Vec<u8>,
        open: bool,
        close: bool,
    ) -> io::Result<()> {
        if open {
            part.splice(0..0, BEGIN_SYNCHRONIZED.iter().copied());
        }
        if close {
            part.extend_from_slice(END_SYNCHRONIZED.1);
        }
        self.writer.write_all(part)?;
        self.writer.flush()
    }

    /// Whether a begun frame's synchronized update is still open.
    #[cfg(any(windows, test))]
    pub(crate) fn frame_open(&self) -> bool {
        self.undo
            .iter()
            .any(|(mode, _)| *mode == END_SYNCHRONIZED.0)
    }

    /// The whole frame reached the device: its update is closed, and a shown
    /// cursor needs no restoration.
    pub(crate) fn end_frame(&mut self, cursor_hidden: bool) {
        self.undo.retain(|(mode, _)| {
            *mode != END_SYNCHRONIZED.0 && (cursor_hidden || *mode != SHOW_CURSOR.0)
        });
    }

    /// A frame failed partway: close its update now, bounded by the writer's
    /// own deadline. If that fails too, the journal still holds the close.
    pub(crate) fn abort_frame(&mut self) -> io::Result<()> {
        let Some(index) = self
            .undo
            .iter()
            .position(|(mode, _)| *mode == END_SYNCHRONIZED.0)
        else {
            return Ok(());
        };
        // CAN cancels any partial escape sequence the failed write left open.
        self.writer.write_all(b"")?;
        self.writer.write_all(END_SYNCHRONIZED.1)?;
        self.writer.flush()?;
        self.undo.remove(index);
        Ok(())
    }
}

impl<W: Write> super::Present for RestoreGuard<W> {
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()> {
        let synchronized = self.begin_frame(cursor_hidden);
        match self.write_frame(frame, synchronized, synchronized) {
            Ok(()) => {
                self.end_frame(cursor_hidden);
                Ok(())
            }
            Err(error) => {
                let _ = self.abort_frame();
                Err(error)
            }
        }
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        cursor_hidden: bool,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        if expected.is_some_and(|expected| expected != self.outside) {
            return Ok(None);
        }
        if !frame.is_empty() {
            self.present(frame, cursor_hidden)?;
        }
        Ok(Some(self.outside))
    }
    fn outside_writes(&mut self) -> Option<u64> {
        Some(self.outside)
    }
}

impl<W: Write> Drop for RestoreGuard<W> {
    fn drop(&mut self) {
        // A transient write failure during teardown leaves journal entries to
        // retry. Bound the retry so a disconnected terminal cannot hang exit.
        if self.restore().is_err() {
            let _ = self.restore();
        }
    }
}
