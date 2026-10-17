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
    /// The mouse protocol selected before ours displaced it, restored with it.
    displaced_mouse: Option<u16>,
}

/// Mouse tracking protocols are one selection: setting one replaces another
/// (click 1000, button-event 1002, any-event 1003), so restoring ours means
/// selecting the one it displaced, not only resetting ours.
const MOUSE_PROTOCOLS: [u16; 3] = [1000, 1002, 1003];

impl<W: Write> RestoreGuard<W> {
    /// No terminal changes occur until setup or `enable_mode` is called.
    pub fn new(writer: W, caps: &Capabilities) -> Self {
        Self {
            writer,
            undo: Vec::new(),
            modes: caps.modes.clone(),
            desired: Vec::new(),
            displaced_mouse: None,
        }
    }

    /// Enable focus, paste, grapheme clustering, and the requested screen.
    /// Unknown/permanent states are preserved; no speculative reset on exit.
    pub fn setup(&mut self, screen: ScreenMode) -> io::Result<()> {
        for mode in [1004, 2004, 2027] {
            self.enable_mode(mode)?;
        }
        if screen == ScreenMode::Alternate
            && let Some(screen) = self.alternate_screen()?
        {
            self.report_wheel(screen)?;
        }
        if let Err(error) = self.writer.flush() {
            let _ = self.restore();
            return Err(error);
        }
        Ok(())
    }

    /// Enter the alternate screen; the proof it is active, only when its
    /// state is confirmed (set by us, or already set).
    fn alternate_screen(&mut self) -> io::Result<Option<OnAlternateScreen>> {
        self.enable_mode(1049)?;
        Ok(matches!(
            self.modes.get(&1049),
            Some(ModeState::Set | ModeState::AlwaysSet)
        )
        .then_some(OnAlternateScreen(())))
    }

    /// The alternate screen has no native scrollback, so it owns the wheel:
    /// button-event reports (1002), SGR-encoded (1006) because the decoder
    /// reads only SGR. Encoding first, so no legacy report can precede it;
    /// neither without the other. Requiring the proof keeps reporting off the
    /// primary screen, whose own wheel scrolling and selection stay native.
    fn report_wheel(&mut self, _screen: OnAlternateScreen) -> io::Result<()> {
        let mutable = |mode| self.modes.get(&mode).is_some_and(|state| state.supported());
        // Every other protocol's state must be known and changeable, or ours
        // could displace a selection that restoration cannot select again.
        let known = |mode| {
            matches!(
                self.modes.get(&mode),
                Some(
                    ModeState::Set
                        | ModeState::Reset
                        | ModeState::Unsupported
                        | ModeState::AlwaysReset
                )
            )
        };
        if mutable(1006) && mutable(1002) && known(1000) && known(1003) {
            self.enable_mode(1006)?;
            self.enable_mode(1002)?;
        }
        Ok(())
    }

    /// Enable a queried DEC private mode, remembering its exact prior state.
    pub fn enable_mode(&mut self, mode: u16) -> io::Result<()> {
        if self.modes.get(&mode) != Some(&ModeState::Reset) {
            return Ok(());
        }
        let displaced = MOUSE_PROTOCOLS
            .contains(&mode)
            .then(|| {
                MOUSE_PROTOCOLS
                    .into_iter()
                    .find(|other| self.modes.get(other) == Some(&ModeState::Set))
            })
            .flatten();
        let undo = match displaced {
            Some(previous) => format!("\x1b[?{previous}h"),
            None => format!("\x1b[?{mode}l"),
        };
        // Journal before writing: write_all can fail after emitting a prefix.
        self.undo.push((mode, Cow::Owned(undo.into_bytes())));
        if let Some(previous) = displaced {
            self.modes.insert(previous, ModeState::Reset);
            self.displaced_mouse = Some(previous);
        }
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
        &mut self.writer
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
                if MOUSE_PROTOCOLS.contains(&mode)
                    && let Some(previous) = self.displaced_mouse.take()
                {
                    self.modes.insert(previous, ModeState::Set);
                }
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
}

/// Made only by `RestoreGuard::alternate_screen` on a confirmed alternate screen.
struct OnAlternateScreen(());

impl<W: Write> Drop for RestoreGuard<W> {
    fn drop(&mut self) {
        // A transient write failure during teardown leaves journal entries to
        // retry. Bound the retry so a disconnected terminal cannot hang exit.
        if self.restore().is_err() {
            let _ = self.restore();
        }
    }
}
