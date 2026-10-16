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
    undo: Vec<(u16, Vec<u8>)>,
    modes: std::collections::BTreeMap<u16, ModeState>,
    desired: Vec<u16>,
}

impl<W: Write> RestoreGuard<W> {
    /// No terminal changes occur until setup or enable_mode is called.
    pub fn new(writer: W, caps: &Capabilities) -> Self {
        Self {
            writer,
            undo: Vec::new(),
            modes: caps.modes.clone(),
            desired: Vec::new(),
        }
    }

    /// Enable focus, paste, grapheme clustering, and the requested screen.
    /// Unknown/permanent states are preserved; no speculative reset on exit.
    pub fn setup(&mut self, screen: ScreenMode) -> io::Result<()> {
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
        if self.modes.get(&mode) != Some(&ModeState::Reset) {
            return Ok(());
        }
        let undo = format!("\x1b[?{mode}l").into_bytes();
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

impl<W: Write> Drop for RestoreGuard<W> {
    fn drop(&mut self) {
        // A transient write failure during teardown leaves journal entries to
        // retry. Bound the retry so a disconnected terminal cannot hang exit.
        if self.restore().is_err() {
            let _ = self.restore();
        }
    }
}
