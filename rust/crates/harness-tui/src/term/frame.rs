use std::io::{self, Write};

use ratatui::{
    Frame, Terminal,
    backend::{Backend, ClearType, CrosstermBackend, WindowSize},
    buffer::Cell,
    layout::{Position, Size},
};

/// A terminal device that presents whole frames under its restoration
/// journal (see `RestoreGuard`): the session's `Output`, or a guard itself.
pub trait Present {
    /// Present one frame. `cursor_hidden` is the cursor's visibility after it.
    /// The buffer's contents are consumed. On error, a begun synchronized
    /// update has been closed or remains journaled for restoration.
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()>;

    /// How many times the device was reached other than by a presented frame
    /// (setup, restore, suspend and resume, direct writes), or `None` when
    /// unknown. While it is unchanged since the last frame, the screen state
    /// that frame left is still the device's.
    fn outside_writes(&mut self) -> Option<u64>;
}

impl<P: Present + ?Sized> Present for &mut P {
    fn present(&mut self, frame: &mut Vec<u8>, cursor_hidden: bool) -> io::Result<()> {
        (**self).present(frame, cursor_hidden)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        (**self).outside_writes()
    }
}

/// One device call of a frame presented in bounded parts.
#[cfg(any(windows, test))]
pub(crate) struct Part {
    bytes: Vec<u8>,
    first: bool,
    last: bool,
}

/// The longest prefix of `bytes`, at most `limit` long, that ends on a UTF-8
/// character boundary, so no device call splits a character. A boundary is
/// any byte other than a continuation byte (`0b10xx_xxxx`), and a character is
/// at most four bytes, so one lies within the last four positions; input that
/// is not UTF-8 falls back to `limit`.
#[cfg(any(windows, test))]
pub(crate) fn char_prefix(bytes: &[u8], limit: usize) -> usize {
    if bytes.len() <= limit {
        return bytes.len();
    }
    (limit.saturating_sub(3)..=limit)
        .rev()
        .find(|&end| end > 0 && bytes.get(end).is_some_and(|byte| byte & 0xC0 != 0x80))
        .unwrap_or(limit)
}

/// Split a frame into calls of at most `chunk` bytes, each ending on a
/// character boundary, for devices whose calls each run under their own
/// deadline (the Windows console owner).
#[cfg(any(windows, test))]
pub(crate) fn present_parts(
    frame: &[u8],
    chunk: usize,
    mut call: impl FnMut(Part) -> io::Result<()>,
) -> io::Result<()> {
    if chunk == 0 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let (mut rest, mut first) = (frame, true);
    while !rest.is_empty() {
        let (bytes, tail) = rest.split_at(char_prefix(rest, chunk));
        rest = tail;
        call(Part {
            bytes: bytes.to_vec(),
            first,
            last: rest.is_empty(),
        })?;
        first = false;
    }
    Ok(())
}

#[cfg(any(windows, test))]
impl Part {
    /// Write this part under the guard's journal: the first part journals the
    /// frame and opens its update, the last closes it.
    pub(crate) fn apply<W: Write>(
        mut self,
        guard: &mut super::RestoreGuard<W>,
        cursor_hidden: bool,
    ) -> io::Result<()> {
        let synchronized = if self.first {
            guard.begin_frame(cursor_hidden)
        } else {
            guard.frame_open()
        };
        guard.write_frame(
            &mut self.bytes,
            self.first && synchronized,
            self.last && synchronized,
        )?;
        if self.last {
            guard.end_frame(cursor_hidden);
        }
        Ok(())
    }
}

/// Frame bytes retained until the terminal commits a completed frame.
/// Backend flushes inside one draw are deferred, so the device receives the
/// frame at once instead of one write per queued command.
struct FrameOutput<D: Present> {
    device: D,
    frame: Vec<u8>,
    cursor_hidden: bool,
}

impl<D: Present> Write for FrameOutput<D> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.frame.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<D: Present> FrameOutput<D> {
    fn commit(&mut self) -> io::Result<()> {
        if self.frame.is_empty() {
            return Ok(());
        }
        // Taken, so a device that unwinds cannot leave a half-presented frame
        // to be presented again; returned with its capacity, so steady-state
        // frames allocate nothing here.
        let mut frame = std::mem::take(&mut self.frame);
        let presented = self.device.present(&mut frame, self.cursor_hidden);
        frame.clear();
        self.frame = frame;
        presented
    }
}

/// Crossterm encoding over a deferred frame, with the terminal size supplied
/// by the caller's one query per frame instead of re-queried by every draw.
struct FrameBackend<D: Present> {
    output: FrameOutput<D>,
    size: Size,
    shown: Shown,
    /// The device's outside-write count when `shown` was last ours.
    ours: Option<u64>,
}

/// The cursor the device shows after the last presented frame. A command that
/// restates it is not encoded, so a frame identical to the screen encodes no
/// bytes and reaches no device. `None` is unknown: the command is encoded.
/// Valid only while the device was reached by nothing but our frames since:
/// any outside write, failed frame or resize makes it unknown.
#[derive(Default)]
struct Shown {
    visible: Option<bool>,
    position: Option<Position>,
}

impl<D: Present> FrameBackend<D> {
    // Encoding is stateless; borrowing the frame keeps ownership here.
    fn crossterm(&mut self) -> CrosstermBackend<&mut FrameOutput<D>> {
        CrosstermBackend::new(&mut self.output)
    }
}

impl<D: Present> Backend for FrameBackend<D> {
    type Error = io::Error;
    fn draw<'a, I: Iterator<Item = (u16, u16, &'a Cell)>>(&mut self, content: I) -> io::Result<()> {
        // An empty diff would encode only an attribute reset, which the
        // previous drawn frame already ended with.
        let mut content = content.peekable();
        if content.peek().is_none() {
            return Ok(());
        }
        self.shown.position = None;
        self.crossterm().draw(content)
    }
    fn append_lines(&mut self, lines: u16) -> io::Result<()> {
        self.shown.position = None;
        self.crossterm().append_lines(lines)
    }
    fn hide_cursor(&mut self) -> io::Result<()> {
        self.output.cursor_hidden = true;
        if self.shown.visible.replace(false) == Some(false) {
            return Ok(());
        }
        self.crossterm().hide_cursor()
    }
    fn show_cursor(&mut self) -> io::Result<()> {
        self.output.cursor_hidden = false;
        if self.shown.visible.replace(true) == Some(true) {
            return Ok(());
        }
        self.crossterm().show_cursor()
    }
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        self.crossterm().get_cursor_position()
    }
    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        let position = position.into();
        if self.shown.position.replace(position) == Some(position) {
            return Ok(());
        }
        self.crossterm().set_cursor_position(position)
    }
    fn clear(&mut self) -> io::Result<()> {
        self.shown.position = None;
        self.crossterm().clear()
    }
    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.shown.position = None;
        self.crossterm().clear_region(clear_type)
    }
    fn size(&self) -> io::Result<Size> {
        Ok(self.size)
    }
    fn window_size(&mut self) -> io::Result<WindowSize> {
        Ok(WindowSize {
            columns_rows: self.size,
            pixels: Size::default(),
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        Backend::flush(&mut self.crossterm())
    }
}

// Dropping the terminal queues ratatui's cursor restoration (shown again)
// into the frame; teardown presents it before releasing the device. If the
// device is already closed, its journal has restored the cursor.
impl<D: Present> Drop for FrameBackend<D> {
    fn drop(&mut self) {
        let _ = self.output.commit();
    }
}

/// The only full-screen presentation path: each draw reaches the device as
/// one presented frame, wrapped in synchronized output (DECSET 2026) by the
/// device when the probed terminal supports it. A draw identical to the screen
/// (unchanged buffer and cursor) reaches no device at all.
pub struct FrameTerminal<D: Present> {
    terminal: Terminal<FrameBackend<D>>,
}

impl<D: Present> FrameTerminal<D> {
    /// Present frames of `size` to `device`.
    pub fn new(device: D, size: Size) -> io::Result<Self> {
        Ok(Self {
            terminal: Terminal::new(FrameBackend {
                output: FrameOutput {
                    device,
                    frame: Vec::new(),
                    cursor_hidden: false,
                },
                size,
                shown: Shown::default(),
                ours: None,
            })?,
        })
    }

    /// Render at the current terminal size and present the frame at once.
    /// A size change repaints the whole screen exactly as ratatui's autoresize.
    pub fn draw(&mut self, size: Size, render: impl FnOnce(&mut Frame<'_>)) -> io::Result<()> {
        let backend = self.terminal.backend_mut();
        let outside = backend.output.device.outside_writes();
        if outside.is_none() || outside != backend.ours || backend.size != size {
            backend.shown = Shown::default();
        }
        backend.ours = outside;
        backend.size = size;
        let drawn = self.terminal.draw(render).map(drop);
        let backend = self.terminal.backend_mut();
        let committed = backend.output.commit();
        if drawn.is_err() || committed.is_err() {
            // The device may hold any part of the frame.
            backend.shown = Shown::default();
        }
        drawn.and(committed)
    }
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
