use std::io::{self, Write};

use ratatui::{
    Frame, Terminal,
    backend::{Backend, ClearType, CrosstermBackend, WindowSize},
    buffer::{Buffer, Cell, CellDiffOption, CellWidth},
    layout::{Position, Size},
    style::{Color, Modifier},
};

use super::RegionScroll;

/// A terminal device that presents whole frames under its restoration
/// journal (see `RestoreGuard`): the session's `Output`, or a guard itself.
pub trait Present {
    /// Present one frame with the given effects. The buffer's contents are
    /// consumed. On error, a begun synchronized update and set margins have
    /// been undone or remain journaled for restoration.
    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()>;
}

/// What a frame leaves on the device that restoration must undo if the
/// frame is cut short at any byte.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameEffects {
    /// The cursor is hidden after the frame.
    pub cursor_hidden: bool,
    /// The frame sets scrolling margins (DECSTBM), resetting them itself.
    pub margins: bool,
}

impl<P: Present + ?Sized> Present for &mut P {
    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()> {
        (**self).present(frame, effects)
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
        effects: FrameEffects,
    ) -> io::Result<()> {
        let synchronized = if self.first {
            guard.begin_frame(effects)
        } else {
            guard.frame_open()
        };
        guard.write_frame(
            &mut self.bytes,
            self.first && synchronized,
            self.last && synchronized,
        )?;
        if self.last {
            guard.end_frame(effects);
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
    effects: FrameEffects,
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
        let presented = self.device.present(&mut frame, self.effects);
        self.effects.margins = false;
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
        self.crossterm().draw(content)
    }
    fn append_lines(&mut self, lines: u16) -> io::Result<()> {
        self.crossterm().append_lines(lines)
    }
    fn hide_cursor(&mut self) -> io::Result<()> {
        self.output.effects.cursor_hidden = true;
        self.crossterm().hide_cursor()
    }
    fn show_cursor(&mut self) -> io::Result<()> {
        self.output.effects.cursor_hidden = false;
        self.crossterm().show_cursor()
    }
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        self.crossterm().get_cursor_position()
    }
    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.crossterm().set_cursor_position(position)
    }
    fn clear(&mut self) -> io::Result<()> {
        self.crossterm().clear()
    }
    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
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
/// device when the probed terminal supports it. A frame with an unchanged
/// buffer and cursor still commits only its cursor placement. A frame whose
/// band of rows moved (`draw_scrolled`) scrolls the band on the device first
/// and repaints only the rows that then differ.
pub struct FrameTerminal<D: Present> {
    terminal: Terminal<FrameBackend<D>>,
    /// The frame on the device, kept to shift for a region scroll: ratatui
    /// keeps its own copy private.
    shown: Buffer,
}

impl<D: Present> FrameTerminal<D> {
    /// Present frames of `size` to `device`.
    pub fn new(device: D, size: Size) -> io::Result<Self> {
        Ok(Self {
            terminal: Terminal::new(FrameBackend {
                output: FrameOutput {
                    device,
                    frame: Vec::new(),
                    effects: FrameEffects::default(),
                },
                size,
            })?,
            shown: Buffer::empty(ratatui::layout::Rect::default()),
        })
    }

    /// Render at the current terminal size and present the frame at once.
    /// A size change repaints the whole screen exactly as ratatui's autoresize.
    pub fn draw(&mut self, size: Size, render: impl FnOnce(&mut Frame<'_>)) -> io::Result<()> {
        self.draw_scrolled(size, None, render)
    }

    /// `draw`, where `scroll` says a band of rows moved since the previous
    /// frame: the device scrolls the band in place, inside the same frame,
    /// and only rows that still differ are repainted.
    pub fn draw_scrolled(
        &mut self,
        size: Size,
        scroll: Option<RegionScroll>,
        render: impl FnOnce(&mut Frame<'_>),
    ) -> io::Result<()> {
        self.terminal.backend_mut().size = size;
        let drawn = self.scroll_and_draw(scroll, render);
        let committed = self.terminal.backend_mut().output.commit();
        if drawn.is_err() || committed.is_err() {
            // The device may hold any part of the frame: never shift it.
            self.shown.area = ratatui::layout::Rect::default();
        }
        drawn.and(committed)
    }

    fn scroll_and_draw(
        &mut self,
        scroll: Option<RegionScroll>,
        render: impl FnOnce(&mut Frame<'_>),
    ) -> io::Result<()> {
        // A resize clears the device and both frames; the stale copy then
        // has another area and is never shifted.
        self.terminal.autoresize()?;
        if let Some(scroll) = scroll {
            let Self { terminal, shown } = self;
            let mut frame = std::mem::take(&mut terminal.backend_mut().output.frame);
            let applied = scroll.apply(shown, terminal.current_buffer_mut(), &mut frame);
            let output = &mut terminal.backend_mut().output;
            output.frame = frame;
            if applied? {
                output.effects.margins = true;
                // The shifted copy becomes the frame the next diff is against.
                terminal.swap_buffers();
            }
        }
        let mut repaired = Vec::new();
        let shown = &mut self.shown;
        let completed = self.terminal.draw(|frame| {
            render(frame);
            repaired = repair_wide_trails(shown, frame.buffer_mut());
        })?;
        self.shown.area = completed.buffer.area;
        self.shown.content.clone_from(&completed.buffer.content);
        if !repaired.is_empty() {
            // AlwaysUpdate is a one-frame encoding instruction, not displayed
            // state. Reinstall the clean baseline so repeats remain free.
            for index in repaired {
                self.shown.content[index].diff_option = CellDiffOption::None;
            }
            self.terminal
                .current_buffer_mut()
                .content
                .clone_from(&self.shown.content);
            self.terminal.swap_buffers();
        }
        Ok(())
    }
}

/// A wide glyph paints its style over trailing cells even though the logical
/// buffer stores reset cells there. If a new wide glyph covers the old lead,
/// ratatui skips that lead and can miss its newly uncovered trailing style.
/// Force those uncovered cells through the diff before cursor placement.
fn repair_wide_trails(previous: &Buffer, next: &mut Buffer) -> Vec<usize> {
    let mut repaired = Vec::new();
    if previous.area != next.area || next.area.width == 0 {
        return repaired;
    }
    let width = usize::from(next.area.width);
    let visible = Modifier::REVERSED
        | Modifier::UNDERLINED
        | Modifier::SLOW_BLINK
        | Modifier::RAPID_BLINK
        | Modifier::CROSSED_OUT;
    for (row, (old, new)) in previous
        .content
        .chunks(width)
        .zip(next.content.chunks_mut(width))
        .enumerate()
    {
        // Ordinary one-cell rows need no width/style walk or scratch storage.
        if old.iter().all(|cell| {
            cell.symbol().len() == 1 && !matches!(cell.diff_option, CellDiffOption::ForcedWidth(_))
        }) {
            continue;
        }
        let (mut covered_until, mut styled_until) = (0, 0);
        for (column, (old, new)) in old.iter().zip(new.iter_mut()).enumerate() {
            let covered = column < covered_until;
            if !covered {
                covered_until = column + usize::from(new.cell_width().max(1));
                if column < styled_until && new.diff_option == CellDiffOption::None {
                    new.diff_option = CellDiffOption::AlwaysUpdate;
                    repaired.push(row * width + column);
                }
            }
            if old.fg != Color::Reset || old.bg != Color::Reset || old.modifier.intersects(visible)
            {
                styled_until = styled_until.max(column + usize::from(old.cell_width()));
            }
        }
    }
    repaired
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
