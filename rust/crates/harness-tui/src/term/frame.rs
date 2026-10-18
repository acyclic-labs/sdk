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
    /// The negotiated profile supports bounded margins and row scrolling.
    fn supports_scroll_regions(&self) -> bool {
        false
    }

    /// Present one frame with its restoration effects.
    /// The buffer's contents are consumed. On error, a begun synchronized
    /// update has been closed or remains journaled for restoration.
    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()>;

    /// How many times the device was reached other than by a presented frame
    /// (setup, restore, suspend and resume, direct writes), or `None` when
    /// unknown. While it is unchanged since the last frame, the screen state
    /// that frame left is still the device's.
    fn outside_writes(&mut self) -> Option<u64>;

    /// Compare the observation and present under one lifecycle boundary, even
    /// for an empty frame. None rejects without writing or changing the buffer;
    /// Some returns the count at that presentation. An absent precondition is
    /// used only when encoding already restates the complete requested cursor.
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        effects: FrameEffects,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>>;
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
    fn supports_scroll_regions(&self) -> bool {
        (**self).supports_scroll_regions()
    }

    fn present(&mut self, frame: &mut Vec<u8>, effects: FrameEffects) -> io::Result<()> {
        (**self).present(frame, effects)
    }
    fn outside_writes(&mut self) -> Option<u64> {
        (**self).outside_writes()
    }
    fn present_if_current(
        &mut self,
        frame: &mut Vec<u8>,
        effects: FrameEffects,
        expected: Option<u64>,
    ) -> io::Result<Option<u64>> {
        (**self).present_if_current(frame, effects, expected)
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
    fn commit(&mut self, expected: Option<u64>) -> io::Result<Option<u64>> {
        // Empty frames must cross the same boundary as nonempty ones: the
        // signal worker can invalidate a cursor while the caller is rendering.
        let mut frame = std::mem::take(&mut self.frame);
        let presented = self
            .device
            .present_if_current(&mut frame, self.effects, expected);
        if !matches!(presented, Ok(None)) {
            self.effects.margins = false;
            frame.clear();
        }
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
    position_written: bool,
}

/// Cursor and default attribute state after the last presented frame. A command that
/// restates it is not encoded, so a frame identical to the screen encodes no
/// bytes and reaches no device. `None` is unknown: the command is encoded.
/// Valid only while the device was reached by nothing but our frames since:
/// any outside write, failed frame or resize makes it unknown.
#[derive(Clone, Copy, Default)]
struct Shown {
    visible: Option<bool>,
    position: Option<Position>,
    attributes: bool,
}

impl<D: Present> FrameBackend<D> {
    fn reset_attributes(&mut self) -> io::Result<()> {
        if !self.shown.attributes {
            self.output.write_all(b"\x1b[0m")?;
            self.shown.attributes = true;
        }
        Ok(())
    }

    // Encoding is stateless; borrowing the frame keeps ownership here.
    fn crossterm(&mut self) -> CrosstermBackend<&mut FrameOutput<D>> {
        CrosstermBackend::new(&mut self.output)
    }
}

impl<D: Present> Backend for FrameBackend<D> {
    type Error = io::Error;
    fn draw<'a, I: Iterator<Item = (u16, u16, &'a Cell)>>(&mut self, content: I) -> io::Result<()> {
        // Crossterm starts each diff assuming default attributes. Restore that
        // precondition even for an empty diff after outside output or failure.
        self.reset_attributes()?;
        let mut content = content.peekable();
        if content.peek().is_none() {
            return Ok(());
        }
        self.shown.position = None;
        self.crossterm().draw(content)
    }
    fn append_lines(&mut self, lines: u16) -> io::Result<()> {
        self.reset_attributes()?;
        self.shown.position = None;
        self.crossterm().append_lines(lines)
    }
    fn hide_cursor(&mut self) -> io::Result<()> {
        self.output.effects.cursor_hidden = true;
        if self.shown.visible.replace(false) == Some(false) {
            return Ok(());
        }
        self.crossterm().hide_cursor()
    }
    fn show_cursor(&mut self) -> io::Result<()> {
        self.output.effects.cursor_hidden = false;
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
        self.position_written = true;
        self.crossterm().set_cursor_position(position)
    }
    fn clear(&mut self) -> io::Result<()> {
        self.reset_attributes()?;
        self.shown.position = None;
        self.crossterm().clear()
    }
    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.reset_attributes()?;
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
        let _ = self.output.commit(None);
    }
}

/// The only full-screen presentation path: each draw reaches the device as
/// one presented frame, wrapped in synchronized output (DECSET 2026) by the
/// device when the probed terminal supports it. A draw identical to the screen
/// (unchanged buffer and cursor) reaches no device at all. Scroll hints
/// shift only a supported device and the retained baseline together.
pub struct FrameTerminal<D: Present> {
    terminal: Terminal<FrameBackend<D>>,
    /// One retained screen baseline, exchanged with the render buffer.
    shown: Buffer,
    staging: Buffer,
    scroll_regions: bool,
    invalid: bool,
    /// A draw is in progress. Still set at the next draw, the previous one
    /// unwound (render or device panic) after the baseline may have moved.
    drawing: bool,
    repaired: Vec<usize>,
    cursor: Vec<u8>,
    #[cfg(test)]
    work: FrameWork,
}

#[cfg(test)]
#[derive(Default, Debug, PartialEq, Eq)]
struct FrameWork {
    diff: usize,
    reset: usize,
    terminal_diff: usize,
    terminal_reset: usize,
}

// Restore buffer custody even when the caller's render closure unwinds.
struct RenderBuffer<'a, 'b> {
    frame: &'a mut Frame<'b>,
    staging: &'a mut Buffer,
}

impl Drop for RenderBuffer<'_, '_> {
    fn drop(&mut self) {
        self.frame.buffer_mut().reset();
        std::mem::swap(self.frame.buffer_mut(), self.staging);
    }
}

impl<D: Present> FrameTerminal<D> {
    /// Present frames of `size` to `device`.
    pub fn new(device: D, size: Size) -> io::Result<Self> {
        let scroll_regions = device.supports_scroll_regions();
        Ok(Self {
            terminal: Terminal::new(FrameBackend {
                output: FrameOutput {
                    device,
                    frame: Vec::new(),
                    effects: FrameEffects::default(),
                },
                size,
                shown: Shown::default(),
                ours: None,
                position_written: false,
            })?,
            shown: Buffer::empty(ratatui::layout::Rect::default()),
            staging: Buffer::default(),
            scroll_regions,
            invalid: false,
            drawing: false,
            repaired: Vec::new(),
            cursor: Vec::new(),
            #[cfg(test)]
            work: FrameWork::default(),
        })
    }

    /// Render at the current terminal size and present the frame at once.
    /// A size change repaints the whole screen exactly as ratatui's autoresize.
    pub fn draw(&mut self, size: Size, render: impl FnOnce(&mut Frame<'_>)) -> io::Result<()> {
        self.draw_scrolled(size, None, render)
    }

    /// Draw a frame, using a supported row-scroll hint before its diff.
    pub fn draw_scrolled(
        &mut self,
        size: Size,
        scroll: Option<RegionScroll>,
        render: impl FnOnce(&mut Frame<'_>),
    ) -> io::Result<()> {
        let backend = self.terminal.backend_mut();
        let outside = backend.output.device.outside_writes();
        let unwound = std::mem::replace(&mut self.drawing, true);
        let invalidated =
            unwound || outside.is_none() || outside != backend.ours || backend.size != size;
        backend.ours = outside;
        backend.size = size;
        if invalidated {
            self.invalidate();
        }
        let drawn = self.scroll_and_draw(scroll, render);
        let expected = self.terminal.backend_mut().ours;
        let presented = self.terminal.backend_mut().output.commit(expected);
        let committed = match presented {
            Ok(None) => {
                // Outside output can change cells as well as attributes/cursor.
                // Re-encode the retained target against the empty staging buffer;
                // no second render, screen clone, or assumed device baseline.
                let desired = self.terminal.backend_mut().shown;
                let restored = self.recover_contents();
                let backend = self.terminal.backend_mut();
                let repair = restored.and_then(|()| match desired.visible {
                    Some(true) => backend.show_cursor().and_then(|()| {
                        desired
                            .position
                            .map_or(Ok(()), |position| backend.set_cursor_position(position))
                    }),
                    Some(false) => backend.hide_cursor(),
                    None => Ok(()),
                });
                repair.and_then(|()| backend.output.commit(None))
            }
            result => result,
        }
        .and_then(|count| count.ok_or_else(|| io::ErrorKind::Interrupted.into()));
        let backend = self.terminal.backend_mut();
        if let Ok(count) = committed.as_ref() {
            backend.ours = Some(*count);
        }
        if drawn.is_err() || committed.is_err() {
            // The device may hold any part of the frame.
            self.invalidate();
        }
        self.drawing = false;
        drawn.and(committed.map(|_| ()))
    }

    // Cell, cursor and attribute assumptions share one invalidation path.
    fn invalidate(&mut self) {
        self.invalid = true;
        self.terminal.backend_mut().shown = Shown::default();
    }

    fn recover_contents(&mut self) -> io::Result<()> {
        let mut bytes = std::mem::take(&mut self.terminal.backend_mut().output.frame);
        bytes.clear();
        // Restate every device assumption before repainting: attributes,
        // margins, cells, then the requested cursor at the checked boundary.
        bytes.extend_from_slice(b"\x1b[0m");
        if self.scroll_regions {
            bytes.extend_from_slice(b"\x1b[r");
        }
        bytes.extend_from_slice(b"\x1b[2J");
        let encoded = encode_diff(&self.staging, &self.shown, &mut bytes);
        let backend = self.terminal.backend_mut();
        backend.output.frame = bytes;
        backend.output.effects.margins = self.scroll_regions;
        backend.shown = Shown {
            attributes: true,
            ..Shown::default()
        };
        encoded
    }

    fn scroll_and_draw(
        &mut self,
        scroll: Option<RegionScroll>,
        render: impl FnOnce(&mut Frame<'_>),
    ) -> io::Result<()> {
        let previous_area = self.terminal.get_frame().area();
        let size = self.terminal.backend().size;
        let requested = ratatui::layout::Rect::new(0, 0, size.width, size.height);
        if requested != previous_area
            && self.terminal.current_buffer_mut().area == ratatui::layout::Rect::default()
        {
            // Resize the owned pair, rather than allocating two temporary
            // viewport buffers while our reusable storage sits outside Terminal.
            std::mem::swap(self.terminal.current_buffer_mut(), &mut self.shown);
            self.terminal.swap_buffers();
            std::mem::swap(self.terminal.current_buffer_mut(), &mut self.staging);
        }
        self.terminal.autoresize()?;
        let area = self.terminal.get_frame().area();
        if self.invalid && area == previous_area {
            // We own the retained baseline. Clear through the backend without
            // Terminal::clear's synchronous cursor query on the output path.
            self.terminal.backend_mut().clear()?;
        }
        let reclaimed = self.terminal.current_buffer_mut().area != ratatui::layout::Rect::default();
        if reclaimed {
            // Reclaim both buffers, including their capacity. The swap clears
            // the baseline; ordinary draws then see two genuinely empty buffers.
            self.staging = std::mem::take(self.terminal.current_buffer_mut());
            self.terminal.swap_buffers();
            self.shown = std::mem::take(self.terminal.current_buffer_mut());
        }
        if self.staging.area != area {
            self.staging.resize(area);
        }
        let retained = !self.invalid && self.shown.area == area;
        if !retained && !reclaimed {
            self.shown.resize(area);
            self.shown.reset();
        }
        self.invalid = false;
        self.terminal.backend_mut().reset_attributes()?;
        let mut bytes = std::mem::replace(
            &mut self.terminal.backend_mut().output.frame,
            std::mem::take(&mut self.cursor),
        );
        let margins = if retained && self.scroll_regions {
            match scroll {
                Some(scroll) => scroll.apply(&mut self.shown, &mut bytes)?,
                None => false,
            }
        } else {
            false
        };
        let Self {
            terminal,
            shown,
            staging,
            repaired,
            cursor,
            ..
        } = self;
        #[cfg(test)]
        let mut work = FrameWork::default();
        #[cfg(test)]
        let mut returned_cells = 0;
        let mut encoded = Ok(());
        terminal.backend_mut().position_written = false;
        let drawn = terminal
            .draw(|frame| {
                std::mem::swap(frame.buffer_mut(), staging);
                let render_buffer = RenderBuffer { frame, staging };
                render(render_buffer.frame);
                let next = render_buffer.frame.buffer_mut();
                repair_wide_trails(shown, next, repaired);
                // Ratatui's Buffer::diff_iter remains the encoder input. Owning its
                // baseline lets scrolling rotate cells and retain heap symbols.
                encoded = encode_diff(shown, next, &mut bytes);
                #[cfg(test)]
                {
                    work.diff = shown.content.len().min(next.content.len());
                }
                for &index in repaired.iter() {
                    next.content[index].diff_option = CellDiffOption::None;
                }
                std::mem::swap(shown, next);
                #[cfg(test)]
                {
                    work.reset = next.content.len();
                }
                drop(render_buffer);
                #[cfg(test)]
                {
                    returned_cells = frame.buffer_mut().content.len();
                }
            })
            .map(drop);
        #[cfg(test)]
        {
            if drawn.is_ok() {
                // After ratatui's swap, current is the buffer it just reset.
                // These are the actual operands of its diff and reset passes.
                work.terminal_reset = terminal.current_buffer_mut().content.len();
                work.terminal_diff = work.terminal_reset.min(returned_cells);
            }
            self.work = work;
        }
        let backend = terminal.backend_mut();
        if !bytes.is_empty() && !backend.position_written {
            let position = backend.shown.position.take();
            if backend.shown.visible == Some(true)
                && let Some(position) = position
            {
                backend.set_cursor_position(position)?;
            }
        }
        let output = &mut backend.output;
        bytes.append(&mut output.frame);
        *cursor = std::mem::replace(&mut output.frame, bytes);
        // Pair the effect with its encoded reset only after the body is back.
        // A render panic must not let cursor teardown retire older margins.
        output.effects.margins = margins;
        drawn.and(encoded)
    }
}

// Empty diffs must not invoke crossterm's formatting/reset path. Both ordinary
// rendering and checked full-screen recovery use this encoder choke point.
fn encode_diff(previous: &Buffer, next: &Buffer, bytes: &mut Vec<u8>) -> io::Result<()> {
    let mut content = previous.diff_iter(next).peekable();
    if content.peek().is_none() {
        return Ok(());
    }
    CrosstermBackend::new(bytes).draw(content)
}

/// A wide glyph paints its style over trailing cells even though the logical
/// buffer stores reset cells there. If a new wide glyph covers the old lead,
/// ratatui skips that lead and can miss its newly uncovered trailing style.
/// Force those uncovered cells through the diff before cursor placement.
fn repair_wide_trails(previous: &Buffer, next: &mut Buffer, repaired: &mut Vec<usize>) {
    repaired.clear();
    if previous.area != next.area || next.area.width == 0 {
        return;
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
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;

#[cfg(all(test, acyclic_harness_tui_loom))]
#[path = "frame_model.rs"]
mod frame_model;
