use std::io::{self, Write};

use ratatui::{
    Frame, Terminal,
    backend::{Backend, ClearType, CrosstermBackend, WindowSize},
    buffer::Cell,
    layout::{Position, Size},
};

const BEGIN_SYNCHRONIZED: &[u8] = b"\x1b[?2026h";
const END_SYNCHRONIZED: &[u8] = b"\x1b[?2026l";

/// Frame bytes retained until the terminal commits a completed frame.
/// Backend flushes inside one draw are deferred, so the device receives one
/// write per frame (bounded per call only by the device writer itself).
struct FrameOutput<W: Write> {
    device: W,
    frame: Vec<u8>,
    synchronized: bool,
}

impl<W: Write> Write for FrameOutput<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.frame.is_empty() && self.synchronized && !bytes.is_empty() {
            self.frame.extend_from_slice(BEGIN_SYNCHRONIZED);
        }
        self.frame.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<W: Write> FrameOutput<W> {
    fn commit(&mut self) -> io::Result<()> {
        if self.frame.is_empty() {
            return Ok(());
        }
        if self.synchronized {
            self.frame.extend_from_slice(END_SYNCHRONIZED);
        }
        // Keep the capacity: steady-state frames allocate nothing here.
        let written = self.device.write_all(&self.frame);
        self.frame.clear();
        written.and_then(|()| self.device.flush())
    }
}

/// Crossterm encoding over a deferred frame, with the terminal size supplied
/// by the caller's one query per frame instead of re-queried by every draw.
struct FrameBackend<W: Write> {
    output: FrameOutput<W>,
    size: Size,
}

impl<W: Write> FrameBackend<W> {
    // Encoding is stateless; borrowing the frame keeps ownership here.
    fn crossterm(&mut self) -> CrosstermBackend<&mut FrameOutput<W>> {
        CrosstermBackend::new(&mut self.output)
    }
}

impl<W: Write> Backend for FrameBackend<W> {
    type Error = io::Error;
    fn draw<'a, I: Iterator<Item = (u16, u16, &'a Cell)>>(&mut self, content: I) -> io::Result<()> {
        self.crossterm().draw(content)
    }
    fn append_lines(&mut self, lines: u16) -> io::Result<()> {
        self.crossterm().append_lines(lines)
    }
    fn hide_cursor(&mut self) -> io::Result<()> {
        self.crossterm().hide_cursor()
    }
    fn show_cursor(&mut self) -> io::Result<()> {
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

/// The only full-screen presentation path: each draw reaches the device as
/// one write, wrapped in synchronized output (DECSET 2026) when the probed
/// terminal supports it. A frame with an unchanged buffer and cursor still
/// commits only its cursor placement.
pub struct FrameTerminal<W: Write> {
    terminal: Terminal<FrameBackend<W>>,
}

impl<W: Write> FrameTerminal<W> {
    /// `synchronized` comes from `Capabilities::mode(2026).supported()`.
    pub fn new(device: W, size: Size, synchronized: bool) -> io::Result<Self> {
        Ok(Self {
            terminal: Terminal::new(FrameBackend {
                output: FrameOutput {
                    device,
                    frame: Vec::new(),
                    synchronized,
                },
                size,
            })?,
        })
    }

    /// Render at the current terminal size and commit the frame in one write.
    /// A size change repaints the whole screen exactly as ratatui's autoresize.
    pub fn draw(&mut self, size: Size, render: impl FnOnce(&mut Frame<'_>)) -> io::Result<()> {
        self.terminal.backend_mut().size = size;
        let drawn = self.terminal.draw(render).map(drop);
        let committed = self.terminal.backend_mut().output.commit();
        drawn.and(committed)
    }
}
