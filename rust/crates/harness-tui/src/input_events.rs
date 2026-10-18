use crate::key_event::{InputEvent, MouseEventKind};
use crate::{InputEffect, PromptInput, Viewport};
use ratatui::layout::Rect;

/// Rows per wheel notch: the xterm, VTE and Windows default. Speed comes from
/// the device's notch rate (free-spinning wheels, OS-accelerated trackpads), so
/// the application adds no second acceleration curve on top of it.
pub const WHEEL_ROWS: isize = 3;

impl PromptInput {
    /// Consume events already decoded and classified by the terminal layer.
    pub fn event(
        &mut self,
        event: InputEvent,
        area: Rect,
        viewport: &mut Viewport,
        now_ms: u64,
    ) -> impl Iterator<Item = InputEffect> + use<> {
        self.input_width = usize::from(area.width);
        let mut wheel = None;
        let effects = match event {
            InputEvent::Key(key) => self.key(key, now_ms, self.input_width),
            InputEvent::Paste(text) => self.paste(&text, now_ms),
            InputEvent::CommittedText(text) => self.committed_text(&text, now_ms),
            InputEvent::Mouse(mouse) => match mouse.kind {
                MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                    wheel = Some(InputEffect::Scroll(
                        if mouse.kind == MouseEventKind::ScrollUp {
                            WHEEL_ROWS
                        } else {
                            -WHEEL_ROWS
                        },
                    ));
                    Vec::new()
                }
                _ => self.mouse(mouse, area, viewport, now_ms),
            },
            InputEvent::Focus(false) => {
                let effects = self.flush_chord();
                self.editor.break_group();
                self.dragging = false;
                effects
            }
            InputEvent::Resize(_, _) | InputEvent::Focus(true) => self.flush_chord(),
        };
        wheel.into_iter().chain(effects)
    }

    /// Drive the chord-prefix timeout without blocking or spawning a timer task.
    pub fn tick(&mut self, now_ms: u64) -> Vec<InputEffect> {
        self.chord_timeout(now_ms)
    }
}
