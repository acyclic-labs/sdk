use crate::key_event::InputEvent;
use crate::{InputEffect, PromptInput, Viewport};
use ratatui::layout::Rect;

impl PromptInput {
    /// Consume events already decoded and classified by the terminal layer.
    pub fn event(
        &mut self,
        event: InputEvent,
        area: Rect,
        viewport: &mut Viewport,
        now_ms: u64,
    ) -> Vec<InputEffect> {
        self.input_width = usize::from(area.width);
        match event {
            InputEvent::Key(key) => self.key(key, now_ms, self.input_width),
            InputEvent::Paste(text) => self.paste(&text, now_ms),
            InputEvent::CommittedText(text) => self.committed_text(&text, now_ms),
            InputEvent::Mouse(mouse) => self.mouse(mouse, area, viewport, now_ms),
            InputEvent::Focus(false) => {
                let effects = self.flush_chord();
                self.editor.break_group();
                self.dragging = false;
                effects
            }
            InputEvent::Resize(_, _) | InputEvent::Focus(true) => self.flush_chord(),
        }
    }

    /// Drive the chord-prefix timeout without blocking or spawning a timer task.
    pub fn tick(&mut self, now_ms: u64) -> Vec<InputEffect> {
        self.chord_timeout(now_ms)
    }
}
