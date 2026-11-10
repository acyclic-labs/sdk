//! A thin terminal projection of the shared, scoped stream explorer.
use super::*;
use acyclic_harness::{
    agent::AgentPath,
    client::{
        explorer::{LiveExplorer, Row},
        ui::{ReaderCut, UiPath, View as UiView},
    },
    tui::{
        DecodedKey, InputKeymap, Motion, decode_key,
        key_event::{KeyCode, KeyEventKind, MouseButton, MouseEventKind},
    },
};
use acyclic_stream::session::ReadProvider;

pub(super) enum Intent {
    Toggle,
    Input,
    Attach(AgentPath),
    Parent,
    Consumed,
}

pub(super) struct Sidebar {
    pub source: LiveExplorer<ReadProvider>,
    pub focused: bool,
    selected: usize,
    pub failure: Option<String>,
}

impl Sidebar {
    pub fn new(conversation: &Conversation) -> Result<Self, Failure> {
        Ok(Self {
            source: conversation.tree()?,
            focused: false,
            selected: 0,
            failure: None,
        })
    }

    pub fn frame(&mut self, now: u64, height: u16) {
        self.source
            .view
            .set_viewport(usize::from(height.saturating_sub(3)).max(1), 0, 16);
        self.source.view.frame(now);
        self.selected = self
            .selected
            .min(self.source.view.rows().len().saturating_sub(1));
        if let Some((stream, issue)) = self.source.view.reader_ref().issues().next() {
            self.failure = Some(format!("{stream}: {issue:?}"));
        }
    }

    pub fn has_children(&self) -> bool {
        self.source
            .view
            .rows()
            .iter()
            .any(|(_, row)| matches!(row, Row::Agent { .. }))
    }

    pub fn rows(&self, root: &AgentPath, shown: bool) -> Vec<Line<'static>> {
        if !shown {
            return Vec::new();
        }
        if let Some(error) = &self.failure {
            return vec![Line::raw(format!("Tree unavailable: {error}"))];
        }
        let projection = UiView::new(1).expect("nonzero UI page size");
        let status = UiPath::new("status.md").expect("UI status path");
        let mut rows: Vec<_> = self
            .source
            .view
            .rows()
            .iter()
            .enumerate()
            .map(|(index, (_, row))| {
                let marker = if self.focused && index == self.selected {
                    ">"
                } else {
                    " "
                };
                match row {
                    Row::Loading { .. } => Line::raw(format!("{marker} Loading…")),
                    Row::Agent { path, expanded, .. } => {
                        let cut = ReaderCut {
                            reader: self.source.view.reader_ref(),
                            agent: path,
                            file: &|_| None,
                        };
                        let text = projection
                            .project(&cut, &status)
                            .unwrap_or_else(|_| "Loading…".into());
                        let indent = path.depth().saturating_sub(root.depth() + 1).min(8);
                        let branch = if *expanded { '▾' } else { '▸' };
                        Line::raw(format!(
                            "{marker}{}{branch} {} · {}",
                            "  ".repeat(indent),
                            path.id(),
                            text.lines().next().unwrap_or_default()
                        ))
                    }
                }
            })
            .collect();
        if rows.is_empty() {
            rows.push(Line::raw("No children"));
        }
        rows.push(Line::raw("Enter attach · ←/→ fold · Esc input"));
        rows
    }

    pub fn event(
        &mut self,
        event: &InputEvent,
        keys: &InputKeymap,
        shown: bool,
        area: Rect,
    ) -> Option<Intent> {
        if let InputEvent::Mouse(mouse) = event {
            if shown && area.contains(Position::new(mouse.column, mouse.row)) {
                match mouse.kind {
                    MouseEventKind::Down(MouseButton::Left) => {
                        self.focused = true;
                        self.selected = usize::from(mouse.row.saturating_sub(area.y + 1))
                            .min(self.source.view.rows().len().saturating_sub(1));
                    }
                    MouseEventKind::ScrollDown => self.source.view.scroll(3),
                    MouseEventKind::ScrollUp => self.source.view.scroll(-3),
                    _ => {}
                }
                return Some(Intent::Consumed);
            }
        }
        let InputEvent::Key(key) = event else {
            return None;
        };
        if key.kind == KeyEventKind::Release {
            return None;
        }
        let decoded = decode_key(*key, keys);
        if matches!(
            decoded,
            Some(DecodedKey::Action {
                action: Action::App(AppAction::Tree),
                ..
            })
        ) {
            return Some(Intent::Toggle);
        }
        if matches!(
            decoded,
            Some(DecodedKey::Action {
                action: Action::App(AppAction::Parent),
                ..
            })
        ) {
            return Some(Intent::Parent);
        }
        if !shown || !self.focused {
            return None;
        }
        if key.code == KeyCode::Esc {
            self.focused = false;
            return Some(Intent::Input);
        }
        let selected = self
            .source
            .view
            .rows()
            .get(self.selected)
            .and_then(|(_, row)| match row {
                Row::Agent { path, .. } => Some(path.clone()),
                Row::Loading { .. } => None,
            });
        match decoded {
            Some(DecodedKey::Action {
                action: Action::Submit,
                ..
            }) => {
                return selected.map(Intent::Attach).or(Some(Intent::Consumed));
            }
            Some(DecodedKey::Action {
                action: Action::Move(Motion::Left),
                ..
            }) => {
                if let Some(path) = selected {
                    self.source.view.collapse(&path);
                }
            }
            Some(DecodedKey::Action {
                action: Action::Move(Motion::Right),
                ..
            }) => {
                if let Some(path) = selected {
                    self.source.view.expand(&path);
                }
            }
            _ if key.code == KeyCode::Down => {
                if self.selected + 1 < self.source.view.rows().len() {
                    self.selected += 1;
                } else {
                    self.source.view.scroll(1);
                }
            }
            _ if key.code == KeyCode::Up => {
                if self.selected > 0 {
                    self.selected -= 1;
                } else {
                    self.source.view.scroll(-1);
                }
            }
            _ if key.code == KeyCode::PageDown => self
                .source
                .view
                .scroll(self.source.view.rows().len() as i64),
            _ if key.code == KeyCode::PageUp => self
                .source
                .view
                .scroll(-(self.source.view.rows().len() as i64)),
            _ => {}
        }
        Some(Intent::Consumed)
    }
}
