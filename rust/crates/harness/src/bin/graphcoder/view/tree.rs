//! A thin terminal projection of the shared, scoped stream explorer.
use super::*;
use acyclic_harness::{
    agent::AgentPath,
    client::{
        explorer::{LiveExplorer, Row},
        ui::{AgentRow, ReaderCut, View as UiView},
    },
    local::agent_conversation::VisibleFiles,
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
    History(isize),
    Consumed,
}

pub(super) struct Sidebar {
    pub source: LiveExplorer<ReadProvider>,
    pub focused: bool,
    selected: usize,
    pub failure: Option<String>,
    pub files: VisibleFiles,
    history: Vec<AgentPath>,
    history_at: usize,
}

impl Sidebar {
    pub fn new(conversation: &Arc<Conversation>) -> Result<Self, Failure> {
        Ok(Self {
            source: conversation.tree()?,
            focused: false,
            selected: 0,
            failure: None,
            files: VisibleFiles::new(
                Arc::clone(conversation),
                AgentRow::PATHS
                    .into_iter()
                    .map(|path| format!("ui/{path}"))
                    .collect(),
            ),
            history: vec![conversation.agent().clone()],
            history_at: 0,
        })
    }

    pub fn history_target(&self, direction: isize) -> Option<(AgentPath, Option<usize>)> {
        let at = self.history_at.checked_add_signed(direction)?;
        self.history.get(at).cloned().map(|agent| (agent, Some(at)))
    }

    /// Only a successfully attached node enters the navigation history.
    pub fn attached(&mut self, agent: &AgentPath, history: Option<usize>) {
        if let Some(at) = history {
            self.history_at = at;
        } else if self.history[self.history_at] != *agent {
            self.history.truncate(self.history_at + 1);
            self.history.push(agent.clone());
            self.history_at += 1;
        }
    }

    pub fn frame(&mut self, now: u64, height: u16, visible: bool) {
        self.source
            .view
            .set_viewport(usize::from(height.saturating_sub(3)).max(1), 0, 16);
        self.source.view.frame(now);
        self.files.retain(
            self.source
                .view
                .rows()
                .iter()
                .filter_map(|(_, row)| match row {
                    Row::Agent { path, .. } if visible => Some(path.clone()),
                    _ => None,
                }),
        );
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
                        let summary = match self.files.snapshot(path) {
                            Some(Ok(files)) => projection
                                .row(&cut, |name| {
                                    files
                                        .files
                                        .get(&format!("ui/{name}"))
                                        .and_then(Option::as_ref)
                                        .map(|file| file.text.as_str())
                                })
                                .map_err(|error| error.to_string()),
                            Some(Err(error)) => Err(format!("UI unavailable: {error}")),
                            None => Err("Loading…".into()),
                        };
                        let indent = path.depth().saturating_sub(root.depth() + 1).min(8);
                        let branch = if *expanded { '▾' } else { '▸' };
                        let prefix = format!("{marker}{}{branch}", "  ".repeat(indent));
                        match summary {
                            Ok(row) => Line::raw(format!(
                                "{prefix} {} {} · {} · {} · {} · {}",
                                first_line(&row.glyph),
                                first_line(&row.title),
                                first_line(&row.model),
                                first_line(&row.cost),
                                first_line(&row.age),
                                first_line(&row.state)
                            )),
                            Err(error) => Line::raw(format!("{prefix} {} · {error}", path.id())),
                        }
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
        match decoded {
            Some(DecodedKey::Action {
                action: Action::App(AppAction::NavigationBack),
                ..
            }) => return Some(Intent::History(-1)),
            Some(DecodedKey::Action {
                action: Action::App(AppAction::NavigationForward),
                ..
            }) => return Some(Intent::History(1)),
            _ => {}
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

fn first_line(text: &str) -> String {
    text.lines()
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .collect()
}
