//! A thin terminal projection of the shared, scoped stream explorer.
use super::*;
use acyclic_harness::{
    agent::AgentPath,
    client::{
        explorer::{LiveExplorer, Row, SiblingNavigation},
        ui::{AgentRow, ReaderCut, UiPath, View as UiView, title_matches},
    },
    local::agent_conversation::VisibleFiles,
    tui::{
        DecodedKey, InputKeymap, Motion, decode_key,
        key_event::{KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind},
    },
};
use acyclic_stream::session::ReadProvider;

struct Search {
    text: String,
    editing: bool,
    seeking: bool,
    exhausted: bool,
    error: Option<String>,
}

pub(super) enum Intent {
    Toggle,
    Input,
    Attach(AgentPath),
    Diff(AgentPath),
    Wait(AgentPath),
    Merge(AgentPath),
    Discard(AgentPath),
    Parent,
    History(isize),
    Sibling(bool),
    Consumed,
}

pub(super) struct Sidebar {
    pub source: LiveExplorer<ReadProvider>,
    root: AgentPath,
    pub focused: bool,
    selected: usize,
    pub failure: Option<String>,
    pub files: VisibleFiles,
    history: Vec<AgentPath>,
    history_at: usize,
    navigation: Option<SiblingNavigation>,
    navigated: Option<AgentPath>,
    search: Option<Search>,
    matches: Vec<usize>,
    children: bool,
}

impl Sidebar {
    pub fn new(conversation: &Arc<Conversation>) -> Result<Self, Failure> {
        let mut source = conversation.tree()?;
        source.view.orchestrator_view(true);
        Ok(Self {
            source,
            root: conversation.agent().clone(),
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
            navigation: None,
            navigated: None,
            search: None,
            matches: Vec::new(),
            children: false,
        })
    }

    pub fn history_target(&self, direction: isize) -> Option<(AgentPath, Option<usize>)> {
        let at = self.history_at.checked_add_signed(direction)?;
        self.history.get(at).cloned().map(|agent| (agent, Some(at)))
    }

    /// Only a successfully attached node enters the navigation history.
    pub fn attached(&mut self, agent: &AgentPath, history: Option<usize>) {
        self.cancel_navigation();
        self.search = None;
        self.filter();
        if let Some(at) = history {
            self.history_at = at;
        } else if self.history[self.history_at] != *agent {
            self.history.truncate(self.history_at + 1);
            self.history.push(agent.clone());
            self.history_at += 1;
        }
    }

    pub fn cancel_navigation(&mut self) -> bool {
        let pending = self.navigation.take().is_some();
        let ready = self.navigated.take().is_some();
        pending || ready
    }

    pub fn sibling(&mut self, agent: &AgentPath, forward: bool) {
        self.navigated = None;
        self.navigation = self.source.view.sibling_navigation(agent, forward);
    }

    pub fn navigation(&mut self) -> Option<AgentPath> {
        if let Some(agent) = &self.navigated {
            return Some(agent.clone());
        }
        let navigation = self.navigation.as_mut()?;
        match self.source.view.navigate_sibling(navigation) {
            std::task::Poll::Pending => None,
            std::task::Poll::Ready(agent) => {
                self.navigation = None;
                self.navigated = agent;
                self.navigated.clone()
            }
        }
    }

    fn credit(&self, area: Rect) -> usize {
        if area.width > 2 {
            usize::from(
                area.height
                    .saturating_sub(if self.search.is_some() { 4 } else { 3 }),
            )
        } else {
            0
        }
    }

    pub fn frame(&mut self, now: u64, area: Rect) {
        let credit = self.credit(area);
        let visible = credit > 0;
        self.focused &= visible;
        let bytes = u64::from(area.width.saturating_sub(2)) * 4;
        self.source
            .view
            .set_viewport(if visible { credit } else { 1 }, 0, 16);
        self.source.view.descend(
            self.search
                .as_ref()
                .is_some_and(|search| !search.text.is_empty()),
        );
        self.source.view.frame(now);
        self.children = self.has_children();
        self.retain_files(visible, bytes);
        self.filter();
        if self
            .search
            .as_ref()
            .is_some_and(|search| search.seeking && !search.text.is_empty())
        {
            let pending = self.source.view.rows().iter().any(|(_, row)| match row {
                Row::Loading { .. } => true,
                Row::Agent { path, .. } => self.files.snapshot(path).is_none(),
            });
            let error = self
                .source
                .view
                .rows()
                .iter()
                .find_map(|(_, row)| match row {
                    Row::Agent { path, .. } => {
                        self.files.snapshot(path)?.err().map(ToString::to_string)
                    }
                    _ => None,
                });
            if let Some(error) = error {
                if let Some(search) = &mut self.search {
                    search.error = Some(error);
                    search.seeking = false;
                }
            } else if !self.matches.is_empty() {
                if let Some(search) = &mut self.search {
                    search.seeking = false;
                }
            } else if !pending {
                self.search_next();
                self.source.view.frame(now);
                self.retain_files(visible, bytes);
                self.filter();
            }
        }
        self.selected = self.selected.min(self.matches.len().saturating_sub(1));
        self.failure = self
            .source
            .view
            .reader_ref()
            .issues()
            .next()
            .map(|(stream, issue)| format!("{issue:?}: {stream}"));
    }

    fn retain_files(&mut self, visible: bool, bytes: u64) {
        self.files.retain(
            self.source
                .view
                .rows()
                .iter()
                .filter_map(|(_, row)| match row {
                    Row::Agent { path, .. } if visible => Some(path.clone()),
                    _ => None,
                }),
            bytes,
        );
    }

    fn filter(&mut self) {
        self.matches = self
            .source
            .view
            .rows()
            .iter()
            .enumerate()
            .filter_map(|(index, (_, row))| {
                let Some(search) = self
                    .search
                    .as_ref()
                    .filter(|search| !search.text.is_empty())
                else {
                    return Some(index);
                };
                let Row::Agent { path, .. } = row else {
                    return None;
                };
                let files = self.files.snapshot(path)?.ok()?;
                let title = UiPath::new("title")
                    .ok()?
                    .resolve(
                        files
                            .files
                            .get("ui/title")
                            .and_then(Option::as_ref)
                            .map(String::as_str),
                        || Ok(path.id().to_string()),
                    )
                    .ok()?;
                title_matches(&first_line(&title), &search.text).then_some(index)
            })
            .collect();
    }

    fn search_next(&mut self) {
        let Some(search) = &mut self.search else {
            return;
        };
        match self.source.view.next_page() {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(advanced) => {
                search.seeking = advanced;
                search.exhausted = !advanced;
                self.selected = 0;
            }
        }
    }

    pub fn has_children(&self) -> bool {
        if self.failure.is_some() {
            return true;
        }
        if let Some(listing) = self
            .source
            .view
            .reader_ref()
            .cache()
            .listing(self.source.view.root(), 0)
            .filter(|listing| listing.fresh)
        {
            return !listing.children.is_empty() || listing.next.is_some();
        }
        self.children
            || self
                .source
                .view
                .rows()
                .iter()
                .any(|(_, row)| matches!(row, Row::Agent { .. }))
    }

    pub fn rows(&self, root: &AgentPath, shown: bool, columns: u16) -> Vec<Line<'static>> {
        if !shown {
            return Vec::new();
        }
        let projection = UiView::new(1).expect("nonzero UI page size");
        let mut rows: Vec<_> = self
            .matches
            .iter()
            .enumerate()
            .map(|(index, source)| {
                let (_, row) = &self.source.view.rows()[*source];
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
                                        .map(String::as_str)
                                })
                                .map_err(|error| error.to_string()),
                            Some(Err(error)) => Err(format!("UI unavailable: {error}")),
                            None => Err("Loading…".into()),
                        };
                        let indent = path
                            .depth()
                            .saturating_sub(root.depth() + 1)
                            .min(usize::from(columns.saturating_sub(2) / 2));
                        let branch = if *expanded { '▾' } else { '▸' };
                        let prefix = format!("{marker}{}{branch}", "  ".repeat(indent));
                        match summary {
                            Ok(row) => Line::raw(format!(
                                "{prefix} {} {} · {} · {} · {}",
                                first_line(&row.glyph),
                                first_line(&row.title),
                                first_line(&row.model),
                                first_line(&row.cost),
                                first_line(&row.age)
                            )),
                            Err(error) => Line::raw(format!("{prefix} {} · {error}", path.id())),
                        }
                    }
                }
            })
            .collect();
        if rows.is_empty() {
            rows.push(Line::raw(if self.search.is_some() {
                "No matching titles"
            } else {
                "Contributors collapsed · → inspect"
            }));
        }
        if let Some(error) = &self.failure {
            rows.push(Line::raw(format!("Tree update: {error}")));
        }
        if let Some(search) = &self.search {
            let state = search.error.as_deref().unwrap_or(if search.seeking {
                "Searching…"
            } else if search.exhausted {
                "End"
            } else {
                "PgDn next"
            });
            rows.push(Line::raw(format!(
                "/{} · {state}",
                first_line(&search.text)
            )));
        }
        rows.push(Line::raw(
            "Enter attach · d diff · m merge · w wait · x discard · ←/→ fold · Esc input",
        ));
        rows
    }

    pub fn event(&mut self, event: &InputEvent, keys: &InputKeymap, area: Rect) -> Option<Intent> {
        let shown = self.credit(area) > 0;
        self.focused &= shown;
        if shown
            && self.focused
            && let InputEvent::Paste(text) | InputEvent::CommittedText(text) = event
            && let Some(search) = self.search.as_mut().filter(|search| search.editing)
        {
            search.text.push_str(&first_line(text));
            search.seeking = true;
            search.exhausted = false;
            search.error = None;
            self.source.view.home();
            self.selected = 0;
            self.filter();
            return Some(Intent::Consumed);
        }
        if let InputEvent::Mouse(mouse) = event {
            if shown && area.contains(Position::new(mouse.column, mouse.row)) {
                match mouse.kind {
                    MouseEventKind::Down(MouseButton::Left) => {
                        self.focused = true;
                        self.selected = usize::from(mouse.row.saturating_sub(area.y + 1))
                            .min(self.matches.len().saturating_sub(1));
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
                action: Action::App(AppAction::SiblingPrevious),
                ..
            }) => return Some(Intent::Sibling(false)),
            Some(DecodedKey::Action {
                action: Action::App(AppAction::SiblingNext),
                ..
            }) => return Some(Intent::Sibling(true)),
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
        if key.code == KeyCode::Esc {
            if self.cancel_navigation() {
                return Some(Intent::Consumed);
            }
        }
        if !shown || !self.focused {
            return None;
        }
        if key.code == KeyCode::Esc {
            if self.search.take().is_some() {
                self.filter();
                return Some(Intent::Consumed);
            }
            self.focused = false;
            return Some(Intent::Input);
        }
        if matches!(
            decoded,
            Some(DecodedKey::Action {
                action: Action::App(AppAction::Find),
                ..
            })
        ) || (self.search.is_none()
            && key.code == KeyCode::Char('/')
            && key.modifiers.is_empty())
        {
            match self.search.as_mut() {
                Some(search) => search.editing = true,
                None => {
                    self.search = Some(Search {
                        text: String::new(),
                        editing: true,
                        seeking: false,
                        exhausted: false,
                        error: None,
                    })
                }
            }
            return Some(Intent::Consumed);
        }
        if matches!(
            decoded,
            Some(DecodedKey::Action {
                action: Action::CopyOrInterrupt | Action::DeleteOrQuit | Action::App(_),
                ..
            })
        ) {
            return None;
        }
        if let Some(search) = self.search.as_mut().filter(|search| search.editing) {
            let changed = match key.code {
                KeyCode::Char(c)
                    if !key.modifiers.intersects(
                        KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                    ) =>
                {
                    search.text.push(c);
                    true
                }
                KeyCode::Backspace => {
                    search.text.pop();
                    true
                }
                _ => false,
            };
            if changed {
                search.seeking = true;
                search.exhausted = false;
                search.error = None;
                self.source.view.home();
                self.selected = 0;
                self.filter();
                return Some(Intent::Consumed);
            }
        }
        let selected = self
            .matches
            .get(self.selected)
            .and_then(|index| self.source.view.rows().get(*index))
            .and_then(|(_, row)| match row {
                Row::Agent { path, .. } => Some(path.clone()),
                Row::Loading { .. } => None,
            });
        if key.modifiers.is_empty() && !self.search.as_ref().is_some_and(|search| search.editing) {
            match key.code {
                KeyCode::Char('d') => return selected.map(Intent::Diff).or(Some(Intent::Consumed)),
                KeyCode::Char('m') => {
                    return selected.map(Intent::Merge).or(Some(Intent::Consumed));
                }
                KeyCode::Char('x') => {
                    return selected.map(Intent::Discard).or(Some(Intent::Consumed));
                }
                KeyCode::Char('w') => return selected.map(Intent::Wait).or(Some(Intent::Consumed)),
                _ => {}
            }
        }
        match decoded {
            Some(DecodedKey::Action {
                action: Action::Submit,
                ..
            }) => {
                if let Some(search) = &mut self.search {
                    search.editing = false;
                }
                return selected.map(Intent::Attach).or(Some(Intent::Consumed));
            }
            Some(DecodedKey::Action {
                action: Action::Move(Motion::Left),
                ..
            }) => {
                self.source
                    .view
                    .collapse(selected.as_ref().unwrap_or(&self.root));
            }
            Some(DecodedKey::Action {
                action: Action::Move(Motion::Right),
                ..
            }) => {
                self.source
                    .view
                    .expand(selected.as_ref().unwrap_or(&self.root));
            }
            _ if key.code == KeyCode::Down => {
                if self.selected + 1 < self.matches.len() {
                    self.selected += 1;
                } else if self.search.is_some() {
                    self.search_next();
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
            _ if key.code == KeyCode::PageDown && self.search.is_some() => self.search_next(),
            _ if key.code == KeyCode::PageDown => self.source.view.page(true),
            _ if key.code == KeyCode::PageUp => self.source.view.page(false),
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
