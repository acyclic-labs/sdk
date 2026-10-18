//! An agent forest folded from followed status files: preorder rows, structural
//! navigation and a widget whose per-frame work is bounded by the visible rows.
use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, Widget},
};
use std::collections::{BTreeMap, HashMap};

/// Lifecycle shown for one agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentState {
    /// Admitted and not yet settled.
    Busy,
    /// Settled successfully; waiting for the next prompt.
    Idle,
    /// Cancelled by the user.
    Stopped,
    /// Settled with a failure or an indeterminate outcome.
    Failed,
}

impl AgentState {
    fn glyph(self) -> &'static str {
        match self {
            Self::Busy => "● ",
            Self::Idle => "○ ",
            Self::Stopped => "■ ",
            Self::Failed => "✗ ",
        }
    }
}

/// One agent's followed status; the file remains authoritative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStatus {
    /// Parent agent, if any. Unknown or cyclic parents make this a root row.
    pub parent: Option<String>,
    /// Latest lifecycle.
    pub state: AgentState,
    /// The agent settled and waits for the user's attention.
    pub attention: bool,
}

#[derive(Debug)]
struct Row {
    id: String,
    parent: Option<u32>,
    children: Vec<u32>,
    /// Position among the parent's children (or among the roots).
    sibling: u32,
    /// Indentation, which only grows where a parent branches.
    depth: u16,
    /// Branch glyph for the first row of a branch; `Some(true)` is the last one.
    branch: Option<bool>,
    state: AgentState,
    attention: bool,
}

/// Disposable fold. Rebuilt only when the followed status files change; frames
/// and navigation never rebuild it.
#[derive(Debug, Default)]
pub struct AgentTree {
    rows: Vec<Row>,
    roots: Vec<u32>,
    index: HashMap<String, u32>,
    #[cfg(test)]
    builds: usize,
    #[cfg(test)]
    painted: std::cell::Cell<usize>,
}

impl AgentTree {
    /// Replace every agent at one file cut. Row order depends only on the set
    /// of statuses: siblings are ordered by identity, depth-first.
    pub fn replace(&mut self, agents: impl IntoIterator<Item = (String, AgentStatus)>) {
        let nodes: BTreeMap<String, AgentStatus> = agents.into_iter().collect();
        let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut roots = Vec::new();
        for (id, status) in &nodes {
            match status
                .parent
                .as_deref()
                .filter(|parent| *parent != id && nodes.contains_key(*parent))
            {
                Some(parent) => children.entry(parent).or_default().push(id),
                None => roots.push(id.as_str()),
            }
        }
        // Roots are consumed from the back; keep identity order.
        roots.reverse();
        self.rows.clear();
        self.roots.clear();
        self.index.clear();
        // Members of a parent cycle are unreachable from any root; the least
        // unvisited identity of each cycle starts its own tree.
        let mut pending = nodes.keys();
        let mut stack: Vec<(&str, Option<u32>, u16, Option<bool>)> = Vec::new();
        loop {
            let root = match roots.pop() {
                Some(root) => root,
                None => match pending.by_ref().find(|id| !self.index.contains_key(*id)) {
                    Some(id) => id.as_str(),
                    None => break,
                },
            };
            stack.push((root, None, 0, None));
            while let Some((id, parent, depth, branch)) = stack.pop() {
                if self.index.contains_key(id) {
                    continue;
                }
                let row = self.rows.len() as u32;
                self.index.insert(id.to_owned(), row);
                let siblings = match parent {
                    Some(parent) => &mut self.rows[parent as usize].children,
                    None => &mut self.roots,
                };
                let sibling = siblings.len() as u32;
                siblings.push(row);
                let status = &nodes[id];
                self.rows.push(Row {
                    id: id.to_owned(),
                    parent,
                    children: Vec::new(),
                    sibling,
                    depth,
                    branch,
                    state: status.state,
                    attention: status.attention,
                });
                let next = children
                    .get(id)
                    .map(|list| {
                        list.iter()
                            .copied()
                            .filter(|child| !self.index.contains_key(*child))
                            .collect::<Vec<&str>>()
                    })
                    .unwrap_or_default();
                let branches = next.len() > 1;
                for (position, child) in next.iter().enumerate().rev() {
                    stack.push((
                        *child,
                        Some(row),
                        depth.saturating_add(u16::from(branches)),
                        branches.then_some(position + 1 == next.len()),
                    ));
                }
            }
        }
        #[cfg(test)]
        {
            self.builds += 1;
        }
    }

    /// Number of agents.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether no agent is known.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn row(&self, id: &str) -> Option<&Row> {
        self.index.get(id).map(|row| &self.rows[*row as usize])
    }

    /// The displayed parent.
    pub fn parent(&self, id: &str) -> Option<&str> {
        let parent = self.row(id)?.parent?;
        Some(&self.rows[parent as usize].id)
    }

    /// The first displayed child.
    pub fn child(&self, id: &str) -> Option<&str> {
        let child = *self.row(id)?.children.first()?;
        Some(&self.rows[child as usize].id)
    }

    /// The next or previous sibling, wrapping; `None` for an only child.
    pub fn sibling(&self, id: &str, forward: bool) -> Option<&str> {
        let row = self.row(id)?;
        let siblings = match row.parent {
            Some(parent) => &self.rows[parent as usize].children,
            None => &self.roots,
        };
        let count = siblings.len() as u32;
        if count < 2 {
            return None;
        }
        let next = if forward {
            (row.sibling + 1) % count
        } else {
            (row.sibling + count - 1) % count
        };
        Some(&self.rows[siblings[next as usize] as usize].id)
    }

    /// A widget over this fold, highlighting the selected agent.
    pub fn view<'a>(
        &'a self,
        selected: Option<&'a str>,
        theme: &'a ThemeStyles,
    ) -> AgentTreeView<'a> {
        AgentTreeView {
            tree: self,
            selected,
            theme,
        }
    }

    #[cfg(test)]
    pub(crate) fn work(&self) -> (usize, usize) {
        (self.builds, self.painted.get())
    }
}

/// First row of a window of `height` rows that keeps `selected` visible.
fn window(selected: Option<usize>, len: usize, height: usize) -> usize {
    if len <= height {
        return 0;
    }
    selected
        .unwrap_or(0)
        .saturating_sub(height / 2)
        .min(len - height)
}

/// Bordered agent list. Painting visits only the rows inside the area.
pub struct AgentTreeView<'a> {
    tree: &'a AgentTree,
    selected: Option<&'a str>,
    theme: &'a ThemeStyles,
}

impl Widget for AgentTreeView<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        Clear.render(area, buffer);
        let block = Block::default()
            .title("Agents")
            .borders(Borders::ALL)
            .border_style(self.theme.style(ThemeRole::Border))
            .style(self.theme.style(ThemeRole::Popup));
        let inner = block.inner(area);
        block.render(area, buffer);
        if inner.is_empty() {
            return;
        }
        let rows = &self.tree.rows;
        if rows.is_empty() {
            buffer.set_stringn(
                inner.x,
                inner.y,
                "No agents",
                usize::from(inner.width),
                self.theme.style(ThemeRole::Placeholder),
            );
            return;
        }
        let selected = self
            .selected
            .and_then(|id| self.tree.index.get(id))
            .map(|row| *row as usize);
        let height = usize::from(inner.height);
        let start = window(selected, rows.len(), height);
        let right = inner.right();
        for (offset, row) in rows[start..rows.len().min(start + height)]
            .iter()
            .enumerate()
        {
            let y = inner.y + offset as u16;
            let style = if selected == Some(start + offset) {
                self.theme.style(ThemeRole::PopupSelected)
            } else {
                match row.state {
                    AgentState::Busy => self.theme.style(ThemeRole::Text),
                    AgentState::Idle => self.theme.style(ThemeRole::Placeholder),
                    AgentState::Stopped | AgentState::Failed => {
                        self.theme.style(ThemeRole::Diagnostic)
                    }
                }
            };
            buffer.set_style(Rect::new(inner.x, y, inner.width, 1), style);
            let mut x = inner.x;
            let mut put = |text: &str, style: Style| {
                if x < right {
                    x = buffer
                        .set_stringn(x, y, text, usize::from(right - x), style)
                        .0;
                }
            };
            // The identity's width is reserved first; indentation and the
            // branch marker take only the remaining columns, so depth elides
            // the indent and never the state or task id.
            let (indent, branch) = indentation(row, usize::from(inner.width));
            for _ in 0..indent {
                put("  ", style);
            }
            match branch {
                Some(true) => put("└ ", style),
                Some(false) => put("├ ", style),
                None => {}
            }
            put(row.state.glyph(), style);
            put(
                label(&row.id),
                style.add_modifier(ratatui::style::Modifier::BOLD),
            );
            if row.state == AgentState::Stopped {
                put(" STOPPED", style);
            }
            if row.attention {
                put(" !", self.theme.style(ThemeRole::Heading));
            }
            #[cfg(test)]
            self.tree.painted.set(self.tree.painted.get() + 1);
        }
    }
}

/// Indent levels and branch marker that fit beside the row's identity in
/// `width` columns. Each level and the marker are two columns wide.
fn indentation(row: &Row, width: usize) -> (usize, Option<bool>) {
    use unicode_width::UnicodeWidthStr as _;
    let identity = row.state.glyph().width() + label(&row.id).width();
    let mut room = width.saturating_sub(identity) / 2;
    let branch = row.branch.filter(|_| room > 0);
    room -= usize::from(branch.is_some());
    let levels = usize::from(row.depth.saturating_sub(u16::from(row.branch.is_some())));
    (levels.min(room), branch)
}

/// Short, display-safe identity: at most eight characters.
fn label(id: &str) -> &str {
    let end = id.char_indices().nth(8).map_or(id.len(), |(end, _)| end);
    let short = &id[..end];
    if short.chars().any(char::is_control) {
        "?"
    } else {
        short
    }
}

#[cfg(test)]
mod tests;
