use super::*;
use crate::config::default_style;
use proptest::prelude::*;
use ratatui::{Terminal, backend::TestBackend};

fn status(parent: Option<&str>, state: AgentState) -> AgentStatus {
    AgentStatus {
        parent: parent.map(str::to_owned),
        state,
        attention: false,
    }
}

fn theme() -> ThemeStyles {
    crate::PromptInput::default().theme
}

fn paint(tree: &AgentTree, selected: Option<&str>, width: u16, height: u16) -> Vec<String> {
    let theme = theme();
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| frame.render_widget(tree.view(selected, &theme), frame.area()))
        .unwrap();
    let buffer = terminal.backend().buffer();
    buffer
        .content
        .chunks(usize::from(width))
        .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
        .collect()
}

#[test]
fn branches_indent_and_mark_state_selection_and_attention() {
    let mut tree = AgentTree::default();
    tree.replace([
        ("r".into(), status(None, AgentState::Idle)),
        ("a".into(), status(Some("r"), AgentState::Idle)),
        ("b".into(), status(Some("a"), AgentState::Busy)),
        ("c".into(), status(Some("a"), AgentState::Stopped)),
        (
            "d".into(),
            AgentStatus {
                attention: true,
                ..status(Some("c"), AgentState::Failed)
            },
        ),
    ]);
    let rows = paint(&tree, Some("c"), 24, 7);
    assert_eq!(rows[0], "┌Agents────────────────┐");
    assert_eq!(rows[1].trim_end(), "│○ r                   │".trim_end());
    assert_eq!(rows[2], "│○ a                   │");
    assert_eq!(rows[3], "│├ ● b                 │");
    assert_eq!(rows[4], "│└ ■ c STOPPED         │");
    assert_eq!(rows[5], "│  ✗ d !               │");
    let theme = theme();
    let mut terminal = Terminal::new(TestBackend::new(24, 7)).unwrap();
    terminal
        .draw(|frame| frame.render_widget(tree.view(Some("c"), &theme), frame.area()))
        .unwrap();
    let buffer = terminal.backend().buffer();
    let selected = default_style(ThemeRole::PopupSelected);
    assert!(
        selected
            .add_modifier
            .contains(ratatui::style::Modifier::REVERSED)
    );
    assert!(
        buffer[(20, 4)]
            .modifier
            .contains(ratatui::style::Modifier::REVERSED),
        "the selected row is highlighted across the panel"
    );
    assert!(
        !buffer[(20, 3)]
            .modifier
            .contains(ratatui::style::Modifier::REVERSED)
    );
    assert_eq!(tree.parent("d"), Some("c"));
    assert_eq!(tree.child("a"), Some("b"));
    assert_eq!(tree.sibling("b", true), Some("c"));
    assert_eq!(tree.sibling("b", false), Some("c"));
    assert_eq!(tree.sibling("r", true), None);
    assert_eq!(tree.parent("r"), None);
    assert_eq!(tree.child("b"), None);
    assert_eq!(
        paint(&AgentTree::default(), None, 14, 3)[1],
        "│No agents   │"
    );
}

fn forest() -> impl Strategy<Value = Vec<(u16, Option<u16>, u8)>> {
    proptest::collection::vec((0u16..48, proptest::option::of(0u16..56), 0u8..4), 0..48)
}

fn build(entries: &[(u16, Option<u16>, u8)]) -> BTreeMap<String, AgentStatus> {
    let state = [
        AgentState::Busy,
        AgentState::Idle,
        AgentState::Stopped,
        AgentState::Failed,
    ];
    entries
        .iter()
        .map(|(id, parent, kind)| {
            (
                format!("{id:02}"),
                AgentStatus {
                    parent: parent.map(|parent| format!("{parent:02}")),
                    state: state[usize::from(*kind)],
                    attention: kind % 2 == 0,
                },
            )
        })
        .collect()
}

fn shape(tree: &AgentTree) -> Vec<(String, Option<u32>, u16, Option<bool>)> {
    tree.rows
        .iter()
        .map(|row| (row.id.clone(), row.parent, row.depth, row.branch))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    // Property: for arbitrary parent maps, including self-parents, unknown
    // parents and cycles, every agent is one row; navigation is closed over
    // the displayed structure; and rows depend only on the set of statuses.
    #[test]
    fn rows_cover_each_agent_once_and_navigation_is_structural(entries in forest(), seed in any::<u64>()) {
        let nodes = build(&entries);
        let mut tree = AgentTree::default();
        tree.replace(nodes.clone());
        prop_assert_eq!(tree.len(), nodes.len());
        for (index, row) in tree.rows.iter().enumerate() {
            prop_assert_eq!(tree.index[&row.id] as usize, index);
            if let Some(parent) = row.parent {
                prop_assert!((parent as usize) < index, "parents precede children");
                prop_assert_eq!(tree.rows[parent as usize].children[row.sibling as usize] as usize, index);
                prop_assert_eq!(nodes[&row.id].parent.as_deref(), Some(tree.rows[parent as usize].id.as_str()));
            } else {
                prop_assert_eq!(tree.roots[row.sibling as usize] as usize, index);
            }
            let id = row.id.as_str();
            if let Some(child) = tree.child(id) {
                prop_assert_eq!(tree.parent(child), Some(id));
            }
            if let Some(next) = tree.sibling(id, true) {
                prop_assert_eq!(tree.sibling(next, false), Some(id));
                prop_assert_eq!(tree.parent(next), tree.parent(id));
                let mut cursor = next;
                let mut steps = 1;
                while cursor != id {
                    cursor = tree.sibling(cursor, true).unwrap();
                    steps += 1;
                }
                let count = row.parent.map_or(tree.roots.len(), |parent| tree.rows[parent as usize].children.len());
                prop_assert_eq!(steps, count);
            }
        }
        let mut shuffled: Vec<_> = nodes.into_iter().collect();
        let mut state = seed | 1;
        for index in (1..shuffled.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            shuffled.swap(index, (state % (index as u64 + 1)) as usize);
        }
        let mut replay = AgentTree::default();
        replay.replace(shuffled);
        prop_assert_eq!(shape(&replay), shape(&tree));
    }
}

/// A deep, branching forest larger than any viewport.
fn large() -> Vec<(String, AgentStatus)> {
    (0..10_000u32)
        .map(|index| {
            let parent = (index > 0)
                .then(|| format!("{:05}", if index % 7 == 0 { index / 2 } else { index - 1 }));
            (
                format!("{index:05}"),
                status(parent.as_deref(), AgentState::Idle),
            )
        })
        .collect()
}

/// Frames and navigation keystrokes paint exactly the visible rows and never
/// rebuild the fold. `rebuild` re-adds the redundant per-frame work.
fn frames_paint_only_visible_rows(rebuild: bool) {
    let nodes = large();
    let mut tree = AgentTree::default();
    tree.replace(nodes.clone());
    let theme = theme();
    let mut terminal = Terminal::new(TestBackend::new(40, 12)).unwrap();
    let mut selected = "00000".to_owned();
    for frame in 0..100 {
        if rebuild {
            tree.replace(nodes.clone());
        }
        let next = if frame % 3 == 0 {
            tree.child(&selected)
        } else {
            tree.sibling(&selected, true)
                .or_else(|| tree.child(&selected))
        };
        if let Some(next) = next {
            selected = next.to_owned();
        }
        terminal
            .draw(|area| area.render_widget(tree.view(Some(&selected), &theme), area.area()))
            .unwrap();
    }
    assert_ne!(selected, "00000", "navigation moved through the forest");
    assert_eq!(tree.work(), (1, 100 * 10), "builds and painted rows");
}

#[test]
fn frames_and_navigation_paint_only_visible_rows() {
    frames_paint_only_visible_rows(false);
}

#[test]
#[should_panic(expected = "builds and painted rows")]
fn rebuilding_on_each_frame_is_detected_negative_control() {
    frames_paint_only_visible_rows(true);
}
