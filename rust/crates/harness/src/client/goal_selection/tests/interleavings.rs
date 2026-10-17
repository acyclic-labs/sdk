use super::*;
use crate::{
    client::{GoalFileView, GoalPanel},
    task_volume::Change,
};
use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

fn publication(source: &Source, query: Query, position: u64) -> Change {
    let snapshot = source.snapshot(query);
    let mut cursor = snapshot.cursor;
    cursor.position = position;
    cursor.accepted = position;
    cursor.durable = position;
    cursor.commit = [position as u8; 32];
    Change {
        cursor,
        file: snapshot.files.into_iter().next(),
        files: vec![],
        removed: vec![],
    }
}

fn copy(change: &Change) -> Change {
    Change {
        cursor: change.cursor.clone(),
        file: change.file.clone(),
        files: change.files.clone(),
        removed: change.removed.clone(),
    }
}

fn rows(view: crate::client::GoalQueryView<'_>) -> Vec<String> {
    let area = Rect::new(0, 0, 100, 5);
    let mut buffer = Buffer::empty(area);
    GoalPanel { view }.render(area, &mut buffer);
    (1..3)
        .map(|y| {
            (1..99)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect()
}

// Check every prefix of every merge preserving each volume's cursor order.
fn check_interleavings(
    views: ClientViews,
    goal: &[Change],
    head: &[Change],
    cap: u32,
) -> (usize, usize) {
    let (identity, _) = views
        .selections()
        .find(|(_, view)| view.query() == &Query::File(FileKey::Goal))
        .unwrap();
    let view = views.goal_view(identity.0, identity.1).unwrap();
    let blocked =
        matches!(view.goal, GoalFileView::Present { goal } if goal.status == GoalStatus::Blocked);
    let rendered = rows(view.clone());
    let retained = views
        .selections()
        .map(|(_, files)| files)
        .find(|view| view.query() == &Query::File(FileKey::RequestHead))
        .unwrap()
        .progress()
        .unwrap();
    let mut witnesses = (0, 0);
    if blocked {
        assert_eq!(
            rendered[1],
            format!("Reason: Maximum goal continuations reached (limit {cap}).")
        );
        // Only the fixture's fourth head publication acknowledges the blocked
        // bytes. This oracle does not call or repeat the production hash gate.
        let linked = view.request_cursor.unwrap().position == 4;
        let expected = if linked {
            assert_eq!(retained.continuations, cap);
            witnesses.1 += 1;
            format!("Status: blocked | Continuations: {cap}")
        } else {
            witnesses.0 += 1;
            "Status: blocked | Continuations: pending".into()
        };
        assert_eq!(rendered[0], expected);
        assert_eq!(
            serde_json::to_value(&view).unwrap()["progress"].is_null(),
            !linked
        );
        // Both broken gates must violate this same per-prefix rendering oracle:
        // no gate exposes a lagging head; always hiding loses a caught-up count.
        let mut broken = view;
        broken.progress = if linked { None } else { Some(retained) };
        assert_ne!(
            rows(broken)[0],
            expected,
            "negative control escaped the rendering oracle"
        );
    } else {
        assert_eq!(
            rendered[0],
            format!("Status: active | Continuations: {}", retained.continuations)
        );
    }
    for (updates, other, is_goal) in [(goal, head, true), (head, goal, false)] {
        if let Some((first, rest)) = updates.split_first() {
            let mut next = views.clone();
            next.query_change(copy(first)).unwrap();
            let next_witnesses = if is_goal {
                check_interleavings(next, rest, other, cap)
            } else {
                check_interleavings(next, other, rest, cap)
            };
            witnesses.0 += next_witnesses.0;
            witnesses.1 += next_witnesses.1;
        }
    }
    if goal.is_empty() && head.is_empty() {
        assert_eq!(
            rendered[0],
            format!("Status: blocked | Continuations: {cap}"),
            "both final cursors must render the exact retained count"
        );
    }
    witnesses
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn every_rendered_cut_gates_blocked_counts_and_restores_them_at_convergence(
        cap in prop_oneof![Just(2u32), 1u32..=u32::MAX], fraction in any::<u32>(),
    ) {
        futures::executor::block_on(async {
            let mut views = new_views();
            let _follow = views.select_goal_view(&Source::goal("objective"), &Source::head(0), 4096).await.unwrap();
            let blocked = format!("---\nstatus: blocked\nreason: Maximum goal continuations reached (limit {cap}).\n---\nobjective");
            let mut unrelated = publication(&Source::goal(&blocked), Query::File(FileKey::Goal), 4);
            unrelated.file = None;
            let goal = [
                publication(&Source::goal("edited objective").published(2), Query::File(FileKey::Goal), 2),
                publication(&Source::goal(&blocked).published(3), Query::File(FileKey::Goal), 3),
                unrelated,
            ];
            let head = [
                publication(&Source::head(fraction % cap).acknowledging(2), Query::File(FileKey::RequestHead), 2),
                publication(&Source::head(cap).acknowledging(2), Query::File(FileKey::RequestHead), 3),
                publication(&Source::head(cap).acknowledging(3), Query::File(FileKey::RequestHead), 4),
            ];
            let (pending, caught_up) = check_interleavings(views, &goal, &head, cap);
            assert!(pending > 0,
                "must exercise a blocked publication while the head follow is stalled below cap");
            assert!(caught_up > 0, "always-pending negative control needs a caught-up witness");
        });
    }
}

fn repeated_interleavings(
    views: ClientViews,
    goal: &[Change],
    head: &[Change],
    old: u32,
    new: u32,
) -> usize {
    use sha2::{Digest as _, Sha256};
    let selected = views
        .selections()
        .map(|(_, files)| files)
        .find(|view| view.query() == &Query::File(FileKey::Goal))
        .unwrap();
    let cursor = selected.cursor();
    let view = views.goal_view(cursor.volume, cursor.query).unwrap();
    let head_position = view.request_cursor.unwrap().position;
    let goal_position = view.cursor.position;
    let count = if goal_position == 1 && head_position == 1 {
        Some(old)
    } else if goal_position == 3 && head_position == 3 {
        Some(new)
    } else if goal_position == 2 {
        Some(if head_position == 1 { old } else { new })
    } else {
        None
    };
    let expected = format!(
        "Status: {} | Continuations: {}",
        if goal_position == 2 {
            "active"
        } else {
            "blocked"
        },
        count.map_or_else(|| "pending".into(), |n| n.to_string())
    );
    assert_eq!(rows(view.clone())[0], expected);
    assert_eq!(view.progress.map(|progress| progress.continuations), count);
    let mut witnesses = 0;
    if goal_position != 2 && head_position != 2 && count.is_none() {
        let repeated = b"---\nstatus: blocked\nreason: waiting\n---\nobjective";
        // Broken hash matching accepts either blocked publication as the same
        // acknowledgement. These bytes and hashes are identical; their IDs differ.
        let observed = selected.files().get(&FileKey::Goal).unwrap();
        assert_eq!(
            Sha256::digest(&observed.segments[0]),
            Sha256::digest(repeated)
        );
        let mut hash_only = view;
        hash_only.progress = views
            .selections()
            .map(|(_, files)| files)
            .find(|view| view.query() == &Query::File(FileKey::RequestHead))
            .unwrap()
            .progress();
        assert_ne!(
            rows(hash_only)[0],
            expected,
            "hash-only negative control escaped the oracle"
        );
        witnesses += 1;
    }
    for (updates, other, is_goal) in [(goal, head, true), (head, goal, false)] {
        if let Some((first, rest)) = updates.split_first() {
            let mut next = views.clone();
            next.query_change(copy(first)).unwrap();
            witnesses += if is_goal {
                repeated_interleavings(next, rest, other, old, new)
            } else {
                repeated_interleavings(next, other, rest, old, new)
            };
        }
    }
    if goal.is_empty() && head.is_empty() {
        assert_eq!(
            count,
            Some(new),
            "the repeated goal must converge to the new exact count"
        );
    }
    witnesses
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn identical_blocked_text_requires_its_own_publication(old in prop_oneof![Just(0u32), 0u32..u32::MAX]) {
        futures::executor::block_on(async {
            let blocked = "---\nstatus: blocked\nreason: waiting\n---\nobjective";
            let mut views = new_views();
            let _follow = views.select_goal_view(&Source::goal(blocked).published(10), &Source::head(old).acknowledging(10), 4096).await.unwrap();
            let goal = [
                publication(&Source::goal("objective").published(11), Query::File(FileKey::Goal), 2),
                publication(&Source::goal(blocked).published(12), Query::File(FileKey::Goal), 3),
            ];
            let head = [
                publication(&Source::head(old + 1).acknowledging(11), Query::File(FileKey::RequestHead), 2),
                publication(&Source::head(old + 1).acknowledging(12), Query::File(FileKey::RequestHead), 3),
            ];
            assert!(repeated_interleavings(views, &goal, &head, old, old + 1) > 0);
        });
    }
}
