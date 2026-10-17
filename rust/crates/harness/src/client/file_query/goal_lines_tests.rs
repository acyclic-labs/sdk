use super::*;
use crate::{client::GoalQueryView, task_volume::Generation};
use proptest::prelude::*;

fn cursor(position: u64) -> Cursor {
    Cursor {
        volume: [1; 32],
        epoch: uuid::Uuid::nil(),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(&Query::File(FileKey::Goal)).unwrap()).as_bytes(),
        position,
        accepted: position,
        durable: position,
        commit: [3; 32],
    }
}

fn snapshot(cursor: Cursor, text: Option<&str>) -> Snapshot {
    Snapshot {
        generation: Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit: cursor.commit,
        },
        cursor,
        files: text
            .map(|text| FileValue::new(FileKey::Goal, text.as_bytes().to_vec()))
            .into_iter()
            .collect(),
    }
}

fn assert_lines(view: &FileQueryView) {
    assert_eq!(
        view.goal_revision(),
        view.files()
            .get(&FileKey::Goal)
            .and_then(|file| file.publication)
    );
    let expected: Vec<_> = match view.goal() {
        GoalFileView::Present { goal } => goal.markdown.lines().collect(),
        _ => vec![],
    };
    let rendered = GoalQueryView {
        cursor: view.cursor(),
        request_cursor: None,
        goal: view.goal(),
        progress: None,
        line_starts: view.goal_line_starts(),
    };
    assert_eq!(rendered.lines().collect::<Vec<_>>(), expected);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cached_boundaries_match_standard_lines_after_edits_replays_and_resets(
        edits in prop::collection::vec(prop::option::of(prop::collection::vec(prop_oneof![Just('\n'), Just('\r'), any::<char>()], 0..256)), 0..24),
    ) {
        let query = Query::File(FileKey::Goal);
        let mut view = FileQueryView::from_snapshot(query.clone(), snapshot(cursor(1), None), 1, 4096).unwrap();
        let mut position = 1;
        let mut present = false;
        assert_lines(&view);
        for edit in edits {
            let text = edit.map(|chars| chars.into_iter().collect::<String>());
            let removed = if present && text.is_none() { vec![FileKey::Goal] } else { vec![] };
            present = text.is_some();
            position += 1;
            let change = || Change {
                cursor: cursor(position),
                file: text.as_ref().map(|text| FileValue::new(FileKey::Goal, text.as_bytes().to_vec())),
                files: vec![], removed: removed.clone(),
            };
            view.apply(change()).unwrap();
            assert_lines(&view);
            let cache = view.goal_line_starts().clone();
            prop_assert!(!view.apply(change()).unwrap());
            prop_assert!(Arc::ptr_eq(&cache, view.goal_line_starts()));
            position += 1;
            view.apply(Change { cursor: cursor(position), file: None, files: vec![], removed: vec![] }).unwrap();
            prop_assert!(Arc::ptr_eq(&cache, view.goal_line_starts()));
            let reset = FileQueryView::from_snapshot(query.clone(), snapshot(cursor(position), text.as_deref()), 1, 4096).unwrap();
            prop_assert_eq!(&view, &reset);
            assert_lines(&reset);
        }
    }
}
