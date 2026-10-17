use super::*;
use crate::task_volume::Change;

fn cursor(position: u64) -> Cursor {
    Cursor {
        volume: [0; 32],
        epoch: uuid::Uuid::nil(),
        schema: 1,
        query: [0; 32],
        position,
        accepted: position,
        durable: position,
        commit: [0; 32],
    }
}

fn no_rebuild(broken: bool) {
    let mut files = ConfigFiles {
        view: ConfigView::default(),
        cursors: [cursor(0), cursor(0)],
        changes: [
            Box::pin(futures::stream::empty()),
            Box::pin(futures::stream::empty()),
        ],
    };
    let profile = Capabilities::default();
    let mut input = PromptInput::default();
    files.view.apply(&mut input, &profile).unwrap();
    for position in 1..65 {
        let change = Change {
            file: None,
            files: vec![],
            removed: vec![],
            cursor: cursor(position),
        };
        let work = acyclic_test_support::measure(|| {
            files
                .apply_change(Scope::User, Some(Ok(change)), &mut input, &profile)
                .unwrap();
            if broken {
                files.view.apply(&mut input, &profile).unwrap();
            }
        });
        // Every schema/default/keymap parser allocates. Zero allocations is a
        // stricter bound than zero parses and also excludes theme/keymap rebuilding.
        assert_eq!(
            work.count_total, 0,
            "unrelated publications must do zero parse/rebuild work"
        );
        assert_eq!(files.cursors[0].position, position);
    }
}
#[test]
fn unrelated_publications_advance_the_cursor_without_parses() {
    no_rebuild(false);
}
#[test]
#[should_panic(expected = "unrelated publications must do zero parse/rebuild work")]
fn negative_control_detects_rebuilding_on_unrelated_publications() {
    no_rebuild(true);
}
