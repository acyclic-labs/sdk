use super::{Component, FileKey, Query};
use std::collections::BTreeMap;

#[test]
fn folder_ranges_do_not_visit_unrelated_task_history() -> Result<(), Box<dyn std::error::Error>> {
    let mut index = BTreeMap::new();
    for turn in 0..10_000 {
        index.insert(
            FileKey::parse(&format!(".tasks/t/turns/{turn}/chunks/0"))?,
            (),
        );
    }
    for path in [
        ".tasks/t/status.json",
        "plans/main.md",
        "plans/release notes.md",
        "plans/調査.md",
        "plans/\u{10ffff}tail.md",
        ".tasks/t/requests/r.json",
        ".tasks/t/requests/r/started.json",
        ".tasks/t/requests/r/result.json",
        ".tasks/t/context/c/manifest.json",
        ".tasks/t/approvals/a/request.json",
        ".tasks/t/approvals/a/decisions/p.json",
        ".tasks/t/approvals/z/request.json",
        ".tasks/t/approvals/z/decisions/p.json",
        ".tasks/t/notifications/a.json",
        ".tasks/t/notifications/z.json",
        ".tasks/other/approvals/a/request.json",
        ".tasks/other/notifications/a.json",
    ] {
        index.insert(FileKey::parse(path)?, ());
    }
    for (query, expected) in [
        (Query::Requests(Component::new("t")?), 1),
        (Query::Approvals(Component::new("t")?), 4),
        (Query::Notifications(Component::new("t")?), 2),
        (Query::Approvals(Component::new("empty")?), 0),
        (Query::Notifications(Component::new("empty")?), 0),
        (Query::Agents, 0),
        (Query::Plans, 4),
    ] {
        // Count candidates before the query predicate: filtering a task-wide
        // range would still return correct files while scanning all its chunks.
        let candidates: Vec<_> = query
            .ranges()
            .into_iter()
            .flat_map(|range| index.range(range).map(|(key, ())| key))
            .collect();
        assert_eq!(candidates.len(), expected, "{query:?}");
        assert!(candidates.iter().all(|key| query.matches(key)));
        let matching: Vec<_> = index.keys().filter(|key| query.matches(key)).collect();
        assert_eq!(candidates, matching);
    }
    for path in [".volumes/agents/a", ".volumes/agents/z"] {
        index.insert(FileKey::parse(path)?, ());
    }
    let agents: Vec<_> = Query::Agents
        .ranges()
        .into_iter()
        .flat_map(|range| index.range(range).map(|(key, ())| key))
        .collect();
    assert_eq!(agents.len(), 2);
    assert!(agents.iter().all(|key| Query::Agents.matches(key)));
    Ok(())
}

fn task_session_selection<const FILTER: bool>(selected: &str, session: &str, turn: u64) {
    use crate::task_volume::{TaskFile, ToolFile};
    let task = Component::new(selected).unwrap();
    let session = Component::new(session).unwrap();
    let query = Query::TaskSession {
        task: task.clone(),
        session: session.clone(),
    };
    let restored: Query = serde_json::from_slice(&serde_json::to_vec(&query).unwrap()).unwrap();
    assert_eq!(query, restored);
    let mut index = BTreeMap::new();
    for owner in [
        task.clone(),
        Component::new(format!("{selected}-foreign")).unwrap(),
    ] {
        for file in [
            TaskFile::Request(Component::new("r").unwrap()),
            TaskFile::Chunk {
                turn,
                sequence: u64::MAX,
            },
            TaskFile::Tool {
                turn,
                call: Component::new("call").unwrap(),
                file: ToolFile::Result,
            },
            TaskFile::Context(Component::new("context").unwrap()),
        ] {
            index.insert(
                FileKey::Task {
                    task: owner.clone(),
                    file,
                },
                (),
            );
        }
    }
    for owner in [
        session.clone(),
        Component::new(format!("{}-foreign", session.as_str())).unwrap(),
    ] {
        for file in [
            TaskFile::Request(Component::new("r").unwrap()),
            TaskFile::ExecutionResult(Component::new("r").unwrap()),
        ] {
            index.insert(
                FileKey::Session {
                    session: owner.clone(),
                    file,
                },
                (),
            );
        }
    }
    let ranges = query.ranges();
    assert_eq!(ranges.len(), 2);
    let candidates: Vec<_> = ranges
        .into_iter()
        .flat_map(|range| index.range(range).map(|(key, ())| key))
        .collect();
    let namespace: Vec<_> = index
        .keys()
        .filter(|key| {
            matches!(key,FileKey::Task {task:owner,..} if owner==&task)
                || matches!(key,FileKey::Session {session:owner,..} if owner==&session)
        })
        .collect();
    assert_eq!(candidates, namespace);
    assert_eq!(candidates.len(), 6);
    let expected: Vec<_> = index.keys().filter(|key| matches!(key,FileKey::Task {task:owner,file:TaskFile::Chunk {..}|TaskFile::Tool {..}} if owner==&task)
        || matches!(key,FileKey::Session {session:owner,..} if owner==&session)).collect();
    let display: Vec<_> = candidates
        .into_iter()
        .filter(|key| !FILTER || restored.matches(key))
        .collect();
    assert_eq!(
        display, expected,
        "display selection must exclude non-display task files"
    );
    assert_eq!(display.len(), 4);
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Broad ranges cover only the selected namespaces; the display predicate
    // excludes archived task requests/context while retaining session records.
    #[test]
    fn task_session_ranges_cover_namespaces_and_filter_display_files(
        selected in "[a-z]{1,16}", session in "[a-z]{1,16}", turn in proptest::prelude::any::<u64>(),
    ) { task_session_selection::<true>(&selected,&session,turn); }
}
#[test]
#[should_panic(expected = "display selection must exclude non-display task files")]
fn treating_range_candidates_as_display_is_detected_negative_control() {
    task_session_selection::<false>("task", "session", 0);
}
