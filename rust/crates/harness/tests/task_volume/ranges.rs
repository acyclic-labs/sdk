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
        (Query::Requests(Component::new("t")?), 3),
        (Query::Approvals(Component::new("t")?), 4),
        (Query::Notifications(Component::new("t")?), 2),
        (Query::Approvals(Component::new("empty")?), 0),
        (Query::Notifications(Component::new("empty")?), 0),
        (Query::Agents, 0),
        (Query::Plans, 1),
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
