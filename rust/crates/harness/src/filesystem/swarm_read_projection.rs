//! Read-only durable swarm projections shared by local host adapters.
//!
//! These helpers derive bounded pages and recursive agent topology from the
//! caller's refreshed registry snapshot. They retain no state and never open a
//! model worker or mutate a journal.

use super::{LocalSwarmApproval, LocalSwarmSession};
use crate::{Error, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_PAGE_ENTRIES: usize = 1_024;

/// One bounded read page from the durable local swarm index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmPage<T> {
    /// Entries in stable cursor order.
    pub items: Vec<T>,
    /// Cursor for the next page, when more entries remain.
    pub next: Option<String>,
}

/// One registry-backed agent descriptor with its direct child identities.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSwarmAgent {
    /// Durable descriptor for this agent task.
    pub session: LocalSwarmSession,
    /// Direct children in stable identity order.
    pub children: Vec<TaskId>,
}

pub(crate) fn page_by_cursor<T, F>(
    mut items: Vec<T>,
    after: Option<&str>,
    maximum_entries: usize,
    key: F,
    label: &str,
) -> Result<LocalSwarmPage<T>>
where
    F: Fn(&T) -> String,
{
    items.sort_by_key(|item| key(item));
    page_from_sorted(items, after, maximum_entries, key, label)
}

/// Projects an already canonical ordered iterator without materializing entries
/// that precede the cursor or follow the requested page.
pub(crate) fn page_from_sorted<T, I, F>(
    items: I,
    after: Option<&str>,
    maximum_entries: usize,
    key: F,
    label: &str,
) -> Result<LocalSwarmPage<T>>
where
    I: IntoIterator<Item = T>,
    F: Fn(&T) -> String,
{
    if maximum_entries == 0 || maximum_entries > MAX_PAGE_ENTRIES {
        return Err(Error::Invalid(format!(
            "{label} page limit must be between 1 and {MAX_PAGE_ENTRIES}"
        )));
    }
    if after.is_some_and(str::is_empty) {
        return Err(Error::Invalid(format!(
            "{label} page cursor must be nonempty"
        )));
    }
    let mut items = items.into_iter();
    if let Some(cursor) = after {
        let mut found = false;
        while let Some(item) = items.next() {
            if key(&item) == cursor {
                found = true;
                break;
            }
        }
        if !found {
            return Err(Error::Invalid(format!("{label} page cursor is unknown")));
        }
    }
    let mut page = Vec::with_capacity(maximum_entries);
    let mut next = None;
    for item in items {
        if page.len() == maximum_entries {
            next = page.last().map(|item| key(item));
            break;
        }
        page.push(item);
    }
    Ok(LocalSwarmPage { items: page, next })
}

pub(crate) fn recursive_agent_tree(
    sessions: Vec<LocalSwarmSession>,
    root: TaskId,
) -> Result<Vec<LocalSwarmAgent>> {
    let descriptors = sessions
        .into_iter()
        .map(|session| (session.task, session))
        .collect::<BTreeMap<_, _>>();
    if !descriptors.contains_key(&root) {
        return Err(Error::NotFound(format!("local swarm task {root}")));
    }
    let mut children = BTreeMap::<TaskId, Vec<TaskId>>::new();
    for session in descriptors.values() {
        if let Some(parent) = session.parent {
            children.entry(parent).or_default().push(session.task);
        }
    }
    for task_children in children.values_mut() {
        task_children.sort_by_key(|task| task.to_string());
    }

    // Enter/exit markers keep the stable preorder while making traversal depth
    // independent of the host call stack.
    let mut stack = vec![(root, false)];
    let mut visiting = BTreeSet::new();
    let mut output = Vec::new();
    while let Some((task, exiting)) = stack.pop() {
        if exiting {
            visiting.remove(&task);
            continue;
        }
        if !visiting.insert(task) {
            return Err(Error::Storage(
                "local swarm registry contains a cycle".into(),
            ));
        }
        let session = descriptors
            .get(&task)
            .cloned()
            .ok_or_else(|| Error::Storage(format!("local swarm child {task} is missing")))?;
        let direct_children = children.get(&task).cloned().unwrap_or_default();
        output.push(LocalSwarmAgent {
            session,
            children: direct_children.clone(),
        });
        stack.push((task, true));
        for child in direct_children.iter().rev() {
            stack.push((*child, false));
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::LocalSessionPhase;

    fn session(task: TaskId, parent: Option<TaskId>, depth: usize) -> LocalSwarmSession {
        LocalSwarmSession {
            task,
            parent,
            depth,
            task_description: task.to_string(),
            operation: None,
            phase: LocalSessionPhase::Ready,
        }
    }

    #[test]
    fn page_projection_is_bounded_and_cursored() -> Result<()> {
        let first = TaskId::from_bytes([1; 16]);
        let second = TaskId::from_bytes([2; 16]);
        let third = TaskId::from_bytes([3; 16]);
        let page = page_by_cursor(
            vec![third, first, second],
            None,
            2,
            |task| task.to_string(),
            "session",
        )?;
        assert_eq!(page.items, vec![first, second]);
        assert_eq!(page.next, Some(second.to_string()));
        let page = page_by_cursor(
            vec![third, first, second],
            page.next.as_deref(),
            2,
            |task| task.to_string(),
            "session",
        )?;
        assert_eq!(page.items, vec![third]);
        assert!(page.next.is_none());
        assert!(
            page_by_cursor(
                vec![first],
                Some("missing"),
                1,
                |task| task.to_string(),
                "session"
            )
            .is_err()
        );
        assert!(page_by_cursor(vec![first], None, 0, |task| task.to_string(), "session").is_err());
        assert!(
            page_by_cursor(vec![first], None, 1_025, |task| task.to_string(), "session").is_err()
        );
        Ok(())
    }

    #[test]
    fn recursive_projection_is_rooted_and_preserves_grandchildren() -> Result<()> {
        let root = TaskId::from_bytes([1; 16]);
        let child = TaskId::from_bytes([2; 16]);
        let grandchild = TaskId::from_bytes([3; 16]);
        let sibling = TaskId::from_bytes([4; 16]);
        let tree = recursive_agent_tree(
            vec![
                session(sibling, None, 0),
                session(grandchild, Some(child), 2),
                session(root, None, 0),
                session(child, Some(root), 1),
            ],
            root,
        )?;
        assert_eq!(tree.len(), 3);
        assert_eq!(tree[0].session.task, root);
        assert_eq!(tree[0].children, vec![child]);
        assert_eq!(tree[1].session.task, child);
        assert_eq!(tree[1].children, vec![grandchild]);
        assert_eq!(tree[2].session.task, grandchild);
        assert!(recursive_agent_tree(vec![session(root, None, 0)], child).is_err());

        let cycle = recursive_agent_tree(
            vec![session(root, Some(child), 0), session(child, Some(root), 1)],
            root,
        );
        assert!(cycle.is_err());
        Ok(())
    }
}
