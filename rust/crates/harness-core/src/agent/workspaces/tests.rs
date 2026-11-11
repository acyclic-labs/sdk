use super::*;
use crate::agent::AgentId;
use proptest::prelude::*;

fn entry(id: u8, name: String, root: String) -> WorkspaceEntry {
    WorkspaceEntry {
        name,
        kind: WorkspaceKind::Tree,
        source: WorkspaceSource::Subtree(AgentPath::root(&AgentId::from_bytes([id; 16]))),
        root,
        mode: WorkspaceMode::ReadOnly,
        inherit: WorkspaceInherit::Own,
    }
}

proptest! {
    /// A reference list, independently edited by position, is the oracle for
    /// arbitrary successful and refused identity-addressed deltas. Reopening
    /// after every delta proves the folded checkpoint needs no history replay.
    #[test]
    fn workspace_fold_survives_checkpoint_cuts(
        operations in prop::collection::vec((0_u8..4, any::<u8>(), 0_u8..16), 0..200)
    ) {
        let mut table = WorkspaceTable::new(Vec::new()).unwrap();
        let mut reference = Vec::<WorkspaceEntry>::new();
        for (op, id, label) in operations {
            let source = entry(id, format!("w{label}"), String::new());
            let position = reference.iter().position(|prior| prior.source == source.source);
            let mut next = reference.clone();
            let (op, changed, mut accepts) = match (op, position) {
                (0, position) => {
                    next.push(source.clone());
                    (WorkspaceOp::Add, source, position.is_none())
                }
                (1, Some(index)) => {
                    let removed = next.remove(index);
                    (WorkspaceOp::Remove, removed, true)
                }
                (2, Some(index)) => {
                    next[index].name = source.name;
                    (WorkspaceOp::Rename, next[index].clone(), true)
                }
                (3, Some(index)) => {
                    let previous = next[index].source.clone();
                    next[index].source = entry(id.wrapping_add(1), String::new(), String::new()).source;
                    next[index].root = [".git", ".system", ".volumes", ".acyclic-state"][usize::from(label % 4)].into();
                    (WorkspaceOp::Reroot { previous }, next[index].clone(), true)
                }
                (_, None) => (WorkspaceOp::Remove, source, false),
                _ => unreachable!(),
            };
            for (index, candidate) in next.iter().enumerate() {
                accepts &= next[..index].iter().all(|prior| prior.name != candidate.name && prior.source != candidate.source);
            }
            let change = WorkspaceChanged { version: 1, op, entry: changed };
            let bytes = serde_json::to_vec(&change).unwrap();
            let restored: WorkspaceChanged = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(table.apply(&restored).is_ok(), accepts);
            if accepts { reference = next; }
            prop_assert_eq!(table.entries(), reference.as_slice());
            let bytes = serde_json::to_vec(&table).unwrap();
            table = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(table.entries(), reference.as_slice());
        }
    }

    #[test]
    fn invalid_workspace_deltas_are_atomic(id in any::<u8>(), root in prop::sample::select(vec!["..", "a/../b", "/a", "a\\b", "a//b"])) {
        let original = entry(id, "tree".into(), String::new());
        let mut table = WorkspaceTable::new(vec![original.clone()]).unwrap();
        let mut invalid = original;
        invalid.root = root.into();
        let change = WorkspaceChanged { version: 1, op: WorkspaceOp::Reroot { previous: invalid.source.clone() }, entry: invalid };
        let before = table.clone();
        prop_assert!(table.apply(&change).is_err());
        prop_assert_eq!(&table, &before);
        let mut wrong = serde_json::to_value(&before).unwrap();
        wrong["version"] = 0.into();
        prop_assert!(serde_json::from_value::<WorkspaceTable>(wrong).is_err());
    }
}

#[test]
fn lazy_subtree_binding_is_independent_of_peer_count_and_name() {
    let table = WorkspaceTable::new(
        (0..=255)
            .map(|id| entry(id, format!("w{id}"), ".git".into()))
            .collect(),
    )
    .unwrap();
    assert_eq!(table.entries().len(), 256);
    let mut renamed = entry(0, "agents".into(), ".git".into());
    let mut table = table;
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Rename,
            entry: renamed.clone(),
        })
        .unwrap();
    for (kind, mode, inherit) in [
        (
            WorkspaceKind::Volume,
            WorkspaceMode::ReadOnly,
            WorkspaceInherit::Own,
        ),
        (
            WorkspaceKind::Tree,
            WorkspaceMode::ReadWrite,
            WorkspaceInherit::Own,
        ),
        (
            WorkspaceKind::Tree,
            WorkspaceMode::ReadOnly,
            WorkspaceInherit::Fork,
        ),
    ] {
        renamed.kind = kind;
        renamed.mode = mode;
        renamed.inherit = inherit;
        assert!(renamed.check().is_err());
    }
}
