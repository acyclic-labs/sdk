use super::*;
use crate::agent::AgentId;
use proptest::prelude::*;

struct NoTurn;
impl crate::agent::TurnDriver for NoTurn {
    fn step<'a>(
        &'a self,
        _: &'a crate::agent::AgentState,
        _: u64,
        _: Option<crate::agent::Queued>,
        _: acyclic_actors::guest::CancellationToken,
    ) -> crate::agent::TurnFuture<'a> {
        Box::pin(async { Err(Error::Record("workspace changes must not run the model")) })
    }
}

fn entry(id: u8, name: String, root: String) -> WorkspaceEntry {
    WorkspaceEntry {
        id: EntryId::from_bytes([id; 16]),
        role: WorkspaceRole::User,
        name,
        kind: WorkspaceKind::Volume,
        source: WorkspaceSource::Volume(
            crate::conversation::VolumeRef::new(
                crate::resources::ProviderRef::new(
                    "fixture".into(),
                    "filesystem".into(),
                    "1".into(),
                    &mut None,
                )
                .unwrap(),
                format!("volume-{id}"),
                crate::conversation::VolumeClass::Project,
                crate::conversation::VolumeOwner::Project("fixture".into()),
                &mut None,
            )
            .unwrap(),
        ),
        root,
        mode: WorkspaceMode::ReadWrite,
        inherit: WorkspaceInherit::Fork,
        filter: WorkspaceFilter::None,
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
        let path = AgentPath::root(&AgentId::from_bytes([17; 16]));
        let adopted = crate::agent::ForkAdopted { agent: path.id(), parent: None, profile: None, forked_at: 0, conversation_tail: 0, checkpoint: None, volumes: Vec::new(), brief: None, policy: None };
        let mut state = crate::agent::AgentState::adopted(path.clone(), &adopted).unwrap();
        for (sequence, (op, id, label)) in operations.into_iter().enumerate() {
            let source = entry(id, format!("w{label}"), String::new());
            let position = reference.iter().position(|prior| prior.id == source.id);
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
                    next[index].source = entry(id.wrapping_add(1), String::new(), String::new()).source;
                    next[index].root = [".git", ".system", ".volumes", ".acyclic-state"][usize::from(label % 4)].into();
                    (WorkspaceOp::Reroot, next[index].clone(), true)
                }
                (_, None) => (WorkspaceOp::Remove, source, false),
                _ => unreachable!(),
            };
            for (index, candidate) in next.iter().enumerate() {
                accepts &= next[..index].iter().all(|prior| prior.name != candidate.name && prior.id != candidate.id);
            }
            let change = WorkspaceChanged { version: 1, op, entry: changed };
            let bytes = serde_json::to_vec(&change).unwrap();
            let restored: WorkspaceChanged = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(table.apply(&restored).is_ok(), accepts);
            if accepts {
                reference = next;
                let step = futures::executor::block_on(crate::agent::step(
                    &NoTurn, &path, Some(state), (sequence as u64 + 2, 0),
                    crate::agent::Wake::Inbox { sequence: sequence as u64 + 1, record: crate::agent::InboxRecord::WorkspaceChanged(Box::new(restored)) },
                    acyclic_actors::guest::CancellationToken::new(),
                )).unwrap();
                let checkpoint = crate::agent::checkpoint::encode(&step.state).unwrap();
                state = crate::agent::checkpoint::decode(&checkpoint).unwrap();
                prop_assert_eq!(&state.volumes, &reference);
            }
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
        let change = WorkspaceChanged { version: 1, op: WorkspaceOp::Reroot, entry: invalid };
        let before = table.clone();
        prop_assert!(table.apply(&change).is_err());
        prop_assert_eq!(&table, &before);
        let mut wrong = serde_json::to_value(&before).unwrap();
        wrong["version"] = 0.into();
        prop_assert!(serde_json::from_value::<WorkspaceTable>(wrong).is_err());
    }
}

#[test]
fn role_entries_are_stable_and_primary_is_the_first_user() {
    let first = entry(1, "first".into(), ".git".into());
    let second = entry(2, "second".into(), ".system".into());
    let mut tree = entry(3, "arbitrary".into(), String::new());
    tree.kind = WorkspaceKind::Tree;
    tree.role = WorkspaceRole::Tree;
    tree.source = WorkspaceSource::Subtree(AgentPath::root(&AgentId::from_bytes([9; 16])));
    tree.mode = WorkspaceMode::ReadOnly;
    tree.inherit = WorkspaceInherit::Own;
    let mut table = WorkspaceTable::new(vec![tree.clone(), first.clone(), second.clone()]).unwrap();
    let mut home = entry(4, "other-label".into(), String::new());
    home.role = WorkspaceRole::Home;
    home.inherit = WorkspaceInherit::Own;
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Add,
            entry: home.clone(),
        })
        .unwrap();
    assert_eq!(table.primary(), Some(&first));
    for op in [
        WorkspaceOp::Remove,
        WorkspaceOp::Rename,
        WorkspaceOp::Reroot,
    ] {
        let before = table.clone();
        assert!(
            table
                .apply(&WorkspaceChanged {
                    version: 1,
                    op,
                    entry: tree.clone()
                })
                .is_err()
        );
        assert_eq!(table, before);
    }
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Remove,
            entry: first,
        })
        .unwrap();
    assert_eq!(table.primary(), Some(&second));
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Remove,
            entry: second,
        })
        .unwrap();
    assert_eq!(table.primary(), Some(&home));
    let mut rebound = tree.clone();
    rebound.id = EntryId::from_bytes([5; 16]);
    rebound.source = WorkspaceSource::Subtree(AgentPath::root(&AgentId::from_bytes([11; 16])));
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Rebind { previous: tree.id },
            entry: rebound.clone(),
        })
        .unwrap();
    let before = table.clone();
    assert!(
        table
            .apply(&WorkspaceChanged {
                version: 1,
                op: WorkspaceOp::Rebind {
                    previous: rebound.id
                },
                entry: rebound
            })
            .is_err()
    );
    assert_eq!(table, before);
    tree.mode = WorkspaceMode::ReadWrite;
    assert!(tree.check().is_err());
}

#[test]
fn user_names_and_source_aliases_have_no_reserved_role_meaning() {
    let mut entries = (0..=255)
        .map(|id| entry(id, format!("w{id}"), ".git".into()))
        .collect::<Vec<_>>();
    entries[0].name = "home".into();
    entries[1].source = entries[0].source.clone();
    let mut table = WorkspaceTable::new(entries).unwrap();
    let mut changed = table.entries()[0].clone();
    changed.name = "tree".into();
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Rename,
            entry: changed.clone(),
        })
        .unwrap();
    let id = changed.id;
    changed.source = entry(4, "ignored".into(), String::new()).source;
    table
        .apply(&WorkspaceChanged {
            version: 1,
            op: WorkspaceOp::Reroot,
            entry: changed,
        })
        .unwrap();
    assert_eq!(table.primary().unwrap().id, id);
}
