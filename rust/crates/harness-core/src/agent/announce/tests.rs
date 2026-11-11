use super::*;
use crate::agent::*;

fn fixture() -> (WorkspaceTable, Roster) {
    let table = WorkspaceTable::new(vec![WorkspaceEntry {
        name: "tree".into(),
        kind: WorkspaceKind::Tree,
        source: WorkspaceSource::Subtree(AgentPath::root(&AgentId::from_bytes([1; 16]))),
        root: String::new(),
        mode: WorkspaceMode::ReadOnly,
        inherit: WorkspaceInherit::Own,
    }])
    .unwrap();
    let roster = Roster::new(
        "home",
        [("home", PathStyle::Posix), ("box", PathStyle::Windows)]
            .into_iter()
            .map(|(name, path_style)| TargetRecord {
                name: name.into(),
                owner: "owner".into(),
                class: TargetClass::Attached,
                os: "fixture".into(),
                shell: "fixture".into(),
                path_style,
                device_kind: DeviceKind::Desktop,
                state: TargetState::Online,
            })
            .collect(),
    )
    .unwrap();
    (table, roster)
}

fn document(table: &WorkspaceTable, roster: &Roster) -> WorkspaceAnnouncement {
    WorkspaceAnnouncement::new(
        table,
        roster,
        table
            .entries()
            .iter()
            .map(|entry| WorkspaceExposure {
                source: entry.source.clone(),
                paths: [
                    ("home".into(), format!("/agents/a/{}", entry.name)),
                    ("box".into(), format!("C:\\agents\\a\\{}", entry.name)),
                ]
                .into(),
            })
            .collect(),
    )
    .unwrap()
}

/// Selection reuses the roster's digest semantics. Every changed workspace
/// document contributes exactly one delta, including when a history cut drops it.
#[test]
fn workspace_digest_changes_for_every_table_delta() {
    let (mut table, roster) = fixture();
    assert_eq!(
        AnnouncementDocument::announcement(&roster, None),
        roster.announcement(None)
    );
    let mut announced = None;
    for op in 0..4 {
        let document = document(&table, &roster);
        let (bytes, digest) = document
            .announcement(announced)
            .expect("one appended delta");
        let wire: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(wire["default"], table.entries()[0].name);
        assert!(wire["workspaces"][0].get("source").is_none());
        announced = Some(digest);
        assert!(document.announcement(announced).is_none());
        assert!(
            document.announcement(None).is_some(),
            "history cuts reannounce"
        );
        let mut entry = table.entries()[0].clone();
        let operation = match op {
            0 => {
                entry.name = "second".into();
                entry.source =
                    WorkspaceSource::Subtree(AgentPath::root(&AgentId::from_bytes([2; 16])));
                WorkspaceOp::Add
            }
            1 => {
                entry.name = "renamed".into();
                WorkspaceOp::Rename
            }
            2 => {
                let previous = entry.source.clone();
                entry.root = ".git".into();
                WorkspaceOp::Reroot { previous }
            }
            _ => WorkspaceOp::Remove,
        };
        table
            .apply(&WorkspaceChanged {
                version: 1,
                op: operation,
                entry,
            })
            .unwrap();
    }
    let current = document(&table, &roster);
    assert!(current.announcement(announced).is_some());
}

#[test]
fn workspace_announcements_admit_only_actual_target_exposures() {
    let (table, roster) = fixture();
    let source = table.entries()[0].source.clone();
    for (target, path) in [
        ("missing", "/root"),
        ("home", "relative"),
        ("box", "C:relative"),
        ("home", "/root\n"),
    ] {
        assert!(
            WorkspaceAnnouncement::new(
                &table,
                &roster,
                vec![WorkspaceExposure {
                    source: source.clone(),
                    paths: [(target.into(), path.into())].into(),
                }]
            )
            .is_err()
        );
    }
    let offline = WorkspaceAnnouncement::new(
        &table,
        &roster,
        vec![WorkspaceExposure {
            source,
            paths: [("home".into(), "/tree".into())].into(),
        }],
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_slice(offline.canonical().0).unwrap();
    assert!(value["workspaces"][0]["paths"].get("box").is_none());
}
