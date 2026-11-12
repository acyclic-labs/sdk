use super::*;
use crate::agent::*;

fn fixture() -> (WorkspaceTable, Roster) {
    let table = WorkspaceTable::new(vec![WorkspaceEntry {
        id: EntryId::from_bytes([1; 16]),
        role: WorkspaceRole::User,
        filter: WorkspaceFilter::None,
        name: "sdk".into(),
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
                "volume".into(),
                crate::conversation::VolumeClass::Project,
                crate::conversation::VolumeOwner::Project("fixture".into()),
                &mut None,
            )
            .unwrap(),
        ),
        root: String::new(),
        mode: WorkspaceMode::ReadWrite,
        inherit: WorkspaceInherit::Fork,
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
                entry: entry.id,
                paths: [
                    ("home".into(), Some(format!("/agents/a/{}", entry.name))),
                    ("box".into(), Some(format!("C:\\agents\\a\\{}", entry.name))),
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
                entry.id = EntryId::from_bytes([2; 16]);
                WorkspaceOp::Add
            }
            1 => {
                entry.name = "renamed".into();
                WorkspaceOp::Rename
            }
            2 => {
                entry.root = ".git".into();
                WorkspaceOp::Reroot
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
    let id = table.entries()[0].id;
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
                    entry: id,
                    paths: [(target.into(), Some(path.into()))].into(),
                }]
            )
            .is_err()
        );
    }
    let offline = WorkspaceAnnouncement::new(
        &table,
        &roster,
        vec![WorkspaceExposure {
            entry: id,
            paths: [("home".into(), Some("/tree".into())), ("box".into(), None)].into(),
        }],
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_slice(offline.canonical().0).unwrap();
    assert!(value["workspaces"][0]["paths"]["box"].is_null());
}

#[test]
fn internal_workspace_has_neither_an_announcement_nor_a_target_exposure() {
    let (table, roster) = fixture();
    let visible = table.entries()[0].clone();
    let mut internal = visible.clone();
    internal.id = EntryId::from_bytes([3; 16]);
    internal.role = WorkspaceRole::Internal;
    internal.name = "not-a-reserved-name".into();
    internal.inherit = WorkspaceInherit::Own;
    let table = WorkspaceTable::new(vec![visible.clone(), internal.clone()]).unwrap();
    let exposure = || WorkspaceExposure {
        entry: visible.id,
        paths: [("home".into(), Some("/sdk".into()))].into(),
    };
    let announced = WorkspaceAnnouncement::new(&table, &roster, vec![exposure()]).unwrap();
    let document: serde_json::Value = serde_json::from_slice(announced.canonical().0).unwrap();
    assert_eq!(document["workspaces"].as_array().unwrap().len(), 1);
    assert_eq!(document["workspaces"][0]["name"], visible.name);
    assert!(
        WorkspaceAnnouncement::new(
            &table,
            &roster,
            vec![
                exposure(),
                WorkspaceExposure {
                    entry: internal.id,
                    paths: [("home".into(), Some("/hidden".into()))].into(),
                }
            ]
        )
        .is_err()
    );
}
