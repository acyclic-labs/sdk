use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

async fn model<const ALWAYS_SCAN: bool>(retained: usize) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("settings-visits").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut session =
        CommandSession::open(volume, uuid::Uuid::from_bytes([1; 16]), 65536, retained * 8)
            .await
            .unwrap();
    let task = TaskId::from_bytes([2; 16]);
    session
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":task.to_string()}),
        )
        .await
        .unwrap();
    for n in 0..retained {
        let key = files::FileKey::Session {
            session: component(session.session.to_string()).unwrap(),
            file: files::TaskFile::Request(component(n.to_string()).unwrap()),
        };
        session.files.insert(
            key.clone(),
            StateFile::new(files::FileValue::new(key, b"{}".to_vec())),
        );
    }
    let before = session.files.len();
    session.settings_visits = 0;
    for _ in 0..4 {
        crate::client::scroll_tui(&mut session, [3], 1000, |from, _| {
            Some(crate::tui::TranscriptAnchor {
                position: crate::tui::RowPosition {
                    line: from.map_or(1, |from| from.position.line + 1),
                    row: 0,
                },
                ..Default::default()
            })
        })
        .await
        .unwrap();
        if ALWAYS_SCAN {
            session.load_task_settings_impl::<true>().await.unwrap();
        }
    }
    assert_eq!(session.files.len(), before);
    assert_eq!(
        session.settings_visits, 0,
        "scroll must visit zero retained command files"
    );
    // Task changes still replace settings, including clearing selection.
    session
        .write_state(
            CommandFileRoot::Task,
            "settings.json",
            &json!({"theme":"old"}),
        )
        .await
        .unwrap();
    session
        .write_state(CommandFileRoot::Task, "custom.json", &json!({"old":true}))
        .await
        .unwrap();
    session.settings_visits = 0;
    let next = TaskId::from_bytes([3; 16]);
    session
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":next.to_string()}),
        )
        .await
        .unwrap();
    assert!(
        !session
            .files
            .contains_key(&ui(&format!("task-{task}"), "settings.json").unwrap())
    );
    assert!(
        !session
            .files
            .contains_key(&ui(&format!("task-{task}"), "custom.json").unwrap())
    );
    assert_eq!(
        session.settings_visits, 2,
        "task changes visit only that task's two UI files"
    );
    assert_eq!(session.loaded_task, Some(Some(next)));
    session
        .write_state(CommandFileRoot::Session, "layout.json", &json!({}))
        .await
        .unwrap();
    assert_eq!(session.loaded_task, Some(None));
}
#[tokio::test]
async fn selected_task_settings_do_not_visit_retained_commands() {
    for n in [32, 64] {
        model::<false>(n).await;
    }
}
#[tokio::test]
#[should_panic(expected = "scroll must visit zero retained command files")]
async fn negative_control_always_scanning_retained_commands_is_detected() {
    model::<true>(32).await;
}

async fn scroll_cut<const CACHED: bool>(modal: bool, before_snapshot: bool) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("scroll-cut").unwrap(),
        )
        .await
        .unwrap(),
    );
    let id = uuid::Uuid::from_bytes([1; 16]);
    let mut owner = CommandSession::open(volume.clone(), id, 65536, 128)
        .await
        .unwrap();
    owner
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"transcript_top":scroll_anchor(2)}),
        )
        .await
        .unwrap();
    let mut input = CommandSession::open(volume.clone(), id, 65536, 128)
        .await
        .unwrap();
    let layout = if modal {
        json!({"open_view":"shortcuts", "view_back":20, "transcript_top":scroll_anchor(7), "peer":"retained"})
    } else {
        json!({"transcript_top":scroll_anchor(7), "peer":"retained"})
    };
    owner
        .write_state(CommandFileRoot::Session, "layout.json", &layout)
        .await
        .unwrap();
    if before_snapshot {
        input = CommandSession::open(volume, id, 65536, 128).await.unwrap();
    }
    if CACHED {
        let mut cached = input
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap();
        let from = crate::client::tui_anchor(&input).unwrap().unwrap();
        cached["transcript_top"] = json!(scroll_anchor(from.position.line + 3));
        input
            .write_state(CommandFileRoot::Session, "layout.json", &cached)
            .await
            .expect("scroll must re-resolve at the new cut");
    } else {
        // The same shared entry point used by PageUp/wheel in GraphCoder.
        crate::client::scroll_tui(&mut input, [5, -2], 100, |from, rows| {
            Some(scroll_anchor(
                from.unwrap()
                    .position
                    .line
                    .saturating_add_signed(rows.saturating_neg()),
            ))
        })
        .await
        .expect("scroll must re-resolve at the new cut");
    }
    let actual = input
        .state(CommandFileRoot::Session, "layout.json")
        .unwrap()
        .unwrap();
    assert_eq!(actual["peer"], "retained");
    assert_eq!(
        actual["transcript_top"],
        json!(scroll_anchor(if modal { 7 } else { 10 }))
    );
    if modal {
        assert_eq!(actual["view_back"], 23);
    }
}

#[tokio::test]
async fn scrolling_re_resolves_the_focused_layout_after_a_cut_change() {
    for modal in [false, true] {
        for before_snapshot in [false, true] {
            scroll_cut::<false>(modal, before_snapshot).await;
        }
    }
}

#[tokio::test]
#[should_panic(expected = "scroll must re-resolve at the new cut")]
async fn negative_control_cached_scroll_fails_on_an_advanced_cut() {
    scroll_cut::<true>(false, false).await;
}

fn scroll_anchor(line: usize) -> crate::tui::TranscriptAnchor {
    crate::tui::TranscriptAnchor {
        position: crate::tui::RowPosition { line, row: 0 },
        ..Default::default()
    }
}
