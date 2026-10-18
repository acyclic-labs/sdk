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
        session
            .files
            .insert(key.clone(), files::FileValue::new(key, b"{}".to_vec()));
    }
    let before = session.files.len();
    session.settings_visits = 0;
    for _ in 0..4 {
        crate::client::scroll_tui(&mut session, [3], 1000)
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
