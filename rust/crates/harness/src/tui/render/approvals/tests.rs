use super::*;
use crate::{
    client::FileQueryView,
    interaction::{ApprovalSubject, Interaction},
    task_volume::{Component, FileValue, Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

async fn pending(subject: ApprovalSubject) -> crate::task_volume::Snapshot {
    pending_many(vec![(3, 4, subject)]).await
}

async fn pending_many(requests: Vec<(u8, u8, ApprovalSubject)>) -> crate::task_volume::Snapshot {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("approval-display").unwrap(),
    )
    .await
    .unwrap();
    for (task, id, subject) in requests {
        let request = Interaction::approval(
            "Allow tool execution?".into(),
            crate::OperationId::from_bytes([1; 16]),
            [2; 32],
            subject,
        )
        .unwrap();
        let file = FileValue::new(
            FileKey::Task {
                task: Component::new(crate::TaskId::from_bytes([task; 16]).to_string()).unwrap(),
                file: TaskFile::ApprovalRequest(
                    Component::new(crate::InteractionId::from_bytes([id; 16]).to_string()).unwrap(),
                ),
            },
            serde_json::to_vec(&request).unwrap(),
        );
        volume
            .append(volume.head().await.unwrap(), &file)
            .await
            .unwrap();
        let status = FileValue::new(
            FileKey::Task {
                task: Component::new(crate::TaskId::from_bytes([task; 16]).to_string()).unwrap(),
                file: TaskFile::Status,
            },
            br#"{"state":"active"}"#.to_vec(),
        );
        volume
            .append(volume.head().await.unwrap(), &status)
            .await
            .unwrap();
    }
    volume.snapshot(&Query::Root, 16).await.unwrap()
}

fn paint(subject: ApprovalSubject) -> Vec<String> {
    futures::executor::block_on(async {
        let snapshot = pending(subject).await;
        let files = FileQueryView::from_snapshot(Query::Root, snapshot, 16, 8192).unwrap();
        let task = Component::new(crate::TaskId::from_bytes([3; 16]).to_string()).unwrap();
        rows(Some(files.task(&task)))
            .iter()
            .map(ToString::to_string)
            .collect()
    })
}

#[tokio::test]
async fn only_the_first_pending_subject_in_the_answered_task_is_shown() {
    let tool = |command: &str| ApprovalSubject::Tool {
        route: crate::interaction::ApprovalRoute::Policy,
        tool: "shell".into(),
        call_id: command.into(),
        arguments: serde_json::json!({"command": command}),
    };
    let snapshot = pending_many(vec![
        (2, 1, tool("foreign task")),
        (3, 5, tool("later request")),
        (3, 4, tool("first request")),
    ])
    .await;
    let files = FileQueryView::from_snapshot(Query::Root, snapshot, 16, 8192).unwrap();
    let task = Component::new(crate::TaskId::from_bytes([3; 16]).to_string()).unwrap();
    let painted = rows(Some(files.task(&task)))
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(painted.contains("first request"));
    assert!(!painted.contains("later request"));
    assert!(!painted.contains("foreign task"));
    assert_eq!(painted.matches("Approve this action? [y/n]").count(), 1);
}

#[tokio::test]
async fn human_subject_is_painted_in_the_scrollable_transcript() {
    use crate::{
        client::{ClientViewLimits, ClientViews, QueryViewLimits},
        tui::{
            ClientRenderer, PromptInput, Viewport,
            ratatui::{Terminal, backend::TestBackend},
        },
    };
    let snapshot = pending(ApprovalSubject::Tool {
        route: crate::interaction::ApprovalRoute::Policy,
        tool: "shell".into(),
        call_id: "call-42".into(),
        arguments: serde_json::json!({"command": "rm -rf ./important"}),
    })
    .await;
    let mut views = ClientViews::new(
        StreamPath::new("approval-display").unwrap(),
        ClientViewLimits {
            agents: 1,
            selected: 1,
            overview_values: 16,
            overview_bytes: 8192,
            query: QueryViewLimits {
                files: 16,
                segments: 16,
                bytes: 8192,
            },
        },
    )
    .unwrap();
    views
        .query_snapshot(Query::Root, snapshot, 16, 8192)
        .unwrap();
    let authority = crate::wire::Authority {
        kind: crate::wire::AggregateKind::Task as i32,
        id: crate::TaskId::from_bytes([3; 16]).to_string(),
    };
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &authority);
    // Refolding the same request must replace its projection, not append it.
    let count = renderer.screen().transcript.len();
    renderer.update(&views, &authority);
    assert_eq!(renderer.screen().transcript.len(), count);
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let mut viewport = Viewport::default();
    terminal
        .draw(|frame| {
            renderer
                .screen()
                .render(frame, &PromptInput::default(), &mut viewport, 0, 0);
        })
        .unwrap();
    let painted = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect::<String>();
    let command = painted
        .find("rm -rf ./important")
        .expect("exact command must be painted");
    let answer = painted
        .find("Approve this action? [y/n]")
        .expect("human answer must be painted");
    assert!(command < answer);
    assert!(painted.contains("call-42"));
    assert!(
        renderer.screen().notifications.is_empty(),
        "the two-row notice area cannot hold an approval"
    );
}

#[test]
fn exact_tool_subject_precedes_the_human_answer() {
    let arguments = serde_json::json!({
        "command": "rm -rf ./important",
        "path": "./important",
        "nested": {"replace": false},
    });
    let subject = ApprovalSubject::Tool {
        route: crate::interaction::ApprovalRoute::Policy,
        tool: "shell".into(),
        call_id: "call-42".into(),
        arguments,
    };
    let painted = paint(subject.clone());
    assert_eq!(painted.first().unwrap(), "Approval: Allow tool execution?");
    assert_eq!(painted.last().unwrap(), "Approve this action? [y/n]");
    let visible = painted[1..painted.len() - 1].join("\n");
    assert_eq!(
        serde_json::from_str::<ApprovalSubject>(&visible).unwrap(),
        subject
    );
    // The former generic-prompt projection exposes no bound action at all.
    let former = "Approval: Allow tool execution? [y/n]";
    assert!(!former.contains("rm -rf ./important"));
    assert!(visible.contains("rm -rf ./important"));
}

#[test]
fn subject_controls_remain_visible_and_opaque_preimages_stay_private() {
    let painted = paint(ApprovalSubject::Tool {
        route: crate::interaction::ApprovalRoute::Policy,
        tool: "shell\u{1b}[2J".into(),
        call_id: "call\u{202e}id".into(),
        arguments: serde_json::json!({"command": "echo\nnext\u{1b}[31m"}),
    })
    .join("\n");
    assert!(!painted.contains('\u{1b}'));
    assert!(!painted.contains('\u{202e}'));
    assert!(painted.contains("\\u001b"));
    assert!(painted.contains("\\u{202e}"));
    let opaque = paint(ApprovalSubject::Opaque {}).join("\n");
    assert!(opaque.contains("\"kind\": \"opaque\""));
    assert!(!opaque.contains("arguments"));
}
