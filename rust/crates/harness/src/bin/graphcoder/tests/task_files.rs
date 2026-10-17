use super::*;
use acyclic_harness::{
    client::TaskFileClient,
    task_volume::{Channel, Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

#[tokio::test]
async fn followed_task_files_render_without_a_wire_connection() {
    let files = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("composer-files").unwrap(),
    )
    .await
    .unwrap();
    let task = acyclic_harness::TaskId::from_bytes([6; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let cut = files.snapshot(&Query::Tasks, 16).await.unwrap();
    let mut client = TaskFileClient::observe(&files, &owner, 16, 4096)
        .await
        .unwrap();
    let mut renderer = ClientRenderer::default();
    renderer.update(client.views(), &owner);
    assert!(!picture(&renderer).contains("File-backed answer"));
    let mut file = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Chunk {
                execution: acyclic_harness::OperationId::from_bytes([0; 16]),
                turn: 0,
                sequence: 0,
            },
        },
        b"**File-backed answer**".to_vec(),
    );
    file.channel = Some(Channel::Text);
    files.append(cut.generation, &file).await.unwrap();
    client.next_change().await.unwrap();
    renderer.update(client.views(), &owner);
    assert!(picture(&renderer).contains("File-backed answer"));
    assert!(!picture(&renderer).contains("**File-backed answer**"));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    // A session's file outcomes remain ordered, replayable and visible after
    // owner dispatch selects a different admitted task for each prompt.
    #[test]
    fn command_outcomes_remain_in_the_session_transcript(texts in proptest::collection::vec("[a-z]{1,12}", 1..5)) {
        struct NoFiles;
        impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("session-transcript").unwrap()).await.unwrap());
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "owner".into() };
            let mut commands = acyclic_harness::client::CommandSession::open(volume.clone(), acyclic_harness::OperationId::new().to_string().parse().unwrap(), 65_536, 128).await.unwrap();
            let mut client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536).await.unwrap();
            let mut expected = Vec::new();
            for text in texts {
                commands.submit("prompt", &NoFiles).await.unwrap();
                let request = commands.take_request().unwrap();
                let value = serde_json::json!({"text":text,"selected_task":acyclic_harness::TaskId::new()});
                commands.consume(&request, |_,_,_| async { Ok(value) }).await.unwrap();
                expected.push(text);
                client.project_commands(&commands, &owner).unwrap();
                client.project_commands(&commands, &owner).unwrap();
                let key = format!("AGGREGATE_KIND_TASK:{}", owner.id);
                proptest::prop_assert_eq!(&client.views().transcripts[&key].streaming_text, &expected.join("\n\n"));
                let mut renderer = ClientRenderer::default(); renderer.update(client.views(), &owner);
                let visible = picture(&renderer);
                for text in &expected { proptest::prop_assert!(visible.contains(text)); }
                let mut replay = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536).await.unwrap();
                replay.project_commands(&commands, &owner).unwrap();
                proptest::prop_assert_eq!(&replay.views().transcripts[&key], &client.views().transcripts[&key]);
            }
            Ok(())
        })?;
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(12))]
    // The incremental projection equals the independently paged retained archive
    // after eviction, unrelated layout writes, history cuts and full reopen.
    #[test]
    fn archive_projection_survives_retention_and_history_cuts(
        texts in proptest::collection::vec("[a-z]{1,12}", 8..14),
        cut in 0usize..14,
    ) {
        struct NoFiles;
        impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use acyclic_harness::client::{CommandFileRoot, CommandSession};
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("archive-retention").unwrap()).await.unwrap());
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "archive".into() };
            let mut commands = CommandSession::open(volume.clone(), OperationId::new().to_string().parse().unwrap(), 65_536, 24).await.unwrap();
            let mut client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536).await.unwrap();
            let key = "AGGREGATE_KIND_TASK:archive";
            for (index, text) in texts.iter().enumerate() {
                commands.submit("prompt", &NoFiles).await.unwrap();
                let request = commands.take_request().unwrap();
                commands.consume(&request, |_,_,_| async { Ok(serde_json::json!({"text":text})) }).await.unwrap();
                let mut layout = commands.state(CommandFileRoot::Session, "layout.json").unwrap().unwrap();
                layout["unrelated_navigation"] = serde_json::json!(index);
                if index == cut { layout["history_after"] = serde_json::json!(commands.output_sequence()); }
                commands.write_state(CommandFileRoot::Session, "layout.json", &layout).await.unwrap();
                let mut expected = Vec::new();
                for back in 0..24 {
                    let Some(value) = commands.output_page(back).unwrap() else { break };
                    expected.push(value["ok"]["text"].as_str().unwrap().to_owned());
                }
                expected.reverse();
                client.project_commands(&commands, &owner).unwrap();
                proptest::prop_assert_eq!(&client.views().transcripts[key].streaming_text, &expected.join("\n\n"));
                let mut replay = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65_536).await.unwrap();
                replay.project_commands(&commands, &owner).unwrap();
                proptest::prop_assert_eq!(&replay.views().transcripts[key], &client.views().transcripts[key]);
            }
            Ok(())
        })?;
    }
}
