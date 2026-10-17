//! Native source chunks are ordinary immutable files on the shared SDK binding.
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::{
    client::jsonl,
    external_executor::{ExternalHost, request},
    task_volume::{FileKey, FileValue, Query, TaskFile},
};
use futures::FutureExt as _;
use native_hooks::*;
use serde_json::json;
use tokio::io::AsyncWriteExt as _;

fn inputs(index: usize) -> Result<(Vec<FileValue>, FileValue, String)> {
    // Larger than a transport record, delivered as independent chunk files.
    let mut files = (0..9)
        .map(|sequence| {
            Ok(FileValue::new(
                FileKey::parse(&format!("workspace/native/cut-{index}/chunk-{sequence}"))?,
                vec![u8::try_from(index)?; 12 * 1024],
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = FileValue::new(
        FileKey::parse(&format!("workspace/native/cut-{index}/manifest.json"))?,
        serde_json::to_vec(&json!({"chunks":files.len(),"bytes":9*12*1024}))?,
    );
    files.push(manifest.clone());
    let lines = files
        .iter()
        .map(jsonl::request)
        .collect::<acyclic_harness::Result<Vec<_>>>()?
        .join("\n")
        + "\n";
    Ok((files, manifest, lines))
}

async fn upload(
    root: &std::path::Path,
    binding: &std::path::Path,
    task: &str,
    manifest: &FileValue,
    lines: &str,
) -> Result<std::process::Output> {
    let mut process = command(root)
        .env("ACYCLIC_HARNESS_BINDING", binding)
        .args(["__files", task, &manifest.key.path()])
        .spawn()?;
    let sent = process
        .stdin
        .take()
        .ok_or("missing file stdin")?
        .write_all(lines.as_bytes())
        .await;
    let output = process.wait_with_output().await?;
    if output.status.success() {
        sent?;
    }
    Ok(output)
}

fn delivered(output: &std::process::Output, manifest: &FileValue) -> Result {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::str::from_utf8(&output.stdout)?;
    let mut lines = text.lines();
    let (query, head) = jsonl::parse_snapshot(lines.next().ok_or("missing file snapshot")?)?;
    assert_eq!(query, Query::File(manifest.key.clone()));
    let mut value = head.files.first().map(|file| {
        file.segments
            .iter()
            .flat_map(|bytes| bytes.iter().copied())
            .collect::<Vec<_>>()
    });
    let mut cursor = head.cursor;
    for line in lines {
        let change = jsonl::parse_change(line)?;
        assert_eq!(change.cursor.query, cursor.query);
        assert!(change.cursor.position > cursor.position);
        if let Some(file) = change.file {
            assert_eq!(file.key, manifest.key);
            value = Some(file.bytes.to_vec());
        }
        cursor = change.cursor;
    }
    assert_eq!(value.as_deref(), Some(manifest.bytes.as_ref()));
    Ok(())
}

#[tokio::test]
async fn overlapping_cli_source_uploads_follow_each_file_and_reopen_without_republication() -> Result
{
    let fixture = Fixture::new()?;
    let mut owner = Some(fixture.start_owner().await?);
    let result = std::panic::AssertUnwindSafe(async {
        let session = uuid::Uuid::new_v4().to_string();
        let event = json!({"sessionId":session,"cwd":fixture.path()});
        let task = request(ExternalHost::Pi, "session_start", event.clone())?.task_id();
        hook(fixture.path(), "pi", "session_start", event).await?;
        let binding = fixture.path().parent().unwrap().join("input-binding.json");
        std::fs::write(
            &binding,
            serde_json::to_vec(
                &json!({"provider":"local","workspace":fixture.path(),"task":task}),
            )?,
        )?;
        let (left, left_manifest, left_lines) = inputs(0)?;
        let (right, right_manifest, right_lines) = inputs(1)?;
        let task_text = task.to_string();
        let (a, b) = tokio::try_join!(
            upload(
                fixture.path(),
                &binding,
                &task_text,
                &left_manifest,
                &left_lines
            ),
            upload(
                fixture.path(),
                &binding,
                &task_text,
                &right_manifest,
                &right_lines
            )
        )?;
        delivered(&a, &left_manifest)?;
        delivered(&b, &right_manifest)?;
        let path = format!("external/requests/{task}");
        let volume = files(fixture.path(), &path).await?;
        let mut expected = left.into_iter().chain(right).collect::<Vec<_>>();
        expected.sort_by(|a, b| a.key.cmp(&b.key));
        let snapshot = volume.snapshot(&Query::Inputs, expected.len()).await?;
        assert_eq!(snapshot.files, expected);
        let before = volume.head().await?;
        drop(volume);
        fixture.close_owner(owner.take().unwrap()).await?;
        owner = Some(fixture.start_owner().await?);
        delivered(
            &upload(
                fixture.path(),
                &binding,
                &task_text,
                &left_manifest,
                &left_lines,
            )
            .await?,
            &left_manifest,
        )?;
        let volume = files(fixture.path(), &path).await?;
        assert_eq!(
            volume.head().await?,
            before,
            "an identical upload added records after restart"
        );
        let mut foreign = task.into_bytes();
        foreign[0] ^= 1;
        let foreign = acyclic_harness::TaskId::from_bytes(foreign).to_string();
        assert!(
            !upload(
                fixture.path(),
                &binding,
                &foreign,
                &left_manifest,
                &left_lines
            )
            .await?
            .status
            .success()
        );
        let forged = FileValue::new(
            FileKey::Task {
                task: acyclic_harness::task_volume::Component::new(task_text)?,
                file: TaskFile::Status,
            },
            b"{}".to_vec(),
        );
        assert!(
            !upload(
                fixture.path(),
                &binding,
                &task.to_string(),
                &left_manifest,
                &jsonl::request(&forged)?
            )
            .await?
            .status
            .success()
        );
        assert_eq!(
            volume.head().await?,
            before,
            "a rejected owner or task substitution published input"
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .catch_unwind()
    .await;
    let stopped = match owner {
        Some(owner) => fixture.close_owner(owner).await,
        None => Ok(()),
    };
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    stopped
}
