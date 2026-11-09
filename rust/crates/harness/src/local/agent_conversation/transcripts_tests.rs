//! Finish during a real payload read must drain the newly committed suffix.
use super::*;
use crate::LocalHarness;
use crate::local::tests::{volume, volume_config};
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use std::{io::Write, sync::Mutex};
use tokio::sync::oneshot;

struct ObservedFile {
    file: std::fs::File,
    line: Vec<u8>,
    started: Option<oneshot::Sender<()>>,
}

impl Write for ObservedFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.file.write(bytes)?;
        self.line.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        if !self.line.is_empty() {
            let row: serde_json::Value = serde_json::from_slice(&self.line)?;
            if row["kind"] == "execution" {
                let record: crate::executor::ExecutionRecord =
                    serde_json::from_value(row["record"].clone())?;
                if matches!(
                    record.event,
                    crate::executor::ExecutionEvent::ModelStarted { .. }
                ) && let Some(started) = self.started.take()
                {
                    let _ = started.send(());
                }
            }
            self.line.clear();
        }
        Ok(())
    }
}

#[tokio::test]
async fn finish_during_payload_read_drains_committed_records() -> Result<()> {
    let (release, released) = oneshot::channel();
    let (entered, entering) = oneshot::channel();
    let gate = Mutex::new(Some((entered, released)));
    let provider = ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
        .with_dispatch_observer(move |_| {
            let gate = gate.lock().unwrap().take().expect("one dispatch");
            Box::pin(async move {
                let (entered, released) = gate;
                let _ = entered.send(());
                released.await.map_err(failed)?;
                Ok(())
            })
        });
    let directory = tempfile::tempdir().map_err(failed)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(volume_config(
            directory.path(),
            project,
            Arc::new(provider),
        )?)
        .await?,
    );
    let connection = harness.connect().await?;
    let conversation = Conversation::open(connection.clone());
    let mut observer = conversation.observe(Some(0)).await?;
    let input = conversation
        .send("drain", "finish after export starts")
        .await?;
    entering.await.map_err(failed)?;
    let initial = conversation.execution_page(input, 0, false).await?;
    assert!(!initial.settled);
    assert!(initial.page.as_ref().expect("live journal").next.is_none());

    let path = directory.path().join("transcript.jsonl");
    let (started, starting) = oneshot::channel();
    let file = ObservedFile {
        file: std::fs::File::create(&path).map_err(failed)?,
        line: Vec::new(),
        started: Some(started),
    };
    let (finished, finish) = watch::channel(false);
    let exporting = journal(conversation.clone(), file, "drain", finish);
    tokio::pin!(exporting);
    tokio::select! {
        biased;
        result = &mut exporting => panic!("export ended before model payload: {result:?}"),
        result = starting => result.map_err(failed)?,
    }
    // The journal emitted ModelStarted and parked on its real Session payload
    // read. Keep that future suspended while the model appends and settles.
    release.send(()).expect("model waiting");
    conversation.settle("drain", input, &mut observer).await?;
    finished.send_replace(true);
    exporting.await?;

    let final_page = conversation.execution_page(input, 0, false).await?;
    assert!(final_page.settled);
    let expected = final_page.page.expect("settled journal");
    assert!(expected.next.is_none());
    assert!(expected.end > initial.page.expect("initial journal").end);
    let bytes = std::fs::read(path).map_err(failed)?;
    let records = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<serde_json::Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(failed)?
        .into_iter()
        .filter(|row| row["kind"] == "execution" && row["input"] == input)
        .map(|row| row["record"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::json!(records),
        serde_json::to_value(expected.items).map_err(failed)?
    );
    Ok(())
}
