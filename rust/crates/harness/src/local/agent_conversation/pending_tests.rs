//! Reattachment reads the original accepted input, not the sending client.
use super::*;
use crate::local::tests::{volume, volume_config};
use crate::test_support::{ScriptedModelProvider, ScriptedTurn};
use std::sync::Mutex;
use tokio::sync::oneshot;

#[tokio::test]
async fn reattached_pending_question_is_complete_and_retires_on_settlement() -> Result<()> {
    let (entered, entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gate = Mutex::new(Some((entered, released)));
    let provider = ScriptedModelProvider::new([
        ScriptedTurn::Text("done".into()),
        ScriptedTurn::Text("done".into()),
    ])
    .with_dispatch_observer(move |_| {
        let gate = gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some((entered, released)) = gate {
                let _ = entered.send(());
                released.await.map_err(failed)?;
            }
            Ok(())
        })
    });
    let directory = tempfile::tempdir().map_err(failed)?;
    let project = volume(directory.path()).await?;
    let config =
        volume_config(directory.path(), project, Arc::new(provider))?.without_default_tools();
    let harness = Arc::new(crate::local::tests::open_scripted(config).await?);
    let connection = harness.connect().await?;
    let original = Conversation::open(connection.clone());
    eprintln!("pending-reattach: reading empty cut");
    assert!(original.pending_inputs().await?.is_empty());
    eprintln!("pending-reattach: admitting large question");
    let phrase = "pending durable evidence 🙂 ";
    let question = phrase.repeat(crate::client::MAX_READ_BYTES as usize / phrase.len() + 1);
    assert!(question.len() as u64 > crate::client::MAX_READ_BYTES);
    let inbox = original.send("pending-question", &question).await?;
    eprintln!("pending-reattach: awaiting first dispatch");
    tokio::select! {
        entered = entering => entered.map_err(failed)?,
        settled = original.wait_current_input() => {
            let entries = original.entries(0, original.settled().await?).await?;
            let mut outputs = Vec::new();
            for entry in &entries { outputs.push(original.output_text(entry).await?); }
            return Err(Error::Invalid(format!("input settled before dispatch: {settled:?}; {outputs:?}")));
        }
    }
    eprintln!("pending-reattach: admitting second question");
    let second = "SECOND_ACCEPTED_QUESTION";
    let later = original.send("later-pending-question", second).await?;
    drop(original);
    let reopened = Conversation::open(connection);
    eprintln!("pending-reattach: recovering full pending bodies");
    assert_eq!(
        reopened.pending_inputs().await?,
        vec![(inbox, question.clone()), (later, second.into())]
    );
    eprintln!("pending-reattach: narrow preview");
    let mut preview = reopened
        .input_query(AgentPageRequest::new(1, 32, None).map_err(failed)?)
        .await
        .map_err(failed)?;
    loop {
        match preview.next().await.map_err(failed)? {
            Some(crate::client::InputPreviewStep::Progress) => {}
            Some(crate::client::InputPreviewStep::Selected(Some(preview))) => {
                assert!(!preview.text.is_empty() && second.starts_with(&preview.text));
                assert_eq!(preview.bytes, second.len() as u64);
                break;
            }
            _ => {
                return Err(Error::Invalid(
                    "narrow pending-session preview failed".into(),
                ));
            }
        }
    }
    drop(preview);
    eprintln!("pending-reattach: releasing dispatch");
    release
        .send(())
        .map_err(|_| Error::Invalid("dispatch gate closed".into()))?;
    reopened.wait_current_input().await?;
    eprintln!("pending-reattach: verifying settled cut");
    assert!(
        reopened.pending_inputs().await?.is_empty(),
        "settled input became pending again"
    );
    Ok(())
}
