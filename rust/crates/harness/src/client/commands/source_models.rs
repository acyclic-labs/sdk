use super::super::model::check;
use super::*;
use crate::task_volume::{FileKey, FileValue, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};
use std::{
    future::Future as _,
    task::{Context, Poll},
};
struct NoReads;
impl ContentResidencyVerifier for NoReads {}
fn source_wakeup(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("source-model")?,
        ))?);
        let mut client = block_on(CommandSession::open(
            volume.clone(),
            uuid::Uuid::from_bytes([1; 16]),
            32_768,
            64,
        ))?;
        let mut sources = block_on(CommandSources::observe(
            &mut client,
            &volume,
            DEFAULT_COMMANDS_JSON.to_vec(),
            vec![],
            vec![],
            Arc::new(NoReads),
            Default::default(),
            32_768,
            64,
        ))?;
        let cut = block_on(volume.snapshot(&Query::Inputs, 64))?;
        let publisher = loom::thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(volume.append(
                    cut.generation,
                    &FileValue::new(
                        FileKey::parse(".codex/prompts/followed.md")?,
                        b"Updated $ARGUMENTS".to_vec(),
                    ),
                ))
            }
        });
        if broken {
            publisher
                .join()
                .map_err(|_| "source publisher panicked")??;
            // Broken: opening a new follow drops the already committed edit.
            sources.changes = block_on(volume.observe(Query::Inputs, None, 64))?.changes;
            let mut next = Box::pin(sources.changed());
            assert!(
                matches!(
                    next.as_mut()
                        .poll(&mut Context::from_waker(std::task::Waker::noop())),
                    Poll::Ready(Ok(()))
                ),
                "source wakeup lost"
            );
        } else {
            block_on(sources.changed())?;
            publisher
                .join()
                .map_err(|_| "source publisher panicked")??;
            block_on(sources.refresh(&mut client))?;
            let resolution = block_on(client.catalog().resolve("/followed now", &NoReads, 32_768))?;
            assert!(
                matches!(resolution, CommandResolution::Prompt { text, .. } if text == "Updated now")
            );
        }
        Ok(())
    });
}
#[test]
fn source_catalog_snapshot_follow_preserves_racing_file_edits() {
    source_wakeup(false);
}
#[test]
#[should_panic(expected = "source wakeup lost")]
fn negative_control_detects_reopening_the_source_subscription() {
    source_wakeup(true);
}
