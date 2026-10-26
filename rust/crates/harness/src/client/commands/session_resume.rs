//! Reconnect the command inbox at its last completely folded cut.
use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(super) async fn resume_follow(&mut self) -> Result<bool> {
        let query = files::Query::CommandSession {
            session: component(self.session.to_string())?,
            recent: u32::try_from((self.maximum_files / 4).max(1))
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let observed = self
            .volume
            .observe(query.clone(), Some(self.cursor.clone()), self.maximum_files)
            .await
            .map_err(storage)?;
        if observed.reset && observed.snapshot.is_none() {
            return Err(Error::Conflict("command reset snapshot missing".into()));
        }
        if let Some(snapshot) = observed.snapshot {
            // Build the complete disposable fold before publishing the reset.
            // The volume primitive validates the snapshot's history identity.
            let progress = snapshot.cursor != self.cursor;
            let replacement = Self::from_snapshot(
                self.volume.clone(),
                self.writer.clone(),
                self.session,
                self.maximum_bytes,
                self.maximum_files,
                snapshot,
                observed.changes,
            )?;
            *self = replacement;
            return Ok(progress);
        }
        self.changes = observed.changes;
        Ok(false)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use acyclic_stream::{MemoryStream, StreamPath};

    async fn check<const RESUME: bool>() -> Result<()> {
        let volume = Arc::new(
            files::TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("command-follow-resume")?,
            )
            .await?,
        );
        let session = uuid::Uuid::from_bytes([19; 16]);
        let mut reader = CommandSession::open(volume.clone(), session, 65536, 128).await?;
        let mut writer = CommandSession::open(volume.clone(), session, 65536, 128).await?;
        reader.changes = Box::pin(futures::stream::empty());
        let operation = writer
            .publish_resolution(CommandResolution::Prompt {
                text: "new request".into(),
                source: None,
                mentions: Vec::new(),
            })
            .await?;
        if !RESUME {
            assert!(
                reader.changes.next().await.is_some(),
                "command follow must resume"
            );
        }
        let request = reader.next_request().await?;
        assert!(request.key.path().contains(&operation.to_string()));
        let calls = std::cell::Cell::new(0);
        let result = reader
            .consume(&request, |_, _, _| async {
                calls.set(calls.get() + 1);
                Ok(json!({"text":"done"}))
            })
            .await?;
        assert!(result.is_some());
        reader.changes = Box::pin(futures::stream::empty());
        // Resume the inbox after settling this exact effect. A repeat consume
        // observes its stored outcome and never executes the closure again.
        assert!(
            reader
                .consume(&request, |_, _, _| async {
                    calls.set(calls.get() + 1);
                    Ok(json!({"text":"duplicate"}))
                })
                .await?
                .is_none()
        );
        assert_eq!(calls.get(), 1);
        Ok(())
    }
    #[tokio::test]
    async fn closed_command_inbox_resumes_without_replaying_an_effect() -> Result<()> {
        check::<true>().await
    }
    #[tokio::test]
    #[should_panic(expected = "command follow must resume")]
    async fn negative_control_exiting_the_command_inbox_loses_its_request() {
        check::<false>().await.expect("fixture");
    }
}
