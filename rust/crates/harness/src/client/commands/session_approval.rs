//! All clients submit approval decisions to the same exact-intent files.
use super::*;
impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(super) async fn submit_approval(&mut self, input: &str) -> Result<Option<OperationId>> {
        // Presentation and cancellation commands remain usable while a tool
        // awaits a decision; only ordinary input is an approval response.
        if input.trim_start().starts_with('/') {
            return Ok(None);
        }
        let Some(task) = self.active_task().or(self.selected_task()?) else {
            return Ok(None);
        };
        let cancellation = files::cancellation_key(task)?;
        if self
            .read_file(&cancellation)
            .await?
            .is_some_and(|file| files::is_cancellation(task, &file.key, &file.bytes))
        {
            return Ok(None);
        }
        let snapshot = self
            .volume
            .snapshot(
                &files::Query::PendingApprovals(files::Component::new(task.to_string())?),
                self.maximum_files,
            )
            .await?;
        for file in &snapshot.files {
            if !matches!(
                file.key,
                files::FileKey::Task {
                    file: files::TaskFile::ApprovalRequest(_),
                    ..
                }
            ) {
                continue;
            }
            if file.bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid(
                    "approval request exceeds client file bound".into(),
                ));
            }
            let request = files::FileApproval::decode(file)?;
            if request.task != task || request.key()? != file.key {
                return Err(Error::Invalid(
                    "approval request path differs from its task".into(),
                ));
            }
            let approved = match input.trim().to_ascii_lowercase().as_str() {
                "y" | "yes" => true,
                "n" | "no" => false,
                _ => return Err(Error::Invalid("approval requires yes or no".into())),
            };
            self.approvals
                .as_ref()
                .ok_or_else(|| {
                    Error::Unauthorized("client has no authenticated approval connection".into())
                })?
                .decide(file, approved)
                .await?;
            return Ok(Some(request.binding()?.operation_id));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{MemoryStream, StreamPath};

    #[tokio::test]
    async fn clients_without_an_authenticated_connection_cannot_publish_decisions() -> Result<()> {
        for approved in [false, true] {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("approval-client").unwrap(),
                )
                .await?,
            );
            let mut client =
                CommandSession::open(volume.clone(), uuid::Uuid::from_u128(7), 65536, 32).await?;
            let task = TaskId::new();
            client
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &json!({"selected_task":task}),
                )
                .await?;
            let pending = files::Query::PendingApprovals(component(task.to_string())?);
            let mut observed = volume.observe(pending.clone(), None, 1).await?;
            assert!(observed.snapshot.take().unwrap().files.is_empty());
            use futures::StreamExt as _;
            let operation_id = OperationId::new();
            let request_key = files::FileKey::Task {
                task: component(task.to_string())?,
                file: files::TaskFile::ApprovalRequest(component(
                    crate::InteractionId::new().to_string(),
                )?),
            };
            let request_file = files::FileValue::new(
                request_key,
                serde_json::to_vec(&crate::interaction::Interaction::Approval {
                    prompt: "approve action".into(),
                    operation_id,
                    action_digest: [7; 32],
                    subject: crate::interaction::ApprovalSubject::Opaque {},
                })
                .map_err(codec)?,
            );
            let request = files::FileApproval::decode(&request_file)?;
            volume.append(volume.head().await?, &request_file).await?;
            let opened = observed.changes.next().await.unwrap()?;
            assert_eq!(opened.file.unwrap().key, request.key()?);
            struct NoReads;
            impl ContentResidencyVerifier for NoReads {}
            client.catalog.themes.push("y".into());
            for (form, text) in [("find", "y"), ("feedback", "n"), ("theme", "y")] {
                client
                    .write_state(
                        CommandFileRoot::Session,
                        "layout.json",
                        &json!({"selected_task":task,"open_view":form,"view_input":form}),
                    )
                    .await?;
                #[cfg(all(feature = "tui", feature = "terminal"))]
                if form == "feedback" {
                    use crate::tui::{
                        ClientRenderer, PromptInput, Viewport,
                        ratatui::{Terminal, backend::TestBackend},
                    };
                    let mut renderer = ClientRenderer::default();
                    let screen = renderer.screen_for(&client)?;
                    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
                    terminal
                        .draw(|frame| {
                            screen.render(
                                frame,
                                &PromptInput::default(),
                                &mut Viewport::default(),
                                0,
                                0,
                            );
                        })
                        .unwrap();
                    let visible = terminal
                        .backend()
                        .buffer()
                        .content
                        .iter()
                        .map(|cell| cell.symbol())
                        .collect::<String>();
                    assert!(visible.contains("Enter saves locally"));
                }
                client.submit(text, &NoReads).await?;
                if form == "theme" {
                    assert_eq!(
                        client
                            .state(CommandFileRoot::Config, "theme.json")?
                            .unwrap()["name"],
                        text
                    );
                    assert_eq!(volume.snapshot(&request.query()?, 4).await?.files.len(), 1);
                    continue;
                }
                let name = if form == "find" {
                    "search.json"
                } else {
                    "feedback.json"
                };
                assert_eq!(
                    client.state(CommandFileRoot::Session, name)?.unwrap()["text"],
                    text
                );
                assert_eq!(
                    volume.snapshot(&request.query()?, 4).await?.files.len(),
                    1,
                    "form input answered the approval"
                );
            }
            assert!(client.submit_approval("perhaps").await.is_err());
            assert_eq!(client.submit_approval("/quit").await?, None);
            let query = request.query()?;
            // Negative control: unrecognized input and presentation commands
            // cannot silently approve the pending action.
            assert_eq!(volume.snapshot(&query, 4).await?.files.len(), 1);
            // A raw volume handle is not an authenticated human connection.
            // The former writer accepted this same yes/no input and persisted a verdict.
            assert!(matches!(
                client
                    .submit(if approved { "y" } else { "n" }, &NoReads)
                    .await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(volume.snapshot(&query, 4).await?.files.len(), 1);
            assert_eq!(volume.snapshot(&pending, 1).await?.files.len(), 1);
            assert!(
                client.pending.is_empty(),
                "approval response became a model prompt"
            );
        }
        Ok(())
    }
}
