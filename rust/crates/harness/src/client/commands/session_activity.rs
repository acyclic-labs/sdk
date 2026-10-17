use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    /// Stock admission selected by the oldest outstanding request, including a
    /// claimed request whose canonical model chunks are already being followed.
    pub fn active_task(&self) -> Option<TaskId> {
        self.pending.iter().find_map(|request| {
            let id = match &request.key {
                files::FileKey::Task {
                    file: files::TaskFile::Request(id),
                    ..
                }
                | files::FileKey::Session {
                    file: files::TaskFile::Request(id),
                    ..
                } => id,
                _ => return None,
            };
            files::command_query::identity(id)
                .map(|(_, operation)| crate::runtime::task_id_for_admission(operation))
        })
    }

    /// Verify that editing refers to a retained prompt from this UI session,
    /// then submit a replacement with immutable provenance.
    pub async fn replace_message(&mut self, message: &str, text: String) -> Result<()> {
        let operation = OperationId::parse(message)?;
        let id = self.request_id(operation)?;
        let original = self
            .files
            .iter()
            .find_map(|(key, value)| match key {
                files::FileKey::Task {
                    file: files::TaskFile::Request(request),
                    ..
                }
                | files::FileKey::Session {
                    file: files::TaskFile::Request(request),
                    ..
                } if request == &id => Some(value),
                _ => None,
            })
            .ok_or_else(|| Error::NotFound("editable prompt request".into()))?;
        if !matches!(
            serde_json::from_slice::<CommandResolution>(&original.bytes).map_err(codec)?,
            CommandResolution::Prompt { .. }
                | CommandResolution::Submit {
                    request: CommandRequest::Replace { .. }
                }
        ) {
            return Err(Error::Invalid("only user prompts can be edited".into()));
        }
        if text.is_empty() {
            return Err(Error::Invalid("replacement prompt is empty".into()));
        }
        self.publish_input_resolution(CommandResolution::Submit {
            request: CommandRequest::Replace {
                message: operation,
                text,
            },
        })
        .await?;
        Ok(())
    }

    /// Publish a cancellation request for the active admission, retaining its target.
    pub async fn cancel_active(&mut self) -> Result<()> {
        let task = self
            .active_task()
            .ok_or_else(|| Error::Invalid("no active turn".into()))?;
        // One immutable request per admitted task. It is intentionally outside
        // the prompt inbox: cancellation is observed while that inbox is busy.
        let key = files::FileKey::Task {
            task: component(task.to_string())?,
            file: files::TaskFile::Request(component(format!("cancel-{task}"))?),
        };
        let request = CommandResolution::Submit {
            request: CommandRequest::Cancel { task },
        };
        let bytes = serde_json::to_vec(&request).map_err(codec)?;
        if let Some(saved) = self.read_file(&key).await? {
            return if saved.bytes.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(Error::Conflict("cancellation request differs".into()))
            };
        }
        self.publish_rebased::<true, true>(key, bytes, true).await
    }

    /// Whether an observed request for this session or selected task lacks its
    /// immutable outcome. Taking a request or losing a dispatch does not clear it.
    pub fn pending_execution(&self) -> Result<bool> {
        let selected = self.selected_task()?.map(|task| task.to_string());
        Ok(self.files.keys().any(|key| {
            let own = match key {
                files::FileKey::Session {
                    session,
                    file: files::TaskFile::Request(_),
                } => session.as_str() == self.session.to_string(),
                files::FileKey::Task {
                    task,
                    file: files::TaskFile::Request(_),
                } => selected.as_deref() == Some(task.as_str()),
                _ => false,
            };
            own && related(key, files::TaskFile::ExecutionResult)
                .is_some_and(|outcome| !self.files.contains_key(&outcome))
        }))
    }
}
