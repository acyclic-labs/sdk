use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(super) async fn confirm_request(
        &mut self,
        confirmation: Value,
        accepted: bool,
    ) -> Result<OperationId> {
        let cut = self.generation;
        let operation = OperationId::parse(
            confirmation["operation"]
                .as_str()
                .ok_or_else(|| Error::Invalid("confirmation operation missing".into()))?,
        )?;
        let request: CommandRequest =
            serde_json::from_value(confirmation["request"].clone()).map_err(codec)?;
        if !matches!(request, CommandRequest::Delete | CommandRequest::ClearAll) {
            return Err(Error::Invalid("invalid confirmation request".into()));
        }
        if accepted {
            let id = self.request_id(operation)?;
            let file = files::TaskFile::Request(id);
            let key = if request == CommandRequest::Delete {
                let task = TaskId::parse(
                    confirmation["task"]
                        .as_str()
                        .ok_or_else(|| Error::Invalid("confirmation task missing".into()))?,
                )?;
                files::FileKey::Task {
                    task: component(task.to_string())?,
                    file,
                }
            } else {
                files::FileKey::Session {
                    session: component(self.session.to_string())?,
                    file,
                }
            };
            let bytes = serde_json::to_vec(&request).map_err(codec)?;
            let saved = self.read_file(&key).await?;
            if self.generation != cut
                && self
                    .state(CommandFileRoot::Session, "layout.json")?
                    .and_then(|layout| layout.get("confirmation").cloned())
                    .as_ref()
                    != Some(&confirmation)
            {
                return Err(Error::Conflict(
                    "confirmation changed; review the current layout".into(),
                ));
            }
            match saved {
                Some(saved) if saved.bytes.as_ref() == bytes => {}
                Some(_) => return Err(Error::Conflict("confirmation request changed".into())),
                // A layout withdrawal must invalidate this checked cut. Never
                // rebase a destructive confirmation past a conflicting write.
                None => self.publish(key, bytes, true).await?,
            }
        }
        let mut layout = self
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or_else(|| json!({}));
        if layout.get("confirmation") != Some(&confirmation) {
            return Ok(operation);
        }
        layout
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
            .remove("confirmation");
        self.write_state(CommandFileRoot::Session, "layout.json", &layout)
            .await?;
        Ok(operation)
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
#[path = "session_confirmation/tests.rs"]
mod tests;
