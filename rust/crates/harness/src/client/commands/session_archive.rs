use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(crate) fn first_output_sequence(&self) -> Option<u64> {
        self.outputs.keys().next().copied()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::client::commands) fn view_bounds(&self) -> (usize, usize) {
        (self.maximum_files, self.maximum_bytes as usize)
    }

    pub(super) async fn apply_outcome(&mut self, result: &Value) -> Result<()> {
        if let Some(task) = result.get("selected_task") {
            loop {
                let mut layout = self
                    .state(CommandFileRoot::Session, "layout.json")?
                    .unwrap_or_else(|| serde_json::json!({}));
                layout["selected_task"] = task.clone();
                if result["cleared"] == true {
                    layout["history_after"] = serde_json::json!(self.output_sequence());
                }
                match self
                    .write_state(CommandFileRoot::Session, "layout.json", &layout)
                    .await
                {
                    Ok(()) => break,
                    Err(Error::Conflict(message))
                        if message == "command query advanced; retry from the new cut" =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Last successfully rendered result, independent of the query cursor.
    pub fn rendered_sequence(&self) -> Result<u64> {
        Ok(self
            .state(CommandFileRoot::Session, "layout.json")?
            .and_then(|layout| layout["rendered_output"].as_u64())
            .unwrap_or(0))
    }

    /// Acknowledge output only after the renderer accepts it. A competing layout
    /// write is followed and merged again; navigation cannot be overwritten.
    pub async fn acknowledge_output(&mut self, sequence: u64) -> Result<()> {
        loop {
            let mut layout = self
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or(json!({}));
            if layout["rendered_output"].as_u64().unwrap_or(0) >= sequence {
                return Ok(());
            }
            layout["rendered_output"] = json!(sequence);
            match self
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await
            {
                Ok(()) => return Ok(()),
                Err(Error::Conflict(message))
                    if message == "command query advanced; retry from the new cut" =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Whether this exact error is stored in the request's immutable outcome.
    /// A claim, successful result or different storage failure is insufficient.
    pub async fn failure_is_saved(
        &mut self,
        request: &files::FileValue,
        failure: &Error,
    ) -> Result<bool> {
        let key = related(&request.key, files::TaskFile::ExecutionResult)
            .ok_or_else(|| Error::Invalid("not a command request".into()))?;
        let Some(file) = self.read_file(&key).await? else {
            return Ok(false);
        };
        let outcome: Value = serde_json::from_slice(&file.bytes).map_err(codec)?;
        Ok(outcome["error"].as_str() == Some(failure.to_string().as_str()))
    }

    pub(super) async fn read_file(
        &mut self,
        key: &files::FileKey,
    ) -> Result<Option<files::FileValue>> {
        let snapshot = self
            .volume
            .snapshot(&files::Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if snapshot.generation.volume != self.generation.volume
            || snapshot.generation.epoch != self.generation.epoch
        {
            return Err(Error::Conflict(
                "command query history changed; reopen".into(),
            ));
        }
        while self.generation.position < snapshot.generation.position {
            self.advance().await?;
        }
        Ok(snapshot.files.into_iter().next())
    }

    pub(super) async fn load_task_settings(&mut self) -> Result<()> {
        let Some(task) = self.selected_task()? else {
            return Ok(());
        };
        let directory = format!("task-{task}");
        self.files.retain(|key, _| !matches!(key, files::FileKey::Ui { directory: dir, .. } if dir.as_str().starts_with("task-") && dir.as_str() != directory));
        let key = ui(&directory, "settings.json")?;
        if !self.files.contains_key(&key) {
            if let Some(file) = self.read_file(&key).await? {
                self.fold(file)?;
            }
        }
        Ok(())
    }

    fn evict_oldest(&mut self) -> bool {
        let Some((_, key)) = self.outputs.pop_first() else {
            return false;
        };
        self.files.remove(&key);
        for make in [files::TaskFile::Request, files::TaskFile::ExecutionClaim] {
            if let Some(key) = related(&key, make) {
                self.files.remove(&key);
            }
        }
        true
    }

    pub(super) fn make_room(&mut self, additional: usize) -> Result<()> {
        while self
            .files
            .len()
            .checked_add(additional)
            .is_none_or(|n| n > self.maximum_files)
        {
            if !self.evict_oldest() {
                return Err(Error::Conflict(
                    "active command inbox must drain before publication".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn trim_archive(&mut self) {
        // Retain recent outcomes and identities; older files remain queryable.
        while self.outputs.len() > (self.maximum_files / 4).max(1) {
            self.evict_oldest();
        }
    }

    pub(super) fn fold_activity_file(&mut self, file: &files::FileValue) -> Result<()> {
        if file.key == self.state_key(CommandFileRoot::Session, "activity.json")? {
            let value: (u64, String) = serde_json::from_slice(&file.bytes).map_err(codec)?;
            if self
                .activity_cursor
                .as_ref()
                .is_none_or(|(sequence, _)| *sequence < value.0)
            {
                self.activity_cursor = Some(value);
            }
        }
        Ok(())
    }

    pub(super) async fn persist_activity(&mut self) -> Result<()> {
        while let Some(value) = &self.activity_cursor {
            let previous = self.generation;
            let bytes = serde_json::to_vec(value).map_err(codec)?;
            match self
                .publish(
                    self.state_key(CommandFileRoot::Session, "activity.json")?,
                    bytes,
                    false,
                )
                .await
            {
                Ok(()) => break,
                Err(Error::Conflict(_))
                    if self.generation.position > previous.position
                        && self.next_change.is_none() =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

// Stop the serializer at the encoded bound, including JSON escaping and wrapper
// fields. Overflow still publishes a terminal outcome; it cannot strand a claim.
struct Output {
    bytes: Vec<u8>,
    maximum: u64,
    overflow: bool,
}
impl std::io::Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.bytes.len() as u64)
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > self.maximum)
        {
            self.overflow = true;
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn output_bytes(sequence: u64, result: &Result<Value>, maximum: u64) -> Result<Vec<u8>> {
    use serde::{Serializer as _, ser::SerializeMap as _};
    let mut output = Output {
        bytes: Vec::new(),
        maximum,
        overflow: false,
    };
    let encoded = (|| -> serde_json::Result<()> {
        let mut serializer = serde_json::Serializer::new(&mut output);
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("sequence", &sequence)?;
        let completion = result.as_ref().map_or_else(
            |error| crate::client::HeadlessOutcome::from_error(error),
            |_| crate::client::HeadlessOutcome::Completed,
        );
        map.serialize_entry("completion", &completion)?;
        match result {
            Ok(value) => map.serialize_entry("ok", value)?,
            Err(error) => map.serialize_entry("error", &error.to_string())?,
        }
        map.end()
    })();
    if output.overflow {
        output.bytes.clear();
        output.overflow = false;
        serde_json::to_writer(
            &mut output,
            &json!({"sequence":sequence,"completed":true,"output_omitted":true,
                "completion":result.as_ref().map_or_else(|error| crate::client::HeadlessOutcome::from_error(error), |_| crate::client::HeadlessOutcome::Completed)}),
        )
        .map_err(codec)?;
    } else {
        encoded.map_err(codec)?;
    }
    Ok(output.bytes)
}
