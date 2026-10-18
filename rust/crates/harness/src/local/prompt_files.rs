//! Retain submitted file pins before admission; recovery uses this immutable input.
use super::*;

fn input_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.prompt-files/v1\0");
    hash.update(operation.to_string().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
impl LocalHarness {
    pub(super) async fn retain_prompt_input(
        &self,
        operation: OperationId,
        input: &ModelContent,
    ) -> Result<()> {
        input.validate_user_input(&mut None)?;
        if !input
            .parts()
            .iter()
            .any(|part| matches!(part, crate::model::ModelContentPart::File { .. }))
        {
            return Ok(());
        }
        let reader = self.client_reader()?;
        for part in input.parts() {
            if let crate::model::ModelContentPart::File { file, .. } = part {
                reader.verify(file, self.limits.file_bytes, None)?.await?;
            }
        }
        let bytes = serde_json::to_vec(input).map_err(storage)?;
        self.input_publisher()?
            .stage_once(
                input_operation(operation),
                &format!("inputs/{operation}.prompt.json"),
                &bytes,
                "application/json",
                "submitted prompt",
                None,
            )?
            .await?;
        Ok(())
    }
    pub(super) async fn prompt_input(
        &self,
        operation: OperationId,
        prompt: &str,
        publisher: &dyn ContentPublisher,
        maximum_bytes: u64,
    ) -> Result<ModelContent> {
        let (file, hold) = publisher
            .observe_staged_once(input_operation(operation), maximum_bytes, None)?
            .await?;
        let Some(file) = file else {
            return Ok(ModelContent::Text(prompt.into()));
        };
        if file.path() != format!("inputs/{operation}.prompt.json") {
            return Err(Error::Conflict("submitted prompt has another path".into()));
        }
        let reader = self.client_reader()?;
        let (bytes, _read_hold) = crate::conversation::verified_content_bytes(
            reader.as_ref(),
            &file,
            maximum_bytes,
            None,
        )
        .await?;
        let mut input: ModelContent = serde_json::from_slice(&bytes).map_err(storage)?;
        input.validate_user_input(&mut None)?;
        match &mut input {
            ModelContent::Text(text)
            | ModelContent::Part(crate::model::ModelContentPart::Text { text }) => {
                *text = prompt.into()
            }
            ModelContent::Part(_) => {
                return Err(Error::Invalid("submitted prompt lost its text".into()));
            }
            ModelContent::Parts(parts) => {
                let Some(crate::model::ModelContentPart::Text { text }) = parts.first_mut() else {
                    return Err(Error::Invalid("submitted prompt lost its text".into()));
                };
                *text = prompt.into();
            }
        }
        drop(hold);
        Ok(input)
    }
}
