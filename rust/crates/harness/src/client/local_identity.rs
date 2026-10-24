//! Launch identity belongs to a submission, independent of its prompt bytes.
use crate::task_volume::{FileKey, FileValue, Query};
use crate::{Error, OperationId, Result, local::LocalHarness};

/// Reopen a retained session by clearing only its presentation exit flag.
/// Selection, prompts, outcomes and admission identities remain in their files.
pub async fn resume_local_identity(
    local: &LocalHarness,
    explicit: Option<OperationId>,
) -> Result<OperationId> {
    let files = local.task_files();
    let operation = if let Some(operation) = explicit {
        operation
    } else {
        let key = FileKey::parse(".ui/workspace/interactive.json")?;
        let saved = files.snapshot(&Query::File(key), 1).await?;
        let file = saved
            .files
            .first()
            .ok_or_else(|| Error::NotFound("interactive conversation".into()))?;
        serde_json::from_slice(&file.bytes).map_err(|error| Error::Invalid(error.to_string()))?
    };
    let key = FileKey::parse(&format!(".ui/session-{operation}/layout.json"))?;
    loop {
        let saved = files.snapshot(&Query::File(key.clone()), 1).await?;
        let file = saved
            .files
            .first()
            .ok_or_else(|| Error::NotFound("interactive session layout".into()))?;
        let mut layout: serde_json::Value = serde_json::from_slice(&file.bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let object = layout
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("session layout must be an object".into()))?;
        if object.remove("exit_requested").is_none() {
            return Ok(operation);
        }
        let value = FileValue::new(
            key.clone(),
            serde_json::to_vec(&layout).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        match files.write(saved.generation, &value).await {
            Ok(_) => return Ok(operation),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

/// Allocate a fresh submission unless the caller provides its retained identity.
#[must_use]
pub fn local_identity(explicit: Option<OperationId>) -> OperationId {
    explicit.unwrap_or_default()
}

/// Keep the selected interactive session available to an explicit resume.
pub async fn select_local_identity(local: &LocalHarness, operation: OperationId) -> Result<()> {
    let files = local.task_files();
    let key = FileKey::parse(".ui/workspace/interactive.json")?;
    let value = FileValue::new(
        key.clone(),
        serde_json::to_vec(&operation).map_err(|error| Error::Invalid(error.to_string()))?,
    );
    loop {
        let saved = files.snapshot(&Query::File(key.clone()), 1).await?;
        match files.write(saved.generation, &value).await {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}
