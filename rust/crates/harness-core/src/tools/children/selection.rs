use super::*;
use crate::runtime::OwnedTaskMailbox;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
thread_local! {
    pub(super) static IGNORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static SWALLOW_SAVE_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static DROP_METADATA: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn key(task: crate::TaskId, operation: crate::OperationId) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: component(task.to_string())?,
        file: crate::task_volume::TaskFile::ChildWait(component(operation.to_string())?),
    })
}

pub(super) async fn load<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    key: &FileKey,
    binding: &Value,
) -> Result<Option<Value>> {
    #[cfg(test)]
    #[cfg(not(acyclic_harness_loom))]
    if IGNORE.with(std::cell::Cell::get) {
        return Ok(None);
    }
    let snapshot = files
        .inner
        .snapshot_files(&[key.clone()], &Query::File(key.clone()))
        .await
        .map_err(storage)?;
    let Some(file) = snapshot.files.first() else {
        return Ok(None);
    };
    let value: Value = serde_json::from_slice(&file.bytes).map_err(storage)?;
    let binding = serde_json::to_value(
        blake3::hash(&serde_json::to_vec(binding).map_err(invalid)?).as_bytes(),
    )
    .map_err(invalid)?;
    if value.get("input") != Some(&binding) {
        return Err(Error::Conflict("wait operation was rebound".into()));
    }
    let mut result = value
        .get("result")
        .cloned()
        .ok_or_else(|| invalid("wait selection has no result"))?;
    if value["inline"] == true {
        let path = result["agents"][0]["result"]["file"]["path"]
            .as_str()
            .ok_or_else(|| invalid("wait selection has no result path"))?;
        let key = FileKey::parse(
            path.strip_prefix(".volumes/")
                .ok_or_else(|| invalid("wait result is outside the volume"))?,
        )
        .map_err(storage)?;
        let snapshot = files
            .inner
            .snapshot_files(&[key.clone()], &Query::File(key))
            .await
            .map_err(storage)?;
        let file = snapshot
            .files
            .first()
            .ok_or_else(|| invalid("selected immutable result is absent"))?;
        result["agents"][0]["result"]["outcome"] = serde_json::to_value(
            crate::task_volume::ChildTurnResult::parse(&file.bytes)
                .map_err(storage)?
                .outcome,
        )
        .map_err(storage)?;
    }
    #[cfg(test)]
    #[cfg(not(acyclic_harness_loom))]
    if DROP_METADATA.with(std::cell::Cell::get) {
        if let Some(outcome) = result["agents"][0]["result"]["outcome"]["Succeeded"].as_object_mut()
        {
            outcome.remove("metadata");
        }
    }
    Ok(Some(result))
}

pub(super) async fn save<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    key: FileKey,
    binding: Value,
    result: Value,
    owner: Option<&dyn OwnedTaskMailbox>,
) -> Result<()> {
    #[cfg(test)]
    #[cfg(not(acyclic_harness_loom))]
    if IGNORE.with(std::cell::Cell::get) {
        return Ok(());
    }
    let binding = blake3::hash(&serde_json::to_vec(&binding).map_err(invalid)?);
    let mut result = result;
    let inline = result["agents"][0]["result"]
        .as_object_mut()
        .is_some_and(|result| result.remove("outcome").is_some());
    let file = FileValue::new(
        key.clone(),
        serde_json::to_vec(&json!({"input":binding.as_bytes(),"result":result,"inline":inline}))
            .map_err(invalid)?,
    );
    if file.bytes.len() as u64 > files.maximum_bytes() {
        return Err(invalid("wait selection exceeds the original file bound"));
    }
    let snapshot = files
        .inner
        .snapshot_files(&[], &Query::File(key))
        .await
        .map_err(storage)?;
    crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
        match owner {
            Some(owner) => {
                let (condition, deadline) = owner.file_publication_condition().await?;
                Ok::<_, Error>((vec![condition], deadline))
            }
            None => Ok((Vec::new(), None)),
        }
    })
    .await?;
    Ok(())
}
