//! Admission-time inheritance and disposable, file-backed ancestry memoization.
use super::*;
use crate::{
    runtime::TaskStateProvider,
    task_volume::{FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;

fn key(task: TaskId, name: &str) -> Result<FileKey> {
    let path = if matches!(name, "settings" | "context") {
        format!(".ui/task-{task}/{name}.json")
    } else {
        format!(".tasks/{task}/context/command-{name}/manifest.json")
    };
    FileKey::parse(&path).map_err(storage)
}
async fn read<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    key: &FileKey,
    maximum: u64,
) -> Result<Option<Value>> {
    let cut = Box::pin(volume.snapshot(&Query::File(key.clone()), 1))
        .await
        .map_err(storage)?;
    cut.files
        .first()
        .map(|file| decode(&file.bytes, maximum))
        .transpose()
}
fn decode(bytes: &[u8], maximum: u64) -> Result<Value> {
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid("inherited state exceeds file bound".into()));
    }
    serde_json::from_slice(bytes).map_err(storage)
}
fn validate(value: &Value) -> Result<()> {
    if !value.is_object()
        || !value["model"].is_null() && !value["model"].is_string()
        || !value["summary"].is_null()
            && !value["summary"].is_string()
            && crate::context::SummarySelection::deserialize(&value["summary"]).is_err()
    {
        return Err(Error::Invalid(
            "invalid inherited settings or context".into(),
        ));
    }
    serde_json::from_value::<Option<TaskId>>(
        value
            .get("parent")
            .cloned()
            .ok_or_else(|| Error::Invalid("inherited parent is absent".into()))?,
    )
    .map_err(storage)?;
    Ok(())
}
// Claim the immutable parent before copying values. Partial preparation can be
// completed by that parent only; admission follows both retained value files.
async fn read_inheritance<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    maximum: u64,
) -> Result<Option<Value>> {
    let Some(parent) = read(volume, &key(task, "inherited")?, maximum).await? else {
        return Ok(None);
    };
    let mut value = json!({"parent":parent});
    for field in ["model", "summary"] {
        let Some(saved) = read(volume, &key(task, &format!("inherited-{field}"))?, maximum).await?
        else {
            return Ok(None);
        };
        value[field] = saved;
    }
    validate(&value)?;
    Ok(Some(value))
}
async fn retain_inheritance<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    mut value: Value,
    maximum: u64,
) -> Result<Value> {
    validate(&value)?;
    let parent = serde_json::from_value(value["parent"].clone()).map_err(storage)?;
    value["parent"] = claim_parent(volume, task, parent, maximum).await?;
    for field in ["model", "summary"] {
        value[field] = retain(
            volume,
            key(task, &format!("inherited-{field}"))?,
            value[field].take(),
            maximum,
        )
        .await?;
    }
    Ok(value)
}
fn require_parent(retained: &Value, expected: Option<TaskId>) -> Result<()> {
    #[cfg(all(test, acyclic_harness_loom))]
    if model::broken_parent() {
        return Ok(());
    }
    if retained != &json!(expected) {
        return Err(Error::Conflict(
            "task inheritance belongs to a different parent".into(),
        ));
    }
    Ok(())
}
async fn claim_parent<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    parent: Option<TaskId>,
    maximum: u64,
) -> Result<Value> {
    let retained = retain(volume, key(task, "inherited")?, json!(parent), maximum).await?;
    require_parent(&retained, parent)?;
    Ok(retained)
}
async fn prepare<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    parent: Option<TaskId>,
    maximum: u64,
    admission: BoxProviderFuture<'_, Result<Option<Option<TaskId>>>>,
    resolve: impl AsyncFnOnce() -> Result<Value>,
) -> Result<()> {
    // The owner is authoritative even when a legacy/crashed admission has no
    // inheritance files. Reject its changed parent before any cache publication.
    let admitted = admission.await?;
    #[cfg(all(test, acyclic_harness_loom))]
    let admitted = if model::unchecked_admission() {
        None
    } else {
        admitted
    };
    if let Some(admitted) = admitted {
        require_parent(&json!(admitted), parent)?;
    }
    if let Some(saved) = read_inheritance(volume, task, maximum).await? {
        return require_parent(&saved["parent"], parent);
    }
    claim_parent(volume, task, parent, maximum).await?;
    let mut value = resolve().await?;
    value["parent"] = json!(parent);
    let retained = retain_inheritance(volume, task, value, maximum).await?;
    require_parent(&retained["parent"], parent)
}
// Admission is the outer lock, followed by task-volume/provider leaf locks.
// The guard covers preparation and admission only, never tool/model execution.
pub(super) fn admit_prepared<'a, T: Send + 'a>(
    gate: &'a crate::sync::owned::Arc<crate::sync::owned::Mutex<()>>,
    preparation: BoxProviderFuture<'a, Result<()>>,
    admit: BoxProviderFuture<'a, Result<T>>,
) -> BoxProviderFuture<'a, Result<T>> {
    Box::pin(async move {
        super::admission::serialized(gate, async || {
            preparation.await?;
            admit.await
        })
        .await
    })
}
async fn cached_parent<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    maximum: u64,
) -> Result<Option<Option<TaskId>>> {
    read(volume, &key(task, "inherited")?, maximum)
        .await?
        .map(|value| serde_json::from_value(value).map_err(storage))
        .transpose()
}
// First publication pins inheritance. A competing creator's accepted value,
// including after a lost acknowledgement, is always the value returned.
async fn retain<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    key: FileKey,
    value: Value,
    maximum: u64,
) -> Result<Value> {
    let mut cut = Box::pin(volume.snapshot(&Query::File(key.clone()), 1))
        .await
        .map_err(storage)?;
    let bytes = serde_json::to_vec(&value).map_err(storage)?;
    decode(&bytes, maximum)?;
    loop {
        if let Some(file) = cut.files.first() {
            return decode(&file.bytes, maximum);
        }
        match Box::pin(volume.create(cut.generation, &FileValue::new(key.clone(), bytes.clone())))
            .await
        {
            Ok(_) => return Ok(value),
            Err(crate::task_volume::Error::Conflict(_)) => {
                cut = Box::pin(volume.snapshot(&Query::File(key.clone()), 1))
                    .await
                    .map_err(storage)?;
            }
            Err(error) => return Err(storage(error)),
        }
    }
}
impl LocalHarness {
    pub(super) async fn prepare_inheritance(
        &self,
        task: TaskId,
        parent: Option<TaskId>,
    ) -> Result<()> {
        prepare(
            &self.task_files.inner,
            task,
            parent,
            self.limits.file_bytes,
            Box::pin(async {
                match self.runtime.task_host().observe_admission(task).await {
                    Ok(record) => Ok(Some(record.parent)),
                    Err(Error::NotFound(_)) => Ok(None),
                    Err(error) => Err(error),
                }
            }),
            async || match parent {
                Some(parent) => self.binding_inheritance(parent).await,
                None => match self.continuation_selection(task).await? {
                    Some(selected) => {
                        let checkpoint = self.read_checkpoint(&selected.checkpoint).await?;
                        Ok(json!({"model":checkpoint.boundary.model,"summary":null}))
                    }
                    None => Ok(json!({"model":null,"summary":null})),
                },
            },
        )
        .await
    }
    fn inherited(&self, task: TaskId) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(async move {
            if let Some(value) =
                read_inheritance(&self.task_files.inner, task, self.limits.file_bytes).await?
            {
                return Ok(value);
            }
            // Upgrade admissions made before inheritance was retained. Stop at the
            // first saved binding and publish each missing prefix exactly once.
            let record = self.runtime.task_host().observe_admission(task).await?;
            let context = self
                .runtime
                .harness()
                .durable_context(task, record.operation_id, None)
                .await?;
            let ancestors = context.ancestors(task)?;
            futures::pin_mut!(ancestors);
            let mut missing = Vec::new();
            let mut parent = None;
            let mut value = json!({"model":null,"summary":null});
            while let Some(candidate) = futures::TryStreamExt::try_next(&mut ancestors).await? {
                if let Some(saved) =
                    read_inheritance(&self.task_files.inner, candidate, self.limits.file_bytes)
                        .await?
                {
                    value = saved;
                    parent = Some(candidate);
                    break;
                }
                missing.push(candidate);
            }
            for candidate in missing.into_iter().rev() {
                if let Some(parent) = parent {
                    value = self.apply_own_binding(parent, value).await?;
                }
                value["parent"] = json!(parent);
                value = retain_inheritance(
                    &self.task_files.inner,
                    candidate,
                    value,
                    self.limits.file_bytes,
                )
                .await?;
                parent = Some(candidate);
            }
            Ok(value)
        })
    }
    #[cfg(test)]
    pub(super) async fn resolved_inheritance(&self, task: TaskId) -> Result<Value> {
        let mut value = self.binding_inheritance(task).await?;
        if value["summary"].is_object() {
            value["summary"] = json!(self.resolve_summary(&value["summary"]).await?);
        }
        Ok(value)
    }
    async fn binding_inheritance(&self, task: TaskId) -> Result<Value> {
        self.apply_own_binding(task, self.inherited(task).await?)
            .await
    }
    /// Resolves a not-yet-admitted task without walking runtime ancestry.
    /// Parented and continuation admissions prepare their inheritance first;
    /// an ordinary root has only its own retained settings and global default.
    #[cfg(feature = "native-execution")]
    pub(super) async fn admission_model(&self, task: TaskId) -> Result<Model> {
        let mut model = self.command_model(None).await?;
        let selected = match self.own_value(task, "model").await? {
            Some(name) => Some(name),
            None => read_inheritance(&self.task_files.inner, task, self.limits.file_bytes)
                .await?
                .and_then(|value| {
                    value
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }),
        };
        if let Some(name) = selected {
            model.name = name;
        }
        model.validate()?;
        Ok(model)
    }
    /// The parent already claimed by admission preparation, without requiring
    /// a runnable task or constructing its ephemeral physical tool mount.
    #[cfg(feature = "native-execution")]
    pub(super) async fn admission_parent(&self, task: TaskId) -> Result<Option<TaskId>> {
        Ok(
            cached_parent(&self.task_files.inner, task, self.limits.file_bytes)
                .await?
                .flatten(),
        )
    }
    async fn resolve_summary(&self, value: &Value) -> Result<Option<String>> {
        if value.is_object() {
            let selected = crate::context::SummarySelection::deserialize(value).map_err(storage)?;
            self.checkpoint_text(&selected).await.map(Some)
        } else {
            Ok(value.as_str().map(str::to_owned))
        }
    }
    pub(super) async fn resolved_value(&self, task: TaskId, field: &str) -> Result<Option<String>> {
        if let Some(value) = self.own_value(task, field).await? {
            return Ok(Some(value));
        }
        let inherited = self.inherited(task).await?;
        if field == "summary" {
            self.resolve_summary(&inherited[field]).await
        } else {
            Ok(inherited[field].as_str().map(str::to_owned))
        }
    }
    async fn own_value(&self, task: TaskId, field: &str) -> Result<Option<String>> {
        let name = if field == "model" {
            "settings"
        } else {
            "context"
        };
        let Some(value) = read(
            &self.task_files.inner,
            &key(task, name)?,
            self.limits.file_bytes,
        )
        .await?
        else {
            return if field == "summary" {
                match self.continuation_selection(task).await? {
                    Some(selected) => self.checkpoint_text(&selected).await.map(Some),
                    None => Ok(None),
                }
            } else {
                Ok(None)
            };
        };
        if !value.is_object() {
            return Err(Error::Invalid(format!("{name} must be an object")));
        }
        if field == "summary" && value.get("checkpoint").is_some() {
            let selected = serde_json::from_value(value).map_err(storage)?;
            return self.checkpoint_text(&selected).await.map(Some);
        }
        match value.get(field) {
            Some(text) => text
                .as_str()
                .map(|text| Some(text.to_owned()))
                .ok_or_else(|| Error::Invalid(format!("{field} must be text"))),
            None if field == "model" => Ok(None),
            None => Err(Error::Invalid("compacted context missing summary".into())),
        }
    }
    async fn apply_own_binding(&self, task: TaskId, mut value: Value) -> Result<Value> {
        validate(&value)?;
        if let Some(model) = self.own_value(task, "model").await? {
            value["model"] = json!(model);
        }
        let (cut, selected, _) = super::compact::checkpoints::capture_selection(
            &self.task_files.inner,
            task,
            self.limits.file_bytes,
        )
        .await?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        tests::compaction_race::after_capture(task).await?;
        if let Some(mut selected) = selected {
            if value["model"].is_null() {
                value["model"] = json!(
                    self.read_checkpoint(&selected.checkpoint)
                        .await?
                        .boundary
                        .model
                );
            }
            selected.through.get_or_insert(cut.cursor);
            value["summary"] = serde_json::to_value(selected).map_err(storage)?;
            return Ok(value);
        }
        let context_key = key(task, "context")?;
        if let Some(file) = cut.files.iter().find(|file| file.key == context_key) {
            // The legacy inline fallback belongs to this observation too. A fresh
            // read could resolve a first checkpoint published after capture.
            let mut context = decode(&file.bytes, self.limits.file_bytes)?;
            let context = context
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("context must be an object".into()))?;
            let summary = context
                .remove("summary")
                .ok_or_else(|| Error::Invalid("compacted context missing summary".into()))?;
            if !summary.is_string() {
                return Err(Error::Invalid("summary must be text".into()));
            }
            value["summary"] = summary;
        }
        Ok(value)
    }
    pub(super) async fn command_has_ancestor(
        &self,
        task: Option<TaskId>,
        scope: TaskId,
    ) -> Result<bool> {
        has_ancestor(
            &self.task_files.inner,
            task,
            scope,
            self.limits.file_bytes,
            |task| {
                Box::pin(async move {
                    if let Some(parent) =
                        cached_parent(&self.task_files.inner, task, self.limits.file_bytes).await?
                    {
                        return Ok(parent);
                    }
                    serde_json::from_value(self.inherited(task).await?["parent"].clone())
                        .map_err(storage)
                })
            },
        )
        .await
    }
}
async fn has_ancestor<'a, P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: Option<TaskId>,
    scope: TaskId,
    maximum: u64,
    parent_of: impl Fn(TaskId) -> BoxProviderFuture<'a, Result<Option<TaskId>>> + Sync,
) -> Result<bool> {
    let mut cursor = task;
    let mut missing = Vec::new();
    let mut visited = HashSet::new();
    let name = format!("ancestor-{scope}");
    let mut matches = false;
    while let Some(current) = cursor {
        if current == scope {
            matches = true;
            break;
        }
        if !visited.insert(current) {
            return Err(Error::Invalid("cyclic task inheritance".into()));
        }
        if let Some(saved) = read(volume, &key(current, &name)?, maximum).await? {
            matches = saved
                .as_bool()
                .ok_or_else(|| Error::Invalid("ancestor cache must be boolean".into()))?;
            break;
        }
        cursor = parent_of(current).await?;
        missing.push(current);
    }
    // The control file bounds the scope set. Each (task, scope) edge is
    // visited once; later descendants stop at their parent's cached answer.
    for task in missing {
        let saved = retain(volume, key(task, &name)?, json!(matches), maximum).await?;
        if saved != matches {
            return Err(Error::Conflict("task ancestry cache changed".into()));
        }
    }
    Ok(matches)
}

#[cfg(all(test, acyclic_harness_loom))]
mod model;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
