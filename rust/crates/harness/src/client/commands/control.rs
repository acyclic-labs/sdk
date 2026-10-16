//! File-backed desired execution state, applied at owner dispatch boundaries.
use super::*;
use crate::{
    TaskId,
    task_volume::{FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::StreamProvider;
use futures::StreamExt as _;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Desired subtree state. Later ancestor/all actions supersede earlier actions;
/// a later child resume can independently resume that subtree.
pub struct CommandControl {
    all: Option<(u64, bool)>,
    tasks: BTreeMap<TaskId, (u64, bool)>,
}
impl CommandControl {
    /// Reconstruct only from a bounded authoritative file.
    pub fn from_bytes(bytes: &[u8], maximum: u64) -> Result<Self> {
        if bytes.len() as u64 > maximum {
            return Err(Error::Invalid("control file exceeds bound".into()));
        }
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
    }
    /// Latest matching scope wins, independent of the order of lineage input.
    pub fn paused(&self, lineage: &[TaskId]) -> bool {
        lineage
            .iter()
            .filter_map(|task| self.tasks.get(task))
            .chain(self.all.as_ref())
            .max_by_key(|(revision, _)| revision)
            .is_some_and(|(_, paused)| *paused)
    }
    pub(crate) fn set(&mut self, task: Option<TaskId>, revision: u64, paused: bool) {
        if let Some(task) = task {
            self.tasks.insert(task, (revision, paused));
        } else {
            self.all = Some((revision, paused));
        }
    }
    pub(crate) fn key() -> Result<FileKey> {
        FileKey::parse(".ui/config/task-control.json")
            .map_err(|error| Error::Invalid(error.to_string()))
    }
    pub(crate) async fn change<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        task: Option<TaskId>,
        lineage: &[TaskId],
        paused: Option<bool>,
        maximum: u64,
    ) -> Result<bool> {
        let key = Self::key()?;
        let snapshot = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        let mut control = snapshot
            .files
            .first()
            .map(|file| Self::from_bytes(&file.bytes, maximum))
            .transpose()?
            .unwrap_or_default();
        let paused = paused.unwrap_or_else(|| !control.paused(lineage));
        control.set(task, snapshot.generation.position, paused);
        let bytes =
            serde_json::to_vec(&control).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > maximum {
            return Err(Error::Invalid("control file exceeds bound".into()));
        }
        volume
            .append(snapshot.generation, &FileValue::new(key, bytes))
            .await
            .map_err(storage)?;
        Ok(paused)
    }
    /// Subscribe before testing the snapshot. No polling, task-local flag or
    /// separate wake channel survives across suspension or process restart.
    pub async fn wait<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        lineage: &[TaskId],
        maximum: u64,
    ) -> Result<()> {
        let mut observed = volume
            .observe(Query::File(Self::key()?), None, 1)
            .await
            .map_err(storage)?;
        let snapshot = observed
            .snapshot
            .take()
            .ok_or_else(|| Error::Conflict("control snapshot absent".into()))?;
        let mut control = snapshot
            .files
            .first()
            .map(|file| Self::from_bytes(&file.bytes, maximum))
            .transpose()?
            .unwrap_or_default();
        while control.paused(lineage) {
            if let Some(file) = observed
                .changes
                .next()
                .await
                .ok_or_else(|| Error::Storage("control follow ended".into()))?
                .map_err(storage)?
                .file
            {
                control = Self::from_bytes(&file.bytes, maximum)?;
            }
        }
        Ok(())
    }
}
fn storage(error: crate::task_volume::Error) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(all(test, acyclic_loom))]
#[path = "control_models.rs"]
mod models;
