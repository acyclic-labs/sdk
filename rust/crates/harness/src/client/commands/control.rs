//! File-backed desired execution state, applied at owner dispatch boundaries.
use super::*;
use crate::{
    TaskId,
    task_volume::{FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
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
    async fn paused_matching<'a>(
        &self,
        matches: &(impl Fn(TaskId) -> BoxProviderFuture<'a, Result<bool>> + Sync),
    ) -> Result<bool> {
        let mut latest = self.all;
        for (task, state) in &self.tasks {
            if latest.is_none_or(|(revision, _)| revision < state.0) && matches(*task).await? {
                latest = Some(*state);
            }
        }
        Ok(latest.is_some_and(|(_, paused)| paused))
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
        Self::change_matching(
            volume,
            task,
            |task| Box::pin(async move { Ok(lineage.contains(&task)) }),
            paused,
            maximum,
        )
        .await
    }
    pub(crate) async fn change_matching<'a, P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        task: Option<TaskId>,
        matches: impl Fn(TaskId) -> BoxProviderFuture<'a, Result<bool>> + Sync,
        paused: Option<bool>,
        maximum: u64,
    ) -> Result<bool> {
        let snapshot = volume
            .snapshot(&Query::File(Self::key()?), 1)
            .await
            .map_err(storage)?;
        Self::publish_matching(volume, task, &matches, paused, maximum, snapshot).await
    }
    #[cfg(all(test, acyclic_loom))]
    async fn publish_change<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        task: Option<TaskId>,
        lineage: &[TaskId],
        paused: Option<bool>,
        maximum: u64,
        snapshot: crate::task_volume::Snapshot,
    ) -> Result<bool> {
        Self::publish_matching(
            volume,
            task,
            &|task| {
                let lineage = &lineage;
                Box::pin(async move { Ok(lineage.contains(&task)) })
            },
            paused,
            maximum,
            snapshot,
        )
        .await
    }
    async fn publish_matching<'a, P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        task: Option<TaskId>,
        matches: &(impl Fn(TaskId) -> BoxProviderFuture<'a, Result<bool>> + Sync),
        paused: Option<bool>,
        maximum: u64,
        mut snapshot: crate::task_volume::Snapshot,
    ) -> Result<bool> {
        let key = Self::key()?;
        loop {
            let mut control = snapshot
                .files
                .first()
                .map(|file| Self::from_bytes(&file.bytes, maximum))
                .transpose()?
                .unwrap_or_default();
            let desired = match paused {
                Some(paused) => paused,
                None => !control.paused_matching(matches).await?,
            };
            control.set(task, snapshot.generation.position, desired);
            let bytes =
                serde_json::to_vec(&control).map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() as u64 > maximum {
                return Err(Error::Invalid("control file exceeds bound".into()));
            }
            match volume
                .append(snapshot.generation, &FileValue::new(key.clone(), bytes))
                .await
            {
                Ok(_) => return Ok(desired),
                Err(crate::task_volume::Error::Conflict(_)) => {
                    snapshot = volume
                        .snapshot(&Query::File(key.clone()), 1)
                        .await
                        .map_err(storage)?;
                }
                Err(error) => return Err(storage(error)),
            }
        }
    }
    /// Subscribe before testing the snapshot. No polling, task-local flag or
    /// separate wake channel survives across suspension or process restart.
    pub async fn wait<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        lineage: &[TaskId],
        maximum: u64,
    ) -> Result<()> {
        Self::wait_matching(
            volume,
            |task| Box::pin(async move { Ok(lineage.contains(&task)) }),
            maximum,
        )
        .await
    }
    pub(crate) async fn wait_matching<'a, P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        matches: impl Fn(TaskId) -> BoxProviderFuture<'a, Result<bool>> + Sync,
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
        while control.paused_matching(&matches).await? {
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

#[cfg(all(test, not(acyclic_loom)))]
mod invariants {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Bounded control scopes use the same latest-revision rule whether
        // ancestry comes from a slice or an asynchronous memoized matcher.
        #[test]
        fn matching_scopes_preserve_control_precedence(
            states in prop::collection::vec(any::<bool>(), 0..9),
            membership in any::<u16>(), global_revision in 0u64..10, global in any::<bool>(),
        ) {
            let mut control = CommandControl::default();
            control.set(None, global_revision, global);
            let mut lineage = Vec::new();
            for (index, state) in states.into_iter().enumerate() {
                let task = TaskId::from_bytes([index as u8; 16]);
                control.set(Some(task), index as u64 + 1, state);
                if membership & (1 << index) != 0 { lineage.push(task); }
            }
            let actual = futures::executor::block_on(control.paused_matching(
                &|task| { let lineage = &lineage; Box::pin(async move { Ok(lineage.contains(&task)) }) },
            )).unwrap();
            prop_assert_eq!(actual, control.paused(&lineage));
        }
    }
}
