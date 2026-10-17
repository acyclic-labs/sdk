//! Lock order: registry -> host setup/provider; providers never enter the registry.
use super::*;
use crate::sync::owned::{Arc as StateArc, Mutex, lock_owned};
use std::collections::BTreeMap;

pub(super) struct Opened<T, R> {
    entries: BTreeMap<String, StateArc<T>>,
    resources: Vec<R>,
}

impl<T, R> Default for Opened<T, R> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            resources: Vec::new(),
        }
    }
}

async fn open_once<T, R>(
    gate: &StateArc<Mutex<Opened<T, R>>>,
    key: String,
    open: impl AsyncFnOnce() -> Result<(StateArc<T>, R)>,
) -> Result<StateArc<T>> {
    let mut state = lock_owned(StateArc::clone(gate)).await;
    if let Some(value) = state.entries.get(&key) {
        return Ok(StateArc::clone(value));
    }
    #[cfg(acyclic_harness_loom)]
    if model::unlocked() {
        drop(state);
        let (value, resource) = open().await?;
        let mut state = lock_owned(StateArc::clone(gate)).await;
        state.entries.insert(key, StateArc::clone(&value));
        state.resources.push(resource);
        return Ok(value);
    }
    let (value, resource) = open().await?;
    state.entries.insert(key, StateArc::clone(&value));
    state.resources.push(resource);
    Ok(value)
}

async fn close_after_workers<T, R>(
    state: &StateArc<Mutex<Opened<T, R>>>,
    join: impl AsyncFn(R) -> Result<()>,
    close: impl AsyncFn(StateArc<T>) -> Result<()>,
) -> Result<()> {
    // The caller has stopped admission. Release the registry lock before
    // joining effects or entering provider shutdown, including on failure.
    let opened = {
        let mut state = lock_owned(StateArc::clone(state)).await;
        std::mem::take(&mut *state)
    };
    #[cfg(acyclic_harness_loom)]
    if model::early_close() {
        let closed = finish_all(opened.entries.into_values(), close).await;
        // Retain the deliberate ordering error, but join simulated workers
        // before exposing it so their captures drop inside the Loom execution.
        let drained = finish_all(opened.resources, join).await;
        return closed.and(drained);
    }
    let drained = finish_all(opened.resources, join).await;
    let closed = finish_all(opened.entries.into_values(), close).await;
    drained.and(closed)
}

async fn finish_all<T>(
    items: impl IntoIterator<Item = T>,
    finish: impl AsyncFn(T) -> Result<()>,
) -> Result<()> {
    let mut failed = None;
    for item in items {
        if let Err(error) = finish(item).await {
            failed.get_or_insert(error);
        }
    }
    failed.map_or(Ok(()), Err)
}

fn take_owner<T>(value: StateArc<T>) -> Result<T> {
    StateArc::try_unwrap(value)
        .map_err(|_| Error::Conflict("workspace still has an execution owner".into()))
}

pub(super) struct Registry {
    root: PathBuf,
    stop: Arc<crate::task_volume::TaskVolume<LocalStream>>,
    instance: uuid::Uuid,
    state: StateArc<Mutex<Opened<LocalHarness, tokio::task::JoinHandle<Result<()>>>>>,
}

impl Registry {
    pub(super) fn new(
        root: PathBuf,
        stop: Arc<crate::task_volume::TaskVolume<LocalStream>>,
        instance: uuid::Uuid,
    ) -> Self {
        Self {
            root,
            stop,
            instance,
            state: StateArc::new(Mutex::new(Opened::default())),
        }
    }

    pub(super) async fn get(&self, workspace: &str) -> Result<StateArc<LocalHarness>> {
        open_once(&self.state, workspace.into(), async || {
            let path: PathBuf = super::super::volume::decode(&read(
                &self
                    .root
                    .join("workspaces")
                    .join(format!("{workspace}.json")),
            )?)
            .map_err(storage)?;
            let (key, path) = credential::workspace(&path)?;
            if key != workspace {
                return Err(Error::Unauthorized("workspace binding was replaced".into()));
            }
            let harness =
                StateArc::new(LocalHarness::open(LocalHarnessConfig::external(path)).await?);
            let driver = StateArc::clone(&harness);
            let stop = Arc::clone(&self.stop);
            let instance = self.instance;
            let worker = tokio::spawn(async move { server::drive(driver, stop, instance).await });
            Ok((harness, worker))
        })
        .await
    }

    pub(super) async fn drain(&self) -> Result<()> {
        close_after_workers(
            &self.state,
            async |worker| worker.await.map_err(storage)?,
            async |harness| {
                let harness = take_owner(harness)?;
                harness.close().await
            },
        )
        .await
    }
}

#[cfg(acyclic_harness_loom)]
mod model;
