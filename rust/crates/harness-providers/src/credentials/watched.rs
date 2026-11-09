//! Register before reading; publish only into the observed source generation.
use crate::sync::Mutex;
use acyclic_auth::credential::{CredentialCacheError, CredentialWatch};
use acyclic_harness_core::{Error, Result};
use futures::FutureExt as _;
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

pub(crate) type Watch = Arc<dyn Fn() -> CredentialWatch + Send + Sync>;

struct Observation<T> {
    value: T,
    revision: u64,
    watch: Option<CredentialWatch>,
    native: Option<acyclic_harness_core::input_watch::InputWatch>,
    unavailable: bool,
}

pub(crate) struct Watched<T> {
    paths: Vec<PathBuf>,
    source: Option<Watch>,
    state: Mutex<Observation<T>>,
}

impl<T: Default> Watched<T> {
    pub(crate) fn new(paths: Vec<PathBuf>, source: Option<Watch>) -> Self {
        Self {
            paths,
            source,
            state: Mutex::new(Observation {
                value: T::default(),
                revision: 0,
                watch: None,
                native: None,
                unavailable: false,
            }),
        }
    }

    fn observe(&self, state: &mut Observation<T>) -> Result<()> {
        if state.unavailable {
            return Err(Error::Invalid("credential source watch unavailable".into()));
        }
        let changed = match state.watch.as_mut().map(|watch| watch.now_or_never()) {
            Some(Some(Err(_))) => {
                state.value = T::default();
                state.unavailable = true;
                return Err(Error::Invalid("credential source watch unavailable".into()));
            }
            Some(None) => false,
            None | Some(Some(Ok(()))) => true,
        };
        if changed {
            state.watch = Some(if let Some(source) = &self.source {
                source()
            } else {
                if state.native.is_none() {
                    state.native = Some(acyclic_harness_core::input_watch::InputWatch::new(
                        self.paths.clone(),
                    )?);
                }
                let native = state
                    .native
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("credential watch missing".into()))?;
                native.arm()?;
                let file = native.credential_watch();
                let mut events = events().subscribe();
                events.borrow_and_update();
                CredentialWatch::from_custodian(file.validity(), async move {
                    let event = async move {
                        events
                            .changed()
                            .await
                            .map_err(|_| CredentialCacheError::Unavailable)
                    };
                    futures::pin_mut!(file, event);
                    match futures::future::select(file, event).await {
                        futures::future::Either::Left((result, _))
                        | futures::future::Either::Right((result, _)) => result,
                    }
                })
            });
            state.value = T::default();
            state.revision = state.revision.wrapping_add(1);
        }
        Ok(())
    }

    pub(crate) fn read<R>(&self, read: impl FnOnce(&mut T) -> Result<R>) -> Result<(u64, R)> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Invalid("credential resolver lock poisoned".into()))?;
        self.observe(&mut state)?;
        Ok((state.revision, read(&mut state.value)?))
    }

    pub(crate) fn publish(&self, revision: u64, value: T) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Invalid("credential resolver lock poisoned".into()))?;
        self.observe(&mut state)?;
        if revision == state.revision {
            state.value = value;
        }
        Ok(())
    }
}

pub(super) fn events() -> &'static tokio::sync::watch::Sender<u64> {
    static EVENTS: OnceLock<tokio::sync::watch::Sender<u64>> = OnceLock::new();
    EVENTS.get_or_init(|| tokio::sync::watch::channel(0).0)
}
