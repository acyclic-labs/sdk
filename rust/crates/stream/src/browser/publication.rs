use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum Key {
    Records(StreamPath),
    Children(Option<StreamPath>),
}

// Declared before the cache guard, so early return/cancellation drops that guard
// before dispatching notifications. No user waker runs under a cache lock.
pub(super) struct Dispatch {
    provider: BrowserStream,
    pub(super) keys: BTreeSet<Key>,
    pub(super) repair: bool,
    pub(super) failed: bool,
}
impl Dispatch {
    pub(super) fn new(provider: &BrowserStream) -> Self {
        Self {
            provider: provider.clone(),
            keys: BTreeSet::new(),
            repair: false,
            failed: false,
        }
    }
}
impl Drop for Dispatch {
    fn drop(&mut self) {
        if self.failed {
            let _ = self.provider.inner.publications.interests.notify_all();
        } else {
            for key in &self.keys {
                let _ = self.provider.inner.publications.interests.notify(key);
            }
        }
        if self.repair {
            let _ = self.provider.inner.publications.wake(false);
        }
    }
}

pub(super) struct Before(BTreeMap<StreamPath, Option<u64>>);
impl Before {
    pub(super) async fn capture(
        provider: &MemoryStream,
        command: &Command,
    ) -> Result<Self, StreamError> {
        let destinations = match command {
            Command::Append(request) => vec![request.path.clone()],
            Command::Fork(request) => vec![request.destination.clone()],
            Command::Commit(request) => request
                .mutations
                .iter()
                .map(|mutation| match mutation {
                    CommitMutation::Append { path, .. } => path.clone(),
                    CommitMutation::Fork { destination, .. } => destination.clone(),
                })
                .collect(),
        };
        let mut paths = BTreeSet::new();
        for path in destinations {
            let mut next = Some(path);
            while let Some(path) = next {
                next = path.parent();
                paths.insert(path);
            }
        }
        let mut before = BTreeMap::new();
        for path in paths {
            before.insert(path.clone(), tail(provider, path).await?);
        }
        Ok(Self(before))
    }
    pub(super) async fn collect(
        self,
        provider: &MemoryStream,
        keys: &mut BTreeSet<Key>,
    ) -> Result<(), StreamError> {
        for (path, before) in self.0 {
            let after = tail(provider, path.clone()).await?;
            if after != before {
                keys.insert(Key::Records(path.clone()));
                if before.is_none() && after.is_some() {
                    keys.insert(Key::Children(path.parent()));
                }
            }
        }
        Ok(())
    }
}
async fn tail(provider: &MemoryStream, path: StreamPath) -> Result<Option<u64>, StreamError> {
    match provider.tail(path).await {
        Ok(tail) => Ok(Some(tail)),
        Err(StreamError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

impl BrowserStream {
    pub(super) async fn refresh_publication(&self, force: bool) -> Result<(), StreamError> {
        let mut dispatch = Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        if !force && cache.valid {
            return Ok(());
        }
        #[cfg(test)]
        self.inner.refreshes.set(self.inner.refreshes.get() + 1);
        let transaction = self.transaction().inspect_err(|_| cache.valid = false)?;
        self.refresh(&transaction, &mut cache, &mut dispatch.keys)
            .await?;
        Ok(())
    }
    pub(super) async fn with_cached_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut dispatch = Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        if !cache.valid {
            let transaction = self.transaction()?;
            dispatch.repair = true;
            let result = self
                .refresh(&transaction, &mut cache, &mut dispatch.keys)
                .await;
            dispatch.repair = false;
            dispatch.failed = result.is_err();
            result?;
        }
        #[cfg(test)]
        self.inner
            .follow_reads
            .set(self.inner.follow_reads.get() + 1);
        operation(&cache.provider).await
    }
}
