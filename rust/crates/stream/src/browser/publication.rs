use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum Key {
    Records(StreamPath),
    Children(Option<StreamPath>),
}

/// Retained outside the replaceable replay cache, dispatched after durability.
#[derive(Default)]
pub(super) struct Changes {
    keys: BTreeSet<Key>,
    records: BTreeMap<StreamPath, (Vec<Record>, u64)>,
}

// Declared before the cache guard, so early return/cancellation drops that guard
// before dispatching notifications. No user waker runs under a cache lock.
pub(super) struct Dispatch {
    provider: BrowserStream,
    pub(super) keys: Changes,
    pub(super) repair: bool,
    pub(super) failed: bool,
}
impl Dispatch {
    pub(super) fn new(provider: &BrowserStream) -> Self {
        Self {
            provider: provider.clone(),
            keys: Changes::default(),
            repair: false,
            failed: false,
        }
    }
}
impl Drop for Dispatch {
    fn drop(&mut self) {
        if self.failed {
            let _ = self.provider.inner.publications.interests.notify_all();
            self.provider.inner.publications.invalidate_follows();
        } else {
            for key in &self.keys.keys {
                let _ = self.provider.inner.publications.interests.notify(key);
            }
            for (path, (records, tail)) in &self.keys.records {
                let index = self.provider.inner.publications.follows.borrow().get(path).cloned();
                if let Some(index) = index {
                    index.lock().unwrap_or_else(std::sync::PoisonError::into_inner).publish(records, *tail);
                }
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
        keys: &mut Changes,
        publications: &discovery::Publications,
    ) -> Result<(), StreamError> {
        for (path, before) in self.0 {
            let after = tail(provider, path.clone()).await?;
            if after != before {
                keys.keys.insert(Key::Records(path.clone()));
                let followed = publications.follows.borrow().contains_key(&path);
                if let Some(after) = after
                    && followed
                {
                    let records = keys.records.entry(path.clone()).or_default();
                    records.1 = after;
                    let mut next = before.unwrap_or(0);
                    while next < after {
                        let mut batch = provider.read(ReadRequest { path: path.clone(), from: next, limit: crate::wire::StreamLimit::MaxItems as u32 }).await?;
                        let initial = next;
                        while let Some(record) = batch.next().await {
                            let record = record?;
                            next = record.sequence.checked_add(1).ok_or(StreamError::OutOfRange)?;
                            records.0.push(record);
                        }
                        if next <= initial { return Err(StreamError::Unavailable); }
                    }
                }
                if before.is_none() && after.is_some() {
                    keys.keys.insert(Key::Children(path.parent()));
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
