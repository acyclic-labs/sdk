use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum Key {
    Records(StreamPath),
}
impl std::borrow::Borrow<str> for Key {
    fn borrow(&self) -> &str {
        match self {
            Self::Records(path) => path.as_str(),
        }
    }
}

/// Retained outside the replaceable replay cache, dispatched after durability.
#[derive(Default)]
pub(super) struct Changes {
    keys: BTreeSet<Key>,
    records: BTreeMap<StreamPath, (Vec<Record>, u64)>,
    created: BTreeMap<StreamPath, u64>,
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
            for (path, tail) in &self.keys.created {
                let index = self
                    .provider
                    .inner
                    .publications
                    .follows
                    .get(path)
                    .ok()
                    .flatten();
                if let Some(index) = index {
                    index
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .created(*tail);
                }
            }
            for (path, (records, tail)) in &self.keys.records {
                let index = self
                    .provider
                    .inner
                    .publications
                    .follows
                    .get(path)
                    .ok()
                    .flatten();
                if let Some(index) = index {
                    index
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .publish(records, *tail);
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
        command: &WriteRequest,
        publications: &notifications::Publications,
    ) -> Result<Self, StreamError> {
        let destinations = match command {
            WriteRequest::Append(request) => vec![(request.path.clone(), false)],
            WriteRequest::Fork(request) => vec![(request.destination.clone(), true)],
            WriteRequest::Commit(request) => request
                .mutations
                .iter()
                .map(|mutation| match mutation {
                    CommitMutation::Append { path, .. } => (path.clone(), false),
                    CommitMutation::Fork { destination, .. } => (destination.clone(), true),
                })
                .collect(),
        };
        let mut paths = BTreeSet::new();
        for (path, fork) in destinations {
            // A fork can create a followed descendant without appending to it.
            // Consult active subscriptions, never enumerate the forked subtree.
            if fork {
                let lower = format!("{}/", path.as_str());
                let upper = format!("{}0", path.as_str());
                let range = (
                    std::ops::Bound::Included(lower.as_str()),
                    std::ops::Bound::Excluded(upper.as_str()),
                );
                for Key::Records(path) in publications.interests.keys_in::<str, _>(range)? {
                    paths.insert(path);
                }
                paths.extend(publications.follows.keys_in::<str, _>(range)?);
            }
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
        publications: &notifications::Publications,
    ) -> Result<(), StreamError> {
        for (path, before) in self.0 {
            let after = tail(provider, path.clone()).await?;
            if after != before {
                keys.keys.insert(Key::Records(path.clone()));
                let followed = publications.follows.get(&path)?.is_some();
                if before.is_none()
                    && let Some(after) = after
                    && followed
                {
                    keys.created.insert(path.clone(), after);
                }
                if let Some(after) = after
                    && followed
                {
                    let records = keys.records.entry(path.clone()).or_default();
                    records.1 = after;
                    let mut next = before.unwrap_or(0);
                    while next < after {
                        let mut batch = provider
                            .read(ReadRequest {
                                key: None,
                                path: path.clone(),
                                from: next,
                                limit: crate::wire::StreamLimit::MaxItems as u32,
                                at_least: Default::default(),
                            })
                            .await?;
                        let initial = next;
                        while let Some(record) = batch.next().await {
                            let record = record?;
                            next = record
                                .sequence
                                .checked_add(1)
                                .ok_or(StreamError::OutOfRange)?;
                            records.0.push(record);
                        }
                        if next <= initial {
                            return Err(StreamError::Unavailable);
                        }
                    }
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
        self.refresh(&mut cache, &mut dispatch.keys).await?;
        Ok(())
    }
    pub(super) async fn with_cached_provider<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryStream) -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut dispatch = Dispatch::new(self);
        let mut cache = self.inner.cache.lock().await;
        if !cache.valid {
            dispatch.repair = true;
            let result = self.refresh(&mut cache, &mut dispatch.keys).await;
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

pub(super) struct Observer<'a> {
    pub(super) stream: &'a BrowserStream,
    pub(super) changed: &'a mut Changes,
    pub(super) cursor: u64,
    pub(super) announced: u64,
    pub(super) before: Option<Before>,
}
impl crate::replicated::ReplayObserver for Observer<'_> {
    fn observe<'a>(
        &'a mut self,
        provider: &'a MemoryStream,
        command: &'a WriteRequest,
        after: bool,
    ) -> impl Future<Output = Result<(), StreamError>> + ProviderTask + 'a {
        async move {
            if after {
                if let Some(before) = self.before.take() {
                    before
                        .collect(provider, self.changed, &self.stream.inner.publications)
                        .await?;
                    self.announced = self.cursor + 1;
                }
                self.cursor += 1;
            } else {
                self.before = if self.cursor >= self.announced {
                    Some(Before::capture(provider, command, &self.stream.inner.publications).await?)
                } else {
                    None
                };
            }
            Ok(())
        }
    }
}
