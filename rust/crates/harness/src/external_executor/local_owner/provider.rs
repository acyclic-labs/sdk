//! Native clients may publish requests, never scheduler state or owner results.
use acyclic_stream::*;
use async_trait::async_trait;
use std::sync::Arc;

pub(super) struct ClientStream {
    stream: Arc<LocalStream>,
    write: &'static str,
    read: &'static [&'static str],
}

impl ClientStream {
    pub(super) fn workspace(stream: Arc<LocalStream>) -> Self {
        Self {
            stream,
            write: "external/requests",
            read: &["external/requests", "external/results"],
        }
    }
    pub(super) fn control(stream: Arc<LocalStream>) -> Self {
        Self {
            stream,
            write: "owner/control",
            read: &["owner/control"],
        }
    }
    fn readable(&self, path: &StreamPath) -> Result<(), StreamError> {
        if self.read.iter().any(|parent| self.matches(parent, path)) {
            Ok(())
        } else {
            Err(StreamError::AccessDenied)
        }
    }
    fn writable(&self, path: &StreamPath) -> Result<(), StreamError> {
        if self.matches(self.write, path) {
            Ok(())
        } else {
            Err(StreamError::AccessDenied)
        }
    }
    fn children_readable(&self, parent: Option<&StreamPath>) -> Result<(), StreamError> {
        if self.write != "owner/control"
            && parent.is_some_and(|parent| self.read.contains(&parent.as_str()))
        {
            Ok(())
        } else {
            Err(StreamError::AccessDenied)
        }
    }
    fn matches(&self, parent: &str, path: &StreamPath) -> bool {
        if self.write == "owner/control" {
            path.as_str() == parent
        } else {
            super::super::local_paths::task(parent, path).is_ok()
        }
    }
    fn commit_allowed(&self, request: &CommitRequest) -> Result<(), StreamError> {
        for condition in &request.conditions {
            match condition {
                CommitCondition::Head { path, .. } | CommitCondition::Tail { path, .. } => {
                    self.readable(path)?
                }
                _ => return Err(StreamError::AccessDenied),
            }
        }
        for mutation in &request.mutations {
            match mutation {
                CommitMutation::Append { path, .. } => self.writable(path)?,
                _ => return Err(StreamError::AccessDenied),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StreamProvider for ClientStream {
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        // Retry identities are account-wide and carry no path to authorize.
        // Hooks recover through their immutable request/result files instead.
        Err(StreamError::AccessDenied)
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.readable(&path)?;
        self.stream.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.readable(&path)?;
        self.stream.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.readable(&path)?;
        self.stream.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.writable(&request.path)?;
        self.stream.append(request).await
    }
    async fn fork(&self, _: ForkRequest) -> Result<ForkReceipt, StreamError> {
        Err(StreamError::AccessDenied)
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.readable(&request.path)?;
        self.stream.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.readable(&path)?;
        self.stream.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.readable(&path)?;
        self.stream.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.children_readable(request.parent.as_ref())?;
        self.stream.children(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.children_readable(request.parent.as_ref())?;
        if let (Some(parent), Some(after)) = (&request.parent, &request.after) {
            if super::super::local_paths::task(parent.as_str(), after).is_err() {
                return Err(StreamError::AccessDenied);
            }
        }
        self.stream.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_allowed(&request)?;
        self.stream.commit(request).await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_allowed(&request)?;
        self.stream.commit_before(request, deadline).await
    }
    async fn read_commit(&self, _: CommitId) -> Result<CommittedEnvelope, StreamError> {
        Err(StreamError::AccessDenied)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
