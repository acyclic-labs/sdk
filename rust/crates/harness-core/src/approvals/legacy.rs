//! Adapts the existing task host to the agent-neutral approver implementation.
//! Deleted with that host by the agent switchover; it owns no traversal or wake.
use super::{
    ApprovalCreate, ApprovalFiles, ApprovalPolicy, ApprovalRequest, ApprovalReviewFile, Approver,
    NextStage,
};
use crate::{
    Error, Result, TaskId,
    conversation::Limits,
    interaction::ApprovalDecisionFile,
    model::{Model, ModelProvider},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskFiles},
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use futures::FutureExt as _;
use std::{collections::BTreeMap, sync::Arc};

/// Existing-host entry point over its already loaded immutable snapshot.
pub fn next_stage<'a>(
    policy: &'a ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<TaskId>,
    request: &FileValue,
    files: &BTreeMap<FileKey, FileValue>,
    start: usize,
) -> Result<NextStage<'a>> {
    let request_id = request_id(request)?.as_str();
    let request_view =
        ApprovalRequest::new(request_id, &request.bytes, Limits::default().file_bytes)?;
    let snapshot = Snapshot { request, files };
    // Snapshot reads return Ready in their first poll: no storage, executor,
    // blocking or follow operation is admitted by this compatibility entry.
    let parent = parent.map(|parent| format!("parent:{parent}"));
    super::chain::next_stage_with_parent(
        policy,
        digest,
        parent.as_deref(),
        &request_view,
        &snapshot,
        start,
    )
    .now_or_never()
    .ok_or_else(|| Error::Invalid("approval snapshot unexpectedly suspended".into()))?
}

struct Snapshot<'a> {
    request: &'a FileValue,
    files: &'a BTreeMap<FileKey, FileValue>,
}
impl ApprovalFiles for Snapshot<'_> {
    fn read<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let key = key(self.request, path)?;
            self.files
                .get(&key)
                .map(|file| {
                    super::files::check_bound(file.bytes.len(), maximum_bytes)?;
                    Ok(file.bytes.to_vec())
                })
                .transpose()
        })
    }
    fn create<'a>(
        &'a self,
        _: &'a str,
        _: Vec<u8>,
        _: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>> {
        Box::pin(async { Err(Error::Unsupported("approval snapshot is read-only".into())) })
    }
}

fn key(request: &FileValue, path: &str) -> Result<FileKey> {
    let FileKey::Task { task, .. } = &request.key else {
        return Err(Error::Invalid("not a task approval request".into()));
    };
    let parsed = FileKey::parse(&format!(".tasks/{}/{path}", task.as_str()))?;
    match &parsed {
        FileKey::Task {
            file: TaskFile::ApprovalReview { request: id, .. },
            ..
        } if id == request_id(request)? => Ok(parsed),
        _ => Err(Error::Invalid("not an approval review path".into())),
    }
}

/// Request component of an approval request file.
pub fn request_id(file: &FileValue) -> Result<&Component> {
    match &file.key {
        FileKey::Task {
            file: TaskFile::ApprovalRequest(request),
            ..
        } => Ok(request),
        _ => Err(Error::Invalid("not an approval request".into())),
    }
}
/// Canonical review-file key for a request and authenticated principal.
pub fn review_key(request: &FileValue, principal: &str) -> Result<FileKey> {
    let FileKey::Task { task, .. } = &request.key else {
        return Err(Error::Invalid("not a task file".into()));
    };
    Ok(FileKey::Task {
        task: task.clone(),
        file: TaskFile::ApprovalReview {
            request: request_id(request)?.clone(),
            principal: Component::new(ApprovalDecisionFile::principal_file(principal))?,
        },
    })
}

/// Existing-host request fingerprint; the shared codec hashes the original bytes.
pub fn fingerprint(request: &FileValue) -> Result<String> {
    super::chain::fingerprint(&request.bytes)
}

struct Volume<'a, P: ?Sized> {
    files: &'a TaskFiles<P>,
    request: &'a FileValue,
}
impl<P: StreamProvider + ?Sized> ApprovalFiles for Volume<'_, P> {
    fn read<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let key = key(self.request, path)?;
            let snapshot = self.files.snapshot(&Query::File(key), 1).await?;
            let Some(file) = snapshot.files.first() else {
                return Ok(None);
            };
            super::files::check_bound(file.bytes.len(), maximum_bytes)?;
            crate::task_volume::publish_durable(&self.files.inner, file, None).await?;
            Ok(Some(file.bytes.to_vec()))
        })
    }
    fn create<'a>(
        &'a self,
        path: &'a str,
        bytes: Vec<u8>,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>> {
        Box::pin(async move {
            super::files::check_bound(bytes.len(), maximum_bytes)?;
            let key = key(self.request, path)?;
            let write = FileValue::new(key.clone(), bytes);
            loop {
                let snapshot = self.files.snapshot(&Query::File(key.clone()), 1).await?;
                if let Some(file) = snapshot.files.first() {
                    super::files::check_bound(file.bytes.len(), maximum_bytes)?;
                    crate::task_volume::publish_durable(&self.files.inner, file, None).await?;
                    return Ok(ApprovalCreate::Existing(file.bytes.to_vec()));
                }
                match self.files.inner.create(snapshot.generation, &write).await {
                    Ok(_) => {
                        crate::task_volume::publish_durable(&self.files.inner, &write, None)
                            .await?;
                        return Ok(ApprovalCreate::Created);
                    }
                    Err(
                        crate::task_volume::Error::Conflict(_)
                        | crate::task_volume::Error::ImmutableConflict(_),
                    ) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        })
    }
}

/// Existing task-host adapter; all automatic evaluation is storage-neutral.
pub async fn automatic_review<P: StreamProvider + ?Sized>(
    models: &BTreeMap<String, (Model, Arc<dyn ModelProvider>)>,
    files: &TaskFiles<P>,
    request: &FileValue,
    approver: &Approver,
    expected: ApprovalReviewFile,
    limits: Limits,
) -> Result<ApprovalReviewFile> {
    let id = request_id(request)?.as_str();
    let request_view = ApprovalRequest::new(id, &request.bytes, limits.file_bytes)?;
    super::automatic::automatic_review(
        models,
        &Volume { files, request },
        &request_view,
        approver,
        expected,
        limits,
    )
    .await
}

#[cfg(test)]
mod tests;
