//! [`ApprovalFiles`] on an agent's private volume, in the internal interaction
//! area: no content writer or tool write grant reaches it, so only the
//! approval owner and authenticated responders create approval files.

use super::{FilesystemHost, InternalContentClass};
use crate::{
    IdempotencyKey, Result,
    approvals::{ApprovalCreate, ApprovalFiles},
    conversation::{ContentGrant, VolumeRef},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Approval files of one agent, under its owner's private-volume write grant.
pub(in crate::filesystem) struct AgentApprovalFiles<A: 'static, O: 'static> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
}

impl<A, O> AgentApprovalFiles<A, O> {
    pub(in crate::filesystem) const fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        grant: ContentGrant,
    ) -> Self {
        Self {
            host,
            volume,
            grant,
        }
    }
}

/// Storage path of approval-relative `path`.
fn internal(path: &str) -> String {
    format!("{}{path}", InternalContentClass::Interaction.prefix())
}

impl<A, O> ApprovalFiles for AgentApprovalFiles<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn read<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let cancellation = CancellationToken::new();
            let fs = &self.host.filesystem;
            let workspace = fs
                .open_workspace(
                    self.volume.storage_name(&mut None)?,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| super::map_error(failure.error))?
                .value;
            // The committed head is durable, so its bytes may advance a stage.
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| super::map_error(failure.error))?
                .value;
            match head
                .read(
                    &format!("/{}", internal(path)),
                    maximum_bytes,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
            {
                Ok(receipt) => Ok(Some(receipt.value.to_vec())),
                Err(failure) => {
                    let error = super::map_error(failure.error);
                    if error.code() == crate::wire::ErrorCode::NotFound {
                        Ok(None)
                    } else {
                        Err(error)
                    }
                }
            }
        })
    }

    fn create<'a>(
        &'a self,
        path: &'a str,
        bytes: Vec<u8>,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>> {
        Box::pin(async move {
            if let Some(existing) = self.read(path, maximum_bytes).await? {
                return Ok(ApprovalCreate::Existing(existing));
            }
            // One retry identity per path: a second, different body conflicts
            // instead of replacing the first.
            let key = IdempotencyKey(format!("approval-file:{path}"));
            let written = self
                .host
                .put_internal_content(
                    &self.volume,
                    &self.grant,
                    &internal(path),
                    &bytes,
                    "application/json",
                    path.rsplit('/').next().unwrap_or(path),
                    maximum_bytes,
                    &key,
                    InternalContentClass::Interaction,
                    None,
                )
                .await;
            match written {
                Ok(_) => Ok(ApprovalCreate::Created),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => self
                    .read(path, maximum_bytes)
                    .await?
                    .map(ApprovalCreate::Existing)
                    .ok_or(error),
                Err(error) => Err(error),
            }
        })
    }
}
