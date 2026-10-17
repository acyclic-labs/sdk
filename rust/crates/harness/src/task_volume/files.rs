//! Owner-authorized file access to the private task volume.
use super::{Cursor, Error, FileKey, FileValue, Generation, Observation, Owner, Query, TaskVolume};
use crate::{
    AgentId,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AuthorityVerifier, Scope},
};
use acyclic_stream::{StreamPath, StreamProvider};
use std::sync::Arc;

/// The caller's file writer and live queries for one private task volume.
/// Execution and approval records can only be published by their internal owners.
pub struct TaskFiles<P: ?Sized> {
    pub(crate) inner: TaskVolume<P>,
    volume: VolumeRef,
    maximum_bytes: u64,
}

impl<P: StreamProvider + ?Sized> TaskFiles<P> {
    /// Binds existing private-volume owner grants to the supplied Streams provider.
    pub async fn open(
        provider: Arc<P>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
    ) -> crate::Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || maximum_bytes == 0
            || maximum_bytes == u64::MAX
        {
            return Err(crate::Error::Invalid(
                "task files require a private volume and finite file bound".into(),
            ));
        }
        if !owns_volume(volume, scope.agent()) {
            return Err(crate::Error::Unauthorized(
                "task files require the owning agent".into(),
            ));
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::authorize_volume(
                verifier,
                scope,
                volume,
                operation,
                maximum_bytes,
                &mut None,
            )?;
        }
        let path = StreamPath::new(format!(
            "harness/task-volumes/{}",
            volume.storage_name(&mut None)?
        ))
        .map_err(storage)?;
        let inner = TaskVolume::open(provider, path).await.map_err(storage)?;
        Ok(Self {
            inner,
            volume: volume.clone(),
            maximum_bytes,
        })
    }

    pub(crate) const fn maximum_bytes(&self) -> u64 {
        self.maximum_bytes
    }

    /// Exact private volume whose owner grants authorized this binding.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Writes a request or editable private file against an exact publication cut.
    pub async fn write(&self, expected: Generation, file: &FileValue) -> super::Result<Generation> {
        authorize_write(&file.key, file.bytes.len() as u64, self.maximum_bytes)?;
        self.inner.append(expected, file).await
    }

    /// Reads the authorized private volume at one exact query cut.
    pub async fn snapshot(
        &self,
        query: &Query,
        maximum_files: usize,
    ) -> super::Result<super::Snapshot> {
        self.inner.snapshot(query, maximum_files).await
    }

    /// Captures an atomic snapshot and push follow, or resumes a durable cursor.
    pub async fn observe(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> super::Result<Observation> {
        self.inner.observe(query, cursor, maximum_files).await
    }
}

impl<P: StreamProvider + ?Sized> super::FileQuerySource for TaskFiles<P> {
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> acyclic_stream::BoxProviderFuture<'_, super::Result<Observation>> {
        super::FileQuerySource::observe_files(
            &self.inner,
            query,
            cursor,
            maximum_files,
            maximum_bytes,
        )
    }
}

fn owns_volume(volume: &VolumeRef, agent: Option<AgentId>) -> bool {
    matches!(volume.owner(), VolumeOwner::Agent(owner) if agent == Some(*owner))
}

fn authorize_write(key: &FileKey, bytes: u64, maximum: u64) -> super::Result<()> {
    if !matches!(key.owner(), Owner::Caller | Owner::Agent) {
        return Err(Error::Unauthorized);
    }
    if bytes > maximum {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}

fn storage(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage(error.to_string())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

#[cfg(test)]
mod fixtures;
#[cfg(all(test, acyclic_harness_loom))]
mod models;
