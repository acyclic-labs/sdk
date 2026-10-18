use crate::kernel::PublishedGeneration;
use crate::path::PortablePath;
use crate::wire::filesystem::v2 as wire;
use crate::{Sequence, VolumeId, WorkspaceName};
use acyclic_stream::{CommitId, StreamError};
use bytes::Bytes;
use prost::Message;

mod encode;

#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
mod provider;
#[cfg(all(feature = "session", not(target_arch = "wasm32")))]
pub use provider::Provider;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed Session filesystem subscription.
pub struct Subscription {
    /// Volume identity.
    pub volume_id: VolumeId,
    /// Canonical workspace name.
    pub workspace_name: WorkspaceName,
    /// Selected path prefix.
    pub path_prefix: PortablePath,
    /// Delivery mode.
    pub delivery: wire::SessionFsDelivery,
    /// Initial position.
    pub start_next: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed Session filesystem publication.
pub struct Publication {
    /// Published generation.
    pub generation: PublishedGeneration,
    /// Owning workspace name.
    pub workspace_name: WorkspaceName,
    /// Authority sequence.
    pub sequence: Sequence,
    /// Stream commit identity.
    pub commit_id: CommitId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed Session filesystem cursor.
pub struct Cursor {
    /// Original subscription.
    pub subscription: Subscription,
    /// Continuation position.
    pub next: u64,
    /// Preceding Stream commit.
    pub preceding_commit_id: CommitId,
}

fn version(value: u32) -> Result<(), StreamError> {
    if value == 1 {
        Ok(())
    } else {
        Err(StreamError::Unsupported)
    }
}
fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], StreamError> {
    value.try_into().map_err(|_| StreamError::InvalidArgument)
}
fn decode<M: Message + Default>(value: &[u8]) -> Result<M, StreamError> {
    if value.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(StreamError::Capacity);
    }
    M::decode(value).map_err(|_| StreamError::InvalidArgument)
}

impl Subscription {
    /// Encodes the typed payload.
    pub fn encode(&self) -> Result<Bytes, StreamError> {
        self.validate()?;
        Ok(encode::subscription(self))
    }
    /// Decodes a typed payload.
    pub fn decode(value: &[u8]) -> Result<Self, StreamError> {
        Self::from_wire(decode(value)?)
    }
    fn validate(&self) -> Result<(), StreamError> {
        if self.delivery == wire::SessionFsDelivery::Unspecified {
            return Err(StreamError::InvalidArgument);
        }
        PortablePath::validate(self.path_prefix.as_str(), crate::VolumeLimits::default())
            .map_err(|_| StreamError::InvalidArgument)?;
        Ok(())
    }
    fn from_wire(value: wire::SessionFsSubscription) -> Result<Self, StreamError> {
        version(value.version)?;
        let delivery = wire::SessionFsDelivery::try_from(value.delivery)
            .map_err(|_| StreamError::InvalidArgument)?;
        if delivery == wire::SessionFsDelivery::Unspecified {
            return Err(StreamError::InvalidArgument);
        }
        let volume_id = VolumeId::from_bytes(fixed(&value.volume_id)?);
        Ok(Self {
            workspace_name: workspace_name(volume_id, value.workspace)?,
            volume_id,
            path_prefix: PortablePath::parse(&value.path_prefix, crate::VolumeLimits::default())
                .map_err(|_| StreamError::InvalidArgument)?,
            delivery,
            start_next: value.start_next,
        })
    }
}

impl Publication {
    /// Returns the public filesystem generation reference.
    pub fn generation_ref(&self) -> wire::GenerationRef {
        wire::GenerationRef {
            workspace: Some(workspace_ref(
                self.generation.volume_id,
                &self.workspace_name,
            )),
            generation_id: self.generation.generation_root.digest.as_bytes().to_vec(),
        }
    }

    /// Encodes the typed payload.
    pub fn encode(&self) -> Result<Bytes, StreamError> {
        if self.generation.generation_root.kind != crate::ObjectKind::GenerationRoot {
            return Err(StreamError::InvalidArgument);
        }
        if self.sequence.get() == 0 || self.sequence.get() == u64::MAX {
            return Err(StreamError::InvalidArgument);
        }
        Ok(encode::publication(self))
    }
    /// Decodes a typed payload.
    pub fn decode(value: &[u8]) -> Result<Self, StreamError> {
        Self::from_wire(decode(value)?)
    }
    fn from_wire(value: wire::SessionFsPublication) -> Result<Self, StreamError> {
        version(value.version)?;
        let generation = value.generation.ok_or(StreamError::InvalidArgument)?;
        if value.sequence == 0 || value.sequence == u64::MAX {
            return Err(StreamError::InvalidArgument);
        }
        let volume_id = VolumeId::from_bytes(fixed(&value.volume_id)?);
        Ok(Self {
            workspace_name: workspace_name(volume_id, generation.workspace)?,
            generation: PublishedGeneration {
                volume_id,
                generation_root: crate::ObjectId {
                    kind: crate::ObjectKind::GenerationRoot,
                    digest: crate::Digest::from_bytes(fixed(&generation.generation_id)?),
                },
            },
            sequence: Sequence::new(value.sequence),
            commit_id: CommitId::from_bytes(fixed(&value.commit_id)?),
        })
    }
}

impl Cursor {
    /// Encodes the typed payload.
    pub fn encode(&self) -> Result<Bytes, StreamError> {
        Self::encode_parts(&self.subscription, self.next, self.preceding_commit_id)
    }
    fn encode_parts(
        subscription: &Subscription,
        next: u64,
        preceding_commit_id: CommitId,
    ) -> Result<Bytes, StreamError> {
        subscription.validate()?;
        if next < 2 || next <= subscription.start_next {
            return Err(StreamError::InvalidArgument);
        }
        Ok(encode::cursor(subscription, next, preceding_commit_id))
    }
    /// Decodes a typed payload.
    pub fn decode(value: &[u8]) -> Result<Self, StreamError> {
        Self::from_wire(decode(value)?)
    }
    fn from_wire(value: wire::SessionFsCursor) -> Result<Self, StreamError> {
        version(value.version)?;
        let subscription =
            Subscription::from_wire(value.subscription.ok_or(StreamError::InvalidArgument)?)?;
        if value.next < 2 || value.next <= subscription.start_next {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            subscription,
            next: value.next,
            preceding_commit_id: CommitId::from_bytes(fixed(&value.preceding_commit_id)?),
        })
    }
}

fn workspace_ref(volume: VolumeId, name: &WorkspaceName) -> wire::WorkspaceRef {
    wire::WorkspaceRef {
        workspace_id: volume.into_bytes().to_vec(),
        name: name.as_str().into(),
    }
}
fn workspace_name(
    volume: VolumeId,
    value: Option<wire::WorkspaceRef>,
) -> Result<WorkspaceName, StreamError> {
    let value = value.ok_or(StreamError::InvalidArgument)?;
    if value.workspace_id != volume.into_bytes() {
        return Err(StreamError::InvalidArgument);
    }
    let name = WorkspaceName::new(
        &value.name,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|_| StreamError::InvalidArgument)?
    .value;
    if name.as_str() != value.name {
        return Err(StreamError::InvalidArgument);
    }
    Ok(name)
}
