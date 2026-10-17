//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{
    DecodeReservation, FilesystemContentVerifier, FilesystemHost, InternalContentClass,
    PreparationIo,
};
use crate::contract::capability;
use crate::contract::next_revision;
use crate::executor::with_history_io;
use crate::wire::ErrorCode;
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, ApplyResult, Authority, AuthorityVerifier, Command, SchemaRegistry, Scope},
    interaction::{
        ApprovalBinding, Interaction, InteractionOutcome, InteractionResolution,
        InteractionResponse, InteractionTicket, ResolutionReceipt,
    },
    runtime::InteractionResolver,
    store::StreamAggregate,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamProvider;
use std::sync::Arc;
use uuid::Uuid;

/// Stages participant data privately and commits only references to one conversation history.
pub struct FilesystemInteractionHost<P, A, O> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    owner_scope: Scope,
    private_volume: VolumeRef,
    maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

mod content;
pub(in crate::filesystem) use content::admit_interaction;
mod publication;
mod resolution;

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds the conversation and its authenticated private content owner.
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        authority: &Authority,
        schemas: &SchemaRegistry,
        binding: super::FilesystemJournalBinding<'_>,
    ) -> Result<Self> {
        let super::FilesystemJournalBinding {
            volume: private_volume,
            verifier,
            scope: owner_scope,
            maximum_bytes,
            io,
        } = binding;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = with_history_io(io, |work| {
            verifier.verify_audience(authority, work)?;
            if authority.kind != crate::core::AggregateKind::Conversation
                || private_volume.class() != VolumeClass::AgentPrivate
                || private_volume.provider() != &host.provider
                || maximum_bytes == 0
            {
                return Err(interaction_boundary(
                    io,
                    ErrorCode::Invalid,
                    "interaction host requires a conversation, private volume, and positive limit",
                ));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                ContentGrant::authorize_volume(
                    verifier,
                    owner_scope,
                    private_volume,
                    operation,
                    u64::MAX,
                    work,
                )?;
            }
            Ok(Self {
                stream,
                host,
                verifier: verifier.copy(work)?,
                schemas: schemas.copy(work)?,
                owner_scope: owner_scope.copy(work)?,
                private_volume: private_volume.copy(work)?,
                maximum_bytes,
                #[cfg(not(target_arch = "wasm32"))]
                _native_metadata: None,
            })
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut result = result;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            result._native_metadata = Some(io.take_native_custody(io.owned_since(before)?)?);
        }
        Ok(result)
    }
}

pub(super) fn interaction_boundary(
    io: Option<&PreparationIo>,
    code: ErrorCode,
    reason: &'static str,
) -> Error {
    if code == ErrorCode::NotFound && io.is_none_or(|io| io.original_native_work().is_none()) {
        Error::NotFound(reason.into())
    } else {
        crate::executor::stock_boundary(io, code, reason)
    }
}
