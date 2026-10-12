//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::fork_preparer_io::preparation_slot_bytes;
use super::{DecodeReservation, PreparationIo, PreparationRecord};
use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
use crate::contract::capability;
use crate::contract::next_revision;
use crate::wire::ErrorCode;
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, ApplyResult, Authority, AuthorityVerifier, Command, SchemaRegistry, Scope},
    interaction::{
        ApprovalBinding, Interaction, InteractionKind, InteractionOutcome, InteractionResolution,
        InteractionResponse, InteractionTicket, ResolutionReceipt,
    },
    runtime::InteractionResolver,
    store::StreamAggregate,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
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
mod publication;
mod resolution;

impl<P, A, O> InteractionResolver for FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn inspect<'a>(
        &'a self,
        scope: Scope,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<(InteractionTicket, Option<InteractionResolution>)>>> {
        Box::pin(async move {
            self.verifier.verify(&scope, &mut None)?;
            let result = self.read(id, None).await?;
            if let Some((ticket, _)) = &result
                && !scope.capabilities().contains(&ticket.viewer_grant())
                && !scope.capabilities().contains(&ticket.responder_grant())
            {
                return Err(Error::Unauthorized(
                    "scope lacks interaction view grant".into(),
                ));
            }
            Ok(result)
        })
    }

    fn resolve_answer<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>> {
        Box::pin(FilesystemInteractionHost::resolve_answer(
            self,
            operation_id,
            scope,
            id,
            expected_version,
            response,
        ))
    }

    fn resolve_approval<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>> {
        Box::pin(FilesystemInteractionHost::resolve_approval(
            self,
            operation_id,
            scope,
            id,
            expected_version,
            approved,
            reason,
        ))
    }
}

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds the authenticated conversation and private content owner.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        authority: &Authority,
        verifier: &AuthorityVerifier,
        schemas: &SchemaRegistry,
        owner_scope: &Scope,
        private_volume: &VolumeRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (verifier, schemas, owner_scope, private_volume) = crate::executor::with_history_io(
            io,
            |work| {
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
                Ok((
                    verifier.copy(work)?,
                    schemas.copy(work)?,
                    owner_scope.copy(work)?,
                    private_volume.copy(work)?,
                ))
            },
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata =
            match io.filter(|io| io.original_native_work().is_some()) {
                Some(io) => {
                    io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                    Some(io.take_native_custody(
                        io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            interaction_boundary(
                                Some(io),
                                ErrorCode::Invalid,
                                "interaction host custody underflows",
                            )
                        })?,
                    )?)
                }
                None => None,
            };
        let _ = before;
        Ok(Self {
            stream,
            host,
            verifier,
            schemas,
            owner_scope,
            private_volume,
            maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        })
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
        super::fork_preparer_io::failure(io, code, reason)
    }
}
