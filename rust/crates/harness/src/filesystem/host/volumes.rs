use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Forks selected volume paths at an exact generation.
    pub async fn fork_volume(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        source_generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        self.fork_volume_pinned(
            source,
            source_generation,
            destination,
            paths,
            idempotency_key,
        )
        .await
    }

    pub(in crate::filesystem) async fn fork_volume_pinned(
        &self,
        source: &VolumeRef,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        source.validate(&mut None)?;
        child.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(child.provider())?;
        if source == child {
            return Err(Error::Invalid(
                "volume fork requires a distinct destination".into(),
            ));
        }
        let source_name = source.storage_name(&mut None)?;
        let source_workspace = self
            .open(&workspace_ref(self.provider.clone(), &source_name)?)
            .await?;
        let generation = self
            .generation(&source_workspace, source_generation)
            .await?;
        self.retain_generation(
            &workspace_ref(self.provider.clone(), &source_name)?,
            source_generation,
        )
        .await?;
        let child_name = child.storage_name(&mut None)?;
        let options = ForkOptions::from_generation(generation, filesystem_key(idempotency_key));
        let options = match paths {
            Some(paths) => options.inherit_paths(paths),
            None => options,
        };
        let forked = source_workspace
            .fork(
                &child_name,
                options,
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::default(),
                    }))
                },
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        let child_workspace = workspace_ref(self.provider.clone(), &child_name)?;
        let initial = forked
            .operation_generation(
                filesystem_key(idempotency_key),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?
            .ok_or_else(|| Error::Storage("fork creation has no durable generation".into()))?;
        let child_generation = self.generation_ref_id(initial.id())?;
        self.retain_generation(&child_workspace, &child_generation)
            .await?;
        Ok(WorkspaceObservation {
            workspace: child_workspace,
            generation: child_generation,
        })
    }

    /// Plans a destination-authorized volume import.
    pub async fn prepare_volume_import(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
    ) -> Result<JoinPlan<A, O>> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        source.validate(&mut None)?;
        destination.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(destination.provider())?;
        let source_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &source.storage_name(&mut None)?,
            )?)
            .await?;
        let destination_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &destination.storage_name(&mut None)?,
            )?)
            .await?;
        let source_generation = self.generation(&source_workspace, generation).await?;
        let target = destination_workspace
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        source_workspace
            .join_into(&destination_workspace)
            .plan_at(
                &source_generation,
                &target,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))
    }

    /// Creates the unique physical workspace for a logical volume.
    pub async fn create_volume(&self, volume: &VolumeRef) -> Result<WorkspaceObservation> {
        volume.validate(&mut None)?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name(&mut None)?;
        self.filesystem
            .create_workspace(
                &name,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?)
            .await
    }
}

#[cfg(not(target_arch = "wasm32"))]
use crate::durable_host::TaskJournalOwner;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::NativeMaterializationWork;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::StreamProvider;

/// Canonical native ROOT bind identity; this derives an operation, not authority.
#[cfg(not(target_arch = "wasm32"))]
pub fn native_root_output_binding_operation(
    audience: &crate::core::Authority,
    volume: &VolumeRef,
    creation: acyclic_fs::OperationId,
    work: &NativeMaterializationWork,
) -> Result<crate::OperationId> {
    let io = PreparationIo::from_native_work(work)?;
    let encoded = io.encode(
        &(
            "harness/native-root-output-bind/v2",
            audience,
            volume,
            crate::OperationId::from_bytes(creation.into_bytes()),
        ),
        work.remaining()?.bytes_encoded,
        true,
    )?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.len() as u64,
        ..Default::default()
    })?;
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(&encoded).as_bytes()[..16]);
    Ok(crate::OperationId::from_bytes(bytes))
}

/// Authenticates the original ROOT Bind and returns its canonical command bytes.
#[allow(
    clippy::too_many_arguments,
    reason = "original creator, owner and paid authority are distinct"
)]
#[cfg(not(target_arch = "wasm32"))]
pub async fn bind_original_root_conversation_with_native_work<P: StreamProvider>(
    stream: &std::sync::Arc<P>,
    authority: (crate::core::Authority, crate::core::AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation,
    owner: &TaskJournalOwner<P>,
    private_volume: &VolumeRef,
    original_creation: acyclic_fs::OperationId,
    limits: crate::conversation::Limits,
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::OwnedBytes> {
    let _authority_custody = authority_custody;
    let (audience, issuer, scope) = authority;
    let invalid = |reason| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    };
    if audience.kind != crate::core::AggregateKind::Conversation
        || scope.agent().is_none()
        || !scope.capabilities().contains("conversation:bind")
        || limits.file_bytes == 0
    {
        return Err(invalid(
            "ROOT binding requires its original conversation authority and finite event allowance",
        ));
    }
    owner
        .verify(
            false,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
    let verifier_io = PreparationIo::from_native_work(work)?;
    let verifier = issuer.verifier(&mut Some(&mut verifier_io.history_io()))?;
    let retained = verifier_io.owned_bytes()?;
    let _verifier_custody = verifier_io.into_native_custody(retained)?;
    let operation =
        native_root_output_binding_operation(&audience, private_volume, original_creation, work)?;
    let mut reader_io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let reader =
        crate::store::HistoryReader::new(stream, &audience, &verifier, &mut Some(&mut reader_io))?;
    let reader_custody = reader_io.into_custody()?;
    let mut event_io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let event = reader
        .operation_event(operation, limits.file_bytes, &mut Some(&mut event_io))
        .await?
        .0;
    event_io.return_staged_output()?;
    let event_custody = event_io.into_custody()?;
    let original_revision = match event.as_ref() {
        None => None,
        Some(event) => match &event.payload {
            crate::core::EventPayload::ConversationBound { agent }
                if Some(*agent) == scope.agent()
                    && event.scope.agent() == scope.agent()
                    && event.scope.id() == scope.id()
                    && event.scope.capabilities() == scope.capabilities()
                    && event.causal_parent.is_none() =>
            {
                Some(
                    event
                        .revision
                        .checked_sub(1)
                        .ok_or_else(|| invalid("original ROOT binding has no command revision"))?,
                )
            }
            _ => {
                return Err(invalid(
                    "ROOT binding differs from the protected original creation",
                ));
            }
        },
    };
    drop(event);
    drop(event_custody);
    drop(reader);
    drop(reader_custody);
    let mut aggregate_io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    crate::store::history::HistoryIo::check_cancellation(&aggregate_io)?;
    let mut aggregate = crate::store::StreamAggregate::open(
        stream,
        audience,
        verifier,
        crate::core::SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut Some(&mut aggregate_io),
    )
    .await?;
    aggregate_io.return_staged_output()?;
    let _aggregate_custody = aggregate_io.into_custody()?;
    aggregate.set_limits(limits)?;
    if original_revision.is_none()
        && aggregate
            .reducer()
            .conversation()
            .and_then(|state| state.agent)
            .is_some()
    {
        return Err(invalid(
            "ROOT conversation changed during original binding admission",
        ));
    }
    let key = work.display(&format_args!(
        "harness/native-root-output-bind/v2/{operation}"
    ))?;
    let key = (crate::IdempotencyKey::new(key.0)?, key.1);
    let agent = scope.agent().expect("validated ROOT binding actor");
    let command = crate::core::Command {
        operation_id: operation,
        idempotency_key: key.0,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.reducer().revision()),
        causal_parent: None,
        action: crate::core::Action::BindConversation { agent },
        scope,
    };
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let encoded = crate::store::history::canonical_bytes_with_work(
        &command,
        crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut io),
    )?;
    let encoded_custody =
        crate::store::history::HistoryIo::take_native_custody(&mut io, encoded.capacity() as u64)?;
    let encoded = acyclic_stream::OwnedBytes::from_vec_with_guard(
        encoded,
        encoded_custody,
        work.runtime_meter(),
    )
    .map_err(|failure| native_owned_bytes_failure(failure, work))?;
    crate::store::history::HistoryIo::retain_owned(
        &mut io,
        std::mem::size_of::<crate::core::ApplyResult>() as u64,
    )?;
    let applied = aggregate
        .execute(command, Some(owner), &mut Some(&mut io))
        .await?;
    drop(applied);
    Ok(encoded)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_owned_bytes_failure(
    failure: acyclic_stream::PreparedOwnedBytesFailure<acyclic_fs::NativeAllocation>,
    work: &NativeMaterializationWork,
) -> Error {
    match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
            Error::from(work.runtime_error(error))
        }
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native owned byte buffer violated its fixed initialized ownership invariant",
        },
    }
}
