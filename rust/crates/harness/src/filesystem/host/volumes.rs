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
    ) -> Result<WorkspaceObservation>
    where
        A: 'static,
    {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        if crate::core::filesystem_scope_label(
            destination_grant.policy_scope().capabilities(),
            &mut None,
        )?
        .is_some()
        {
            return Err(Error::FilesystemAccessConfiguration(
                "structured fork destination requires an existing policy view",
            ));
        }
        self.fork_volume_pinned(
            source,
            source_grant,
            source_generation,
            destination,
            paths,
            idempotency_key,
            None,
            None,
        )
        .await
    }

    pub(in crate::filesystem) async fn fork_volume_pinned(
        &self,
        source: &VolumeRef,
        grant: &ContentGrant,
        selected: &GenerationRef,
        child: &VolumeRef,
        paths: Option<Vec<String>>,
        key: &IdempotencyKey,
        io: Option<&PreparationIo>,
        preparation: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<WorkspaceObservation>
    where
        A: 'static,
    {
        let host = self;

        if io.is_some_and(|io| io.original_native_work().is_some()) && preparation.is_none() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task-bound native fork requires its original issued creation guard",
            ));
        }
        if let Some(task) = preparation {
            task.verify(false, io)?.await?;
        }
        grant.require(source, VolumeOperation::Read, io)?;
        let owned = if io.is_none() {
            Some(PreparationIo::for_unmeasured(
                &acyclic_fs::CancellationToken::new(),
            )?)
        } else {
            None
        };
        let io = io.or(owned.as_ref()).ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "volume fork has no work context",
            )
        })?;
        source.validate(&mut Some(&mut io.history_io()))?;
        child.validate(&mut Some(&mut io.history_io()))?;
        if source.provider() != &host.provider
            || child.provider() != &host.provider
            || source == child
        {
            return Err(io.invalid("volume fork has a foreign or identical destination"));
        }
        let reference = fork_preparer_io::content_workspace(&host.provider, source, Some(io))?;
        let workspace = host.open(&reference, Some(io)).await?;
        let pinned = host.generation(&workspace, selected, Some(io)).await?;
        fork_preparer_io::retain(&pinned, Some(io)).await?;
        let name = crate::executor::with_history_io(Some(io), |work| child.storage_name(work))?;
        let operation = fork_preparer_io::operation_key(key, Some(io))?;
        let forked = workspace
            .with_stable_policy(|| async {
                let access = super::volume_access::compile(
                    &workspace,
                    grant,
                    Some(pinned.id()),
                    VolumeOperation::Read,
                    &[
                        acyclic_fs::access::Action::Read,
                        acyclic_fs::access::Action::List,
                        acyclic_fs::access::Action::Fork,
                    ],
                    io,
                )
                .await?;
                super::super::access::require_current(&workspace, &access.0, io).await?;
                drop(access);
                let options = acyclic_fs::ForkOptions::from_generation(pinned, operation);
                let options = match paths {
                    Some(paths) => options.inherit_paths(paths),
                    None => options,
                };
                let forked = match preparation {
                    Some(task) => io.sdk(
                        workspace
                            .fork::<_, _, Error>(
                                &name,
                                options,
                                acyclic_fs::PublicationPermit::Unrestricted,
                                io,
                                io.cancellation(),
                                task.as_ref(),
                            )
                            .await,
                    )?,
                    None => io.sdk(
                        workspace
                            .fork::<_, _, Error>(
                                &name,
                                options,
                                acyclic_fs::PublicationPermit::Unrestricted,
                                io,
                                io.cancellation(),
                                &mut |_| {
                                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                                        value: (),
                                        work: WorkCounters::default(),
                                    }))
                                },
                            )
                            .await,
                    )?,
                };
                Ok::<_, Error>(forked)
            })
            .await
            .map_err(map_error)??;
        io.remaining()?;
        let initial = io
            .sdk(
                forked
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )?
            .ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "fork creation has no durable original generation",
                )
            })?;
        fork_preparer_io::retain(&initial, Some(io)).await?;
        let observed = WorkspaceObservation {
            workspace: WorkspaceRef::new(
                host.provider.clone_with_io(io)?,
                name.into_bytes(),
                None,
            )?,
            generation: fork_preparer_io::generation_ref(&host.provider, initial.id(), Some(io))?,
        };
        if io.original_native_work().is_none()
            && let Some(task) = preparation
        {
            task.verify(false, Some(io))?.await?;
        }
        Ok(observed)
    }

    /// Plans a destination-authorized volume import.
    pub async fn prepare_volume_import<'a>(
        &self,
        source: &VolumeRef,
        source_grant: &'a ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &'a ContentGrant,
    ) -> Result<VolumeImportPlan<'a, A, O>> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        source.validate(&mut None)?;
        destination.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(destination.provider())?;
        let source_workspace = self
            .open(
                &workspace_ref(self.provider.clone(), &source.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        let destination_workspace = self
            .open(
                &workspace_ref(self.provider.clone(), &destination.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        let source_generation = self.generation(&source_workspace, generation, None).await?;
        let target = destination_workspace
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let source_access = super::volume_access::compile(
            &source_workspace,
            source_grant,
            Some(source_generation.id()),
            VolumeOperation::Read,
            &[
                acyclic_fs::access::Action::Read,
                acyclic_fs::access::Action::List,
            ],
            &io,
        )
        .await?;
        let destination_access = super::volume_access::compile(
            &destination_workspace,
            destination_grant,
            None,
            VolumeOperation::Write,
            &[acyclic_fs::access::Action::MergeInto],
            &io,
        )
        .await?;
        let plan = source_workspace
            .join_into(&destination_workspace)
            .plan_at(
                &source_generation,
                &target,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        super::super::access::require_current(&source_workspace, &source_access.0, &io).await?;
        super::super::access::require_current(&destination_workspace, &destination_access.0, &io)
            .await?;
        Ok(VolumeImportPlan::new(plan, source_grant, destination_grant))
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
        let digest =
            blake3::derive_key("acyclic harness volume initialization v1", name.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest[..16]);
        self.filesystem
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                &name,
                acyclic_fs::VolumeInitialization {
                    config: acyclic_fs::VolumeConfig::portable(
                        if self.filesystem.capabilities().durable {
                            acyclic_fs::Lifecycle::Durable
                        } else {
                            acyclic_fs::Lifecycle::Ephemeral
                        },
                    ),
                    access_policy: Some(access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::from_bytes(operation),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| map_error(failure.error))?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?, None)
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
        (audience, verifier, crate::core::SchemaRegistry::new()),
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
    let agent = scope
        .agent()
        .ok_or_else(|| invalid("ROOT binding has no owning agent"))?;
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
