use super::*;
use crate::wire::filesystem::v2::filesystem_service_server::FilesystemService as _;

include!("access_tests.rs");
#[path = "metadata_tests.rs"]
mod metadata_tests;

struct TestIssuer;

struct TestSourceProvider {
    generation: std::sync::Mutex<Option<GenerationId>>,
}

#[tonic::async_trait]
impl FilesystemSourceProvider for TestSourceProvider {
    async fn state(&self, _scope: HostedSourceScope) -> Result<HostedSourceResult, Status> {
        Ok(HostedSourceResult {
            state: HostedSourceState::NeedsRescan(HostedSourceInvalidation::QueueOverflow),
            generation_id: None,
        })
    }

    async fn reconcile(
        &self,
        operation: HostedSourceOperation,
    ) -> Result<HostedSourceResult, Status> {
        if operation.idempotency_key != IdempotencyKey::from_bytes([9; 16]) {
            return Err(Status::invalid_argument(
                "unexpected reconciliation identity",
            ));
        }
        Ok(HostedSourceResult {
            state: HostedSourceState::Clean,
            generation_id: *self
                .generation
                .lock()
                .map_err(|_| Status::internal("lock"))?,
        })
    }

    async fn rescan(
        &self,
        _operation: HostedSourceOperation,
    ) -> Result<HostedSourceResult, Status> {
        Ok(HostedSourceResult {
            state: HostedSourceState::Clean,
            generation_id: *self
                .generation
                .lock()
                .map_err(|_| Status::internal("lock"))?,
        })
    }

    async fn seal(&self, _operation: HostedSourceOperation) -> Result<HostedSourceResult, Status> {
        Ok(HostedSourceResult {
            state: HostedSourceState::Sealed,
            generation_id: *self
                .generation
                .lock()
                .map_err(|_| Status::internal("lock"))?,
        })
    }
}

#[tonic::async_trait]
impl FilesystemCredentialIssuer for TestIssuer {
    fn mount_enabled(&self) -> bool {
        true
    }

    fn s3_enabled(&self) -> bool {
        true
    }

    async fn issue(&self, request: CredentialGrantRequest) -> Result<CredentialGrant, Status> {
        Ok(match request.kind {
            CredentialKind::Mount => CredentialGrant::Bearer {
                endpoint: "mount://test".to_owned(),
                token: hex::encode(request.workspace_id.into_bytes()),
                expires_at_unix_seconds: request.expires_at_unix_seconds,
            },
            CredentialKind::S3 => CredentialGrant::S3 {
                endpoint: "https://s3.test".to_owned(),
                bucket: "opaque-bucket".to_owned(),
                region: "auto".to_owned(),
                access_key_id: "access".to_owned(),
                secret_access_key: "secret".to_owned(),
                session_token: String::new(),
                expires_at_unix_seconds: request.expires_at_unix_seconds,
            },
        })
    }
}

fn operation(value: u8) -> Option<wire::OperationOptions> {
    Some(wire::OperationOptions {
        idempotency_key: operation_bytes(value),
    })
}

fn operation_bytes(value: u8) -> Vec<u8> {
    let mut id = [0_u8; 16];
    id[15] = value;
    id.to_vec()
}

fn mutation(value: wire::mutation::Mutation) -> wire::Mutation {
    wire::Mutation {
        mutation: Some(value),
    }
}

#[tokio::test]
async fn handshake_and_creation_expose_every_canonical_profile()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let advertised = service
        .handshake(Request::new(wire::HandshakeRequest { protocol: None }))
        .await?
        .into_inner()
        .capabilities
        .ok_or("missing capabilities")?;
    let profiles = [
        wire::FilesystemProfile::Portable,
        wire::FilesystemProfile::Posix,
        wire::FilesystemProfile::Windows,
        wire::FilesystemProfile::Browser,
    ];
    assert_eq!(advertised.profiles, profiles.map(|profile| profile as i32));
    for (index, selected) in profiles.into_iter().enumerate() {
        let created = service
            .create_workspace(Request::new(wire::CreateWorkspaceRequest {
                name: format!("profile-{index}"),
                profile: selected as i32,
                operation: operation(u8::try_from(index + 1)?),
            }))
            .await?
            .into_inner()
            .workspace
            .ok_or("missing workspace")?;
        assert_eq!(created.profile, selected as i32);
        let observed = service
            .observe(Request::new(wire::ObserveRequest {
                workspace: created.workspace.clone(),
                operation_id: operation_bytes(u8::try_from(index + 1)?),
            }))
            .await?
            .into_inner();
        assert_eq!(observed.state, "completed");
        assert_eq!(
            observed.outcome.and_then(|outcome| outcome.generation),
            created.head
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_provider_controls_capability_and_all_lifecycle_rpcs()
-> Result<(), Box<dyn std::error::Error>> {
    let absent = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let capabilities = absent
        .handshake(Request::new(wire::HandshakeRequest { protocol: None }))
        .await?
        .into_inner()
        .capabilities
        .ok_or("missing capabilities")?;
    assert!(!capabilities.source_reconciliation);

    let provider = Arc::new(TestSourceProvider {
        generation: std::sync::Mutex::new(None),
    });
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .with_source_provider(provider.clone());
    let capabilities = service
        .handshake(Request::new(wire::HandshakeRequest { protocol: None }))
        .await?
        .into_inner()
        .capabilities
        .ok_or("missing capabilities")?;
    assert!(capabilities.source_reconciliation);
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "source".to_owned(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing workspace")?;
    let workspace = created.workspace.ok_or("missing workspace reference")?;
    let head = created.head.ok_or("missing head")?;
    *provider
        .generation
        .lock()
        .map_err(|_| "provider lock poisoned")? = Some(generation_id(&head.generation_id)?);

    let state = service
        .get_source_state(Request::new(wire::SourceStateRequest {
            workspace: Some(workspace.clone()),
        }))
        .await?
        .into_inner();
    assert_eq!(state.state, wire::SourceState::NeedsRescan as i32);
    assert_eq!(
        state.reason,
        wire::SourceInvalidationReason::QueueOverflow as i32
    );
    assert!(state.generation.is_none());

    let reconciled = service
        .reconcile_source(Request::new(wire::SourceOperationRequest {
            workspace: Some(workspace.clone()),
            operation: Some(wire::OperationOptions {
                idempotency_key: vec![9; 16],
            }),
        }))
        .await?
        .into_inner();
    assert_eq!(reconciled.state, wire::SourceState::Clean as i32);
    assert_eq!(reconciled.generation.as_ref(), Some(&head));

    for response in [
        service
            .rescan_source(Request::new(wire::SourceOperationRequest {
                workspace: Some(workspace.clone()),
                operation: operation(10),
            }))
            .await?
            .into_inner(),
        service
            .seal_source(Request::new(wire::SourceOperationRequest {
                workspace: Some(workspace),
                operation: operation(11),
            }))
            .await?
            .into_inner(),
    ] {
        assert_eq!(response.generation.as_ref(), Some(&head));
    }
    Ok(())
}

#[tokio::test]
async fn wire_adapter_uses_one_engine_for_bounded_customer_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .with_credential_issuer(Arc::new(TestIssuer));
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "main".to_owned(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing workspace")?;
    let initial = created.head.ok_or("missing head")?;
    let workspace = created.workspace.ok_or("missing workspace ref")?;

    let credential = service
        .issue_mount_credential(Request::new(wire::CredentialRequest {
            workspace: Some(workspace.clone()),
            generation: Some(initial.clone()),
            writable: false,
            expires_after_seconds: 60,
            operation: operation(8),
        }))
        .await?
        .into_inner();
    assert_eq!(credential.endpoint, "mount://test");
    assert!(matches!(
        credential.credential,
        Some(wire::credential_response::Credential::BearerToken(token)) if !token.is_empty()
    ));

    let committed = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: Some(initial),
            mutations: vec![
                mutation(wire::mutation::Mutation::CreateDirectory(
                    wire::CreateDirectory {
                        path: "/dir".to_owned(),
                        metadata: None,
                    },
                )),
                mutation(wire::mutation::Mutation::CreateFile(wire::CreateFile {
                    path: "/dir/value".to_owned(),
                    contents: b"value".to_vec(),
                    metadata: None,
                })),
            ],
            operation: operation(2),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner();
    assert_eq!(committed.status, wire::MutationStatus::Committed as i32);
    let generation = committed.generation.ok_or("missing generation")?;
    let observed = service
        .observe(Request::new(wire::ObserveRequest {
            workspace: Some(workspace.clone()),
            operation_id: operation_bytes(2),
        }))
        .await?
        .into_inner();
    assert_eq!(observed.state, "completed");
    assert_eq!(
        observed
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.generation.as_ref()),
        Some(&generation)
    );
    let unknown = service
        .cancel(Request::new(wire::CancelRequest {
            workspace: Some(workspace.clone()),
            operation_id: operation_bytes(99),
        }))
        .await?
        .into_inner()
        .operation
        .ok_or("missing cancellation observation")?;
    assert_eq!(unknown.state, "unknown");
    assert!(unknown.outcome.is_none());
    let read = service
        .read(Request::new(wire::ReadRequest {
            generation: Some(generation.clone()),
            path: "/dir/value".to_owned(),
            range: None,
            maximum_bytes: 5,
        }))
        .await?
        .into_inner();
    assert_eq!(read.contents, b"value");

    let listed = service
        .list_directory(Request::new(wire::ListDirectoryRequest {
            generation: Some(generation.clone()),
            path: "/dir".to_owned(),
            page: Some(wire::PageOptions {
                maximum_items: 8,
                after: None,
            }),
        }))
        .await?
        .into_inner()
        .page
        .ok_or("missing directory page")?;
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(
        listed.entries[0]
            .name
            .as_ref()
            .map(|name| name.bytes.as_slice()),
        Some(b"value".as_slice())
    );

    let pinned = service
        .pin(Request::new(wire::RetainGenerationRequest {
            generation: Some(generation.clone()),
            identity: "release".to_owned(),
            operation: operation(3),
        }))
        .await?
        .into_inner();
    assert_eq!(pinned.status, wire::MutationStatus::Committed as i32);

    let forked = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            selection: None,
            source: Some(generation),
            destination_name: "agent".to_owned(),
            operation: operation(4),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing fork")?;
    assert_ne!(
        forked
            .workspace
            .as_ref()
            .ok_or("missing fork ref")?
            .workspace_id,
        workspace.workspace_id
    );

    let main_head = service
        .get_head(Request::new(wire::GetHeadRequest {
            workspace: Some(workspace.clone()),
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing main head")?;
    let fork_head = forked.head.clone().ok_or("missing fork head")?;
    let no_op_plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(main_head.clone()),
            target: Some(fork_head.clone()),
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let changed = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: Some(main_head),
            mutations: vec![mutation(wire::mutation::Mutation::Write(wire::Write {
                path: "/dir/value".to_owned(),
                offset: 0,
                contents: b"new!!".to_vec(),
            }))],
            operation: operation(5),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing changed generation")?;
    let plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(changed.clone()),
            target: Some(fork_head),
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let fresh_no_op = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(no_op_plan.clone()),
            operation: operation(12),
        }))
        .await?
        .into_inner();
    assert_eq!(fresh_no_op.status, wire::JoinStatus::NoChanges as i32);
    let joined = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(plan.clone()),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(joined.status, wire::JoinStatus::Applied as i32);
    let joined_generation = joined.generation.ok_or("missing joined generation")?;
    let no_op_before_advance = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(no_op_plan.clone()),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(
        no_op_before_advance.status,
        wire::JoinStatus::IdempotencyConflict as i32
    );
    let advanced_target = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: Some(joined_generation.clone()),
            mutations: vec![mutation(wire::mutation::Mutation::CreateFile(
                wire::CreateFile {
                    path: "/target-only".to_owned(),
                    contents: b"target".to_vec(),
                    metadata: None,
                },
            ))],
            operation: operation(11),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing advanced target generation")?;
    let repeated = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(plan.clone()),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(repeated.status, wire::JoinStatus::AlreadyApplied as i32);
    assert_eq!(repeated.generation.as_ref(), Some(&joined_generation));
    let no_op_after_advance = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(no_op_plan),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(
        no_op_after_advance.status,
        wire::JoinStatus::IdempotencyConflict as i32
    );
    let mut altered_bounds = plan.clone();
    altered_bounds.maximum_generations += 1;
    altered_bounds.plan_id = join_plan_id(
        altered_bounds.source.as_ref().ok_or("missing source")?,
        altered_bounds
            .expected_target
            .as_ref()
            .ok_or("missing target")?,
        altered_bounds
            .common_ancestor
            .as_ref()
            .ok_or("missing common ancestor")?,
        join_history(altered_bounds.history)?,
        altered_bounds.maximum_generations,
        altered_bounds.maximum_changes,
        altered_bounds.maximum_conflicts,
    )
    .to_vec();
    let altered_retry = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(altered_bounds),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(
        altered_retry.status,
        wire::JoinStatus::IdempotencyConflict as i32
    );
    let stale = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(plan),
            operation: operation(9),
        }))
        .await?
        .into_inner();
    assert_eq!(stale.status, wire::JoinStatus::StaleTarget as i32);
    assert_eq!(stale.generation.as_ref(), Some(&advanced_target));

    let further_changed = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: Some(changed.clone()),
            mutations: vec![mutation(wire::mutation::Mutation::CreateFile(
                wire::CreateFile {
                    path: "/dir/next".to_owned(),
                    contents: b"next".to_vec(),
                    metadata: None,
                },
            ))],
            operation: operation(10),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing further generation")?;
    let different_plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(further_changed),
            target: Some(advanced_target),
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let conflicting_retry = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(different_plan),
            operation: operation(6),
        }))
        .await?
        .into_inner();
    assert_eq!(
        conflicting_retry.status,
        wire::JoinStatus::IdempotencyConflict as i32
    );

    let conflict_target = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            selection: None,
            source: Some(changed.clone()),
            destination_name: "conflict-target".to_owned(),
            operation: operation(20),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing conflict target")?;
    let conflict_source = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            selection: None,
            source: Some(changed.clone()),
            destination_name: "conflict-source".to_owned(),
            operation: operation(21),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing conflict source")?;
    let benign_source = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            selection: None,
            source: Some(changed.clone()),
            destination_name: "benign-source".to_owned(),
            operation: operation(22),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing benign source")?;

    let conflict_target_head = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: conflict_target.head,
            mutations: vec![mutation(wire::mutation::Mutation::Write(wire::Write {
                path: "/dir/value".to_owned(),
                offset: 0,
                contents: b"target".to_vec(),
            }))],
            operation: operation(23),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing conflict target generation")?;
    let conflict_source_head = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: conflict_source.head,
            mutations: vec![mutation(wire::mutation::Mutation::Write(wire::Write {
                path: "/dir/value".to_owned(),
                offset: 0,
                contents: b"source".to_vec(),
            }))],
            operation: operation(24),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing conflict source generation")?;
    let conflict_plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(conflict_source_head),
            target: Some(conflict_target_head.clone()),
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let fresh_conflict = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(conflict_plan.clone()),
            operation: operation(25),
        }))
        .await?
        .into_inner();
    assert_eq!(fresh_conflict.status, wire::JoinStatus::Conflicted as i32);

    let benign_source_head = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: benign_source.head,
            mutations: vec![mutation(wire::mutation::Mutation::CreateFile(
                wire::CreateFile {
                    path: "/benign".to_owned(),
                    contents: b"benign".to_vec(),
                    metadata: None,
                },
            ))],
            operation: operation(26),
            maximum_conflicts: 32,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("missing benign source generation")?;
    let benign_plan = service
        .plan_join(Request::new(wire::PlanJoinRequest {
            source: Some(benign_source_head),
            target: Some(conflict_target_head),
            maximum_changes: 32,
            maximum_conflicts: 8,
            maximum_generations: 32,
            history: wire::JoinHistory::Merge as i32,
        }))
        .await?
        .into_inner();
    let benign_join = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(benign_plan),
            operation: operation(27),
        }))
        .await?
        .into_inner();
    assert_eq!(benign_join.status, wire::JoinStatus::Applied as i32);
    let bound_conflict = service
        .apply_join(Request::new(wire::ApplyJoinRequest {
            plan: Some(conflict_plan),
            operation: operation(27),
        }))
        .await?
        .into_inner();
    assert_eq!(
        bound_conflict.status,
        wire::JoinStatus::IdempotencyConflict as i32
    );

    let mut exported = service
        .export(Request::new(wire::ExportRequest {
            generation: Some(changed),
            after: Vec::new(),
            maximum_objects: 64,
            maximum_bytes: 1024 * 1024,
        }))
        .await?
        .into_inner();
    let mut export_chunks = Vec::new();
    while let Some(chunk) = exported.next().await {
        export_chunks.push(chunk?);
    }
    assert!(export_chunks.len() > 1);
    assert!(export_chunks[0].object_id.is_empty());
    assert!(
        export_chunks
            .last()
            .ok_or("missing export terminal")?
            .terminal
    );

    let deleted = service
        .delete_workspace(Request::new(wire::DeleteWorkspaceRequest {
            workspace: Some(workspace),
            operation: operation(7),
        }))
        .await?
        .into_inner();
    assert_eq!(deleted.status, wire::MutationStatus::Committed as i32);
    Ok(())
}
