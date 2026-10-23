#[tokio::test]
async fn restrictive_policy_blocks_wire_reads_and_both_credential_issuers()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
        .with_credential_issuer(Arc::new(TestIssuer));
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "restricted".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("missing workspace")?;
    let reference = created.workspace.ok_or("missing reference")?;
    let owner = service
        .filesystem
        .open_workspace(
            "restricted",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    crate::access::fixtures::publish_policy(
        &owner,
        &crate::access::fixtures::denied(crate::access::Action::Read, "secrets/**"),
    )
    .await?;
    assert_eq!(
        service
            .workspace(Some(reference.clone()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::PermissionDenied
    );
    for kind in [CredentialKind::Mount, CredentialKind::S3] {
        let error = service
            .issue_credential(
                Request::new(wire::CredentialRequest {
                    workspace: Some(reference.clone()),
                    generation: None,
                    writable: true,
                    expires_after_seconds: 60,
                    operation: operation(2),
                }),
                kind,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
    Ok(())
}

struct DeadlineIssuer {
    maximum: u64,
    widen: bool,
}

#[tonic::async_trait]
impl FilesystemCredentialIssuer for DeadlineIssuer {
    fn mount_enabled(&self) -> bool {
        true
    }

    fn s3_enabled(&self) -> bool {
        false
    }

    async fn issue(&self, request: CredentialGrantRequest) -> Result<CredentialGrant, Status> {
        assert!(request.expires_after_seconds <= 60);
        assert!(u64::try_from(request.expires_at_unix_seconds).unwrap() <= self.maximum);
        Ok(CredentialGrant::Bearer {
            endpoint: "mount://deadline".into(),
            token: "bounded".into(),
            expires_at_unix_seconds: request.expires_at_unix_seconds + i64::from(self.widen),
        })
    }
}

#[tokio::test]
async fn credential_deadlines_are_intersected_and_a_wider_issuer_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    for widen in [false, true] {
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + 60;
        let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?
            .with_credential_issuer(Arc::new(DeadlineIssuer {
                maximum: deadline,
                widen,
            }));
        let created = service
            .create_workspace(Request::new(wire::CreateWorkspaceRequest {
                name: "expiring".into(),
                profile: wire::FilesystemProfile::Portable as i32,
                operation: operation(1),
            }))
            .await?
            .into_inner()
            .workspace
            .ok_or("missing workspace")?;
        let owner = service
            .filesystem
            .open_workspace("expiring", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value;
        let mut policy: crate::access::Policy =
            serde_json::from_slice(crate::access::INITIAL_POLICY)?;
        policy.version += 1;
        policy.layers[0][0].conditions.expires_at = Some(deadline);
        crate::access::fixtures::publish_policy(&owner, &policy).await?;
        let result = service
            .issue_mount_credential(Request::new(wire::CredentialRequest {
                workspace: created.workspace,
                generation: None,
                writable: true,
                expires_after_seconds: 600,
                operation: operation(2),
            }))
            .await;
        if widen {
            assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
        } else {
            assert_eq!(
                result?.into_inner().expires_at_unix_seconds,
                i64::try_from(deadline)?
            );
        }
    }
    Ok(())
}

thread_local! {
    static OMIT_EXPORT_FENCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn omitted_export_fence() -> bool {
    OMIT_EXPORT_FENCE.get()
}

async fn export_revocation_cut(
    cut: usize,
    resumed: bool,
    omitted: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "export-revocation".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("workspace")?;
    let generation = service
        .apply_transaction(Request::new(wire::ApplyTransactionRequest {
            base: created.head,
            mutations: vec![mutation(wire::mutation::Mutation::CreateFile(
                wire::CreateFile {
                    path: "/secret".into(),
                    contents: vec![17; crate::kernel::MAXIMUM_INLINE_FILE_BYTES + 1],
                    metadata: None,
                },
            ))],
            operation: operation(2),
            maximum_conflicts: 8,
        }))
        .await?
        .into_inner()
        .generation
        .ok_or("generation")?;
    let owner = service
        .filesystem
        .open_workspace(
            "export-revocation",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let mut output = service
        .export(Request::new(wire::ExportRequest {
            generation: Some(generation),
            after: if resumed {
                1_u64.to_le_bytes().to_vec()
            } else {
                vec![]
            },
            maximum_objects: 64,
            maximum_bytes: 1024 * 1024,
        }))
        .await?
        .into_inner();
    for _ in 0..cut {
        output
            .next()
            .await
            .ok_or("export ended before revocation")??;
    }
    crate::access::fixtures::publish_policy(
        &owner,
        &crate::access::fixtures::denied(crate::access::Action::Read, "**"),
    )
    .await?;
    OMIT_EXPORT_FENCE.set(omitted);
    let next = output.next().await;
    OMIT_EXPORT_FENCE.set(false);
    assert!(
        matches!(next, Some(Err(ref error)) if error.code() == tonic::Code::PermissionDenied),
        "export disclosed a chunk after revocation"
    );
    Ok(())
}

#[tokio::test]
async fn export_checks_manifest_objects_and_resumed_streams_after_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    for cut in [0, 1, 2] {
        export_revocation_cut(cut, false, false).await?;
    }
    export_revocation_cut(0, true, false).await
}

#[tokio::test]
#[should_panic(expected = "export disclosed a chunk after revocation")]
async fn removed_export_fence_negative_control() {
    export_revocation_cut(1, false, true).await.unwrap();
}

thread_local! {
    static REVOKE_DURING_FORK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) async fn fork_policy_cut<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    source: &Workspace<A, O>,
) {
    if !REVOKE_DURING_FORK.replace(false) {
        return;
    }
    let policy = crate::access::fixtures::denied(crate::access::Action::Read, "**");
    assert!(
        crate::access::fixtures::publish_policy(source, &policy)
            .await
            .is_err(),
        "wire fork allowed source revocation before destination publication"
    );
}

#[tokio::test]
async fn wire_fork_freezes_source_policy_through_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "fork-policy-cut".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("workspace")?;
    REVOKE_DURING_FORK.set(true);
    let forked = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            source: created.head.clone(),
            destination_name: "forked-policy-cut".into(),
            selection: None,
            operation: operation(2),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("fork")?;
    assert_eq!(
        forked.workspace.ok_or("reference")?.name,
        "forked-policy-cut"
    );
    let owner = service
        .filesystem
        .open_workspace(
            "fork-policy-cut",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    crate::access::fixtures::publish_policy(
        &owner,
        &crate::access::fixtures::denied(crate::access::Action::Read, "**"),
    )
    .await?;
    let denied = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            source: created.head,
            destination_name: "revoked-fork".into(),
            selection: None,
            operation: operation(3),
        }))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(
        service
            .filesystem
            .open_workspace(
                "revoked-fork",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn wire_fork_rejects_expiring_source_authority() -> Result<(), Box<dyn std::error::Error>> {
    let service = FilesystemWireService::new(Fs::memory(), FilesystemWireLimits::default())?;
    let created = service
        .create_workspace(Request::new(wire::CreateWorkspaceRequest {
            name: "expiring-fork".into(),
            profile: wire::FilesystemProfile::Portable as i32,
            operation: operation(1),
        }))
        .await?
        .into_inner()
        .workspace
        .ok_or("workspace")?;
    let owner = service
        .filesystem
        .open_workspace(
            "expiring-fork",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let mut policy: crate::access::Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    policy.version += 1;
    policy.layers[0][0].conditions.expires_at = Some(u64::MAX);
    crate::access::fixtures::publish_policy(&owner, &policy).await?;
    let result = service
        .fork_workspace(Request::new(wire::ForkWorkspaceRequest {
            source: created.head,
            destination_name: "expired-fork-target".into(),
            selection: None,
            operation: operation(2),
        }))
        .await
        .unwrap_err();
    assert_eq!(result.code(), tonic::Code::PermissionDenied);
    Ok(())
}
