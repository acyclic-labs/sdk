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
