#[cfg(all(test, feature = "memory", feature = "distributed"))]
mod tests {
    use super::*;
    use crate::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer;
    use crate::{
        EmbeddedCapabilities, FilesystemSourceProvider, FilesystemWireLimits,
        FilesystemWireService, HostedSourceOperation, HostedSourceScope,
    };
    use rcgen::generate_simple_self_signed;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    struct ClientTestSourceProvider {
        generation: std::sync::Mutex<Option<GenerationId>>,
    }

    #[tonic::async_trait]
    impl FilesystemSourceProvider for ClientTestSourceProvider {
        async fn state(&self, _scope: HostedSourceScope) -> Result<HostedSourceResult, Status> {
            Ok(HostedSourceResult {
                state: HostedSourceState::NeedsRescan(HostedSourceInvalidation::QueueOverflow),
                generation_id: None,
            })
        }

        async fn reconcile(
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

        async fn rescan(
            &self,
            operation: HostedSourceOperation,
        ) -> Result<HostedSourceResult, Status> {
            self.reconcile(operation).await
        }

        async fn seal(
            &self,
            _operation: HostedSourceOperation,
        ) -> Result<HostedSourceResult, Status> {
            Ok(HostedSourceResult {
                state: HostedSourceState::Sealed,
                generation_id: *self
                    .generation
                    .lock()
                    .map_err(|_| Status::internal("lock"))?,
            })
        }
    }

    #[tokio::test]
    async fn hosted_constructor_uses_the_same_canonical_engine_over_real_grpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let embedded = crate::MemoryFs::memory();
        let limits = FilesystemWireLimits {
            maximum_response_bytes: 1_024,
            maximum_transaction_mutations: 2,
            maximum_page_items: 2,
            ..FilesystemWireLimits::default()
        };
        let source_provider = Arc::new(ClientTestSourceProvider {
            generation: std::sync::Mutex::new(None),
        });
        let service = FilesystemServiceServer::new(
            FilesystemWireService::new(embedded, limits)?
                .with_source_provider(source_provider.clone()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stop_rx.await;
                })
                .await
        });

        let endpoint = format!("http://{address}");
        let mut minimum_options = HostedFsOptions::new(&endpoint, "test-account-token");
        minimum_options.maximum_response_bytes = MINIMUM_HANDSHAKE_RESPONSE_BYTES;
        let minimum_hosted = HostedFs::connect(minimum_options).await?;
        assert_eq!(
            minimum_hosted.capabilities().maximum_response_bytes,
            u64::try_from(MINIMUM_HANDSHAKE_RESPONSE_BYTES)? - MAX_BYTE_RESPONSE_ENVELOPE_BYTES
        );

        let mut options = HostedFsOptions::new(endpoint, "test-account-token");
        options.maximum_response_bytes = 1_024 + usize::try_from(MAX_BYTE_RESPONSE_ENVELOPE_BYTES)?;
        let hosted = HostedFs::connect(options).await?;
        assert_eq!(hosted.capabilities().maximum_response_bytes, 1_024);
        assert_eq!(hosted.capabilities().maximum_transaction_mutations, 2);
        assert_eq!(hosted.capabilities().maximum_page_items, 2);
        let workspace = hosted
            .create_workspace(
                "hosted",
                EmbeddedProfile::Portable,
                IdempotencyKey::from_bytes([1; 16]),
            )
            .await?;
        let head_bytes: [u8; 32] = workspace.head.generation_id.as_slice().try_into()?;
        *source_provider
            .generation
            .lock()
            .map_err(|_| "source provider lock poisoned")? =
            Some(GenerationId::new(Digest::from_bytes(head_bytes)));
        assert_eq!(
            workspace.source_state().await?.state,
            HostedSourceState::NeedsRescan(HostedSourceInvalidation::QueueOverflow)
        );
        let reconciled = workspace
            .reconcile_source(IdempotencyKey::from_bytes([15; 16]))
            .await?;
        assert_eq!(reconciled.state, HostedSourceState::Clean);
        assert_eq!(
            reconciled.generation_id,
            *source_provider
                .generation
                .lock()
                .map_err(|_| "source provider lock poisoned")?
        );
        let rescanned = workspace
            .rescan_source(IdempotencyKey::from_bytes([16; 16]))
            .await?;
        assert_eq!(rescanned.state, HostedSourceState::Clean);
        let sealed = workspace.seal(IdempotencyKey::from_bytes([17; 16])).await?;
        assert_eq!(sealed.id(), workspace.head.generation_id);
        let mut transaction = workspace.begin_transaction(IdempotencyKey::from_bytes([2; 16]));
        transaction.put_file("/value", vec![b'x'; 1_024]);
        let outcome = transaction.commit(1).await?;
        assert!(matches!(
            wire::MutationStatus::try_from(outcome.status),
            Ok(wire::MutationStatus::Committed)
        ));
        let workspace = hosted.open_workspace("hosted").await?;
        let head = workspace.head().await?;
        assert_eq!(head.read("/value", 1_024).await?.len(), 1_024);
        let child = head
            .fork("child", IdempotencyKey::from_bytes([3; 16]), None)
            .await?;
        let sibling = head
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]), None)
            .await?;
        let stale_target = head
            .fork("stale-target", IdempotencyKey::from_bytes([14; 16]), None)
            .await?;
        assert_eq!(
            child.head().await?.read("/value", 1_024).await?.len(),
            1_024
        );
        let child_before = child.head().await?;
        let mut child_change = child.begin_transaction(IdempotencyKey::from_bytes([4; 16]));
        child_change.write("/value", 0, b"joined".to_vec());
        let child_change = child_change.commit(1).await?;
        assert_eq!(child_change.status, wire::MutationStatus::Committed as i32);
        let child = hosted.open_workspace("child").await?;
        let child_after = child.head().await?;
        let child_diff = child_before.diff(&child_after, 2).await?;
        assert_eq!(child_diff.from.as_ref(), Some(&child_before.reference));
        assert_eq!(child_diff.to.as_ref(), Some(&child_after.reference));
        assert_eq!(child_diff.files.len(), 1);
        assert!(child_diff.bindings.is_empty());
        assert!(!child_diff.truncated);
        let plan = child_after
            .plan_join(&workspace.head().await?, wire::JoinHistory::Merge, 2, 2, 2)
            .await?;
        let mut tampered = plan.clone();
        let Some(common_ancestor) = tampered.common_ancestor.as_mut() else {
            return Err("join plan has no common ancestor".into());
        };
        let Some(identity_byte) = common_ancestor.generation_id.first_mut() else {
            return Err("generation identity is empty".into());
        };
        *identity_byte ^= 0x01;
        assert!(matches!(
            hosted
                .apply_join(tampered, IdempotencyKey::from_bytes([7; 16]))
                .await,
            Err(HostedFsError::Status(status)) if status.code() == tonic::Code::InvalidArgument
        ));
        let stale_plan = child_after
            .plan_join(
                &stale_target.head().await?,
                wire::JoinHistory::Merge,
                2,
                2,
                2,
            )
            .await?;
        let mut target_change =
            stale_target.begin_transaction(IdempotencyKey::from_bytes([12; 16]));
        target_change.create_directories("/target-only");
        let target_change = target_change.commit(1).await?;
        assert_eq!(target_change.status, wire::MutationStatus::Committed as i32);
        let target_before_stale_apply = stale_target.head().await?;
        let stale = hosted
            .apply_join(stale_plan, IdempotencyKey::from_bytes([8; 16]))
            .await?;
        assert_eq!(stale.status, wire::JoinStatus::StaleTarget as i32);
        assert_eq!(
            stale.generation.as_ref(),
            Some(&target_before_stale_apply.reference)
        );
        let target_after_stale_apply = stale_target.head().await?;
        assert_eq!(
            target_after_stale_apply.reference,
            target_before_stale_apply.reference
        );
        assert_eq!(
            target_after_stale_apply.read("/value", 1_024).await?,
            vec![b'x'; 1_024]
        );
        target_after_stale_apply.stat("/target-only").await?;
        let joined = hosted
            .apply_join(plan, IdempotencyKey::from_bytes([13; 16]))
            .await?;
        assert_eq!(joined.status, wire::MutationStatus::Committed as i32);
        let workspace = hosted.open_workspace("hosted").await?;
        assert!(
            workspace
                .head()
                .await?
                .read("/value", 1_024)
                .await?
                .starts_with(b"joined")
        );
        let sibling_plan = child_after
            .plan_join(&sibling.head().await?, wire::JoinHistory::Merge, 2, 2, 2)
            .await?;
        let Some(sibling_ancestor) = sibling_plan.common_ancestor.as_ref() else {
            return Err("sibling join plan has no common ancestor".into());
        };
        assert!(sibling_ancestor.workspace.is_none());
        let sibling_joined = hosted
            .apply_join(sibling_plan, IdempotencyKey::from_bytes([9; 16]))
            .await?;
        assert_eq!(
            sibling_joined.status,
            wire::MutationStatus::Committed as i32
        );
        let sibling = hosted.open_workspace("sibling").await?;
        assert!(
            sibling
                .head()
                .await?
                .read("/value", 1_024)
                .await?
                .starts_with(b"joined")
        );
        assert!(matches!(
            child.head().await?.read("/value", 1_025).await,
            Err(HostedFsError::LimitExceeded("read bytes"))
        ));
        assert!(matches!(
            child.head().await?.list_directory("/", None, 3).await,
            Err(HostedFsError::LimitExceeded("directory items"))
        ));
        let mut oversized = child.begin_transaction(IdempotencyKey::from_bytes([10; 16]));
        oversized.create_directories("/one");
        oversized.create_directories("/two");
        oversized.create_directories("/three");
        assert!(matches!(
            oversized.commit(1).await,
            Err(HostedFsError::LimitExceeded("transaction mutations"))
        ));
        let empty = child.begin_transaction(IdempotencyKey::from_bytes([11; 16]));
        assert!(matches!(
            empty.commit(1).await,
            Err(HostedFsError::LimitExceeded("transaction mutations"))
        ));

        stop_tx
            .send(())
            .map_err(|()| "hosted test server disappeared")?;
        server.await??;
        let _ = EmbeddedCapabilities::MEMORY;
        Ok(())
    }

    #[tokio::test]
    async fn private_ca_https_constructor_renews_credentials_and_checks_handshake()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let embedded = crate::MemoryFs::memory();
        let service = FilesystemServiceServer::with_interceptor(
            FilesystemWireService::new(embedded, FilesystemWireLimits::default())?,
            |request: Request<()>| {
                if request
                    .metadata()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer exact-token")
                {
                    return Err(Status::unauthenticated("missing exact bearer credential"));
                }
                Ok(request)
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stop_rx.await;
                })
                .await
        });
        let endpoint = format!("https://localhost:{}", address.port());

        assert!(
            HostedFs::connect(HostedFsOptions::new(&endpoint, "exact-token"))
                .await
                .is_err()
        );
        assert!(matches!(
            HostedFs::connect(
                HostedFsOptions::new(&endpoint, "wrong-token")
                    .with_ca_certificate(certificate_pem.clone())
            )
            .await,
            Err(HostedFsError::Status(status)) if status.code() == tonic::Code::Unauthenticated
        ));
        let hosted = HostedFs::connect(
            HostedFsOptions::new(&endpoint, "exact-token")
                .with_ca_certificate(certificate_pem.clone()),
        )
        .await?;
        assert_eq!(hosted.capabilities().contract_version, "1");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resolved = calls.clone();
        let renewed = HostedFs::connect_with_credential(
            HostedFsOptions::new(endpoint, "ignored-static-token")
                .with_ca_certificate(certificate_pem),
            CredentialProvider::new(move || {
                std::future::ready(
                    if resolved.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < 3 {
                        Ok("exact-token".into())
                    } else {
                        Err(acyclic_auth::credential::CredentialError::Invalid)
                    },
                )
            }),
        )
        .await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        let workspace = renewed
            .create_workspace(
                "renewal-workspace",
                EmbeddedProfile::Portable,
                IdempotencyKey::from_bytes([21; 16]),
            )
            .await?;
        workspace.head().await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert!(matches!(renewed.open_workspace("renewal-workspace").await,
            Err(HostedFsError::Status(status)) if status.code() == tonic::Code::Unauthenticated));

        stop_tx
            .send(())
            .map_err(|()| "hosted TLS test server disappeared")?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn hosted_constructor_rejects_unbounded_ca_and_malformed_handshake()
    -> Result<(), HostedFsError> {
        let debug = format!(
            "{:?}",
            HostedFsOptions::new("https://localhost", "secret-token")
        );
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
        let oversized = HostedFsOptions::new("https://localhost", "token")
            .with_ca_certificate(vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]);
        assert!(matches!(
            HostedFs::connect(oversized).await,
            Err(HostedFsError::InvalidOptions(
                "CA certificate must contain 1 to 65536 bytes"
            ))
        ));

        let mut undersized = HostedFsOptions::new("https://localhost", "token");
        undersized.maximum_response_bytes = MINIMUM_HANDSHAKE_RESPONSE_BYTES - 1;
        assert!(matches!(
            HostedFs::connect(undersized).await,
            Err(HostedFsError::InvalidOptions(
                "request bound must be nonzero and response bound must admit the handshake"
            ))
        ));

        let largest_byte_envelope = wire::ExportChunk {
            cursor: vec![0; 8],
            object_id: vec![0; 33],
            contents: vec![0; 1_024],
            terminal: true,
        };
        assert!(
            u64::try_from(largest_byte_envelope.encoded_len()).unwrap_or(u64::MAX)
                <= 1_024 + MAX_BYTE_RESPONSE_ENVELOPE_BYTES
        );

        let malformed = wire::HandshakeResponse {
            protocol: Some(protocol::HandshakeResponse {
                protocol: Some(protocol::ProtocolIdentity {
                    version: "1".to_owned(),
                    descriptor_digest: "substituted".to_owned(),
                }),
                supported: Some(protocol::CapabilitySet {
                    capabilities: vec![protocol::Capability {
                        name: "filesystem".to_owned(),
                        version: "1".to_owned(),
                    }],
                }),
            }),
            capabilities: Some(wire::Capabilities {
                contract_version: "1".to_owned(),
                profiles: Vec::new(),
                maximum_request_bytes: 1,
                maximum_response_bytes: 1,
                maximum_transaction_mutations: 1,
                maximum_page_items: 1,
                native_mount_credentials: false,
                s3_credentials: false,
                source_reconciliation: false,
            }),
        };
        assert!(matches!(
            validate_handshake(malformed),
            Err(HostedFsError::InvalidResponse(
                "filesystem descriptor digest does not match"
            ))
        ));

        let access = hosted_s3_access(wire::CredentialResponse {
            endpoint: "https://s3.example.test".to_owned(),
            expires_at_unix_seconds: 1_900_000_000,
            credential: Some(wire::credential_response::Credential::S3(
                wire::S3Credential {
                    bucket: "workspace".to_owned(),
                    region: "eu-west-2".to_owned(),
                    access_key_id: "visible-only-to-caller".to_owned(),
                    secret_access_key: "never-log-this-secret".to_owned(),
                    session_token: String::new(),
                },
            )),
        })?;
        assert_eq!(access.region, "eu-west-2");
        assert!(access.session_token.is_empty());
        let debug = format!("{access:?}");
        assert!(!debug.contains("visible-only-to-caller"));
        assert!(!debug.contains("never-log-this-secret"));
        assert!(debug.matches("[REDACTED]").count() >= 3);

        assert!(matches!(
            hosted_s3_access(wire::CredentialResponse {
                endpoint: "https://s3.example.test".to_owned(),
                expires_at_unix_seconds: 1_900_000_000,
                credential: Some(wire::credential_response::Credential::BearerToken(
                    "wrong-kind".to_owned()
                )),
            }),
            Err(HostedFsError::InvalidResponse(
                "S3 credential response has the wrong credential kind"
            ))
        ));
        Ok(())
    }
}
