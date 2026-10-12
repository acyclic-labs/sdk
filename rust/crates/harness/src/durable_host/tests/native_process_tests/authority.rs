use super::*;

#[tokio::test]
async fn result_storage_cannot_mutate_a_pinned_native_view() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let mut authority = fixture.authority();
    authority.results = FilesystemContentPublisher::new(
        fixture.files.clone(),
        &fixture.destination,
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.scope,
        65_536,
        None,
    )?;
    assert!(matches!(
        async {
            Process::new(
                fixture.owner.clone(),
                fixture.command,
                &fixture.plan,
                fixture.view.clone(),
                fixture.approval(),
                authority,
                fixture.view.work(),
            )?
            .await
        }
        .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    let destination = workspace_ref(
        fixture.destination.provider().clone(),
        &fixture.destination.storage_name(&mut None)?,
    )?;
    assert!(
        fixture
            .files
            .read(&destination, None, "/native-process/result.json", 64)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn readonly_schema_storage_does_not_require_result_write_authority() -> Result<()> {
    let mut fixture =
        process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let setup = fixture
        .issuer
        .issue(
            "schema-setup",
            &Capabilities::new([fixture
                .source
                .capability(VolumeOperation::Write, &mut None)?]),
            Some(AgentId::from_bytes([61; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let grant = ContentGrant::verify(
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &setup,
        &fixture.source,
        VolumeOperation::Write,
    )?;
    fixture.plan.result_schema = fixture
        .files
        .put_content(
            &fixture.source,
            &grant,
            "schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
            128,
            &IdempotencyKey::new("readonly-native-schema")?,
            None,
            None,
        )
        .await?;
    assert!(
        fixture
            .owner
            .require_volume_grant(&fixture.source, VolumeOperation::Write, &mut None)
            .is_err()
    );
    let source = workspace_ref(
        fixture.source.provider().clone(),
        &fixture.source.storage_name(&mut None)?,
    )?;
    let before = fixture.files.resolve(&source).await?.generation;
    let status = fixture
        .effects(fixture.provider(fixture.view.work()).await?)?
        .run_task_effect(&fixture.owner, fixture.command, fixture.plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = status else {
        return Err(Error::Storage(format!(
            "readonly schema did not complete: {status:?}"
        )));
    };
    assert_eq!(result.volume(), fixture.results.volume());
    assert_ne!(result.volume(), fixture.plan.result_schema.volume());
    assert_eq!(fixture.files.resolve(&source).await?.generation, before);
    Ok(())
}

#[tokio::test]
async fn original_native_prepared_binding_preserves_approval_and_cancellation_before_launch()
-> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let fixture = process_fixture(
        Arc::new(MemoryStream::default()),
        "write",
        true,
        Some(&work),
    )
    .await?;
    assert!(fixture.view.work().shares_ledger(&work));
    let foreign_cancellation = acyclic_fs::CancellationToken::new();
    let foreign = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &foreign_cancellation,
    )?;
    let foreign_prefix = foreign.receipt();
    let original_prefix = work.receipt();
    assert!(matches!(
        Process::new(
            fixture.owner.clone(),
            fixture.command,
            &fixture.plan,
            fixture.view.clone(),
            fixture.approval(),
            fixture.authority(),
            &foreign,
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            ..
        })
    ));
    assert_eq!(foreign.receipt(), foreign_prefix);
    assert_eq!(work.receipt(), original_prefix);
    let request = fixture.request().await?;
    assert_eq!(
        request.approval_digest(
            fixture.owner.task_binding().0,
            fixture.command,
            Some(&crate::filesystem::PreparationIo::from_native_work(&work)?)
        )?,
        request.approval_digest(fixture.owner.task_binding().0, fixture.command, None)?,
    );
    let process = fixture.provider(&work).await?;
    let effect = task_effect_id(fixture.owner.task_binding().0, fixture.command, &mut None)?;
    let attempt = crate::EffectAttemptId::from_bytes(effect.into_bytes());
    let dispatch = EffectDispatch {
        provider: fixture.plan.provider.clone(),
        effect_id: effect,
        attempt_id: attempt,
        effect_kind: fixture.plan.effect_kind.clone(),
        request: fixture.plan.request.clone(),
        guarantee: fixture.plan.guarantee,
        request_digest: [0; 32],
    };
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(
        process.dispatch_with_native_work(dispatch, &work),
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt(), before);
    assert!(fixture.view.work().cancellation().is_cancelled());
    assert!(
        !fixture
            .view
            .manifest()
            .options
            .root
            .join("dst/output.txt")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn changed_invocations_cannot_use_the_original_approval() -> Result<()> {
    for change in [
        "argv",
        "cwd",
        "environment",
        "bounds",
        "authority",
        "volume-order",
        "mcp-initialization",
        "mcp-operation",
        "mcp-method",
        "mcp-params",
        "mcp-bytes",
        "stdin-bytes",
        "stdin-empty",
    ] {
        let mut fixture = process_fixture(
            Arc::new(MemoryStream::default()),
            if change.starts_with("mcp-") {
                "mcp-approval"
            } else if change == "stdin-bytes" {
                "stdin-bytes"
            } else {
                "write"
            },
            true,
            None,
        )
        .await?;
        drop(fixture.provider(fixture.view.work()).await?);
        let mut request = fixture.request().await?;
        match change {
            "argv" => request.argv.swap(0, 1),
            "cwd" => request.cwd = request.view.volumes[0].path.clone(),
            "environment" => {
                request
                    .environment
                    .insert("NEW_VARIABLE".into(), "changed".into());
            }
            "bounds" => request.timeout_ms += 1,
            "authority" => request.view.volumes[0].authority_revision[0] ^= 1,
            "volume-order" => request.view.volumes.swap(0, 1),
            "stdin-bytes" => request.standard_input = Some(vec![0, 254, b'\n', b'x']),
            "stdin-empty" => request.standard_input = Some(Vec::new()),
            change if change.starts_with("mcp-") => {
                let exchange = request
                    .mcp_stdio
                    .as_mut()
                    .ok_or_else(|| Error::NotFound("MCP descriptor".into()))?;
                match change {
                    "mcp-initialization" => {
                        exchange.initialization = OperationId::from_bytes([73; 16]);
                    }
                    "mcp-operation" => exchange.operation = OperationId::from_bytes([74; 16]),
                    "mcp-method" => exchange.method = crate::mcp::stdio::McpStdioMethod::ListTools,
                    "mcp-params" => {
                        exchange.params = serde_json::json!({"name":"changed","arguments":{}});
                    }
                    "mcp-bytes" => exchange.maximum_bytes += 1,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        let original = fixture.request().await?;
        assert_ne!(
            original.approval_digest(fixture.owner.task_binding().0, fixture.command, None)?,
            request.approval_digest(fixture.owner.task_binding().0, fixture.command, None)?
        );
        fixture.replace_request(&request).await?;
        assert!(
            fixture.provider(fixture.view.work()).await.is_err(),
            "accepted changed {change}"
        );
        assert!(
            !fixture
                .view
                .manifest()
                .options
                .root
                .join("dst/output.txt")
                .exists()
        );
    }
    Ok(())
}

#[tokio::test]
async fn stopped_mcp_response_remains_uncertain() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    let mut request = fixture.request().await?;
    let initialization = OperationId::from_bytes([75; 16]);
    let operation = OperationId::from_bytes([76; 16]);
    request.mcp_stdio = Some(crate::mcp::stdio::McpStdioRequest {
        initialization,
        operation,
        method: crate::mcp::stdio::McpStdioMethod::CallTool,
        params: serde_json::json!({"name":"echo","arguments":{}}),
        maximum_bytes: 4096,
    });
    let value = serde_json::json!({"content":[],"structuredContent":{"sequence":u64::MAX}});
    let mut stdout = crate::contract::canonical_json_bytes(&serde_json::json!({"jsonrpc":"2.0",
        "id":initialization.to_string(),"result":{"protocolVersion":crate::mcp::PROTOCOL_VERSION,
        "capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}))?;
    stdout.push(b'\n');
    stdout.extend(crate::contract::canonical_json_bytes(
        &serde_json::json!({"jsonrpc":"2.0",
        "id":operation.to_string(),"result":value}),
    )?);
    stdout.push(b'\n');
    let mut result = NativeProcessResult {
        success: false,
        exit_code: Some(1),
        stdout,
        stderr: vec![],
        stop: None,
    };
    assert_eq!(request.mcp_response(&result)?, value);
    result.stop = Some(crate::filesystem::NativeProcessStop {
        kind: NativeProcessStopKind::Timeout,
        message: "stopped capture".into(),
        cleanup_completed: true,
    });
    assert!(
        matches!(request.mcp_response(&result), Err(Error::Indeterminate(id)) if id == operation)
    );
    Ok(())
}

#[tokio::test]
#[cfg(windows)]
async fn native_view_rejects_physical_aliases_before_any_materialization() -> Result<()> {
    let fixture = process_fixture(Arc::new(MemoryStream::default()), "write", true, None).await?;
    for names in [["same", "SAME"], ["parent", "PARENT/child"]] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let mut options = fixture.view.manifest().options.clone();
        options.root = directory.path().to_path_buf();
        let operation_directory =
            tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(operation_directory.path().join("target"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::create_dir(operation_directory.path().join("backup"))
            .map_err(|error| Error::Storage(error.to_string()))?;
        let allocation = NativePreparationRoot::witness(
            options.root.clone(),
            operation_directory.path().to_path_buf(),
            options.work_per_volume,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|(error, _)| error)?
        .value;
        let bindings = fixture.view.manifest().volumes[..2]
            .iter()
            .zip(names)
            .map(|(volume, path)| PreparationSelection {
                host: fixture.files.clone(),
                volume: volume.volume.clone(),
                generation: volume.generation.clone(),
                path: path.into(),
                writable: volume.writable,
                verifier: fixture.issuer.verifier(&mut None).expect("verifier copy"),
                scope: fixture.scope.clone(),
                publication: acyclic_fs::PublicationPermit::Unrestricted,
            })
            .collect::<Vec<_>>();
        let failed = NativeVolumeView::prepare(
            &fixture.owner,
            allocation,
            options,
            borrow_preparation_bindings(&bindings),
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or_else(|| Error::Storage("physical alias admitted".into()))?;
        assert!(matches!(failed.0, Error::Invalid(_)));
        assert!(failed.1.backend_read_operations > 0);
        assert!(
            std::fs::read_dir(directory.path())
                .map_err(|error| Error::Storage(error.to_string()))?
                .next()
                .is_none()
        );
    }
    Ok(())
}
