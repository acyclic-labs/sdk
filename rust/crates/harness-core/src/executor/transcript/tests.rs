use super::*;

async fn initial_request(
    journal: &dyn ExecutionJournal,
    prepared: &crate::model::PreparedModelRequest,
) -> Result<FileRef> {
    let file = stage_request(journal, OperationId::new(), "seed", prepared, None, None).await?;
    assert_eq!(file.descriptor().media_type(), "application/json");
    assert_eq!(
        load_model_request(journal, &file, Limits::default(), None)
            .await?
            .bytes(),
        prepared.bytes()
    );
    Ok(file)
}

#[tokio::test]
async fn segmented_reopen_releases_decoder_scratch_on_success_and_failure() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new(
                "test".into(),
                "custody".into(),
                "1".into(),
                serde_json::json!({"sparse": [{"nested": "value"}]}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("payload".repeat(128)),
            }],
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    let seed = initial_request(journal, &prepared).await?;
    let file = stage_request(
        journal,
        OperationId::new(),
        "request",
        &prepared,
        Some(&seed),
        None,
    )
    .await?;
    assert_eq!(file.descriptor().media_type(), REQUEST_MEDIA);
    for native in [false, true] {
        for scoped in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let baseline = io.owned_bytes()?;
            let request = if scoped {
                load_request(journal, &file, Limits::default(), Some(&io)).await?
            } else {
                // Removing the isolated scope retains every
                // manifest and segment beside the returned request.
                load_request_inner(journal, &file, Limits::default(), Some(&io)).await?
            };
            assert_eq!(request.bytes(), prepared.bytes());
            let retained = request.preparation_owned_bytes(&io)?;
            let hold = io.guard_owned(retained)?;
            drop(request);
            drop(hold);
            assert_eq!(
                io.owned_bytes()? == baseline,
                scoped,
                "native={native} scoped={scoped}"
            );
        }
        for encoded in [0, 1, 512, 4096] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget =
                acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            budget.bytes_encoded = encoded;
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let baseline = io.owned_bytes()?;
            let result = load_request(journal, &file, Limits::default(), Some(&io)).await;
            assert!(
                result.is_err(),
                "encoded cut {encoded} unexpectedly completed"
            );
            drop(result);
            assert_eq!(
                io.owned_bytes()?,
                baseline,
                "native={native} encoded={encoded}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn request_proofs_bind_authority_limits_and_every_manifest_component() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let journal = journal.as_ref();
    let authority = crate::core::Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: crate::ConversationId::new().to_string(),
    };
    let issuer = crate::core::AuthorityIssuer::new("one", [1; 32], authority.clone());
    let verifier = issuer.verifier(&mut None)?;
    let other_issuer = crate::core::AuthorityIssuer::new("two", [2; 32], authority);
    let other = other_issuer.verifier(&mut None)?;
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            model: Model::new("test".into(), "proof".into(), "1".into(), Value::Null)?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("test".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: None,
        },
        Limits::default(),
        None,
    )?;
    let seed = initial_request(journal, &prepared).await?;
    let reference = stage_attested_request(
        journal,
        OperationId::new(),
        "proof",
        &prepared,
        Some(&seed),
        &verifier,
        None,
    )
    .await?;
    let manifest = load_manifest(journal, &reference, None).await?;
    let digest = prepared.request_digest();
    assert!(manifest.authenticates(&verifier, Limits::default(), digest, None)?);
    assert!(!manifest.authenticates(&other, Limits::default(), digest, None)?);
    assert!(!manifest.authenticates(
        &verifier,
        Limits {
            render_bytes: 1,
            ..Limits::default()
        },
        digest,
        None
    )?);
    assert_component_proofs(&manifest, &reference, &verifier)?;
    let public = crate::core::AuthorityVerifier::from_public(
        "one".into(),
        verifier.audience().clone(),
        issuer.public_workload_trust()?,
        verifier.public_checkpoint_key(),
    )?;
    drop(issuer);
    let authenticated = manifest.authenticates(&public, Limits::default(), digest, None);
    assert!(
        matches!(authenticated, Ok(true)),
        "public-only manifest verification attempted to sign: {authenticated:?}"
    );
    assert_component_proofs(&manifest, &reference, &public)?;
    assert!(public.model_request_proof([1; 32], &mut None).is_err());
    Ok(())
}

fn assert_component_proofs(
    manifest: &RequestManifest,
    reference: &FileRef,
    verifier: &crate::core::AuthorityVerifier,
) -> Result<()> {
    let value =
        serde_json::to_value(manifest).map_err(|error| Error::Invalid(error.to_string()))?;
    let reference =
        serde_json::to_value(reference).map_err(|error| Error::Invalid(error.to_string()))?;
    let header = serde_json::to_value(&manifest.header)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    for (path, replacement) in [
        ("/header", reference),
        ("/header_digest/0", (manifest.header_digest[0] ^ 1).into()),
        ("/messages/reference", header),
        ("/messages/count", (manifest.messages.count + 1).into()),
        (
            "/messages/content_digest/0",
            (manifest.messages.content_digest[0] ^ 1).into(),
        ),
        (
            "/messages/chain_digest/0",
            (manifest.messages.chain_digest[0] ^ 1).into(),
        ),
        ("/request_digest/0", (manifest.request_digest[0] ^ 1).into()),
        ("/admitted_limits/render_bytes", 1.into()),
        ("/proof", Value::Null),
    ] {
        let mut changed = value.clone();
        *changed
            .pointer_mut(path)
            .ok_or_else(|| Error::Invalid("missing fixture field".into()))? = replacement;
        let changed: RequestManifest =
            serde_json::from_value(changed).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(
            !changed.authenticates(verifier, Limits::default(), changed.request_digest, None)?,
            "changed {path}"
        );
    }
    Ok(())
}
