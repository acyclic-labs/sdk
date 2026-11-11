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
            summary: true,
            compose_tools: false,
            model: Model::new("test".into(), "custody".into(), "1".into())?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("payload".repeat(128)),
            }],
            tools: Vec::new(),
            sampling: Default::default(),
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
    let manifest_bytes = journal.load(&file, None)?.await?;
    let manifest_text = std::str::from_utf8(&manifest_bytes).unwrap();
    assert!(manifest_text.starts_with("{\"$version\":3,"));
    for (index, bytes) in [
        manifest_text.replacen("{\"$version\":3,", "{", 1),
        manifest_text.replacen("{\"$version\":3,", "{\"$version\":0,", 1),
        manifest_text.replacen("{\"$version\":3,", "{\"$version\":1,", 1),
        manifest_text.replacen("{\"$version\":3,", "{\"$version\":2,", 1),
        manifest_text.replacen("{\"$version\":3,", "{\"$version\":4,", 1),
        "{\"$version\":4,malformed body".into(),
    ]
    .into_iter()
    .enumerate()
    {
        let reference = journal
            .stage(
                OperationId::new(),
                &format!("unsupported-manifest-{index}"),
                bytes.as_bytes(),
                REQUEST_MEDIA,
                None,
            )?
            .await?;
        assert_eq!(
            usage_request(journal, &reference)
                .await
                .err()
                .unwrap()
                .code(),
            crate::wire::ErrorCode::Unsupported
        );
        for native in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = native
                .then(|| PreparationIo::from_native_work(&work))
                .transpose()?;
            let baseline = work.live_allocation_bytes();
            assert_eq!(
                load_request(journal, &reference, Limits::default(), io.as_ref())
                    .await
                    .err()
                    .unwrap()
                    .code(),
                crate::wire::ErrorCode::Unsupported
            );
            assert_eq!(work.live_allocation_bytes(), baseline);
            if let Some(io) = &io {
                assert_eq!(io.owned_bytes()?, 0);
            }
            drop(io);
            // The original ledger remains alive through `work`; only its
            // accounting state may remain after decoder custody is dropped.
            assert_eq!(
                work.live_allocation_bytes(),
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
    // A historically signed finite manifest still authenticates after its count
    // fields become optional. Both journal append paths must admit the format
    // before that cached proof can bypass complete request reconstruction.
    let verifier = storage.verifier();
    let current_version = load_manifest(journal, &file, None).await?.version;
    for version in std::iter::once(current_version)
        .chain(1..current_version)
        .chain(std::iter::once(current_version + 1))
    {
        let mut signed = load_manifest(journal, &file, None).await?;
        signed.version = version;
        signed.admitted_limits = Limits {
            file_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
            path_bytes: std::num::NonZeroUsize::new(4096),
            attachments: std::num::NonZeroUsize::new(8),
            render_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
            context_messages: std::num::NonZeroUsize::new(128),
            ..Limits::default()
        };
        signed.proof = Some(signed.attestation(&verifier, None).await?);
        assert!(signed.authenticates(
            &verifier,
            Limits::default(),
            prepared.request_digest(),
            None
        )?);
        let reference =
            stage_manifest(journal, OperationId::new(), "signed-version", &signed, None).await?;
        for conditional in [false, true] {
            let operation = OperationId::new();
            journal
                .append(
                    operation,
                    "started",
                    &ExecutionEvent::Started {
                        request_digest: prepared.request_digest(),
                    },
                    None,
                )?
                .await?;
            let event = ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: prepared.request_digest(),
                request: reference.clone(),
                context: None,
            };
            let result = if conditional {
                match journal.append_if_tail(operation, 1, "signed-start", &event, None) {
                    Ok(future) => future.await.map(|committed| assert!(committed)),
                    Err(error) => Err(error),
                }
            } else {
                match journal.append(operation, "signed-start", &event, None) {
                    Ok(future) => future.await,
                    Err(error) => Err(error),
                }
            };
            if version == current_version {
                result?;
            } else {
                assert_eq!(
                    result.err().unwrap().code(),
                    crate::wire::ErrorCode::Unsupported
                );
            }
            assert_eq!(
                journal.replay(operation, 0, 4, None)?.await?.len(),
                if version == current_version { 2 } else { 1 },
            );
        }
    }
    for native in [false, true] {
        // Exercise both funded encoders, as well as the ordinary writer above.
        // The actual persisted bytes must admit their version before payload
        // decoding and reopen under the same custody that funded the write.
        let funded_storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
        let funded_journal = funded_storage.journal();
        let funded_seed = initial_request(funded_journal.as_ref(), &prepared).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let funded = stage_request(
            funded_journal.as_ref(),
            OperationId::new(),
            "funded-request",
            &prepared,
            Some(&funded_seed),
            Some(&io),
        )
        .await?;
        assert!(
            funded_journal
                .load(&funded, None)?
                .await?
                .starts_with(b"{\"$version\":3,")
        );
        assert_eq!(
            load_request(
                funded_journal.as_ref(),
                &funded,
                Limits::default(),
                Some(&io)
            )
            .await?
            .bytes(),
            prepared.bytes()
        );
        drop(funded);
        drop(funded_seed);
        // Immutable object pages retain custody in the storage adapter. Retire
        // their owners as well as the publication before checking full release.
        drop(funded_journal);
        drop(funded_storage);
        drop(io);
        assert_eq!(
            work.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
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
            assert!(request.request().summary);
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
    // Bind the altered header to its actual digest so version admission, rather
    // than a stale digest, must refuse the unsupported persisted format.
    let mut manifest = load_manifest(journal, &file, None).await?;
    let (mut header, _) = load_json::<RequestHeader>(journal, &manifest.header, None).await?;
    header.version = 2;
    manifest.header_digest = preparation_digest(&header, None)?;
    manifest.header = stage_json(
        journal,
        OperationId::new(),
        "unsupported-header",
        &header,
        None,
    )
    .await?;
    let unsupported = stage_json(
        journal,
        OperationId::new(),
        "unsupported-request",
        &manifest,
        None,
    )
    .await?;
    assert!(
        load_request(journal, &unsupported, Limits::default(), None)
            .await
            .is_err()
    );
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
    let issuer = crate::core::AuthorityRoot::new("one", [1; 32], authority.clone());
    let verifier = issuer.verifier(&mut None)?;
    let other_issuer = crate::core::AuthorityRoot::new("two", [2; 32], authority);
    let other = other_issuer.verifier(&mut None)?;
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("test".into(), "proof".into(), "1".into())?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("test".into()),
            }],
            tools: Vec::new(),
            sampling: Default::default(),
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
    assert!(
        public
            .model_request_proof([1; 32], &mut None)
            .await
            .is_err()
    );
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
        ("/$version", 4.into()),
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
