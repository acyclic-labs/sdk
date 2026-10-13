use super::*;
use futures::TryStreamExt as _;

#[tokio::test]
async fn extension_bytes_are_validated_before_ref_only_publication() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let client = provider;
    let mut unbound = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert!(matches!(
        unbound
            .execute(
                command(1)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    let mut wrong_schemas = SchemaRegistry::new();
    wrong_schemas.register(
        "example.message",
        1,
        json!({
            "type": "object", "properties": {"text": {"type": "integer"}},
            "required": ["text"]
        }),
        [9; 32],
        crate::core::ExtensionForkPolicy::Inherit,
        [],
        None,
    )?;
    let mut invalid = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        wrong_schemas,
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent));
    assert!(matches!(
        invalid
            .execute(
                command(1)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let mut valid = with_content(&client).await?;
    valid
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let records = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        authority().stream_path(&mut None)?,
    )?
    .read(0, 1)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    assert_eq!(records.len(), 1);
    assert!(
        !records[0]
            .value
            .windows(b"hello".len())
            .any(|window| window == b"hello")
    );
    assert!(matches!(
        crate::wire_codec::decode_event(&records[0].value, &mut None)?.0.1.payload,
        crate::core::EventPayload::Custom { record } if record.content == content()?
    ));
    Ok(())
}

#[tokio::test]
async fn migrated_extension_state_is_validated_before_stream_publication() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut valid_schemas = schemas();
    valid_schemas.register(
        "example.message",
        2,
        json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
        [10; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let mut migrations = NativeExtensionMigrations::default();
    migrations.register(
        "example.message",
        1,
        2,
        [10; 32],
        Arc::new(|value| Ok(value.clone())),
    )?;
    assert!(matches!(
        migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|value| Ok(value.clone()))
        ),
        Err(Error::Conflict(_))
    ));
    let migrations = Arc::new(migrations);
    let mut original = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        valid_schemas.clone(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent));
    original
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let migration = Command {
        operation_id: OperationId::from_bytes([2; 16]),
        idempotency_key: IdempotencyKey::new("migrate-extension-state")?,
        expected_revision: 1,
        scope: issuer()
            .issue(
                "owner",
                &Capabilities::new([
                    "extension:migrate".to_owned(),
                    content()?
                        .volume()
                        .capability(crate::conversation::VolumeOperation::Write, &mut None)?,
                ]),
                Some(crate::AgentId::from_bytes([8; 16])),
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::MigrateExtensionState {
            name: "example.message".into(),
            previous: crate::core::EventReference {
                authority: authority(),
                revision: 1,
            },
            to_version: 2,
            content: content()?,
        },
    };
    let mut invalid_schemas = schemas();
    invalid_schemas.register(
        "example.message",
        2,
        json!({"type":"object","required":["text"],"properties":{"text":{"type":"integer"}}}),
        [10; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let mut invalid = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        invalid_schemas,
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent))
        .with_extension_migrations(migrations.clone());
    assert!(matches!(
        invalid
            .execute(
                migration.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(invalid.reducer().revision(), 1);
    let mut missing_executor = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        valid_schemas.clone(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent));
    assert!(matches!(
        missing_executor
            .execute(
                migration.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(missing_executor.reducer().revision(), 1);
    let mut wrong_migrations = NativeExtensionMigrations::default();
    wrong_migrations.register(
        "example.message",
        1,
        2,
        [10; 32],
        Arc::new(|_| Ok(json!({"text":"substituted"}))),
    )?;
    let mut substituted = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        valid_schemas.clone(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent))
        .with_extension_migrations(Arc::new(wrong_migrations));
    assert!(matches!(
        substituted
            .execute(
                migration.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(substituted.reducer().revision(), 1);
    let mut valid = (StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        valid_schemas.clone(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?)
        .with_content_verifier(Arc::new(TestContent))
        .with_extension_migrations(migrations);
    valid
        .execute(
            migration.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert!(matches!(
        valid
            .execute(
                migration,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await?,
        ApplyResult::Replayed { .. }
    ));
    let reopened = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        valid_schemas,
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(
        reopened
            .reducer()
            .extension_state("example.message")
            .map(|(_, state)| state.version),
        Some(2)
    );
    Ok(())
}

#[tokio::test]
async fn extension_migration_retries_without_republishing_content() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut schemas = schemas();
    schemas.register(
        "example.message",
        2,
        json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
        [10; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let mut migrations = NativeExtensionMigrations::default();
    migrations.register(
        "example.message",
        1,
        2,
        [10; 32],
        Arc::new(|value| Ok(value.clone())),
    )?;
    let files = MigrationContentStore::default();
    files.insert(&content()?, EXTENSION_BYTES)?;
    let mut aggregate = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas,
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(files.clone()))
    .with_extension_migrations(Arc::new(migrations));
    aggregate
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let request = ExtensionMigrationRequest {
        operation_id: OperationId::from_bytes([3; 16]),
        expected_revision: 1,
        previous: EventReference {
            authority: authority(),
            revision: 1,
        },
        name: "example.message".into(),
        to_version: 2,
    };
    let publisher = TestPublisher {
        volume: VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "extension-child".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
            &mut None,
        )?,
        stages: std::sync::atomic::AtomicUsize::new(0),
        files: files.clone(),
    };
    let scope = issuer()
        .issue(
            "child-owner",
            &Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes([9; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let unauthorized = issuer()
        .issue(
            "child-unauthorized",
            &Capabilities::new([publisher
                .volume
                .capability(crate::conversation::VolumeOperation::Write, &mut None)?]),
            Some(crate::AgentId::from_bytes([9; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(matches!(
        aggregate
            .migrate_extension_state(request.clone(), unauthorized, &publisher)
            .await,
        Err(Error::Unauthorized(_))
    ));
    let no_destination_write = issuer()
        .issue(
            "child-no-write",
            &Capabilities::new(["extension:migrate"]),
            Some(crate::AgentId::from_bytes([9; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(matches!(
        aggregate
            .migrate_extension_state(request.clone(), no_destination_write, &publisher)
            .await,
        Err(Error::Unauthorized(_))
    ));
    let foreign_writer = issuer()
        .issue(
            "foreign-writer",
            &Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes([8; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(matches!(
        aggregate
            .migrate_extension_state(request.clone(), foreign_writer, &publisher)
            .await,
        Err(Error::Unauthorized(_))
    ));
    let mut stale = request.clone();
    stale.expected_revision = 2;
    assert!(matches!(
        aggregate
            .migrate_extension_state(stale, scope.clone(), &publisher)
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(matches!(
        aggregate
            .migrate_extension_state(request.clone(), scope.clone(), &publisher)
            .await?,
        ApplyResult::Applied { .. }
    ));
    assert!(matches!(
        aggregate
            .migrate_extension_state(request, scope, &publisher)
            .await?,
        ApplyResult::Replayed { .. }
    ));
    assert_eq!(
        publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let (_, migrated) = aggregate
        .reducer()
        .extension_state("example.message")
        .ok_or_else(|| Error::NotFound("migrated extension state".into()))?;
    assert_eq!(migrated.content.volume(), &publisher.volume);
    assert_eq!(
        files
            .read(
                &migrated.content,
                migrated.content.descriptor().byte_length(),
                None
            )?
            .await?
            .as_ref(),
        EXTENSION_BYTES
    );
    Ok(())
}

#[tokio::test]
async fn configured_extension_cannot_publish_a_dangling_or_invalid_reference() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: "configured-agent".into(),
    };
    let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
    let extension = ExtensionDependency {
        name: "example.configured".into(),
        version: 1,
    };
    let mut schemas = SchemaRegistry::new();
    schemas.register(
            extension.name.clone(),
            extension.version,
            json!({"type":"object"}),
            [9; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}}})),
        )?;
    let bytes = br#"{"enabled":true}"#;
    let reference = interaction_file("extensions/configuration.json", bytes)?;
    let command = Command {
        operation_id: OperationId::from_bytes([32; 16]),
        idempotency_key: IdempotencyKey("configure-extension".into()),
        expected_revision: 0,
        scope: issuer
            .issue(
                "owner",
                &Capabilities::new(["extension:configure"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::ConfigureExtension {
            extension,
            content: reference.clone(),
        },
    };
    let mut aggregate = StreamAggregate::open(
        &client,
        authority.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        schemas.clone(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert!(matches!(
        aggregate
            .execute(
                command.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    let mut invalid = std::collections::BTreeMap::new();
    invalid.insert(
        MigrationContentStore::key(&reference)?,
        br#"{"enabled":"wrong"}"#.to_vec(),
    );
    let mut aggregate = aggregate.with_content_verifier(Arc::new(MigrationContentStore(Arc::new(
        std::sync::Mutex::new(invalid),
    ))));
    assert!(
        aggregate
            .execute(
                command.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 0);
    let mut valid = std::collections::BTreeMap::new();
    valid.insert(MigrationContentStore::key(&reference)?, bytes.to_vec());
    let mut aggregate = aggregate.with_content_verifier(Arc::new(MigrationContentStore(Arc::new(
        std::sync::Mutex::new(valid),
    ))));
    aggregate
        .execute(
            command,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let reopened = StreamAggregate::open(
        &client,
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        schemas,
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(reopened.reducer().revision(), 1);
    assert!(matches!(
        &reopened.reducer().events_after(0, 1)?[0].payload,
        crate::core::EventPayload::ExtensionConfigured { record, .. }
            if record.content == reference
    ));
    Ok(())
}
