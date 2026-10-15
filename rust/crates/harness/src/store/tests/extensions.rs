use super::*;
use futures::TryStreamExt as _;

#[tokio::test]
async fn extension_bytes_are_validated_before_ref_only_publication() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let client = provider;
    let mut unbound = StreamAggregate::open(
        &client,
        (authority(), issuer().verifier(&mut None)?, schemas()),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
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
        (authority(), issuer().verifier(&mut None)?, wrong_schemas),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
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
    let records = acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
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
        (
            authority.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            schemas.clone(),
        ),
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
        (
            authority,
            issuer.verifier(&mut None).expect("verifier copy"),
            schemas,
        ),
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
