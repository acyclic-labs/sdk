use super::*;

/// Reconcile predictions against authorized Filesystem edits and their original retries.
#[cfg(feature = "filesystem")]
pub async fn filesystem_trace() -> acyclic_harness::Result<()> {
    use acyclic_harness::{
        conversation::{ContentGrant, VolumeOperation},
        filesystem::{ExactTextReplacement, FilesystemHost, workspace_ref},
    };
    let call = Uuid::from_bytes([60; 16]);
    let projection = |file| Projection::Continuation {
        call,
        result: Arc::new(file),
    };
    let predict = |source: &FileRef, bytes: &[u8]| {
        FileRef::new(
            source.volume().clone(),
            source.path().into(),
            "predicted".into(),
            FileDescriptor::from_bytes(bytes, "text/plain", &mut None)?,
            source.display_name().into(),
            &mut None,
        )
    };
    let filesystem = acyclic_fs::Fs::memory();
    let provider = ProviderRef::new(
        "client-edit".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "client-edit".into(),
    };
    let issuer = AuthorityIssuer::new("client-edit", [4; 32], authority.clone());
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(owner),
            None,
            &mut None,
        )
        .expect("scope proof");
    let read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut None,
    )?;
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let source = host
        .put_content(
            &volume,
            &write,
            "test.txt",
            b"one two",
            "text/plain",
            "test.txt",
            64,
            &IdempotencyKey::new("source")?,
            None,
            None,
            None,
        )
        .await?;
    let workspace = workspace_ref(provider.clone(), &volume.storage_name(&mut None)?)?;
    let before = host.resolve(&workspace, None).await?.generation;
    let mut client = Client::new(
        HarnessViews { recovery: None },
        901,
        0,
        Limits {
            records: 1,
            branches: 2,
            edges: 2,
            bytes: 262_144,
            work: 128,
            retention: 100,
            visible: 2,
        },
    )
    .unwrap();
    let base = snapshot(1, authority.clone(), 0, projection(source.clone()), None);
    client.observe(1, &base).unwrap();
    let branch = client
        .begin(Begin {
            key: 1,
            basis: base.basis,
            operation: Some(op(60)),
            assumption: Assumption::Continuation { call },
            predicted: Arc::new(projection(predict(&source, b"one three")?)),
            dependencies: vec![],
            expires: 50,
        })
        .unwrap();
    assert!(matches!(
        client.view(&1, &[branch]).unwrap().provenance,
        Provenance::Hypothesis { .. }
    ));
    assert_eq!(host.resolve(&workspace, None).await?.generation, before);
    assert_eq!(
        host.read_content(&source, &read, 64, None).await?.as_ref(),
        b"one two"
    );
    client
        .observe(
            1,
            &snapshot(
                1,
                authority.clone(),
                0,
                projection(source.clone()),
                Some((op(60), OperationOutcome::Admitted)),
            ),
        )
        .unwrap();
    let replacement = ExactTextReplacement {
        old_text: "two".into(),
        new_text: "three".into(),
    };
    let key = IdempotencyKey::new(op(60).to_string())?;
    let edited = host
        .edit_text(&source, &read, &write, &replacement, 64, &key)
        .await?;
    let completed = snapshot(
        1,
        authority.clone(),
        1,
        projection(edited.clone()),
        Some((op(60), OperationOutcome::Completed)),
    );
    client.observe(1, &completed).unwrap();
    assert_eq!(
        client.hypothesis(branch).unwrap().prediction,
        PredictionOutcome::Replaced
    );
    assert!(matches!(
        client.view(&1, &[branch]).unwrap().provenance,
        Provenance::Authoritative(_)
    ));
    assert_eq!(
        host.read_content(&edited, &read, 64, None).await?.as_ref(),
        b"one three"
    );
    let stale = client
        .begin(Begin {
            key: 1,
            basis: completed.basis,
            operation: Some(op(61)),
            assumption: Assumption::Continuation { call },
            predicted: Arc::new(projection(predict(&edited, b"one four")?)),
            dependencies: vec![],
            expires: 50,
        })
        .unwrap();
    let user = host
        .put_content(
            &volume,
            &write,
            "test.txt",
            b"user edit",
            "text/plain",
            "test.txt",
            64,
            &IdempotencyKey::new("user")?,
            None,
            None,
            None,
        )
        .await?;
    let head = host.resolve(&workspace, None).await?.generation;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    assert_eq!(
        host.edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?,
        edited
    );
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    let stale_edit = ExactTextReplacement {
        old_text: "three".into(),
        new_text: "four".into(),
    };
    assert!(matches!(
        host.edit_text(
            &edited,
            &read,
            &write,
            &stale_edit,
            64,
            &IdempotencyKey::new(op(61).to_string())?
        )
        .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    client
        .observe(
            1,
            &snapshot(
                1,
                authority,
                2,
                projection(user.clone()),
                Some((op(61), OperationOutcome::Rejected)),
            ),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(stale).unwrap().prediction,
        PredictionOutcome::Removed
    );
    assert_eq!(
        client.hypothesis(stale).unwrap().outcome,
        OperationOutcome::Rejected
    );
    assert_eq!(
        *client.view(&1, &[stale]).unwrap().value,
        projection(user.clone())
    );
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    assert_eq!(
        host.read_content(&user, &read, 64, None).await?.as_ref(),
        b"user edit"
    );
    Ok(())
}
