use super::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario checks publication, exact retry, concurrent user edits and negative controls"
)]
async fn exact_edit_retains_pinned_result_and_rejects_stale_or_foreign_writes() -> Result<()> {
    let provider = ProviderRef::new(
        "portable".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let filesystem = Fs::memory();
    let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "source".into(),
        VolumeClass::Project,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let other = VolumeRef::new(
        provider.clone(),
        "other".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    host.create_volume(&other).await?;
    let issuer = AuthorityIssuer::new(
        "portable",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "portable".into(),
        },
    );
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
                other.capability(VolumeOperation::Write, &mut None)?,
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
    let foreign = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &other,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let source = host
        .put_content(
            &volume,
            &write,
            "test.txt",
            b"one\r\ntwo\n",
            "text/plain",
            "test.txt",
            64,
            &IdempotencyKey::new("source")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let ordinary = OrdinaryPublisher {
        volume: volume.clone(),
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        ordinary.stage_at(OperationId::new(), &source, b"replacement", None, None),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    let replacement = ExactTextReplacement {
        old_text: "two".into(),
        new_text: "three".into(),
    };
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &foreign,
            &replacement,
            64,
            &IdempotencyKey::new("foreign")?
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let key = IdempotencyKey::new("edit")?;
    let edited = host
        .edit_text(&source, &read, &write, &replacement, 64, &key)
        .await?;
    assert_eq!(
        host.read_content(&edited, &read, 64, None).await?.as_ref(),
        b"one\r\nthree\n"
    );
    assert_eq!(
        host.read_content(&source, &read, 64, None).await?.as_ref(),
        b"one\r\ntwo\n"
    );
    let workspace = workspace_ref(provider.clone(), &volume.storage_name(&mut None)?)?;
    let head = host.resolve(&workspace, None).await?.generation;
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
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    assert_eq!(
        host.edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?,
        edited
    );
    assert_eq!(
        host.read_content(&user, &read, 64, None).await?.as_ref(),
        b"user edit"
    );
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &write,
            &replacement,
            64,
            &IdempotencyKey::new("stale")?
        )
        .await,
        Err(Error::Conflict(_))
    ));
    let changed = ExactTextReplacement {
        old_text: "two".into(),
        new_text: "different".into(),
    };
    assert!(matches!(
        host.edit_text(&source, &read, &write, &changed, 64, &key)
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        host.put_content(
            &volume,
            &write,
            "test.txt",
            b"one\r\nthree\n",
            "text/plain",
            "test.txt",
            64,
            &key,
            Some(&head),
            acyclic_harness::conversation::ContentAccess::default()
        )
        .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        host.read_content(&user, &read, 64, None).await?.as_ref(),
        b"user edit"
    );
    Ok(())
}
