#![cfg(feature = "filesystem-local")]

use super::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered persistent scenario retains the exact source, result and user head across provider drop"
)]
async fn exact_edit_reopens_persistent_receipt_without_overwriting_user_changes() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let options = acyclic_fs::LocalOptions::new(directory.path().join("filesystem"));
    let provider = ProviderRef::new(
        "persistent-edit".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "files".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "persistent-edit",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "persistent-edit".into(),
        },
    );
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
    let replacement = ExactTextReplacement {
        old_text: "before".into(),
        new_text: "after".into(),
    };
    let key = IdempotencyKey::new("persistent-edit")?;
    let (source, edited, user) = {
        let filesystem = Fs::local(options.clone())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = FilesystemHost::new(filesystem, &provider, None)?;
        host.create_volume(&volume).await?;
        let source = host
            .put_content(
                &volume,
                &write,
                "file.txt",
                b"before\n",
                "text/plain",
                "file.txt",
                64,
                &IdempotencyKey::new("source")?,
                None,
                None,
                None,
            )
            .await?;
        let edited = host
            .edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?;
        let user = host
            .put_content(
                &volume,
                &write,
                "file.txt",
                b"user\n",
                "text/plain",
                "file.txt",
                64,
                &IdempotencyKey::new("user")?,
                None,
                None,
                None,
            )
            .await?;
        (source, edited, user)
    };
    let filesystem = Fs::local(options)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let workspace = workspace_ref(provider, &volume.storage_name(&mut None)?)?;
    let head = host.resolve(&workspace, None).await?.generation;
    for maximum_bytes in [0, u64::MAX] {
        assert!(matches!(
            host.edit_text(&source, &read, &write, &replacement, maximum_bytes, &key)
                .await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            host.put_content(
                &volume,
                &write,
                "file.txt",
                b"after\n",
                "text/plain",
                "file.txt",
                maximum_bytes,
                &key,
                Some(&head),
                None,
                None
            )
            .await,
            Err(Error::Invalid(_))
        ));
    }
    assert_eq!(
        host.edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?,
        edited
    );
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    assert_eq!(
        host.read_content(&edited, &read, 64, None).await?.as_ref(),
        b"after\n"
    );
    assert_eq!(
        host.read_content(&user, &read, 64, None).await?.as_ref(),
        b"user\n"
    );
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &write,
            &replacement,
            64,
            &IdempotencyKey::new("new-stale-edit")?
        )
        .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(host.resolve(&workspace, None).await?.generation, head);
    Ok(())
}
