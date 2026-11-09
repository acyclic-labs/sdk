use super::*;
use crate::{
    AgentId,
    conversation::VolumeOwner,
    core::{AggregateKind, Authority, AuthorityRoot},
};

fn grants(volume: &VolumeRef) -> Result<(ContentGrant, ContentGrant)> {
    let issuer = AuthorityRoot::new(
        "custody",
        [17; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "custody".into(),
        },
    );
    let agent = match volume.owner() {
        VolumeOwner::Agent(agent) => *agent,
        _ => unreachable!(),
    };
    let scope = issuer.bootstrap_scope(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    Ok((
        ContentGrant::verify_directory_read(&verifier, &scope, volume, "", &mut None)?,
        ContentGrant::verify(
            &verifier,
            &scope,
            volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?,
    ))
}

#[tokio::test]
async fn forked_metadata_stays_in_custody_during_concurrent_publication() -> Result<()> {
    let provider = ProviderRef::new("custody".into(), "filesystem".into(), "1".into(), &mut None)?;
    let mut host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let volume = |id: &str, byte| {
        VolumeRef::new(
            provider.clone(),
            id.into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([byte; 16])),
            &mut None,
        )
    };
    let parent = volume("parent", 1)?;
    let child = volume("child", 2)?;
    let (parent_read, parent_write) = grants(&parent)?;
    let (child_read, child_write) = grants(&child)?;
    let parent_workspace = host.create_volume(&parent).await?.workspace;
    let original = host
        .put_content(
            &parent,
            &parent_write,
            "GOAL.md",
            b"original",
            "text/markdown",
            "GOAL.md",
            64,
            &IdempotencyKey::new("original")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let cut = host.file_generation(&original, None)?;
    let fork = host
        .fork_volume(
            &parent,
            &parent_read,
            &cut,
            &child,
            &child_write,
            None,
            &IdempotencyKey::new("fork")?,
        )
        .await?;

    // Hold the parent's replacement before staging. The child reads the
    // inherited publication while its parent has a concurrent writer, then
    // publishes its own replacement. No timer or scheduler luck is involved.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let pause = std::sync::Mutex::new(Some((entered, released)));
    Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
        let pause = pause.lock().unwrap().take();
        Box::pin(async move {
            if let Some((entered, released)) = pause {
                entered.send(()).unwrap();
                released.await.unwrap();
            }
        })
    }));
    let key = IdempotencyKey::new("parent-replacement")?;
    let writer = host.put_content(
        &parent,
        &parent_write,
        "GOAL.md",
        b"parent replacement",
        "text/markdown",
        "GOAL.md",
        64,
        &key,
        None,
        crate::conversation::ContentAccess::default(),
    );
    let reader = async {
        waiting.await.unwrap();
        let inherited = host
            .read_private_path(
                &child,
                &child_read,
                "GOAL.md",
                Some(&fork.generation),
                64,
                crate::conversation::ContentAccess::default(),
            )
            .await;
        // Release even when the negative control observes the old custody error.
        release.send(()).unwrap();
        let (file, bytes) = inherited?;
        assert_eq!(file.volume(), &child);
        assert_eq!(bytes.as_ref(), b"original");
        let own = host
            .put_content(
                &child,
                &child_write,
                "GOAL.md",
                b"child replacement",
                "text/markdown",
                "GOAL.md",
                64,
                &IdempotencyKey::new("child-replacement")?,
                None,
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        assert_eq!(
            host.read_content(&own, &child_read, 64, None)
                .await?
                .as_ref(),
            b"child replacement"
        );
        // A later publication must not change the original fork's unit.
        assert_eq!(
            host.read_private_path(
                &child,
                &child_read,
                "GOAL.md",
                Some(&fork.generation),
                64,
                crate::conversation::ContentAccess::default()
            )
            .await?
            .1
            .as_ref(),
            b"original"
        );
        Ok::<_, Error>(())
    };
    let (written, read) = futures::join!(writer, reader);
    written?;
    read?;
    assert_eq!(
        host.read(&parent_workspace, None, "/GOAL.md", 64, None)
            .await?
            .as_ref(),
        b"parent replacement"
    );
    Ok(())
}
