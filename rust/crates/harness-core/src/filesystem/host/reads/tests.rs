use super::*;
use crate::{
    AgentId,
    conversation::VolumeOwner,
    core::{AggregateKind, Authority, AuthorityRoot},
};

struct Grants {
    _issuer: AuthorityRoot,
    directory: ContentGrant,
    whole: ContentGrant,
    write: ContentGrant,
}

fn grants(volume: &VolumeRef) -> Result<Grants> {
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
    let read = ContentGrant::verify_directory_read(&verifier, &scope, volume, "", &mut None)?;
    let write = ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Write, &mut None)?;
    let whole = ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Read, &mut None)?;
    Ok(Grants {
        _issuer: issuer,
        directory: read,
        whole,
        write,
    })
}

#[test]
fn forked_metadata_stays_in_custody_during_concurrent_publication() -> Result<()> {
    acyclic_native_runtime::testing::seeded_runtime(0x5052_4956)
        .map_err(|error| Error::Storage(error.to_string()))?
        .block_on(concurrent_publication())
}

async fn concurrent_publication() -> Result<()> {
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
    let parent_grants = grants(&parent)?;
    let child_grants = grants(&child)?;
    let parent_workspace = host.create_volume(&parent).await?.workspace;
    let original = host
        .put_content(
            &parent,
            &parent_grants.write,
            "GOAL.md",
            b"original",
            "text/markdown",
            "GOAL.md",
            Some(64),
            &IdempotencyKey::new("original")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let cut = host.file_generation(&original, None)?;
    let fork = host
        .fork_volume(
            &parent,
            &parent_grants.whole,
            &cut,
            &child,
            &child_grants.write,
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
        &parent_grants.write,
        "GOAL.md",
        b"parent replacement",
        "text/markdown",
        "GOAL.md",
        Some(64),
        &key,
        None,
        crate::conversation::ContentAccess::default(),
    );
    let reader = async {
        waiting.await.unwrap();
        let inherited = host
            .read_private_path(
                &child,
                &child_grants.directory,
                "GOAL.md",
                Some(&fork.generation),
                64,
                crate::conversation::ContentAccess::default(),
            )
            .await;
        let result = async {
            let (file, bytes) = inherited?;
            assert_eq!(file.volume(), &child);
            assert_eq!(bytes.as_ref(), b"original");
            let own = host
                .put_content(
                    &child,
                    &child_grants.write,
                    "GOAL.md",
                    b"child replacement",
                    "text/markdown",
                    "GOAL.md",
                    Some(64),
                    &IdempotencyKey::new("child-replacement")?,
                    None,
                    crate::conversation::ContentAccess::default(),
                )
                .await?;
            assert_eq!(
                host.read_content(&own, &child_grants.whole, 64, None)
                    .await?
                    .as_ref(),
                b"child replacement"
            );
            // Import while the destination still has its original publication
            // metadata. The joined payload must not select that stale descriptor
            // or the child's foreign FileRef, even though both indexes are inherited.
            let source = host.file_generation(&own, None)?;
            let plan = host
                .prepare_volume_import(
                    &child,
                    &child_grants.whole,
                    &source,
                    &parent,
                    &parent_grants.write,
                )
                .await?;
            let outcome = plan
                .apply(
                    acyclic_fs::ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: filesystem_key(&IdempotencyKey::new("import")?),
                    },
                    BTreeMap::new(),
                    acyclic_fs::PublicationPermit::Unrestricted,
                )
                .await?;
            assert!(matches!(outcome, acyclic_fs::JoinOutcome::Applied(_)));
            let (imported, bytes) = host
                .read_private_path(
                    &parent,
                    &parent_grants.directory,
                    "GOAL.md",
                    None,
                    64,
                    crate::conversation::ContentAccess::default(),
                )
                .await?;
            assert_eq!(imported.volume(), &parent);
            assert_eq!(bytes.as_ref(), b"child replacement");
            assert_eq!(
                host.read_content_range(
                    &imported,
                    &parent_grants.whole,
                    acyclic_fs::ByteRange {
                        offset: 0,
                        length: bytes.len() as u64,
                    },
                    None,
                )
                .await?
                .as_ref(),
                b"child replacement"
            );
            assert_eq!(
                host.read_content(&original, &parent_grants.whole, 64, None)
                    .await?
                    .as_ref(),
                b"original"
            );
            // A later publication must not change the original fork's unit.
            assert_eq!(
                host.read_private_path(
                    &child,
                    &child_grants.directory,
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
        }
        .await;
        // Release even when the negative control observes the old custody error.
        release.send(()).unwrap();
        result
    };
    let (written, read) = futures::join!(writer, reader);
    read?;
    // The import won the destination edit while this writer held its old
    // authorization cut. Its refusal must preserve the imported version.
    assert!(matches!(written, Err(Error::Conflict(_))));
    assert_eq!(
        host.read(&parent_workspace, None, "/GOAL.md", 64, None)
            .await?
            .as_ref(),
        b"child replacement"
    );
    Ok(())
}
