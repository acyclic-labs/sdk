use super::*;
use crate::access::Action;
use adapter::tests::access::{deny, publish_test_access};

async fn revoked_chunk(broken: bool) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"abcdefgh")?;
    let view = mounted_view(root.path(), "access-chunks").await?;
    let path = mounted(&["file"]);
    let (_, pin) = view.lookup_pinned(&path)?.ok_or("missing source file")?;
    let mut chunks = Vec::new();
    let result = adapter::tests::return_fence::with_omitted(broken, || {
        view.read_pinned(
            &path,
            pin.ok_or(MountSourceError::Stale)?,
            0,
            8,
            4,
            &mut |offset, bytes| {
                chunks.push(bytes.to_vec());
                if offset == 0 {
                    publish_test_access(
                        &view.authored,
                        vec![deny("revoked", &[Action::Read], "file")],
                    )
                    .map_err(|error| MountSourceError::Engine(error.to_string().into()))?;
                }
                Ok(())
            },
        )
    });
    assert!(
        matches!(result, Err(MountSourceError::AccessDenied)) && chunks == [b"abcd".to_vec()],
        "lazy chunk escaped its revoked cut: {result:?} {chunks:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lazy_chunks_confirm_authority_before_each_sink_call()
-> Result<(), Box<dyn std::error::Error>> {
    revoked_chunk(false).await
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "lazy chunk escaped its revoked cut")]
async fn missing_chunk_fence_negative_control() {
    revoked_chunk(true).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_access_lazy_hides_source_entries_without_truncating_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("a-secret"), b"secret")?;
    std::fs::write(root.path().join("b-public"), b"public")?;
    let view = mounted_view(root.path(), "access-hidden").await?;
    publish_test_access(
        &view.authored,
        vec![deny("hide", &[Action::List, Action::Read], "a-secret")],
    )?;
    let hidden = mounted(&["a-secret"]);
    assert!(view.lookup(&hidden)?.is_none());
    assert!(matches!(
        view.read_range(&hidden, 0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        view.open_file_with_lookup(&hidden),
        Err(MountSourceError::AccessDenied)
    ));
    let page = view.read_directory(&MountPath::root(), None, 1)?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, mounted(&["b-public"]).components()[0]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_access_lazy_content_read_does_not_disclose_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"content")?;
    let view = mounted_view(root.path(), "access-disclosure").await?;
    let path = mounted(&["file"]);
    // Materialize an authored identity first: a denied lookup must not fall
    // through to the demand source or prevent independent content authorization.
    view.write_range(&path, 0, Bytes::from_static(b"authored"))?;
    publish_test_access(
        &view.authored,
        vec![deny("no-list", &[Action::List], "file")],
    )?;
    assert!(view.lookup(&path)?.is_none());
    assert!(view.open_file_with_lookup(&path).is_err());
    let file = view.open_file(&path)?;
    assert_eq!(file.read_range(0, 8)?.as_ref(), b"authored");
    assert!(matches!(file.lookup(), Err(MountSourceError::AccessDenied)));
    assert!(
        view.read_directory(&MountPath::root(), None, 1)?
            .entries
            .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_access_lazy_revokes_existing_and_new_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"source")?;
    let view = mounted_view(root.path(), "access-refresh").await?;
    let path = mounted(&["file"]);
    let old = view.open_file(&path)?;
    publish_test_access(
        &view.authored,
        vec![deny("revoked", &[Action::Read, Action::Write], "file")],
    )?;
    assert!(matches!(
        old.read_range(0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        old.write_range(0, Bytes::from_static(b"pinned")),
        Err(MountSourceError::AccessDenied)
    ));
    assert_eq!(std::fs::read(root.path().join("file"))?, b"source");
    let fresh = view.open_file(&path)?;
    assert!(matches!(
        fresh.read_range(0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(fresh.write_range(0, Bytes::from_static(b"denied")).is_err());
    let detached = view.detach_file(&path)?;
    assert!(
        matches!(
            detached.read_range(0, 6),
            Err(MountSourceError::AccessDenied)
        ),
        "detachment cannot itself acquire the revoked Read right"
    );
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    view.remove(&path, None, &work)?;
    assert!(matches!(
        old.read_range(0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_access_lazy_denied_mutations_do_not_promote_source()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"source")?;
    let view = mounted_view(root.path(), "access-no-promotion").await?;
    let path = mounted(&["file"]);
    publish_test_access(
        &view.authored,
        vec![deny(
            "read-only",
            &[
                Action::Create,
                Action::Write,
                Action::Append,
                Action::Delete,
                Action::Rename,
            ],
            "**",
        )],
    )?;
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    assert!(matches!(
        view.open_file(&path)?
            .write_range(0, Bytes::from_static(b"bad")),
        Err(MountSourceError::ReadOnly)
    ));
    assert!(matches!(
        view.set_attributes(&path, FileMetadata::default(), None, &work),
        Err(MountSourceError::ReadOnly)
    ));
    assert!(view.remove(&path, None, &work).is_err());
    for (source, destination) in [
        (&path, mounted(&["new"])),
        (&mounted(&["other"]), path.clone()),
    ] {
        assert!(view.rename(source, &destination, false, &work).is_err());
    }
    assert!(
        view.authored.lookup(&path)?.is_none(),
        "denial precedes promotion"
    );
    assert_eq!(view.read_range(&path, 0, 6)?.as_ref(), b"source");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_access_lazy_created_handle_keeps_authority_after_unlink()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let view = mounted_view(root.path(), "access-created-unlinked").await?;
    publish_test_access(&view.authored, vec![deny("no-read", &[Action::Read], "**")])?;
    let path = mounted(&["inbox"]);
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let created = view.create_file(&path, FileMetadata::default(), None, &work)?;
    let file = view.open_created(&path, &created)?;
    file.write_range(0, Bytes::from_static(b"contents"))?;
    assert!(matches!(
        file.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    view.remove(&path, Some(created.node.file_id), &work)?;
    assert!(view.lookup(&path)?.is_none());
    assert!(matches!(
        file.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    file.write_range(8, Bytes::from_static(b"!"))?;
    assert!(matches!(
        file.read_range(0, 9),
        Err(MountSourceError::AccessDenied)
    ));
    Ok(())
}
