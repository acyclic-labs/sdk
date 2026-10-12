use super::*;

#[tokio::test]
async fn workspace_creation_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    let mut uninterrupted_commits = None;
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
            let fs: CutFs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(objects), bucket),
                crate::EmbeddedCapabilities::MEMORY,
            );
            cutting.arm(fail_at, cut);
            let attempt = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error);
            let fired = cutting.disarm();
            let committed = cutting.commits();
            let volume = fs.workspace_id("repo")?.volume_id();
            let created =
                first_fork_record(&stream, crate::kernel::volume_authority_id(volume)).await?;
            if let Some(created) = &created {
                assert_eq!(
                    crate::kernel::decode_volume_created(&created.payload, 4 * 1024, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(()))?.volume_id,
                    volume
                );
            }
            if !fired {
                attempt?;
                assert!(created.is_some());
                uninterrupted_commits.get_or_insert(committed);
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            // An interruption leaves no volume or the complete one.
            let opened = fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error);
            assert_eq!(opened.is_ok(), created.is_some());
            let retried = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            retried.write_text("/after.txt", "after").await?;
            assert_eq!(
                fs.open_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
                    .read("/after.txt", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
                OwnedBytes::from_static(b"after")
            );
        }
    }
    assert_eq!(uninterrupted_commits, Some(1));
    Ok(())
}

#[tokio::test]
async fn a_fork_of_the_head_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    // The fork of a published head shares its lineage, which the same
    // commit extends with the new workspace's first generation.
    assert_eq!(fork_through_every_cut(false, None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn an_independent_fork_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    assert_eq!(fork_through_every_cut(true, None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn filtered_forks_reconcile_at_every_provider_cut() -> Result<(), Box<dyn Error>> {
    for advanced in [false, true] {
        for paths in [Vec::new(), vec!["/base.txt".to_owned()]] {
            assert_eq!(fork_through_every_cut(advanced, Some(paths)).await?, 1);
        }
    }
    Ok(())
}
