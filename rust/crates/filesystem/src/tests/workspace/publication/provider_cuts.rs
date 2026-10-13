use super::*;

#[tokio::test]
async fn workspace_creation_recovers_at_every_provider_cut() -> Result<(), Box<dyn Error>> {
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
            let attempt = unbounded!(fs, create_workspace("repo"));
            let fired = cutting.disarm();
            let volume =
                value!(fs.workspace_id("repo", WorkBudget::UNBOUNDED, &CancellationToken::new()))?
                    .volume_id();
            let created =
                first_fork_record(&stream, crate::kernel::volume_authority_id(volume)).await?;
            if let Some(created) = &created {
                assert_eq!(
                    crate::kernel::decode_volume_created(
                        &created.payload,
                        4 * 1024,
                        &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(())
                    )?
                    .volume_id,
                    volume
                );
            }
            if !fired {
                attempt?;
                assert!(created.is_some());
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            let opened = unbounded!(fs, open_workspace("repo"));
            assert_eq!(opened.is_ok(), created.is_some());
            let retried = unbounded!(fs, create_workspace("repo"))?;
            retried.write_text("/after.txt", "after").await?;
            assert_eq!(
                unbounded!(
                    unbounded!(fs, open_workspace("repo"))?,
                    read("/after.txt", 16)
                )?,
                OwnedBytes::from_static(b"after")
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn forks_reconcile_at_every_provider_cut() -> Result<(), Box<dyn Error>> {
    for advanced in [false, true] {
        for paths in [None, Some(Vec::new()), Some(vec!["/base.txt".to_owned()])] {
            fork_through_every_cut(advanced, paths).await?;
        }
    }
    Ok(())
}
