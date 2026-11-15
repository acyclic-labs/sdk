use super::*;
use crate::access::{AccessView, Action, Capability, Generations, Grant, ResourcePattern};

#[tokio::test]
async fn command_provenance_preserves_volume_path_action_and_generation_ceilings()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    for generations in [Generations::All, Generations::Head] {
        for exact in [false, true] {
            let cancellation = CancellationToken::new();
            let work = capture_meter(WorkBudget::UNBOUNDED, &cancellation)?;
            let fs = Fs::memory();
            let workspace = fs
                .create_workspace("command-grant", WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            workspace.write_text("/allowed", "original").await?;
            workspace.write_text("/denied", "private!").await?;
            let checkout = workspace
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    &work,
                    &cancellation,
                )
                .await?;
            let origin = checkout.generation_id();
            let config = checkout.volume_config();
            let source = CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(
                    checkout,
                    MountPublication::Manual,
                )),
                config,
            )?;
            let resource = |path: &str| ResourcePattern {
                lineage_root: None,
                volume: Some(
                    uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string(),
                ),
                generations,
                path: path.into(),
            };
            let view = super::access::compiled(
                &source,
                1,
                vec![],
                Some(Capability {
                    layers: vec![vec![
                        Grant {
                            actions: [Action::List].into_iter().collect(),
                            resource: resource("**"),
                        },
                        Grant {
                            actions: [Action::Read, Action::Write].into_iter().collect(),
                            resource: resource("allowed"),
                        },
                    ]],
                    expires_at: None,
                }),
            )?;
            let view = if exact {
                view.with_data_generation(origin)
            } else {
                view
            };
            let mut source = source.with_access_view(Arc::new(AccessView::new(view)))?;
            source.native_original = Some(work.clone());
            let mut source = Arc::new(source);
            CheckoutMountSource::configure_command(&mut source, workspace.clone(), true)?;
            let path = native_test_path("allowed");
            let file = source.open_file(&path)?;
            file.write_range_with_meter(
                0,
                crate::kernel::OwnedSourceChunk::claimed(
                    acyclic_stream::OwnedBytes::from_static(b"updated!"),
                    0,
                ),
                false,
                &work,
            )?;
            let fork_volume = source.checkout.lock().await.volume_id();
            assert_ne!(
                fork_volume,
                workspace.id().volume_id(),
                "mutation stayed on the source"
            );
            assert_eq!(source.volume_id()?, workspace.id().volume_id());
            assert_eq!(file.read_range(0, 8)?.as_ref(), b"updated!");
            file.write_range_with_meter(
                0,
                crate::kernel::OwnedSourceChunk::claimed(
                    acyclic_stream::OwnedBytes::from_static(b"again!!!"),
                    0,
                ),
                false,
                &work,
            )?;
            assert_eq!(
                source.checkout.lock().await.volume_id(),
                fork_volume,
                "second write forked again"
            );
            assert!(matches!(
                source.read_range(&native_test_path("denied"), 0, 8),
                Err(MountSourceError::AccessDenied)
            ));
            assert!(matches!(
                source.remove(&path, None, &work),
                Err(MountSourceError::AccessDenied)
            ));
            assert_eq!(
                workspace
                    .read("/allowed", 8, &work, &cancellation)
                    .await?
                    .as_ref(),
                b"original"
            );
            assert_eq!(workspace.head(&work, &cancellation).await?.id(), origin);
            if generations == Generations::Head {
                // This capability was for the original head. Moving that head
                // cannot be hidden by the private fork's unchanged policy.
                workspace.write_text("/unrelated", "next head").await?;
                assert!(matches!(
                    file.read_range(0, 8),
                    Err(MountSourceError::AccessDenied)
                ));
            } else {
                source.cancel();
                let (fork, custody) = source
                    .finish_retired_command()
                    .await?
                    .ok_or("missing data fork")?;
                assert_eq!(
                    fork.read("/allowed", 8, &work, &cancellation)
                        .await?
                        .as_ref(),
                    b"again!!!"
                );
                custody.settle().await?;
            }
        }
    }
    Ok(())
}
