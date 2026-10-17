use super::*;

fn single_disclosure_boundary(source: &str) -> bool {
    source.matches(".finish(").count() + source.matches("::finish(").count() == 1
}

#[test]
fn read_results_have_one_checked_output_boundary() {
    assert!(single_disclosure_boundary(include_str!(
        "../../filesystem/src/workspace/read_core.rs"
    )));
}

#[test]
fn direct_output_bypass_negative_control() {
    let old_exit = format!(
        "{}\nwork.finish(result, accounting)",
        include_str!("../../filesystem/src/workspace/read_core.rs")
    );
    assert!(!single_disclosure_boundary(&old_exit));
}

fn assert_denied(result: Result<(), acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>) {
    assert!(
        matches!(result, Err(failure) if matches!(failure.error, acyclic_fs::WorkspaceError::AccessDenied)),
        "coarse disclosure returned revoked bytes"
    );
}

async fn racing_read(protected: bool, range: bool, revoke: bool) -> Result<(), Box<dyn Error>> {
    let objects = objects::CountingObjects::default();
    let fs = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        objects.clone(),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let fixture = Fixture::with_filesystem(fs, true).await?;
    let contents = objects::blob_text(false);
    fixture
        .host
        .put_content(
            &fixture.volume,
            &copies::legacy(&fixture, &fixture.volume, VolumeOperation::Write)?,
            "secrets/blob",
            contents.as_bytes(),
            "text/plain",
            "blob",
            65536,
            &IdempotencyKey::new("coarse-blob")?,
            None,
            None,
            None,
        )
        .await?;
    let cancellation = CancellationToken::new();
    let owner = fixture
        .fs
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let bound = owner
        .clone()
        .bind_coarse_access(&[Action::Read, Action::List], &cancellation)
        .await?;
    let pinned = bound
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let unchecked = owner
        .generation(pinned.id(), WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let (entered, release) = objects.pause(acyclic_fs::ObjectKind::BlobChunk);
    let read = async {
        if !protected {
            // The former path authorized its selected checkout before storage
            // and returned the result without the common disclosure boundary.
            unchecked
                .read("/secrets/blob", 65536, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map(|read| assert_eq!(read.value.as_ref(), contents.as_bytes()))
        } else if range {
            bound
                .read_range(
                    "/secrets/blob",
                    0,
                    contents.len() as u64,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|read| assert_eq!(read.value.bytes.as_ref(), contents.as_bytes()))
        } else {
            bound
                .read("/secrets/blob", 65536, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map(|read| assert_eq!(read.value.as_ref(), contents.as_bytes()))
        }
    };
    let publish = async {
        entered.await?;
        let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
        policy.version += 1;
        let mut deny = policy.layers[0][0].clone();
        deny.id = "revoke-during-coarse-read".into();
        deny.effect = Effect::Deny;
        deny.actions = [Action::Read].into_iter().collect();
        policy.layers[0].push(deny);
        if revoke {
            fixture.publish_policy(policy).await?;
        }
        release.send(()).map_err(|_| "read abandoned its gate")?;
        Ok::<_, Box<dyn Error>>(())
    };
    let (result, revoked) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        futures::join!(read, publish)
    })
    .await?;
    revoked?;
    if revoke {
        assert_denied(result);
    } else {
        result?;
    }
    Ok(())
}

#[tokio::test]
async fn coarse_reads_fence_current_policy_after_storage() -> Result<(), Box<dyn Error>> {
    for range in [false, true] {
        for revoke in [false, true] {
            racing_read(true, range, revoke).await?;
        }
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "coarse disclosure returned revoked bytes")]
async fn pre_storage_only_coarse_read_negative_control() {
    racing_read(false, false, true).await.unwrap();
}
