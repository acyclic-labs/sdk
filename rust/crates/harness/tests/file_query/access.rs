use super::*;
use acyclic_fs::access::{Action, Effect, Policy};
use acyclic_harness::task_volume;
use proptest::prelude::*;

pub(super) async fn publish_policy<O: AsyncObjectStore + 'static>(
    fixture: &Fixture<O>,
    read: bool,
    list: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut policy: Policy =
        serde_json::from_slice(acyclic_harness::filesystem::INITIAL_ACCESS_POLICY)?;
    policy.version += 1;
    if read || list {
        let mut deny = policy.layers[0][0].clone();
        deny.id = "restricted-query".into();
        deny.effect = Effect::Deny;
        deny.actions = [(read, Action::Read), (list, Action::List)]
            .into_iter()
            .filter_map(|(denied, action)| denied.then_some(action))
            .collect();
        deny.resource.path = "plans/**".into();
        policy.layers[0].push(deny);
    }
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .await?
        .value;
    policy_support::publish(&workspace, &policy).await
}

#[tokio::test]
async fn pending_query_snapshots_and_follow_recheck_policy_before_disclosure()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for follow in [false, true] {
        for revoke in [false, true] {
            let objects = CountingObjects::default();
            let filesystem = Fs::new(
                acyclic_fs::StreamAuthorityStore::new(Arc::new(MemoryStream::default())),
                objects.clone(),
                acyclic_fs::EmbeddedCapabilities::MEMORY,
            );
            let fixture = Fixture::with_filesystem(filesystem, objects.reads.clone()).await?;
            let content = objects::blob_text(false);
            fixture.write(&[("plans/a.md", Some(&content))]).await?;
            let mut observation = fixture
                .source
                .observe_files(Query::Plans, None, 2, content.len())
                .await?;
            if follow {
                fixture
                    .write(&[("plans/a.md", Some(&objects::blob_text(true)))])
                    .await?;
            }
            let (entered, release) = objects.pause(acyclic_fs::ObjectKind::BlobChunk);
            let read = async {
                if follow {
                    observation
                        .changes
                        .next()
                        .await
                        .ok_or(task_volume::Error::ResetRequired)?
                        .map(|_| ())
                } else {
                    fixture
                        .source
                        .observe_files(Query::Plans, None, 2, content.len())
                        .await
                        .map(|_| ())
                }
            };
            let publish = async {
                entered.await?;
                if revoke {
                    publish_policy(&fixture, true, false).await?;
                }
                release.send(()).map_err(|_| "read was abandoned")?;
                Ok::<_, Box<dyn std::error::Error>>(())
            };
            let (result, published) =
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    futures::join!(read, publish)
                })
                .await?;
            published?;
            if revoke {
                assert!(
                    matches!(&result, Err(task_volume::Error::FileRead(error)) if error.code() == acyclic_harness::wire::ErrorCode::Unauthorized),
                    "{result:?}"
                );
            } else {
                result?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn query_snapshots_and_follow_fail_closed_after_disclosure_revocation()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for (read, list) in [(true, false), (false, true)] {
        let fixture = Fixture::new().await?;
        fixture
            .write(&[("plans/a.md", Some("- [ ] private"))])
            .await?;
        let mut observation = fixture
            .source
            .observe_files(Query::Plans, None, 2, 64)
            .await?;
        assert_eq!(observation.snapshot.as_ref().unwrap().files.len(), 1);
        publish_policy(&fixture, read, list).await?;
        assert!(matches!(
            fixture
                .source
                .observe_files(Query::Plans, None, 2, 64)
                .await,
            Err(task_volume::Error::FileRead(
                acyclic_harness::Error::FilesystemAccessConfiguration("file query")
            ))
        ));
        assert!(matches!(
            observation.changes.next().await,
            Some(Err(task_volume::Error::FileRead(
                acyclic_harness::Error::FilesystemAccessConfiguration("file query")
            )))
        ));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Raw query projection can disclose bytes only after the common compiler
    // proves Read and List unrestricted; otherwise its typed error precedes data.
    #[test]
    fn restrictive_queries_never_return_a_snapshot(read in any::<bool>(), list in any::<bool>(), name in "[a-z]{1,8}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let fixture = Fixture::new().await?;
            fixture.write(&[(&format!("plans/{name}.md"), Some("- [ ] private"))]).await?;
            publish_policy(&fixture, read, list).await?;
            let result = fixture.source.observe_files(Query::Plans, None, 2, 64).await;
            if read || list {
                assert!(matches!(result, Err(task_volume::Error::FileRead(
                    acyclic_harness::Error::FilesystemAccessConfiguration("file query")))));
            } else {
                assert_eq!(result?.snapshot.unwrap().files.len(), 1);
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
