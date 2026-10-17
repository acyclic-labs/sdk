use super::*;

async fn claim_gap<const RECOVER_REQUEST: bool>() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let provider = text();
    let config =
        crate::test_support::local_harness_config(directory.path(), (model()?, provider.clone()))?
            .without_default_tools();
    let harness = Box::pin(LocalHarness::open(config)).await?;
    let operation = OperationId::new();
    let session = uuid::Uuid::from_bytes(operation.into_bytes());
    let request = crate::client::prepare_local_prompt(operation, "claim before admission")?;
    let files = harness.task_files();
    let cut = files.inner.head().await?;
    let cut = files.inner.append(cut, &request).await?;
    let mut claim = request.clone();
    if let FileKey::Session { file, .. } = &mut claim.key {
        *file = TaskFile::ExecutionClaim(Component::new(operation.to_string())?);
    }
    files.inner.append(cut, &claim).await?;
    // Reopen the owner at the exact durable cut after the execution claim, with
    // no turn, admission, provider request or result ever created.
    let mut owner =
        CommandSession::open(Arc::new(files.inner.reopen().await?), session, 65536, 128).await?;
    let output = if RECOVER_REQUEST {
        consume_local_command(&mut owner, &harness, &request).await
    } else {
        owner
            .consume_with_recovery::<true, _, _, _, _>(
                &request,
                |_, _, _| async { unreachable!() },
                |operation, _, _| {
                    let harness = &harness;
                    async move {
                        let task = crate::runtime::task_id_for_admission(operation);
                        serde_json::to_value(harness.resume(task).await?)
                            .map_err(super::super::super::storage)
                    }
                },
            )
            .await
    };
    assert!(
        output.is_ok(),
        "claimed but unadmitted prompt became terminal: {output:?}"
    );
    assert_eq!(output?.unwrap()["text"], "done");
    assert_eq!(provider.requests().len(), 1);
    assert!(
        consume_local_command(&mut owner, &harness, &request)
            .await?
            .is_none()
    );
    assert_eq!(provider.requests().len(), 1);
    drop(owner);
    drop(files);
    harness.close().await
}
#[tokio::test]
async fn claim_before_admission_recovers_the_immutable_request_once() -> Result<()> {
    claim_gap::<true>().await
}
#[tokio::test]
#[should_panic(expected = "claimed but unadmitted prompt became terminal")]
async fn resume_without_admitting_the_saved_request_is_detected_negative_control() {
    claim_gap::<false>().await.unwrap();
}
