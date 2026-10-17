use super::*;

#[tokio::test]
async fn output_replay_uses_original_content_after_mutable_path_replacement() {
    let fixture = Fixture::new(project()).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    let dispatch = fixture.dispatch().await;
    let original = fixture
        .provider
        .original_artifact_with_native_work(
            dispatch.effect_id,
            dispatch.attempt_id,
            "result",
            &fixture.startup,
        )
        .await
        .unwrap()
        .unwrap();
    let mut allocation = fixture
        .provider
        .read_with_native_work::<PiRootAllocation>(&original.0, &fixture.startup)
        .await
        .unwrap();
    allocation.0.scratch.workspace_id = [124; 16];
    let authority = fixture.owner.authority(&fixture.startup).unwrap();
    let mut io = acyclic_harness::store::HistoryWork::new(
        acyclic_harness::filesystem::PreparationIo::from_native_work(&fixture.startup).unwrap(),
    );
    let write = ContentGrant::verify(
        &authority.verifier,
        &authority.signed,
        &fixture.owner.policy().journal,
        VolumeOperation::Write,
        fixture.owner.policy().maximum_payload_bytes,
        &mut Some(&mut io),
    )
    .unwrap();
    fixture
        .filesystem
        .put_content(
            &fixture.owner.policy().journal,
            &write,
            original.0.path(),
            &serde_json::to_vec(&allocation.0).unwrap(),
            "application/json",
            "result.json",
            fixture.owner.policy().maximum_payload_bytes,
            &IdempotencyKey::new("different-output-operation").unwrap(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let reopened = fixture.reopen().await;
    let result = reopened
        .reconcile_with_native_work(dispatch.attempt_id, &fixture.startup)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.0.status,
        EffectStatus::Succeeded {
            result: original.0.clone()
        }
    );
    assert_eq!(
        reopened
            .dispatch_with_native_work(dispatch.clone(), &fixture.startup)
            .unwrap()
            .await
            .unwrap()
            .0
            .status,
        result.0.status
    );
    assert_eq!(
        fixture
            .provider
            .dispatch_with_native_work(dispatch, &fixture.startup)
            .unwrap()
            .await
            .unwrap()
            .0
            .status,
        result.0.status
    );
}
