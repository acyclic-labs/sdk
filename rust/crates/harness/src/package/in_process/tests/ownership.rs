use super::*;

struct SelectivePolicy(Mutex<Option<AdmissionAction>>);
impl AdmissionPolicy for SelectivePolicy {
    fn authorize<'a>(
        &'a self,
        action: AdmissionAction,
        _: &'a Manifest,
        _: Option<&'a Placement>,
        _: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if *self.0.lock().unwrap() == Some(action) {
                Err(Error::Unauthorized(format!("fixture denied {action:?}")))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn every_entry_point_authorizes_its_own_runtime_supplied_action() {
    let package = package();
    let policy = Arc::new(SelectivePolicy(Mutex::new(None)));
    let effects = Arc::new(Effects::default());
    let host = InProcessRuntime::new(
        Arc::new(MemoryStream::default()),
        package.manifest.bindings.clone(),
        policy.clone(),
        effects.clone(),
    );
    host.install(package.clone()).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    for action in [
        AdmissionAction::Install,
        AdmissionAction::Place,
        AdmissionAction::Deliver,
        AdmissionAction::Recover,
        AdmissionAction::Cancel,
    ] {
        *policy.0.lock().unwrap() = Some(action);
        let result = match action {
            AdmissionAction::Install => host.install(package.clone()).await,
            AdmissionAction::Place => host.place(owner.clone(), json!(0)).await.map(|_| ()),
            AdmissionAction::Deliver => host
                .deliver(owner.clone(), event(2, json!(1)))
                .await
                .map(|_| ()),
            AdmissionAction::Recover => {
                assert!(matches!(
                    host.run(owner.clone()).await,
                    Err(Error::Unauthorized(_))
                ));
                host.recover(owner.clone()).await.map(|_| ())
            }
            AdmissionAction::Cancel => host
                .cancel(owner.clone(), event(3, Value::Null))
                .await
                .map(|_| ()),
        };
        assert!(matches!(result, Err(Error::Unauthorized(_))), "{action:?}");
    }
    *policy.0.lock().unwrap() = None;
    let untouched = host.recover(owner).await.unwrap();
    assert_eq!((untouched.checkpoint.revision, untouched.consumed), (0, 0));
    assert!(!untouched.cancelled);
    assert_eq!(effects.fences.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runtime_subscription_recovers_an_inbox_after_the_attached_client_is_dropped() {
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(10)).await.unwrap();
    let host = Arc::new(host);
    // The deployed runtime retains its owner independently of a client handle.
    let runner = host.clone();
    let placement = owner.clone();
    let (ready, idle) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut subscription = runner.run(placement);
        assert!(futures::poll!(&mut subscription).is_pending());
        ready.send(()).unwrap();
        subscription.await
    });
    idle.await.unwrap();
    let client = host.clone();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    drop(client);
    journal
        .admit_event(event(2, Value::Null), false)
        .await
        .unwrap();
    let settled = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(settled.checkpoint.state, json!(10));
    assert_eq!(settled.checkpoint.revision, 1);
    assert_eq!(settled.consumed, 1);
    assert!(matches!(settled.status, MachineStatus::Completed { .. }));
}

#[tokio::test]
async fn a_failed_move_is_recoverable_and_fences_all_old_owner_entry_points() {
    let (_, package, policy, effects, _) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest.bindings.clone(),
        policy.clone(),
        effects.clone(),
    );
    host.install(package.clone()).await.unwrap();
    let old = placement();
    host.place(old.clone(), json!(10)).await.unwrap();
    let new = Placement {
        epoch: 2,
        node: "node-2".into(),
        ..old.clone()
    };
    effects.unavailable.store(true, Ordering::SeqCst);
    assert!(host.place(new.clone(), json!(10)).await.is_err());
    assert!(host.recover(old.clone()).await.is_err());
    assert!(host.deliver(old.clone(), event(2, json!(1))).await.is_err());
    assert!(host.cancel(old, event(3, Value::Null)).await.is_err());
    drop(host);
    let reopened = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        policy,
        effects.clone(),
    );
    reopened.install(package).await.unwrap();
    effects.unavailable.store(false, Ordering::SeqCst);
    let recovered = reopened.recover(new.clone()).await.unwrap();
    assert_eq!(recovered.placement, new);
    assert_eq!(recovered.checkpoint.state, json!(10));
    assert_eq!(effects.fences.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_node_local_volume_rejects_placement_on_another_node_before_publication() {
    let mut manifest = package().manifest.clone();
    manifest.bindings.placement = VolumePlacement::NodeLocal("node-1".into());
    let package = Arc::new(Counter {
        manifest,
        schema: json!({"type":"integer"}),
    });
    let effects = Arc::new(Effects::default());
    let stream = Arc::new(MemoryStream::default());
    let host = runtime(
        stream.clone(),
        &package,
        Arc::new(Policy(AtomicBool::new(true))),
        effects.clone(),
    );
    host.install(package).await.unwrap();
    let old = placement();
    host.place(old.clone(), json!(0)).await.unwrap();
    let new = Placement {
        epoch: 2,
        node: "node-2".into(),
        ..old.clone()
    };
    assert!(host.place(new.clone(), json!(0)).await.is_err());
    assert!(host.deliver(new.clone(), event(2, json!(1))).await.is_err());
    assert!(host.recover(new.clone()).await.is_err());
    assert!(host.cancel(new, event(3, Value::Null)).await.is_err());
    assert_eq!(host.recover(old).await.unwrap().checkpoint.revision, 0);
    assert_eq!(effects.fences.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn later_deliveries_cannot_overtake_an_admitted_input() {
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    journal
        .admit_event(event(2, json!(3)), false)
        .await
        .unwrap();
    assert!(
        host.deliver(owner.clone(), event(3, json!(4)))
            .await
            .is_err()
    );
    assert_eq!(
        host.recover(owner.clone()).await.unwrap().checkpoint.state,
        json!(3)
    );
    assert_eq!(
        host.deliver(owner, event(3, json!(4)))
            .await
            .unwrap()
            .checkpoint
            .state,
        json!(7)
    );
}

#[tokio::test]
async fn a_node_local_actor_can_recover_a_new_epoch_on_its_own_node() {
    let mut manifest = package().manifest.clone();
    manifest.bindings.placement = VolumePlacement::NodeLocal("node-1".into());
    let package = Arc::new(Counter {
        manifest,
        schema: json!({"type":"integer"}),
    });
    let directory = tempfile::tempdir().unwrap();
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    );
    let effects = Arc::new(Effects::default());
    let host = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        effects.clone(),
    );
    host.install(package).await.unwrap();
    let old = placement();
    host.place(old.clone(), json!(0)).await.unwrap();
    host.deliver(old.clone(), event(2, json!(5))).await.unwrap();
    let next = Placement {
        epoch: 2,
        ..old.clone()
    };
    let recovered = host.place(next.clone(), json!(0)).await.unwrap();
    assert_eq!(recovered.placement, next);
    assert_eq!(recovered.checkpoint.state, json!(5));
    assert_eq!(effects.fences.load(Ordering::SeqCst), 1);
    assert!(host.recover(old).await.is_err());
}
