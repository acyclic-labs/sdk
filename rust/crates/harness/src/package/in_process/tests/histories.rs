use super::*;

async fn trace<P: StreamProvider>(stream: Arc<P>) -> Vec<std::result::Result<Snapshot, String>> {
    let package = package();
    let host = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Effects::default()),
    );
    host.install(package).await.unwrap();
    let owner = placement();
    host.place(owner.clone(), json!(10)).await.unwrap();
    let stale = Placement {
        epoch: 2,
        ..owner.clone()
    };
    use conformance::Action::{Deliver, Recover};
    let actions = [
        Deliver {
            placement: owner.clone(),
            event: event(2, json!(3)),
        },
        Deliver {
            placement: owner.clone(),
            event: event(3, json!(4)),
        },
        Deliver {
            placement: owner.clone(),
            event: event(2, json!(3)),
        },
        Deliver {
            placement: owner.clone(),
            event: event(2, json!(99)),
        },
        Deliver {
            placement: stale,
            event: event(4, json!(1)),
        },
        Recover(owner.clone()),
        Deliver {
            placement: owner.clone(),
            event: event(5, Value::Null),
        },
        Deliver {
            placement: owner.clone(),
            event: event(6, json!(1)),
        },
        Recover(owner),
    ];
    conformance::history(&host, &actions)
        .await
        .into_iter()
        .map(|observation| observation.map_err(|error| error.to_string()))
        .collect()
}

#[tokio::test]
async fn shared_histories_match_on_volatile_and_durable_in_process_hosts() {
    let volatile = trace(Arc::new(MemoryStream::default())).await;
    let directory = tempfile::tempdir().unwrap();
    let durable = trace(Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .unwrap(),
    ))
    .await;
    assert_eq!(durable, volatile);
    // Independent expected values detect a shared runtime regression too.
    for (index, state, revision, consumed) in [
        (0, 13, 1, 1),
        (1, 17, 2, 2),
        (2, 13, 1, 1),
        (5, 17, 2, 2),
        (6, 17, 3, 3),
        (8, 17, 3, 3),
    ] {
        let snapshot = durable[index].as_ref().unwrap();
        assert_eq!(snapshot.checkpoint.state, json!(state));
        assert_eq!(
            (snapshot.checkpoint.revision, snapshot.consumed),
            (revision, consumed)
        );
        assert!(snapshot.pending.is_empty());
    }
    for index in [3, 4, 7] {
        assert!(durable[index].is_err());
    }
    assert!(matches!(
        durable[8].as_ref().unwrap().status,
        MachineStatus::Completed { .. }
    ));
}
