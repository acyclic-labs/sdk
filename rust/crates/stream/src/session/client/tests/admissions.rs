use super::*;

fn ended_sibling(channel: &TransportChannel) -> (Pool, Cursor, Cursor) {
    let (mut pool, receiver_b) = pool::fixture::fixture(channel);
    pool.links.remove("bad");
    let (records, receiver_a) = mpsc::channel(2);
    let route_a = pool.routes.get_mut(&1).unwrap();
    route_a.node = "good".into();
    route_a.records = records;
    let client = Client {
        connections: super::super::super::Connections::new(),
        executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
        commands: mpsc::channel(2).0,
        removed: Arc::clone(&pool.removed),
        next_id: Arc::new(AtomicU64::new(5)),
        active: Arc::clone(&pool.active),
        admissions: Arc::clone(&pool.admissions),
        attempt_epoch: Arc::clone(&pool.attempt_epoch),
        abandoned: Arc::clone(&pool.abandoned),
    };
    let a = client
        .cursor(
            1,
            Arc::clone(&pool.routes.get(&1).unwrap().progress),
            receiver_a,
        )
        .unwrap();
    let b = client
        .cursor(
            3,
            Arc::clone(&pool.routes.get(&3).unwrap().progress),
            receiver_b,
        )
        .unwrap();
    // Their Open commands have already been dispatched into the fixture routes.
    pool.admissions.remove(1).unwrap();
    pool.admissions.remove(3).unwrap();
    assert!(pool.receive_frame(
        "good",
        frame(
            3,
            1,
            Body::Closed(wire::SessionClosed {
                code: StreamError::AccessDenied.code().into(),
                message: String::new(),
            })
        )
    ));
    assert!(!pool.routes.contains_key(&3));
    assert_eq!(pool.active.load(Ordering::Acquire), 2);
    (pool, a, b)
}

#[tokio::test]
async fn terminal_unpolled_cursor_cannot_keep_a_home_reconnect_alive() -> TestResult {
    let fixture = Fixture::open(Arc::new(MemoryStream::default()), "good").await?;
    let (mut pool, a, _terminal_b) = ended_sibling(&fixture._channel);
    let (release, held) = oneshot::channel::<()>();
    // One fixture lock, never nested and released before the credential await.
    let held = std::sync::Arc::new(Mutex::new(Some(held)));
    let called = std::sync::Arc::new(AtomicUsize::new(0));
    let observed = called.clone();
    let credential = grpc::CredentialProvider::new(move || {
        observed.fetch_add(1, Ordering::Relaxed);
        let held = held.lock().unwrap().take();
        async move {
            held.ok_or(grpc::CredentialError::Invalid)?
                .await
                .map_err(|_| grpc::CredentialError::Invalid)?;
            Ok("fixture".into())
        }
    });
    pool.links.get_mut("good").unwrap().channel =
        native::endpoint(fixture._channel.clone(), None, Some(credential));
    pool.reconnect("good")?;
    assert!(
        futures::poll!(future::poll_fn(|cx| pool::poll_dials(&mut pool.dials, cx))).is_pending()
    );
    assert_eq!(called.load(Ordering::Relaxed), 1);
    drop(a);
    pool.close_idle();
    assert_eq!(
        pool.active.load(Ordering::Acquire),
        1,
        "B's output is deliberately undrained"
    );
    assert!(
        !pool.node_interested("good"),
        "a terminal cursor is not a pending Open"
    );
    assert!(
        pool.dials.is_empty(),
        "undrained terminal output must not own a reconnect"
    );
    assert!(
        release.send(()).is_err(),
        "the blocked credential future must be dropped"
    );
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn terminal_unpolled_cursor_cannot_authorize_a_completed_home_reconnect() -> TestResult {
    let fixture = Fixture::open(Arc::new(MemoryStream::default()), "good").await?;
    let (mut pool, a, _terminal_b) = ended_sibling(&fixture._channel);
    pool.reconnect("good")?;
    let (node, epoch, ready) = future::poll_fn(|cx| pool::poll_dials(&mut pool.dials, cx))
        .await
        .ok_or("dial ended")?;
    assert!(ready.is_ok());
    drop(a);
    assert!(
        !pool.node_interested("good"),
        "a terminal cursor is not a pending Open"
    );
    pool.dial_ready(&node, epoch, ready)?;
    assert!(
        pool.links.is_empty(),
        "ownerless completion must not install a Link"
    );
    fixture
        .until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 1)
        .await;
    Ok(())
}
