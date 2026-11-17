use super::*;

#[tokio::test]
async fn cold_service_starts_an_independent_owner_for_blocked_work() -> io::Result<()> {
    static POOL: LazyLock<io::Result<NativeWorkers>> =
        LazyLock::new(|| start_workers("demand-proof", 2));
    let (entered, entering) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let first = crate::task_completion(
        move || {
            entered.send(()).map_err(io::Error::other)?;
            released.recv().map_err(io::Error::other)
        },
        None,
        None,
        None,
        &POOL,
    )?;
    let mut first = std::pin::pin!(first);
    assert!(futures_util::poll!(&mut first).is_pending());
    entering.recv().map_err(io::Error::other)?;
    assert_eq!(
        workers(&POOL, false)?
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .started_workers,
        1,
        "one cold job must not start unused service workers"
    );
    let second = crate::task_completion(|| Ok(7), None, None, None, &POOL)?;
    assert_eq!(second.await?, 7);
    release.send(()).map_err(io::Error::other)?;
    first.await
}
