//! Counts actual owner admission observations on the measured async task.
tokio::task_local! { static CALLS: std::cell::Cell<u64>; }

#[cfg(test)]
pub(crate) async fn measure<F: Future>(operation: F) -> (F::Output, u64) {
    CALLS
        .scope(std::cell::Cell::new(0), async {
            let output = operation.await;
            (output, CALLS.with(std::cell::Cell::get))
        })
        .await
}
pub(crate) fn observed() {
    let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
}
