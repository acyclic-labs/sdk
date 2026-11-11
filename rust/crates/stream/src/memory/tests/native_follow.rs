use super::*;
use crate::test_meter::Meter;
use futures::StreamExt as _;

#[tokio::test]
async fn stop_retires_the_pending_read_without_releasing_the_state_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("native/pending")?;
    let meter = Arc::new(Meter::default());
    let mut request = crate::FollowRequest::new(path.clone(), 0);
    request.when_absent = crate::WhenAbsent::Wait;
    let output = provider.native_follow(request, meter.original()).await??;
    let held = provider.state.write().await;
    let owner = acyclic_native_runtime::retain_future(
        async move {
            let mut output = output;
            let _ = output.value_mut().next().await;
        },
        &meter.original(),
    )?;
    owner.stop()?;
    assert!(owner.is_stopped());
    drop(owner);
    // No detached snapshot producer may wait for this lock after stop.
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(held.signals.get(&path)?.is_none());
    drop(held);
    assert!(provider.state.try_write().is_ok());
    Ok(())
}
