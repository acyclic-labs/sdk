use super::*;
use crate::test_meter::Meter;
use futures::{FutureExt as _, StreamExt as _};

#[tokio::test]
async fn stop_retires_the_pending_read_without_releasing_the_state_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("native/pending")?;
    let meter = Arc::new(Meter::default());
    let mut request = crate::FollowRequest::new(path.clone(), 0);
    request.when_absent = crate::WhenAbsent::Wait;
    let mut output = provider.native_follow(request, meter.original()).await??;
    let held = provider.state.write().await;
    assert!(output.value_mut().next().now_or_never().is_none());
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(output);
    // No detached snapshot producer may wait for this lock after the cursor
    // is destroyed. The production watch's owning-future stop is tested at
    // the real computed UI boundary, using its reservation-capable ledger.
    meter.assert_refunded();
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    assert!(held.signals.get(&path)?.is_none());
    drop(held);
    assert!(provider.state.try_write().is_ok());
    Ok(())
}
