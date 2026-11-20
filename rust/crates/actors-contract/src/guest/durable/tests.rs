use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[test]
fn guest_socket_metadata_round_trips_without_logical_caps() -> Result<(), GuestError> {
    let result = super::super::GuestEventResult::SocketAccept {
        subprotocol: None,
        attachment: Bytes::from(vec![7; 2049]),
        tags: vec!["λ".repeat(257); 11],
    };
    let raw =
        serde_json::to_value(&result).map_err(|error| GuestError::Encode(error.to_string()))?;
    let restored: super::super::GuestEventResult = serde_json::from_value(raw.clone())
        .map_err(|error| GuestError::Encode(error.to_string()))?;
    assert_eq!(restored, result);
    let mut malformed = raw;
    let fields = malformed
        .as_object_mut()
        .ok_or_else(|| GuestError::Encode("socket result is not an object".into()))?;
    fields.insert("tags".into(), serde_json::json!([null]));
    assert!(serde_json::from_value::<super::super::GuestEventResult>(malformed).is_err());
    Ok(())
}
#[tokio::test]
async fn initialization_is_once_before_all_delivery_and_failed_heap_stays_fenced()
-> Result<(), GuestError> {
    let count = Arc::new(AtomicUsize::new(0));
    let initialized = count.clone();
    let activation = Activation::new(async move {
        initialized.fetch_add(1, Ordering::SeqCst);
        Ok(42)
    });
    let (a, b) = futures::join!(
        activation.deliver(async |state| Ok(*state)),
        activation.deliver(async |state| Ok(*state))
    );
    assert_eq!((a?, b?), (42, 42));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let failed = activation
        .block_concurrency_while(async |_| {
            Err::<(), _>(GuestError::Handler("failed initializer".into()))
        })
        .await;
    assert!(failed.is_err());
    assert!(activation.deliver(async |_| Ok(())).await.is_err());
    // Negative control: bypassing the admission gate would execute the event.
    let ungated = async {
        count.fetch_add(1, Ordering::SeqCst);
        Ok::<_, GuestError>(())
    };
    ungated.await?;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    Ok(())
}
#[tokio::test]
async fn cancellation_during_initialization_fences_the_heap() {
    let activation = Activation::<u32>::new(std::future::pending());
    let mut delivery = Box::pin(activation.deliver(async |_| Ok(())));
    assert!(futures::poll!(delivery.as_mut()).is_pending());
    drop(delivery);
    assert!(activation.deliver(async |_| Ok(())).await.is_err());
}
