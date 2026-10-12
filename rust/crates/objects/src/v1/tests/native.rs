use super::*;
use crate::v1;

#[cfg(not(target_arch = "wasm32"))]
struct NativeTransportInputGuard {
    _allocation: acyclic_native_runtime::ReadAllocation,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeTransportInputGuard {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_wire_aliases_keep_original_claim_after_context_retirement()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_native_runtime::NativeReadMeter;
    use std::sync::{Arc, atomic::Ordering::SeqCst};
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let errors = v1::NativeTransportErrors::new(&meter)?;
    let controls = errors;
    let (data, allocation) = controls.copy(b"immutable native wire")?;
    let wire = controls.wire_bytes(data, allocation)?;
    let alias = wire.slice(1..4);
    drop(controls);
    let retained = observed.memory.load(SeqCst);
    assert!(retained > 0);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    let last = alias.clone();
    assert_eq!(last.as_ref(), b"mmu");
    drop(wire);
    drop(alias);
    assert_eq!(observed.memory.load(SeqCst), retained);
    drop(last);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::v1) async fn exercise_native_transport<P: ObjectsProvider>(
    provider: &P,
    physical_get_calls: impl Fn() -> u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_native_runtime::NativeReadMeter;
    use std::sync::{Arc, atomic::Ordering::SeqCst};
    let bucket = wire::BucketRef {
        name: "native.controls".into(),
    };
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            ..Default::default()
        })
        .await?;
    let bytes = acyclic_stream::OwnedBytes::from(vec![b'n'; 131_089]);
    for key in [
        "native-body",
        "terminal-error",
        "truncated",
        "huge-header",
        "corrupt-codec",
        "malformed",
    ] {
        provider
            .put(
                wire::PutObjectHeader {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                },
                bytes.clone(),
            )
            .await?;
    }
    let query = |key: &str| wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    };
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let object = native_transport_get(
        provider,
        query("native-body"),
        bytes.len() as u64,
        meter.clone(),
    )
    .await?;
    assert_eq!(object.body(), &bytes);
    assert!(object.body().matches_meter(&meter));
    assert_eq!(observed.writes.load(SeqCst), 0);
    assert_eq!(observed.reserved.load(SeqCst), 0);
    let clone = object.body().clone();
    let slice = object.body().slice(1..17);
    let empty = object.body().slice(0..0);
    drop(object);
    assert!(observed.memory.load(SeqCst) >= bytes.len() as u64);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    assert_eq!(slice.as_ref(), &bytes[1..17]);
    drop(clone);
    drop(slice);
    assert!(observed.memory.load(SeqCst) > 0);
    drop(empty);
    assert_eq!(observed.memory.load(SeqCst), 0);

    for (cancelled, limit, expected) in [
        (true, u64::MAX, std::io::ErrorKind::Interrupted),
        (false, 1, std::io::ErrorKind::OutOfMemory),
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        observed.cancelled.store(cancelled, SeqCst);
        observed.limit.store(limit, SeqCst);
        let calls_before = physical_get_calls();
        let error = native_transport_get(
            provider,
            query("native-body"),
            bytes.len() as u64,
            observed.clone(),
        )
        .await
        .err()
        .ok_or("native preparation accepted cancellation/quota")?;
        match error.into_cause() {
            v1::NativeObjectsCause::Admission(error) => assert_eq!(error.kind(), expected),
            other => return Err(format!("flattened native admission cause: {other}").into()),
        }
        assert_eq!(observed.memory.load(SeqCst), 0);
        assert_eq!(
            physical_get_calls(),
            calls_before,
            "preparation failure dispatched a real GET"
        );
        assert_eq!(observed.reads.load(SeqCst), 0);
        assert_eq!(observed.writes.load(SeqCst), 0);
        assert_eq!(observed.reserved.load(SeqCst), 0);
    }
    for expected in [
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::OutOfMemory,
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        if expected == std::io::ErrorKind::Interrupted {
            observed.cancel_on_copy.store(65_536, SeqCst);
        } else {
            observed.quota_on_copy.store(65_536, SeqCst);
        }
        let error = native_transport_get(
            provider,
            query("native-body"),
            bytes.len() as u64,
            observed.clone(),
        )
        .await
        .err()
        .ok_or("native transport ignored in-flight cancellation/quota")?;
        let failure = error;
        let object = failure
            .object
            .as_ref()
            .ok_or("native failure lost decoded metadata")?;
        assert_eq!(object.body().as_ref(), &bytes[..65_536]);
        assert!(object.header().object.is_some());
        assert!(
            matches!(&failure.cause, v1::NativeObjectsCause::Admission(error) if error.kind() == expected)
        );
        let prefix_alias = object.body().slice(0..1);
        drop(failure);
        assert!(observed.memory.load(SeqCst) >= bytes.len() as u64);
        drop(prefix_alias);
        assert_eq!(observed.memory.load(SeqCst), 0);
    }
    let observed = Arc::new(NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let (input, custody) = native_get_input(query("native-body"), &meter)?;
    let calls_before = physical_get_calls();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = NativeTransportInputGuard {
        _allocation: custody,
        dropped: dropped.clone(),
    };
    let input = v1::ObjectsNativeRequest::new(input, guard, &meter)?;
    let future = provider.get_native(input, bytes.len() as u64, meter)?;
    assert!(observed.memory.load(SeqCst) > 0);
    assert!(
        !dropped.load(SeqCst),
        "factory detached the original opaque input guard"
    );
    observed.cancelled.store(true, SeqCst);
    drop(future);
    assert!(
        dropped.load(SeqCst),
        "cancellation reconstructed rather than releasing the original guard"
    );
    assert_eq!(
        physical_get_calls(),
        calls_before,
        "dropping an unpolled read dispatched GET"
    );
    assert_eq!(observed.memory.load(SeqCst), 0);

    let foreign = Arc::new(NativeTransportMeter::new());
    let foreign_meter: Arc<dyn NativeReadMeter> = foreign.clone();
    let (input, custody) = native_get_input(query("native-body"), &foreign_meter)?;
    let current = Arc::new(NativeTransportMeter::new());
    let calls_before = physical_get_calls();
    let input = v1::ObjectsNativeRequest::new(input, custody, &foreign_meter)?;
    let error = provider
        .get_native(input, bytes.len() as u64, current.clone())
        .err()
        .ok_or("foreign request custody became native authority")?;
    assert!(
        matches!(error.into_cause(), v1::NativeObjectsCause::Admission(error) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    assert_eq!(current.allocations.load(SeqCst), 0);
    assert_eq!(current.memory.load(SeqCst), 0);
    assert_eq!(foreign.memory.load(SeqCst), 0);
    assert_eq!(physical_get_calls(), calls_before);

    let observed = Arc::new(NativeTransportMeter::new());
    let receipt = native_transport_put(
        provider,
        &bucket,
        "native-put",
        bytes.clone(),
        observed.clone(),
    )
    .await?;
    assert_eq!(receipt.info().size, bytes.len() as u64);
    assert_eq!(observed.reads.load(SeqCst), 0);
    assert_eq!(observed.reserved.load(SeqCst), 0);
    assert!(observed.memory.load(SeqCst) > 0);
    drop(receipt);
    assert_eq!(observed.memory.load(SeqCst), 0);
    assert_eq!(
        provider
            .get(query("native-put"), bytes.len() as u64)
            .await?
            .body(),
        &bytes
    );
    let observed = Arc::new(NativeTransportMeter::new());
    let error = native_transport_put(
        provider,
        &bucket,
        "native-bad-receipt",
        bytes.clone(),
        observed.clone(),
    )
    .await
    .err()
    .ok_or("native publication accepted a malformed acknowledgement")?;
    let failure = error;
    assert!(
        matches!(&failure.cause, v1::NativeObjectsCause::Service(error) if error.code == wire::ErrorCode::Unavailable)
    );
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    assert_eq!(
        failure
            .receipt
            .as_ref()
            .ok_or("native error detached decoded metadata")?
            .info()
            .size,
        bytes.len() as u64 + 1
    );
    assert!(observed.memory.load(SeqCst) > 0);
    drop(failure);
    assert_eq!(observed.memory.load(SeqCst), 0);
    for (key, expected) in [
        ("native-cancelled-put", std::io::ErrorKind::Interrupted),
        ("native-quota-put", std::io::ErrorKind::OutOfMemory),
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        if expected == std::io::ErrorKind::Interrupted {
            observed.cancel_on_copy.store(65_536, SeqCst);
        } else {
            observed.quota_on_copy.store(65_536, SeqCst);
        }
        let error = native_transport_put(provider, &bucket, key, bytes.clone(), observed.clone())
            .await
            .err()
            .ok_or("native PUT published after cancellation/quota")?;
        assert!(
            matches!(error.into_cause(), v1::NativeObjectsCause::Admission(error) if error.kind() == expected)
        );
        assert_eq!(
            provider
                .head(wire::HeadObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                })
                .await
                .err()
                .ok_or("cancelled native PUT became visible")?
                .code,
            wire::ErrorCode::NotFound
        );
    }

    for (key, code) in [
        ("terminal-error", wire::ErrorCode::AccessDenied),
        ("truncated", wire::ErrorCode::Unavailable),
        ("huge-header", wire::ErrorCode::QuotaExceeded),
        ("corrupt-codec", wire::ErrorCode::Unavailable),
        ("malformed", wire::ErrorCode::Unavailable),
    ] {
        let observed = Arc::new(NativeTransportMeter::new());
        let error =
            native_transport_get(provider, query(key), bytes.len() as u64, observed.clone())
                .await
                .err()
                .ok_or("native transport accepted invalid response")?;
        match error.into_cause() {
            v1::NativeObjectsCause::Service(error) => assert_eq!(error.code, code),
            other => return Err(format!("changed native service cause: {other}").into()),
        }
        assert_eq!(observed.memory.load(SeqCst), 0);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_transport_get<P: ObjectsProvider>(
    provider: &P,
    query: wire::GetObjectRequest,
    maximum: u64,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<v1::Object, v1::NativeObjectsError> {
    let (query, custody) = native_get_input(query, &meter)?;
    let request = v1::ObjectsNativeRequest::new(query, custody, &meter)?;
    provider.get_native(request, maximum, meter)?.await
}

#[cfg(not(target_arch = "wasm32"))]
fn native_get_input(
    query: wire::GetObjectRequest,
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<
    (
        wire::GetObjectRequest,
        acyclic_native_runtime::ReadAllocation,
    ),
    v1::NativeObjectsError,
> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(meter);
    let mut text = |value: &str| v1::memory::clone_native_text(value, meter, &mut allocation);
    let query = wire::GetObjectRequest {
        bucket: query
            .bucket
            .as_ref()
            .map(|bucket| text(&bucket.name).map(|name| wire::BucketRef { name }))
            .transpose()?,
        object_key: text(&query.object_key)?,
        if_match: text(&query.if_match)?,
        if_none_match: text(&query.if_none_match)?,
        range: query.range,
    };
    Ok((query, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_transport_put<P: ObjectsProvider>(
    provider: &P,
    bucket: &wire::BucketRef,
    key: &str,
    body: acyclic_stream::OwnedBytes,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<v1::ObjectsPutReceipt, v1::NativeObjectsError> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
    let header = wire::PutObjectHeader {
        bucket: Some(wire::BucketRef {
            name: v1::memory::clone_native_text(&bucket.name, &meter, &mut allocation)?,
        }),
        object_key: v1::memory::clone_native_text(key, &meter, &mut allocation)?,
        ..Default::default()
    };
    let request = v1::ObjectsNativeRequest::new((header, body), allocation, &meter)?;
    provider.put_native(request, meter)?.await
}
