use super::{Error, MemoryObjects, MemoryOptions, ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;

pub(super) fn bucket() -> Option<wire::BucketRef> {
    Some(wire::BucketRef {
        name: "customer.inputs".into(),
    })
}

pub(super) fn identity(key: &str) -> Option<wire::MutationIdentity> {
    Some(wire::MutationIdentity {
        idempotency_key: key.into(),
    })
}

fn put(key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: bucket(),
        object_key: key.into(),
        ..Default::default()
    }
}

async fn provider(options: MemoryOptions) -> Result<MemoryObjects, Error> {
    let provider = MemoryObjects::new(options)?;
    provider
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    Ok(provider)
}

pub(super) fn get(bucket: Option<wire::BucketRef>, key: &str) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket,
        object_key: key.into(),
        ..Default::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod meter;
mod model;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
mod transport;

#[cfg(not(target_arch = "wasm32"))]
pub(super) use meter::NativeTransportMeter;
#[cfg(not(target_arch = "wasm32"))]
pub(super) use native::exercise_native_transport;
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(super) use transport::{exercise_streaming, exercise_uploads};
