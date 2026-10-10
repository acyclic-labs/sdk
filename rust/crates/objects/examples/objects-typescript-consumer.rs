#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_objects::{MemoryObjects, MemoryOptions, ObjectsProvider, wire};
use bytes::Bytes;
use futures::executor::block_on;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let provider = MemoryObjects::new(MemoryOptions::default())?;
        provider
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.example".into(),
                mutation: None,
            })
            .await?;
        let body = Bytes::from_static(b"hello from Rust");
        let info = provider
            .put(
                wire::PutObjectHeader {
                    bucket: Some(wire::BucketRef {
                        name: "customer.example".into(),
                    }),
                    object_key: "welcome.txt".into(),
                    ..Default::default()
                },
                body.clone(),
            )
            .await?;
        let value = provider
            .get(
                wire::GetObjectRequest {
                    bucket: Some(wire::BucketRef {
                        name: "customer.example".into(),
                    }),
                    object_key: "welcome.txt".into(),
                    ..Default::default()
                },
                1024,
            )
            .await?;
        if value.body != body {
            return Err("Rust Objects body parity failed".into());
        }
        println!(
            "{{\"bucket\":\"customer.example\",\"key\":\"welcome.txt\",\"size\":{},\"etag_bytes\":{},\"body\":\"hello from Rust\"}}",
            info.size,
            info.etag.len()
        );
        Ok(())
    })
}
