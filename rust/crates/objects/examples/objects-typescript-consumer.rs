#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_objects::{wire, MemoryObjects, MemoryOptions, ObjectsProvider};
use bytes::Bytes;
use futures::executor::block_on;

fn main() {
    block_on(async {
        let provider = MemoryObjects::new(MemoryOptions::default()).expect("Rust memory provider");
        provider
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.example".into(),
                mutation: None,
            })
            .await
            .expect("create bucket");
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
            .await
            .expect("put object");
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
            .await
            .expect("get object");
        assert_eq!(value.body, body);
        println!(
            "{{\"bucket\":\"customer.example\",\"key\":\"welcome.txt\",\"size\":{},\"etag_bytes\":{},\"body\":\"hello from Rust\"}}",
            info.size,
            info.etag.len()
        );
    });
}
