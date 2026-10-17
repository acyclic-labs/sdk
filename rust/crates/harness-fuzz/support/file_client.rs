//! Actual volume recovery and checkpoint parsing, shared with shrinking tests.
use acyclic_harness::{
    client::{FileClient, FileClientDurability, FileClientLimits},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

pub fn exercise(bytes: &[u8]) {
    // Exercise the public configuration codec without mirroring its schema.
    // Generated success is unconditional, so an always-rejecting decoder fails.
    for durability in [
        FileClientDurability::Accepted,
        FileClientDurability::Durable,
    ] {
        let limits = FileClientLimits {
            commands: bytes.len(),
            bytes: bytes.len() as u64,
            durability,
        };
        let encoded = serde_json::to_vec(&limits).unwrap();
        assert_eq!(
            serde_json::from_slice::<FileClientLimits>(&encoded).unwrap(),
            limits
        );
    }
    if let Ok(limits) = serde_json::from_slice::<FileClientLimits>(bytes) {
        assert_eq!(
            serde_json::from_slice::<FileClientLimits>(&serde_json::to_vec(&limits).unwrap())
                .unwrap(),
            limits,
        );
    }
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let outbox = Arc::new(
            TaskVolume::open(provider.clone(), StreamPath::new("client/files").unwrap())
                .await
                .unwrap(),
        );
        let target = Arc::new(
            TaskVolume::open(provider, StreamPath::new("task/files").unwrap())
                .await
                .unwrap(),
        );
        let limits = FileClientLimits {
            commands: 2,
            bytes: 32,
            durability: FileClientDurability::Accepted,
        };
        let key = FileKey::parse(".tasks/task/requests/op.json").unwrap();
        let payload = &bytes[..bytes.len().min(32)];
        let mut client = FileClient::open(outbox.clone(), target.clone(), limits)
            .await
            .unwrap();
        client
            .enqueue(Write::new(key.clone(), payload.to_vec()))
            .await
            .unwrap();
        assert_eq!(client.pending(), 1);
        drop(client);
        let mut client = FileClient::open(outbox.clone(), target.clone(), limits)
            .await
            .unwrap();
        assert_eq!(client.pending(), 1);
        assert_eq!(client.flush().await.unwrap(), 1);
        drop(client);
        let state_key = FileKey::parse("workspace/client/outbox.json").unwrap();
        let state = outbox
            .snapshot(&Query::File(state_key.clone()), 1)
            .await
            .unwrap();
        let original = state.files[0].bytes.clone();
        let mut conflict: serde_json::Value = serde_json::from_slice(&original).unwrap();
        let old = conflict["target"][0].as_u64().unwrap();
        conflict["target"][0] = serde_json::json!(old ^ 1);
        outbox
            .append(
                state.generation,
                &Write::new(state_key.clone(), serde_json::to_vec(&conflict).unwrap()),
            )
            .await
            .unwrap();
        assert!(
            FileClient::open(outbox.clone(), target.clone(), limits)
                .await
                .is_err(),
            "target rebinding must fail"
        );
        let head = outbox.head().await.unwrap();
        outbox
            .append(head, &Write::new(state_key.clone(), bytes.to_vec()))
            .await
            .unwrap();
        if let Ok(client) = FileClient::open(outbox.clone(), target.clone(), limits).await {
            assert!(client.pending() <= limits.commands);
        }
        let head = outbox.head().await.unwrap();
        outbox
            .append(head, &Write::new(state_key, original))
            .await
            .unwrap();
        let client = FileClient::open(outbox, target.clone(), limits)
            .await
            .unwrap();
        assert_eq!(client.pending(), 0);
        let admitted = target.snapshot(&Query::File(key.clone()), 1).await.unwrap();
        assert_eq!(admitted.files.len(), 1);
        assert_eq!(admitted.files[0].key, key);
        assert_eq!(admitted.files[0].bytes.as_ref(), payload);
    });
}
