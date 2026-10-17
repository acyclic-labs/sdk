use acyclic_stream::{StreamProvider as _, wire};
use prost::Message as _;

pub async fn status(provider: &acyclic_stream::MemoryStream, agent: &str, attention: bool) {
    provider
        .append(acyclic_stream::AppendRequest {
            path: acyclic_stream::StreamPath::new(format!("{agent}/status")).unwrap(),
            records: vec![
                wire::SessionLatestValue {
                    key: b"status".to_vec().into(),
                    value: b"running".to_vec().into(),
                    attention,
                }
                .encode_to_vec()
                .into(),
            ],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
}

pub async fn member(
    parent: &acyclic_harness::task_volume::TaskVolume<acyclic_stream::MemoryStream>,
    reference: &str,
) {
    use acyclic_harness::{
        TaskId,
        task_volume::{ChildState, ChildStatus, FileKey, Write},
    };
    let key = FileKey::parse(&format!("agents/{reference}/status.json")).unwrap();
    let value = ChildStatus {
        task: TaskId::from_bytes([1; 16]),
        state: ChildState::Running,
        settled: false,
        inputs: 1,
    };
    parent
        .append(
            parent.head().await.unwrap(),
            &Write::new(key, serde_json::to_vec(&value).unwrap()),
        )
        .await
        .unwrap();
}
