use acyclic_stream::{StreamProvider as _, wire};

fn publication(
    agent: &str,
    attention: bool,
) -> Result<acyclic_stream::AppendRequest, Box<dyn std::error::Error>> {
    let (root, _) = agent.rsplit_once('/').ok_or("status agent has no parent")?;
    Ok(acyclic_harness::client::status_publication(
        &acyclic_stream::StreamPath::new(root)?,
        &acyclic_stream::StreamPath::new(agent)?,
        wire::SessionLatestValue {
            key: b"status".to_vec().into(),
            value: b"running".to_vec().into(),
            attention,
        },
    )?)
}

pub async fn status(provider: &acyclic_stream::MemoryStream, agent: &str, attention: bool) {
    provider
        .append(publication(agent, attention).unwrap())
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
