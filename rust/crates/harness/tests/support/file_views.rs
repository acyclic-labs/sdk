//! Test publications for the production ClientViews reducer; no legacy event adapter.
use super::harness;
use super::harness::{
    client::ClientViews,
    task_volume::{Change, Channel, Component, FileKey, FileValue, TaskFile},
};

pub use harness::test_support::empty_client_views as empty;
pub fn put(views: &mut ClientViews, key: FileKey, bytes: Vec<u8>, channel: Option<Channel>) {
    let mut file = FileValue::new(key, bytes);
    file.channel = channel;
    let mut cursor = views.selections().next().unwrap().1.cursor().clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views
        .query_change(Change {
            cursor,
            file: Some(file),
            files: vec![],
            removed: vec![],
        })
        .unwrap();
}
pub fn text(views: &mut ClientViews, task: &str, sequence: u64, reasoning: bool, bytes: &[u8]) {
    put(
        views,
        FileKey::Task {
            task: Component::new(task).unwrap(),
            file: TaskFile::Chunk {
                execution: harness::OperationId::from_bytes([0; 16]),
                turn: 0,
                sequence,
            },
        },
        bytes.to_vec(),
        Some(if reasoning {
            Channel::Reasoning
        } else {
            Channel::Text
        }),
    );
}
pub fn document(views: &mut ClientViews, key: FileKey, text: &str) {
    put(views, key, text.as_bytes().to_vec(), None);
}
pub fn status(views: &mut ClientViews, task: &str, state: &str, reason: Option<&str>) {
    put(
        views,
        FileKey::parse(&format!(".tasks/{task}/status.json")).unwrap(),
        serde_json::to_vec(&serde_json::json!({"state":state,"reason":reason})).unwrap(),
        None,
    );
}
pub fn remove(views: &mut ClientViews, key: FileKey) {
    let view = views.selections().next().unwrap().1;
    if !view.files().contains_key(&key) {
        return;
    }
    let mut cursor = view.cursor().clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views
        .query_change(Change {
            cursor,
            file: None,
            files: vec![],
            removed: vec![key],
        })
        .unwrap();
}

pub fn append_text(views: &mut ClientViews, task: &str, reasoning: bool, delta: &str) {
    let sequence = views
        .selections()
        .flat_map(|(_, selection)| selection.files().keys())
        .filter_map(|key| match key {
            FileKey::Task {
                task: owner,
                file: TaskFile::Chunk { sequence, .. },
            } if owner.as_str() == task => Some(*sequence),
            _ => None,
        })
        .max()
        .map_or(0, |sequence| sequence + 1);
    text(views, task, sequence, reasoning, delta.as_bytes());
}
pub fn tool(views: &mut ClientViews, task: &str, name: &str, output: &str) {
    let base = format!(".tasks/{task}/turns/0/tools/call");
    put(views,FileKey::parse(&format!("{base}/request.json")).unwrap(),serde_json::to_vec(&serde_json::json!({
        "operation_id":"00000000-0000-0000-0000-000000000001","call_id":"call","name":name,"arguments":{}
    })).unwrap(),None);
    put(
        views,
        FileKey::parse(&format!("{base}/stdout/0")).unwrap(),
        output.as_bytes().to_vec(),
        None,
    );
}
