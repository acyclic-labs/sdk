use super::*;
use crate::{
    client::CommandResolution,
    task_volume::{Component, FileValue, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};

#[test]
fn idle_pending_prompt_projection_allocates_nothing() {
    let (files, owner, prompt) = futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("prompt-idle").unwrap(),
        )
        .await
        .unwrap();
        let session = Component::new(uuid::Uuid::from_u128(9).to_string()).unwrap();
        let key = FileKey::Session {
            session: session.clone(),
            file: TaskFile::Request(
                Component::new(crate::OperationId::from_bytes([1; 16]).to_string()).unwrap(),
            ),
        };
        let prompt = format!("same{}", "\u{301}".repeat(40));
        let file = FileValue::new(
            key,
            serde_json::to_vec(&CommandResolution::Prompt {
                text: prompt.clone(),
                source: None,
                mentions: Vec::new(),
            })
            .unwrap(),
        );
        volume
            .append(volume.head().await.unwrap(), &file)
            .await
            .unwrap();
        let query = Query::CommandSession {
            session: session.clone(),
            recent: 4,
        };
        let snapshot = volume.snapshot(&query, 16).await.unwrap();
        (
            FileQueryView::from_snapshot(query, snapshot, 16, 8192).unwrap(),
            OutputOwner::Session(session),
            prompt,
        )
    });
    let mut archive = Archive::default();
    archive.update(Some((&files, owner.clone())));
    assert_eq!(
        archive
            .rows()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [format!("You: {prompt}")]
    );
    for count in [32, 64] {
        let mut changed = usize::MAX;
        let allocation = acyclic_test_support::measure(|| {
            for _ in 0..count {
                changed = changed.min(archive.update(Some((&files, owner.clone()))));
            }
        });
        assert_eq!(
            (allocation.count_total, allocation.bytes_total),
            (0, 0),
            "unchanged prompt projection reuses its rows"
        );
        assert_eq!(changed, archive.rows().len());
    }
    assert_eq!(
        archive
            .rows()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [format!("You: {prompt}")]
    );
}
