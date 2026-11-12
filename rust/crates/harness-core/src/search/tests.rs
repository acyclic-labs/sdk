use super::*;
use crate::{
    AgentId, OperationId,
    agent::{AgentRecord as _, InboxRecord, IntentId, Message, Sender},
    filesystem::MemoryHarnessStorage,
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProviderExt as _, search::Cursor,
};
use std::num::NonZeroU32;

#[tokio::test]
async fn file_search_pages_original_ranges_at_one_cut_and_rechecks_read_authority() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([63; 16]), 1_000_000).await?;
    let foreign = MemoryHarnessStorage::new(AgentId::from_bytes([64; 16]), 1_000_000).await?;
    let mut bytes = vec![b'x'; 64 * 1024 - 3];
    bytes.extend_from_slice(b"accepted specimen");
    let original = storage
        .stage(
            OperationId::new(),
            "decisions/body.txt",
            &bytes,
            "text/plain",
            "body",
        )
        .await?;
    let root = crate::context::ContextRoot {
        volume: storage.volume().clone(),
        directory: "decisions".into(),
    };
    let cursor = FileCursor::new(
        root.clone(),
        "decisions".into(),
        FileRootKind::Directory,
        b"accepted specimen".to_vec(),
    )?;
    let budget = NonZeroU32::new(2).ok_or_else(|| Error::Invalid("invalid test chunk".into()))?;
    let first = cursor.page(storage.content_reader(), None, budget).await?;
    assert_eq!(first.work, 2);
    assert!(first.matches.is_empty());
    let next = first
        .next
        .ok_or_else(|| Error::Invalid("missing continuation".into()))?;
    let next: FileCursor = serde_json::from_slice(
        &serde_json::to_vec(&next).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    storage
        .stage(
            OperationId::new(),
            "decisions/body.txt",
            b"replacement",
            "text/plain",
            "body",
        )
        .await?;
    storage
        .stage(
            OperationId::new(),
            "decisions/later.txt",
            b"accepted specimen",
            "text/plain",
            "later",
        )
        .await?;
    let result = next.page(storage.content_reader(), None, budget).await?;
    assert!(result.next.is_none());
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].offset, 64 * 1024 - 3);
    assert!(result.matches[0].file.matches_content(&original));
    let replay = next.page(storage.content_reader(), None, budget).await?;
    assert_eq!(result.matches, replay.matches);
    assert!(
        next.page(foreign.content_reader(), None, budget)
            .await?
            .matches
            .is_empty()
    );
    assert!(FileCursor::new(root, "other".into(), FileRootKind::Directory, Vec::new()).is_err());
    Ok(())
}

#[tokio::test]
async fn referenced_message_search_pins_chunks_and_keeps_file_authority() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([61; 16]), 1_000_000).await?;
    let foreign = MemoryHarnessStorage::new(AgentId::from_bytes([62; 16]), 1_000_000).await?;
    let mut bytes = vec![b'x'; 64 * 1024 - 3];
    bytes.extend_from_slice(b"accepted specimen");
    let file = storage
        .stage(
            OperationId::new(),
            "decisions/body.txt",
            &bytes,
            "text/plain",
            "body",
        )
        .await?;
    let record = InboxRecord::Message(Message {
        from: Sender::User {
            principal: "reviewer".into(),
        },
        id: IntentId::try_from("decision".to_owned())
            .map_err(|error| Error::Invalid(error.to_string()))?,
        body: Box::new(file.clone()),
        attachments: None,
    });
    let provider = MemoryStream::default();
    let path = StreamPath::new("session/decisions")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![
                record
                    .encode()
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            ],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let matcher = ContentMatcher::new(storage.content_reader(), None);
    let budget = NonZeroU32::new(2).ok_or_else(|| Error::Invalid("invalid test chunk".into()))?;
    let first = Cursor::new(path.clone(), b"accepted specimen".to_vec())
        .page_with(&provider, budget, &matcher)
        .await?;
    assert!(
        first.matches.is_empty(),
        "the hit crosses the original range boundary"
    );
    let continuation = first
        .next
        .ok_or_else(|| Error::Invalid("missing continuation".into()))?;
    let encoded =
        serde_json::to_vec(&continuation).map_err(|error| Error::Invalid(error.to_string()))?;
    let continuation: Cursor =
        serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
    let second = continuation.page_with(&provider, budget, &matcher).await?;
    assert_eq!(second.matches.len(), 1);
    assert_eq!(second.matches[0].source.as_ref(), Some(&file));
    assert_eq!(second.matches[0].path, path);
    assert_eq!(second.matches[0].record.sequence, 0);
    let denied = continuation
        .page_with(
            &provider,
            budget,
            &ContentMatcher::new(foreign.content_reader(), None),
        )
        .await?;
    assert!(
        denied.matches.is_empty(),
        "a token cannot grant access to its referenced file"
    );
    Ok(())
}
