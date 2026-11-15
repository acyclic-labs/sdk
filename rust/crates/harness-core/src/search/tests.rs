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

fn json(file: &FileRef) -> serde_json::Value {
    serde_json::to_value(file).unwrap_or_default()
}

#[tokio::test]
async fn referenced_file_search_crosses_windows_and_keeps_file_authority() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([61; 16]), 1_000_000).await?;
    let foreign = MemoryHarnessStorage::new(AgentId::from_bytes([62; 16]), 1_000_000).await?;
    // The literal straddles the first window's edge.
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
        re: None,
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
    assert!(first.matches.is_empty(), "the first window holds no hit");
    let continuation = first
        .next
        .ok_or_else(|| Error::Invalid("missing continuation".into()))?;
    let encoded =
        serde_json::to_vec(&continuation).map_err(|error| Error::Invalid(error.to_string()))?;
    let continuation: Cursor =
        serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
    let second = continuation.page_with(&provider, budget, &matcher).await?;
    assert_eq!(second.matches.len(), 1);
    assert_eq!(
        second.matches[0].source.as_ref().map(json),
        Some(json(&file))
    );
    assert_eq!(second.matches[0].path, path);
    assert_eq!(second.matches[0].record.sequence, 0);
    let replay = continuation.page_with(&provider, budget, &matcher).await?;
    assert_eq!(replay.matches.len(), 1, "a continuation is not consumed");
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
