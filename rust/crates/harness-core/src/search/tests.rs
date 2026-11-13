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

struct LinkedFixture {
    storage: MemoryHarnessStorage,
    foreign: MemoryHarnessStorage,
    provider: MemoryStream,
    path: StreamPath,
    file: crate::conversation::FileRef,
}

async fn linked_fixture() -> Result<LinkedFixture> {
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
    Ok(LinkedFixture {
        storage,
        foreign,
        provider,
        path,
        file,
    })
}

#[tokio::test]
async fn referenced_message_search_pins_chunks_and_keeps_file_authority() -> Result<()> {
    let LinkedFixture {
        storage,
        foreign,
        provider,
        path,
        file,
    } = linked_fixture().await?;
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
    assert_eq!(second.matches[0].source.as_deref(), Some(&file));
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

#[tokio::test]
async fn native_linked_search_keeps_file_custody_and_current_authority()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let fixture = linked_fixture().await?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let matcher = ContentMatcher::with_native_work(fixture.storage.content_reader(), None, &work)?;
    let budget = NonZeroU32::new(2).ok_or("invalid test chunk")?;
    let cursor = Cursor::new(fixture.path.clone(), b"accepted specimen".to_vec());
    let first = cursor
        .native_page_with(&fixture.provider, budget, &matcher, work.runtime_meter())
        .await??;
    assert!(first.value().matches.is_empty());
    let continuation = first
        .value()
        .next
        .as_ref()
        .ok_or("missing original range continuation")?;
    let denied = ContentMatcher::with_native_work(fixture.foreign.content_reader(), None, &work)?;
    let denied_page = continuation
        .native_page_with(&fixture.provider, budget, &denied, work.runtime_meter())
        .await??;
    assert!(denied_page.value().matches.is_empty());
    drop(denied_page);
    let mut second = continuation
        .native_page_with(&fixture.provider, budget, &matcher, work.runtime_meter())
        .await??;
    assert_eq!(second.value().matches.len(), 1);
    let found = second
        .value_mut()
        .matches
        .pop()
        .ok_or("missing native match")?;
    assert_eq!(found.path, fixture.path);
    assert_eq!(found.record.sequence, 0);
    let file = found.source.ok_or("missing original file")?;
    assert_eq!(&*file, &fixture.file);
    drop(second);
    drop(first);
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let bytes = fixture
        .storage
        .content_reader()
        .read_range(
            &file,
            acyclic_fs::ByteRange {
                offset: 64 * 1024 - 3,
                length: 17,
            },
            None,
            Some(&io),
        )?
        .await?;
    assert_eq!(bytes.as_slice(), b"accepted specimen");
    drop(bytes);
    let retained = work.live_allocation_bytes();
    drop(file);
    assert!(
        work.live_allocation_bytes() < retained,
        "original file custody did not retire"
    );
    Ok(())
}
