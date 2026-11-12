use super::*;
use crate::agent::AgentPath;
use acyclic_stream::{AppendRecord, ForkRequest, search::Match, testing::Counted};

fn message(from: Sender, id: &str, body: &FileRef) -> Result<Message> {
    Ok(Message {
        from,
        id: IntentId::try_from(id.to_owned()).map_err(|error| Error::Invalid(error.to_string()))?,
        re: None,
        body: Box::new(body.clone()),
        attachments: None,
    })
}

async fn append(provider: &MemoryStream, path: &str, records: Vec<InboxRecord>) -> Result<()> {
    let records: Vec<AppendRecord> = records
        .into_iter()
        .map(|record| {
            record
                .encode()
                .map(Into::into)
                .map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<_>>()?;
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records,
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

async fn collect(
    provider: &Counted<MemoryStream>,
    mut cursor: Cursor,
    matcher: &ContentMatcher<'_>,
) -> Result<Vec<Match<FileRef>>> {
    let budget = NonZeroU32::new(2).ok_or_else(|| Error::Invalid("invalid test chunk".into()))?;
    let mut matches = Vec::new();
    loop {
        let page = cursor.page_with(provider, budget, matcher).await?;
        assert!(page.work <= budget.get());
        matches.extend(page.matches);
        match page.next {
            Some(next) => cursor = next,
            None => return Ok(matches),
        }
    }
}

#[tokio::test]
async fn prompt_history_projects_original_user_bodies_at_one_authorized_cut() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([73; 16]), 1_000_000).await?;
    let foreign = MemoryHarnessStorage::new(AgentId::from_bytes([74; 16]), 1_000_000).await?;
    let first = storage
        .stage(
            OperationId::new(),
            "first.txt",
            b"PROMPT_VALUE first",
            "text/plain",
            "first",
        )
        .await?;
    let second = storage
        .stage(
            OperationId::new(),
            "second.txt",
            b"PROMPT_VALUE second",
            "text/plain",
            "second",
        )
        .await?;
    let empty = storage
        .stage(OperationId::new(), "empty.txt", b"", "text/plain", "empty")
        .await?;
    let attachment = storage
        .stage(
            OperationId::new(),
            "attachment.txt",
            b"ATTACHMENT_ONLY",
            "text/plain",
            "attachment",
        )
        .await?;
    let denied = foreign
        .stage(
            OperationId::new(),
            "denied.txt",
            b"PROMPT_VALUE private",
            "text/plain",
            "denied",
        )
        .await?;
    let user = || Sender::User {
        principal: "METADATA_ONLY".into(),
    };
    let mut original = message(user(), "original", &first)?;
    original.attachments = Some(Box::new(attachment));
    let provider = MemoryStream::default();
    append(
        &provider,
        "sessions/one/inbox",
        vec![
            InboxRecord::Message(original),
            InboxRecord::Message(message(
                Sender::Host {
                    host: "editor".into(),
                },
                "host",
                &first,
            )?),
            InboxRecord::Message(message(
                Sender::Agent {
                    path: AgentPath::root(&AgentId::from_bytes([75; 16])),
                },
                "agent",
                &first,
            )?),
            InboxRecord::Message(message(user(), "denied", &denied)?),
        ],
    )
    .await?;
    provider
        .fork(ForkRequest {
            source: StreamPath::new("sessions/one/inbox")?,
            destination: StreamPath::new("sessions/fork/inbox")?,
            at_tail: None,
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    // A new submission with identical text is still a distinct original prompt.
    append(
        &provider,
        "sessions/fork/inbox",
        vec![InboxRecord::Message(message(user(), "repeated", &first)?)],
    )
    .await?;
    append(
        &provider,
        "sessions/two/inbox",
        vec![
            InboxRecord::FollowUp(message(user(), "follow-up", &second)?),
            InboxRecord::Message(message(user(), "empty", &empty)?),
        ],
    )
    .await?;
    let counted = Counted::new(provider.clone());
    let matcher = ContentMatcher::user_prompts(storage.content_reader(), None);
    let root = StreamPath::new("sessions")?;
    let cursor = Cursor::new(root.clone(), b"PROMPT_VALUE".to_vec());
    assert_eq!(counted.requests().data(), 0);
    assert_eq!(counted.requests().control(), 0);
    let first_page = cursor
        .page_with(&counted, NonZeroU32::new(2).unwrap(), &matcher)
        .await?;
    assert!(first_page.matches.is_empty());
    assert_eq!(first_page.work, 2);
    let next = first_page
        .next
        .ok_or_else(|| Error::Invalid("missing prompt continuation".into()))?;
    let next: Cursor = serde_json::from_slice(
        &serde_json::to_vec(&next).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        next.page_with(
            &counted,
            NonZeroU32::new(2).unwrap(),
            &ContentMatcher::new(storage.content_reader(), None)
        )
        .await
        .is_err()
    );
    append(
        &provider,
        "sessions/one/inbox",
        vec![InboxRecord::Message(message(user(), "later", &second)?)],
    )
    .await?;
    append(
        &provider,
        "sessions/later/inbox",
        vec![InboxRecord::Message(message(
            user(),
            "new-session",
            &second,
        )?)],
    )
    .await?;
    storage
        .stage(
            OperationId::new(),
            "first.txt",
            b"replacement",
            "text/plain",
            "first",
        )
        .await?;
    let matches = collect(&counted, next.clone(), &matcher).await?;
    assert_eq!(matches.len(), 3);
    assert!(matches.iter().all(|hit| hit.path == hit.record.origin));
    assert_eq!(
        matches
            .iter()
            .filter(|hit| hit.source.as_ref() == Some(&first))
            .count(),
        2
    );
    assert_eq!(
        matches
            .iter()
            .filter(|hit| hit.source.as_ref() == Some(&second))
            .count(),
        1
    );
    assert_eq!(collect(&counted, next.clone(), &matcher).await?, matches);
    let restricted = collect(
        &counted,
        next,
        &ContentMatcher::user_prompts(foreign.content_reader(), None),
    )
    .await?;
    assert_eq!(restricted.len(), 1);
    assert_eq!(restricted[0].source.as_ref(), Some(&denied));
    // Metadata and attachments are not prompt text. Empty bodies remain valid
    // original submissions when enumerating references without a literal.
    for pattern in [b"METADATA_ONLY".as_slice(), b"ATTACHMENT_ONLY"] {
        assert!(
            collect(
                &counted,
                Cursor::new(root.clone(), pattern.to_vec()),
                &matcher
            )
            .await?
            .is_empty()
        );
    }
    let all = collect(&counted, Cursor::new(root, Vec::new()), &matcher).await?;
    assert_eq!(
        all.iter()
            .filter(|hit| hit.source.as_ref() == Some(&empty))
            .count(),
        1
    );
    assert!(all.iter().all(|hit| hit.source.is_some()));
    Ok(())
}
