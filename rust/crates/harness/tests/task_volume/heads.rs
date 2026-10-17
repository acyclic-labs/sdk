use super::*;

fn chunk(sequence: u64, channel: Channel, bytes: Vec<u8>) -> Result<Write> {
    let mut file = Write::new(
        FileKey::parse(&format!(".tasks/t/turns/0/chunks/{sequence}"))?,
        bytes,
    );
    file.channel = Some(channel);
    Ok(file)
}

#[tokio::test]
async fn status_and_text_heads_are_pinned_at_their_cursor() -> Result {
    let (_, volume) = memory().await?;
    let query = Query::Task(Component::new("t")?);
    let initial = volume.snapshot(&query, 0).await?;
    let first = [
        write(".tasks/t/status.json", "running")?,
        chunk(0, Channel::Text, b"one".to_vec())?,
    ];
    let cut = append_all(&volume, initial.generation, &first).await?;
    let snapshot = volume.snapshot(&query, 10).await?;
    let cursor = snapshot.cursor.clone();
    let original_pointer = snapshot
        .files
        .iter()
        .find(|f| f.channel.is_some())
        .ok_or("missing chunk")?
        .bytes
        .as_ptr();
    append_all(
        &volume,
        cut,
        &[
            chunk(1, Channel::Reasoning, b"private".to_vec())?,
            chunk(2, Channel::Text, b"two".to_vec())?,
            write(".tasks/t/status.json", "done")?,
        ],
    )
    .await?;
    let pinned = volume.heads(&query, Some(cursor.clone()), 10).await?;
    let text = pinned
        .files
        .iter()
        .find(|f| f.key.path().ends_with("text.md"))
        .ok_or("missing text head")?;
    assert_eq!(text.length, 3);
    assert_eq!(text.through_sequence, Some(0));
    assert_eq!(
        text.segments.first().ok_or("empty text head")?.as_ptr(),
        original_pointer
    );
    let status = pinned
        .files
        .iter()
        .find(|f| f.key.path().ends_with("status.json"))
        .ok_or("missing status head")?;
    assert_eq!(status.segments.concat(), b"running");
    let mut follow = volume.follow(query.clone(), cursor).await?;
    assert_eq!(
        follow.next().await.ok_or("missing reasoning")??.file,
        Some(chunk(1, Channel::Reasoning, b"private".to_vec())?)
    );
    assert_eq!(
        follow.next().await.ok_or("missing text suffix")??.file,
        Some(chunk(2, Channel::Text, b"two".to_vec())?)
    );
    assert_eq!(
        follow.next().await.ok_or("missing terminal status")??.file,
        Some(write(".tasks/t/status.json", "done")?)
    );
    let text_query = Query::folder(".tasks/t/turns/0/text.md")?;
    let heads = volume.heads(&text_query, None, 10).await?;
    assert_eq!(heads.files.len(), 1);
    assert_eq!(
        heads
            .files
            .first()
            .ok_or("missing current head")?
            .segments
            .concat(),
        b"onetwo"
    );
    assert!(matches!(
        append_all(
            &volume,
            heads.generation,
            &[write(".tasks/t/turns/0/text.md", "forged")?]
        )
        .await,
        Err(Error::InvalidSegment)
    ));
    Ok(())
}

#[tokio::test]
async fn every_delta_is_followable_before_a_subsequent_delta_is_supplied() -> Result {
    let (_, volume) = memory().await?;
    let snapshot = volume.snapshot(&Query::Root, 0).await?;
    let mut follow = volume.follow(Query::Root, snapshot.cursor).await?;
    let file = chunk(0, Channel::Text, b"first".to_vec())?;
    append_all(&volume, snapshot.generation, std::slice::from_ref(&file)).await?;
    let observed = tokio::time::timeout(Duration::from_secs(1), follow.next())
        .await?
        .ok_or("follow ended")??;
    assert_eq!(observed.file, Some(file));
    assert_eq!(observed.cursor.accepted, observed.cursor.position);
    assert_eq!(observed.cursor.durable, 0); // MemoryStream makes no crash-persistence claim.
    let mut cursor = observed.cursor;
    cursor.advance_watermarks(acyclic_stream::StreamWatermarks {
        accepted: cursor.position,
        durable: cursor.position,
    })?;
    assert_eq!(cursor.durable, cursor.position);
    assert!(
        cursor
            .advance_watermarks(acyclic_stream::StreamWatermarks {
                accepted: cursor.position,
                durable: 0
            })
            .is_err()
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn head_plus_follow_equals_follow_from_start(tokens in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..12), 1..12)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let outcome: Result = runtime.block_on(async {
            let (_, volume) = memory().await?;
            let query = Query::Chunks { task: Component::new("t")?, turn: 0 };
            let initial = volume.snapshot(&query, 0).await?;
            let mut generation = initial.generation;
            let mut cursors = vec![initial.cursor];
            for (n, bytes) in tokens.iter().enumerate() {
                generation = append_all(&volume,generation, &[chunk(n as u64, Channel::Text, bytes.clone())?]).await?;
                cursors.push(volume.snapshot(&query, 100).await?.cursor);
            }
            let complete: Vec<u8> = tokens.concat();
            for (cut, cursor) in cursors.into_iter().enumerate() {
                let head = volume.heads(&query, Some(cursor.clone()), 100).await?;
                let mut rendered = head.files.into_iter().flat_map(|f| f.segments.concat()).collect::<Vec<_>>();
                assert_eq!(rendered, tokens.iter().take(cut).flatten().copied().collect::<Vec<_>>());
                let mut follow = volume.follow(query.clone(), cursor).await?;
                for _ in cut..tokens.len() {
                    let event = tokio::time::timeout(Duration::from_secs(1), follow.next()).await?.ok_or("follow ended")??;
                    rendered.extend_from_slice(&event.file.ok_or("unexpected checkpoint")?.bytes);
                }
                assert_eq!(rendered, complete);
            }
            Ok(())
        });
        proptest::prop_assert!(outcome.is_ok(), "{outcome:?}");
    }
}
