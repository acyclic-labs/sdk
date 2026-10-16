use super::*;
use acyclic_harness::task_volume::FileQuerySource as _;

#[tokio::test]
async fn packed_query_snapshot_limits_count_selected_bytes() -> Result {
    for (lengths, maximum_bytes, accepted) in [
        ([64, 0], 64, true),
        ([65, 0], 64, false),
        ([32, 32], 64, true),
        ([32, 33], 64, false),
        ([0, 0], 1, true),
    ] {
        let (_, volume) = memory().await?;
        let mut generation = volume.snapshot(&Query::Root, 0).await?.generation;
        for (name, length) in ["a", "b"].into_iter().zip(lengths) {
            generation = volume
                .append(
                    generation,
                    &write(&format!("plans/{name}.md"), &"x".repeat(length))?,
                )
                .await?;
        }
        volume
            .append(generation, &write("GOAL.md", &"x".repeat(128))?)
            .await?;
        let result = volume
            .observe_files(Query::Plans, None, 2, maximum_bytes)
            .await;
        if accepted {
            let observation = result?;
            let files = observation.snapshot.ok_or("missing snapshot")?.files;
            assert_eq!(
                files.iter().map(|file| file.bytes.len()).sum::<usize>(),
                lengths.into_iter().sum::<usize>()
            );
        } else {
            assert!(matches!(result, Err(Error::LimitExceeded)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn packed_query_follow_checks_live_aggregate_and_replacements() -> Result {
    let (_, volume) = memory().await?;
    let mut generation = volume.snapshot(&Query::Root, 0).await?.generation;
    for name in ["a", "b"] {
        generation = volume
            .append(
                generation,
                &write(&format!("plans/{name}.md"), &"x".repeat(32))?,
            )
            .await?;
    }
    let observation = volume.observe_files(Query::Plans, None, 2, 64).await?;
    let mut changes = observation.changes;
    for (path, length) in [
        ("plans/a.md", 16),
        ("plans/b.md", 48),
        ("GOAL.md", 128),
        ("plans/a.md", 17),
    ] {
        generation = volume
            .append(generation, &write(path, &"x".repeat(length))?)
            .await?;
        let result = tokio::time::timeout(Duration::from_secs(1), changes.next())
            .await?
            .ok_or("missing change")?;
        if path == "plans/a.md" && length == 17 {
            assert!(matches!(result, Err(Error::LimitExceeded)));
            assert!(changes.next().await.is_none());
        } else {
            let change = result?;
            assert_eq!(
                change.file.as_ref().map(|file| file.key.path()),
                (path != "GOAL.md").then(|| path.to_owned())
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn packed_query_reconnect_uses_lengths_at_each_cursor_cut() -> Result {
    let (_, volume) = memory().await?;
    let first = volume.snapshot(&Query::Plans, 2).await?;
    let mut cuts = vec![(first.cursor, 0)];
    let mut generation = first.generation;
    for length in [128, 8, 16, 17, 8] {
        generation = volume
            .append(generation, &write("plans/a.md", &"x".repeat(length))?)
            .await?;
        cuts.push((volume.snapshot(&Query::Plans, 2).await?.cursor, length));
    }
    for (index, (cursor, length)) in cuts.iter().enumerate() {
        let result = volume
            .observe_files(Query::Plans, Some(cursor.clone()), 2, 16)
            .await;
        if *length > 16 {
            assert!(matches!(result, Err(Error::LimitExceeded)));
            continue;
        }
        let observation = result?;
        assert!(observation.snapshot.is_none());
        let mut changes = observation.changes;
        for (_, next_length) in &cuts[index + 1..] {
            let result = tokio::time::timeout(Duration::from_secs(1), changes.next())
                .await?
                .ok_or("missing resumed change")?;
            if *next_length > 16 {
                assert!(matches!(result, Err(Error::LimitExceeded)));
                break;
            }
            assert_eq!(
                result?.file.ok_or("missing plan")?.bytes.len(),
                *next_length
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn packed_query_follow_enforces_file_count_as_well_as_bytes() -> Result {
    let (_, volume) = memory().await?;
    let snapshot = volume.snapshot(&Query::Plans, 1).await?;
    let observation = volume.observe_files(Query::Plans, None, 1, 64).await?;
    let mut changes = observation.changes;
    let generation = volume
        .append(snapshot.generation, &write("plans/a.md", "a")?)
        .await?;
    assert!(changes.next().await.ok_or("missing first plan")?.is_ok());
    volume
        .append(generation, &write("plans/b.md", "b")?)
        .await?;
    assert!(matches!(
        changes.next().await,
        Some(Err(Error::LimitExceeded))
    ));
    Ok(())
}
