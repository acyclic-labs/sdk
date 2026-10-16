use super::super::model::check;
use super::*;
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};

fn activity_race(broken: bool) {
    check(move || {
        // Atomic volume publication linearizes before the cursor cut, between
        // that cut and its CAS, or after the CAS. Explore all three classes;
        // the volume model covers the provider's internal lock schedules.
        for order in 0..3 {
            let volume = Arc::new(block_on(files::TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("activity-model")?,
            ))?);
            let mut client = block_on(CommandSession::open(
                volume.clone(),
                uuid::Uuid::from_bytes([1; 16]),
                32_768,
                64,
            ))?;
            client.activity_cursor = Some((1, "saved cursor".into()));
            let publish = || {
                let cut =
                    block_on(volume.snapshot(&files::Query::File(ui("config", "other.json")?), 1))?
                        .generation;
                block_on(volume.append(
                    cut,
                    &files::FileValue::new(ui("config", "other.json")?, b"{}".to_vec()),
                ))?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            };
            if order < 2 {
                publish()?;
            }
            if order == 0 {
                block_on(client.refresh_files())?;
            }
            let result = if broken {
                block_on(client.publish(
                    client.state_key(CommandFileRoot::Session, "activity.json")?,
                    serde_json::to_vec(client.activity_cursor.as_ref().unwrap())?,
                    false,
                ))
            } else {
                block_on(client.persist_activity())
            };
            assert!(result.is_ok(), "completed effect reported failure");
            if order == 2 {
                publish()?;
            }
            let snapshot = block_on(volume.snapshot(
                &files::Query::File(client.state_key(CommandFileRoot::Session, "activity.json")?),
                1,
            ))?;
            let saved: (u64, String) = serde_json::from_slice(&snapshot.files[0].bytes)?;
            assert_eq!(saved, client.activity_cursor.unwrap());
        }
        Ok(())
    });
}

#[test]
fn completed_effect_cursor_survives_competing_publications() {
    activity_race(false);
}

#[test]
#[should_panic(expected = "completed effect reported failure")]
fn negative_control_detects_returning_a_cursor_conflict() {
    activity_race(true);
}

fn catalog_conflict(broken: bool) {
    check(move || {
        let volume = Arc::new(block_on(files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("catalog-conflict-model")?,
        ))?);
        let session = uuid::Uuid::from_bytes([1; 16]);
        let mut editor = block_on(CommandSession::open(volume.clone(), session, 32_768, 64))?;
        let mut winner = block_on(CommandSession::open(volume.clone(), session, 32_768, 64))?;
        let mut winning: Value = serde_json::from_slice(DEFAULT_COMMANDS_JSON)?;
        let mut stale = winning.clone();
        winning[0]["description"] = json!("winning edit");
        stale[0]["description"] = json!("stale edit");
        block_on(winner.install_catalog(serde_json::to_vec(&winning)?))?;
        let result = if broken {
            // Original defect: silently advance a stale editor's base before
            // publishing its already computed bytes.
            let key = ui("config", "commands.json")?;
            block_on(editor.read_file(&key))?;
            block_on(editor.publish(key, serde_json::to_vec(&stale)?, false))
        } else {
            block_on(editor.install_catalog(serde_json::to_vec(&stale)?))
        };
        assert!(
            matches!(result, Err(Error::Conflict(_))),
            "catalog edit overwritten"
        );
        let saved =
            block_on(volume.snapshot(&files::Query::File(ui("config", "commands.json")?), 1))?;
        assert_eq!(
            serde_json::from_slice::<Value>(&saved.files[0].bytes)?,
            winning
        );
        Ok(())
    });
}
#[test]
fn catalog_edit_conflict_preserves_the_winning_publication() {
    catalog_conflict(false);
}
#[test]
#[should_panic(expected = "catalog edit overwritten")]
fn negative_control_detects_refreshing_away_a_catalog_conflict() {
    catalog_conflict(true);
}
