use super::super::model::check;
use super::*;
use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};

fn activity_race(broken: bool) {
    check(move || {
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
        let cut = client.generation;
        let writer = loom::thread::spawn({
            let volume = volume.clone();
            move || {
                block_on(volume.append(
                    cut,
                    &files::FileValue::new(ui("config", "other.json").unwrap(), b"{}".to_vec()),
                ))
            }
        });
        let result = if broken {
            // Broken: a cursor CAS conflict replaces an already completed effect.
            block_on(client.publish(
                client.state_key(CommandFileRoot::Session, "activity.json")?,
                serde_json::to_vec(client.activity_cursor.as_ref().unwrap())?,
                false,
            ))
        } else {
            block_on(client.persist_activity())
        };
        let _publication = writer.join().map_err(|_| "publisher panicked")?;
        assert!(result.is_ok(), "completed effect reported failure");
        let snapshot = block_on(volume.snapshot(
            &files::Query::File(client.state_key(CommandFileRoot::Session, "activity.json")?),
            1,
        ))?;
        let saved: (u64, String) = serde_json::from_slice(&snapshot.files[0].bytes)?;
        assert_eq!(saved, client.activity_cursor.unwrap());
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
