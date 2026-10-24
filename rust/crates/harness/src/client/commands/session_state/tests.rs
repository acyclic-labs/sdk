use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

fn reads<const PARSE: bool>() {
    let file = StateFile::new(files::FileValue::new(
        ui("config", "settings.json").unwrap(),
        br#"{"padding":["sample","state","values"]}"#.to_vec(),
    ));
    file.json().unwrap();
    let allocation = acyclic_test_support::measure(|| {
        for _ in 0..1000 {
            if PARSE {
                std::hint::black_box(serde_json::from_slice::<Value>(&file.bytes).unwrap());
            } else {
                std::hint::black_box(file.json().unwrap());
            }
        }
    });
    assert_eq!(
        allocation.count_total, 0,
        "JSON is parsed once per accepted file"
    );
}

#[test]
fn cached_state_reads_do_not_parse_or_allocate() {
    reads::<false>();
}

#[test]
#[should_panic(expected = "JSON is parsed once per accepted file")]
fn negative_control_repeated_json_parsing_allocates_per_read() {
    reads::<true>();
}

async fn session() -> CommandSession<MemoryStream> {
    CommandSession::open(
        Arc::new(
            files::TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("state-cache").unwrap(),
            )
            .await
            .unwrap(),
        ),
        uuid::Uuid::from_bytes([86; 16]),
        65536,
        128,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn state_cache_tracks_writes_followed_replacements_invalid_json_and_eviction() {
    let mut session = session().await;
    for root in [
        CommandFileRoot::Config,
        CommandFileRoot::Session,
        CommandFileRoot::Task,
    ] {
        for value in [json!({"value":1}), json!({"value":2})] {
            session
                .write_state(root, "settings.json", &value)
                .await
                .unwrap();
            assert_eq!(
                session.state_ref(root, "settings.json").unwrap(),
                Some(&value)
            );
            assert_eq!(session.state(root, "settings.json").unwrap(), Some(value));
        }
        let key = session.state_key(root, "settings.json").unwrap();
        session
            .fold(files::FileValue::new(key.clone(), b"invalid".to_vec()))
            .unwrap();
        assert!(session.state_ref(root, "settings.json").is_err());
        session
            .fold(files::FileValue::new(key.clone(), b"[]".to_vec()))
            .unwrap();
        assert!(session.state_ref(root, "settings.json").is_err());
        session
            .fold(files::FileValue::new(key.clone(), b"{}".to_vec()))
            .unwrap();
        assert_eq!(
            session.state_ref(root, "settings.json").unwrap(),
            Some(&json!({}))
        );
        session.files.remove(&key);
        assert!(session.state_ref(root, "settings.json").unwrap().is_none());
    }
}

#[test]
#[ignore = "manual cached settings read measurement"]
fn cached_state_measurement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut session = runtime.block_on(session());
    let value =
        json!({"padding":["sample","state","values"],"view":"transcript","selected_task":null});
    for (root, name) in [
        (CommandFileRoot::Config, "settings.json"),
        (CommandFileRoot::Session, "layout.json"),
    ] {
        runtime
            .block_on(session.write_state(root, name, &value))
            .unwrap();
        session.state_ref(root, name).unwrap();
    }
    for loops in [1000, 2000] {
        let start = std::time::Instant::now();
        let allocation = acyclic_test_support::measure(|| {
            for _ in 0..loops {
                for (root, name) in [
                    (CommandFileRoot::Config, "settings.json"),
                    (CommandFileRoot::Session, "layout.json"),
                ] {
                    std::hint::black_box(session.state_ref(root, name).unwrap());
                }
            }
        });
        eprintln!(
            "CLIENT_STATE_AFTER loops={loops} reads={} elapsed_us={} allocations={} allocated_bytes={}",
            loops * 2,
            start.elapsed().as_micros(),
            allocation.count_total,
            allocation.bytes_total
        );
    }
}
