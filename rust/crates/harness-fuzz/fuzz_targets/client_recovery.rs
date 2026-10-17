#![no_main]
use acyclic_harness::client::{ClientStateStore, FileClientState, SessionRecord, jsonl};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(file) = jsonl::parse_request(text) {
            assert_eq!(
                jsonl::parse_request(&jsonl::request(&file).unwrap()).unwrap(),
                file
            );
        }
        if let Ok((query, head)) = jsonl::parse_snapshot(text) {
            let encoded = jsonl::snapshot(&query, &head).unwrap();
            let (query, head) = jsonl::parse_snapshot(&encoded).unwrap();
            assert_eq!(jsonl::snapshot(&query, &head).unwrap(), encoded);
        }
        if let Ok(change) = jsonl::parse_change(text) {
            let encoded = jsonl::change(&change).unwrap();
            let change = jsonl::parse_change(&encoded).unwrap();
            assert_eq!(jsonl::change(&change).unwrap(), encoded);
        }
        if let Ok(resolution) =
            serde_json::from_str::<acyclic_harness::client::CommandResolution>(text)
        {
            let encoded = serde_json::to_string(&resolution).unwrap();
            let decoded =
                serde_json::from_str::<acyclic_harness::client::CommandResolution>(&encoded)
                    .unwrap();
            assert_eq!(resolution, decoded);
        }
        // Exercise arbitrary single filenames without escaping the temporary root.
        let mut components = std::path::Path::new(text).components();
        if text.len() <= 250
            && matches!(components.next(), Some(std::path::Component::Normal(_)))
            && components.next().is_none()
        {
            let directory = tempfile::tempdir().unwrap();
            if let Ok(mut store) = FileClientState::open(&directory.path().join(text)) {
                let record = SessionRecord::default();
                store.replace(&record).unwrap();
                assert_eq!(store.load().unwrap(), vec![record]);
            }
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    std::fs::write(&path, data).unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    let _ = store.load(); // Invalid framing must return an error, never panic.
    drop(store);
    std::fs::remove_file(&path).unwrap();
    if let Ok(record) = SessionRecord::decode(data) {
        let mut store = FileClientState::open(&path).unwrap();
        store.append(&record).unwrap();
        assert_eq!(store.load().unwrap(), vec![record]);
    }
});
