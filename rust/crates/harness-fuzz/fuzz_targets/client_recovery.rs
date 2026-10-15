#![no_main]
use acyclic_harness::client::{ClientStateStore, FileClientState, SessionRecord, jsonl};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(frame) = jsonl::parse_client_frame(text) {
            let json = jsonl::client_frame(&frame).unwrap();
            assert_eq!(jsonl::parse_client_frame(&json).unwrap(), frame);
        }
        if let Ok(frame) = jsonl::parse_server_frame(text) {
            let json = jsonl::server_frame(&frame).unwrap();
            assert_eq!(jsonl::parse_server_frame(&json).unwrap(), frame);
        }
        // Exercise arbitrary single filenames without escaping the temporary root.
        let mut components = std::path::Path::new(text).components();
        if text.len() <= 64
            && matches!(components.next(), Some(std::path::Component::Normal(_)))
            && components.next().is_none()
        {
            let directory = tempfile::tempdir().unwrap();
            let _ = FileClientState::open(&directory.path().join(text));
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
