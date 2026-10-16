use acyclic_harness::{
    client::{ClientStateStore, FileClientState, SessionRecord, jsonl},
    wire,
};
use prost::Message;

pub fn wire(data: &[u8]) {
    if let Ok(frame) = wire::ClientFrame::decode(data) {
        assert_eq!(
            wire::ClientFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
            frame
        );
    }
    if let Ok(frame) = wire::ServerFrame::decode(data) {
        assert_eq!(
            wire::ServerFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
            frame
        );
    }
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(frame) = jsonl::parse_client_frame(text) {
            assert_eq!(
                jsonl::parse_client_frame(&jsonl::client_frame(&frame).unwrap()).unwrap(),
                frame
            );
        }
        if let Ok(frame) = jsonl::parse_server_frame(text) {
            assert_eq!(
                jsonl::parse_server_frame(&jsonl::server_frame(&frame).unwrap()).unwrap(),
                frame
            );
        }
    }
    // Every input also exercises a valid message, including arbitrary UTF-8
    // escaping and the full u64 range, rather than only rejected random bytes.
    let text = String::from_utf8_lossy(data).into_owned();
    let mut number = [0; 8];
    let copied = data.len().min(number.len());
    number[..copied].copy_from_slice(&data[..copied]);
    let frame = wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
            authority: None,
            generation: text,
            through_revision: u64::from_le_bytes(number),
        })),
    };
    assert_eq!(
        jsonl::parse_client_frame(&jsonl::client_frame(&frame).unwrap()).unwrap(),
        frame
    );
}

pub fn journal(data: &[u8]) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    std::fs::write(&path, data).unwrap();
    {
        let mut store = FileClientState::open(&path).unwrap();
        if let Ok(records) = store.load() {
            // Repair of an incomplete trailing record is idempotent.
            assert_eq!(store.load().unwrap(), records);
        }
    }
    std::fs::write(&path, []).unwrap();
    let records = [
        SessionRecord::decode(data).unwrap_or_default(),
        SessionRecord {
            request: Some(wire::ClientFrame {
                frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
                    authority: None,
                    generation: String::from_utf8_lossy(data).into_owned(),
                    through_revision: u64::MAX,
                })),
            }),
            ..Default::default()
        },
    ];
    let mut ends = Vec::new();
    {
        let mut store = FileClientState::open(&path).unwrap();
        for record in &records {
            store.append(record).unwrap();
            ends.push(std::fs::metadata(&path).unwrap().len() as usize);
        }
        assert_eq!(store.load().unwrap(), records);
    }
    let bytes = std::fs::read(&path).unwrap();
    // Boundaries are observed from the production writer, without duplicating
    // the framing, checksums or decoder's partial-record logic.
    for cut in [
        usize::from(data.first().copied().unwrap_or(0)) % (bytes.len() + 1),
        ends[0] - 1,
        ends[0],
        bytes.len() - 1,
        bytes.len(),
    ] {
        std::fs::write(&path, &bytes[..cut]).unwrap();
        let expected = ends.iter().take_while(|&&end| end <= cut).count();
        let mut store = FileClientState::open(&path).unwrap();
        assert_eq!(store.load().unwrap(), records[..expected]);
        assert_eq!(store.load().unwrap(), records[..expected]);
    }
    let mut corrupt = bytes;
    let offset = usize::from(data.last().copied().unwrap_or(0)) % ends[0];
    corrupt[offset] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    assert!(
        store.load().is_err(),
        "a complete corrupt record was accepted"
    );
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
}
