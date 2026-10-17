use acyclic_harness::{
    client::{ClientStateStore, FileClientState, SessionRecord},
    wire,
};
use prost::Message;

pub fn check(data: &[u8]) {
    check_with_faults(data, false, false);
}

pub fn check_with_faults(data: &[u8], dropped_record: bool, unrepaired_tail: bool) {
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
    // Valid outer checksums force arbitrary bytes through the nested protobuf
    // decoder. A random fuzzer cannot ordinarily guess both BLAKE3 checksums.
    let mut framed = (data.len() as u32).to_le_bytes().to_vec();
    framed.extend_from_slice(blake3::hash(data).as_bytes());
    let checksum = blake3::hash(&framed);
    framed.extend_from_slice(checksum.as_bytes());
    framed.extend_from_slice(data);
    std::fs::write(&path, &framed).unwrap();
    {
        let mut store = FileClientState::open(&path).unwrap();
        match SessionRecord::decode(data) {
            Ok(record) => assert_eq!(store.load().unwrap(), [record]),
            Err(_) => assert!(store.load().is_err()),
        }
    }
    framed[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    let checksum = blake3::hash(&framed[..36]);
    framed[36..68].copy_from_slice(checksum.as_bytes());
    std::fs::write(&path, &framed).unwrap();
    {
        let mut store = FileClientState::open(&path).unwrap();
        assert!(store.load().unwrap().is_empty());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
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
        let mut recovered = store.load().unwrap();
        if dropped_record {
            recovered.pop();
        }
        assert_eq!(
            recovered, records,
            "journal lost a complete committed record"
        );
    }
    let bytes = std::fs::read(&path).unwrap();
    let choice = data.iter().fold(0usize, |choice, &byte| {
        choice.wrapping_mul(257).wrapping_add(usize::from(byte))
    });
    // Boundaries are observed from the production writer, without duplicating
    // the framing, checksums or decoder's partial-record logic.
    for cut in [
        choice % (bytes.len() + 1),
        ends[0] - 1,
        ends[0],
        bytes.len() - 1,
        bytes.len(),
    ] {
        std::fs::write(&path, &bytes[..cut]).unwrap();
        let expected = ends.iter().take_while(|&&end| end <= cut).count();
        let repaired_end = expected.checked_sub(1).map_or(0, |index| ends[index]) as u64;
        let mut store = FileClientState::open(&path).unwrap();
        assert_eq!(store.load().unwrap(), records[..expected]);
        if unrepaired_tail {
            std::fs::write(&path, &bytes[..cut]).unwrap();
        }
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            repaired_end,
            "journal retained an incomplete trailing record"
        );
        assert_eq!(store.load().unwrap(), records[..expected]);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), repaired_end);
    }
    let mut corrupt = bytes;
    let offset = choice % corrupt.len();
    corrupt[offset] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    assert!(
        store.load().is_err(),
        "a complete corrupt record was accepted"
    );
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
}
