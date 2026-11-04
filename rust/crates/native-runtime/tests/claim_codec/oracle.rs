//! The persisted claim codec itself, including its checksum for grammar mutations.
#[path = "../../src/claim/record.rs"]
mod record;

use record::{Contents, MAGIC, Process, RECORD_LIMIT, Record};

fn wire(bytes: &[u8]) {
    match Contents::decode(bytes) {
        Contents::Empty => assert!(bytes.is_empty()),
        Contents::Oversized => assert!(bytes.len() > RECORD_LIMIT),
        Contents::Torn => assert!(!bytes.is_empty() && bytes.len() <= RECORD_LIMIT),
        Contents::Record(value) => assert_eq!(value.encode(), bytes, "claim bytes changed"),
    }
}

fn value(bytes: &[u8]) -> Record {
    let bytes = &bytes[..bytes.len().min(RECORD_LIMIT)];
    let split = bytes.len() / 3;
    Record {
        process: Process {
            pid: u32::from_le_bytes(std::array::from_fn(|i| bytes.get(i).copied().unwrap_or(0))),
            start: bytes[..split].to_vec(),
            lock: bytes[split..2 * split].to_vec(),
            lock_identity: bytes[2 * split..].to_vec(),
        },
        token: u64::from_le_bytes(std::array::from_fn(|i| bytes.get(i).copied().unwrap_or(0))),
    }
}

/// Exercise arbitrary wire bytes and checksum-valid grammar mutations of an
/// actual writer output. No alternate parser or checksum is implemented here.
pub fn check(bytes: &[u8]) {
    wire(bytes);
    let overhead = value(&[]).encode().len();
    let value = value(&bytes[..bytes.len().min(RECORD_LIMIT - overhead)]);
    let mut encoded = value.encode();
    let mut offset = MAGIC.len();
    let lengths = [
        4,
        value.process.start.len(),
        value.process.lock.len(),
        value.process.lock_identity.len(),
        8,
    ];
    let fields = lengths.map(|length| {
        let field = offset;
        offset += 4 + length;
        field
    });
    assert_eq!(Contents::decode(&encoded), Contents::Record(value));
    if encoded.len() == RECORD_LIMIT {
        let mut past = encoded.clone();
        past.push(0);
        assert_eq!(Contents::decode(&past), Contents::Oversized);
    }
    let body = encoded.len() - 8;
    let original = encoded[body];
    encoded[body] ^= 1;
    assert_eq!(
        Contents::decode(&encoded),
        Contents::Torn,
        "claim checksum bypassed"
    );
    encoded[body] = original;

    let choice = usize::from(u16::from_le_bytes(std::array::from_fn(|i| {
        bytes.get(i).copied().unwrap_or(0)
    })));
    let cut = choice % encoded.len();
    assert_eq!(
        Contents::decode(&encoded[..cut]),
        if cut == 0 {
            Contents::Empty
        } else {
            Contents::Torn
        }
    );

    // Reach malformed grammar through the real writer and checksum, rather
    // than waiting for random input to collide with a valid checksum.
    match choice % 10 {
        0 => encoded[choice % MAGIC.len()] ^= 0xff,
        1..=5 => {
            let offset = fields[choice % 10 - 1];
            encoded[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        }
        6 => {
            encoded.insert(body, 0);
        }
        7 | 8 => {
            let offset = fields[if choice % 10 == 7 { 0 } else { 4 }];
            encoded[offset..offset + 4].copy_from_slice(&0_u32.to_le_bytes());
        }
        _ => {
            encoded.drain(choice / 10 % body..body);
        }
    }
    let body = encoded.len() - 8;
    let sum = record::checksum(&encoded[..body]);
    encoded[body..].copy_from_slice(&sum);
    if encoded.len() > RECORD_LIMIT {
        assert_eq!(Contents::decode(&encoded), Contents::Oversized);
    } else {
        assert_eq!(
            Contents::decode(&encoded),
            Contents::Torn,
            "claim grammar widened"
        );
    }
}

/// Exhaust every cut of an original writer seed outside the fuzz hot path.
#[cfg(not(fuzzing))]
pub fn check_prefixes(bytes: &[u8]) {
    for cut in 0..bytes.len() {
        assert_eq!(
            Contents::decode(&bytes[..cut]),
            if cut == 0 {
                Contents::Empty
            } else {
                Contents::Torn
            },
            "partial claim admitted at cut {cut}"
        );
    }
}

/// Capture real codec outputs; callers retain these bytes as corpus inputs.
#[cfg(not(fuzzing))]
pub fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    let empty = value(&[]);
    let mut boundary = value(b"native\0\xff\nlock");
    let length = RECORD_LIMIT - boundary.encode().len() + boundary.process.lock.len();
    boundary.process.lock.resize(length, b'/');
    let mut oversized = boundary.clone();
    oversized.process.lock.push(b'/');
    vec![
        ("empty-record", empty.encode()),
        ("native-bytes", value(b"native\0\xff\nlock").encode()),
        ("at-bound", boundary.encode()),
        ("past-bound", oversized.encode()),
    ]
}
