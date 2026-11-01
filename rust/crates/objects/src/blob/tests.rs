use super::*;

#[test]
fn persisted_golden_frames_refuse_other_versions() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = Manifest {
        parts: vec![Part {
            digest: Digest::from_bytes([3; 32]),
            length: 7,
        }],
        edges: vec![Digest::from_bytes([4; 32])],
    };
    let envelope = Envelope {
        kind: Kind::Manifest,
        length: 22,
        stored: 64,
        references: vec![Digest::of(b"persisted codec corpus")],
        version: [1; 16],
        dying: None,
        condemned: 9,
    };
    let frames: [(&str, Vec<u8>, &[u8], fn(&[u8]) -> Result<(), Error>); 5] = [
        ("manifest", manifest.encode(), MANIFEST_MAGIC, |bytes| {
            Manifest::decode(bytes).map(|_| ())
        }),
        (
            "corpus/object_persistence/head-v2-live",
            envelope.encode(),
            ENVELOPE_MAGIC,
            |bytes| Envelope::decode(bytes).map(|_| ()),
        ),
        (
            "retained",
            encode_batch(&[Digest::from_bytes([3; 32])]),
            BATCH_MAGIC,
            |bytes| decode_batch(bytes).map(|_| ()),
        ),
        (
            "incarnation",
            encode_incarnation(7, 9),
            INCARNATION_MAGIC,
            |bytes| decode_incarnation(bytes).map(|_| ()),
        ),
        ("epoch", encode_epoch(9), EPOCH_MAGIC, |bytes| {
            decode_epoch(bytes).map(|_| ())
        }),
    ];
    for (name, encoded, magic, decode) in frames {
        let frame = if name.starts_with("corpus/") {
            name.to_owned()
        } else {
            format!("objects.{name}")
        };
        let golden = acyclic_proto_build::golden::frame(&frame)?;
        assert_eq!(encoded, golden, "{name}: persisted bytes changed");
        decode(&golden)?;
        for version in [b'0', b'1', b'2', b'9']
            .into_iter()
            .filter(|version| *version != magic[magic.len() - 2])
        {
            let mut rejected = golden.clone();
            rejected[magic.len() - 2] = version;
            // No body can be read: reject the version even with a missing body.
            rejected.truncate(magic.len());
            assert_eq!(
                decode(&rejected).map_err(|error| error.code),
                Err(ErrorCode::Unsupported),
                "{name}"
            );
        }
        for length in 0..golden.len() {
            // Retained batches may be empty or contain any number of digests.
            if name != "retained" || length < magic.len() || (length - magic.len()) % 32 != 0 {
                assert!(
                    decode(&golden[..length]).is_err(),
                    "{name}: accepted truncation {length}"
                );
            }
        }
        assert_eq!(
            decode(&golden[magic.len()..]).map_err(|error| error.code),
            Err(ErrorCode::Unsupported),
            "{name}: accepted a headerless record"
        );
        let mut trailing = golden;
        trailing.push(0);
        assert!(
            decode(&trailing).is_err(),
            "{name}: accepted trailing bytes"
        );
    }
    for name in [
        "objects.envelope",
        "corpus/object_persistence/head-live",
        "corpus/object_persistence/head-dying",
    ] {
        let historical = acyclic_proto_build::golden::frame(name)?;
        assert_eq!(
            Envelope::decode(&historical).map_err(|error| error.code),
            Err(ErrorCode::Unsupported),
            "{name}"
        );
    }
    assert_eq!(Manifest::decode(&manifest.encode())?, manifest);
    assert_eq!(Envelope::decode(&envelope.encode())?, envelope);
    assert_eq!(
        decode_batch(&encode_batch(&[Digest::from_bytes([3; 32])]))?,
        vec![Digest::from_bytes([3; 32])]
    );
    assert_eq!(decode_incarnation(&encode_incarnation(7, 9))?, (7, 9));
    assert_eq!(decode_epoch(&encode_epoch(9))?, 9);
    Ok(())
}
