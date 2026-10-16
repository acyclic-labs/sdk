use super::*;

fn encode(members: &BTreeMap<String, Vec<u8>>, compressed: bool) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(
                &mut header,
                format!("fixture-0.1.0/{name}"),
                bytes.as_slice(),
            )
            .unwrap();
    }
    let bytes = archive.into_inner().unwrap();
    if !compressed {
        return bytes;
    }
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&bytes).unwrap();
    gzip.finish().unwrap()
}

proptest::proptest! {
    #[test]
    fn cargo_archive_members_round_trip(
        members in proptest::collection::btree_map(
            "[a-z][a-z0-9_]{0,12}(/[a-z][a-z0-9_]{0,12}){0,2}",
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024),
            0..8,
        ),
        compressed in proptest::prelude::any::<bool>(),
    ) {
        proptest::prop_assert_eq!(
            decode_archive_members(&encode(&members, compressed), "fixture", "0.1.0").unwrap(),
            members,
        );
    }
}

#[test]
fn archive_codec_rejects_paths_links_duplicates_and_truncated_bodies() {
    assert!(decode_archive_members(&[], "fixture", "0.1.0").is_err());
    let members = BTreeMap::from([("src/lib.rs".into(), b"pub fn fixture() {}".to_vec())]);
    for name in [
        "foreign-0.1.0/src/lib.rs",
        "fixture-0.1.0/../escape",
        "fixture-0.1.0//absolute",
        "fixture-0.1.0/C:drive",
        "fixture-0.1.0/back\\slash",
    ] {
        let mut bytes = encode(&members, false);
        let mut header = tar::Header::new_gnu();
        header.as_mut_bytes().copy_from_slice(&bytes[..512]);
        header.as_mut_bytes()[..100].fill(0);
        header.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
        header.set_cksum();
        bytes[..512].copy_from_slice(header.as_bytes());
        assert!(
            decode_archive_members(&bytes, "fixture", "0.1.0").is_err(),
            "{name}"
        );
    }
    for kind in [
        tar::EntryType::Symlink,
        tar::EntryType::Link,
        tar::EntryType::Fifo,
        tar::EntryType::Char,
    ] {
        let mut bytes = encode(&members, false);
        let mut header = tar::Header::new_gnu();
        header.as_mut_bytes().copy_from_slice(&bytes[..512]);
        header.set_entry_type(kind);
        header.set_cksum();
        bytes[..512].copy_from_slice(header.as_bytes());
        assert!(decode_archive_members(&bytes, "fixture", "0.1.0").is_err());
    }
    let bytes = encode(&members, false);
    let duplicate = [&bytes[..1024], &bytes[..1024], &bytes[1024..]].concat();
    assert!(decode_archive_members(&duplicate, "fixture", "0.1.0").is_err());
    assert!(decode_archive_members(&bytes[..515], "fixture", "0.1.0").is_err());
    let mut gzip = encode(&members, true);
    let last = gzip.len() - 1;
    gzip[last] ^= 1;
    assert!(decode_archive_members(&gzip, "fixture", "0.1.0").is_err());
}
