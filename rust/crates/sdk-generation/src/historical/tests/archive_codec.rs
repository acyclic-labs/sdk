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

fn encode_path(name: &str) -> Vec<u8> {
    let members = BTreeMap::from([("src/lib.rs".into(), vec![42])]);
    let mut bytes = encode(&members, false);
    let mut header = tar::Header::new_gnu();
    header.as_mut_bytes().copy_from_slice(&bytes[..512]);
    header.as_mut_bytes()[..100].fill(0);
    assert!(name.len() < 100);
    header.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
    header.set_cksum();
    bytes[..512].copy_from_slice(header.as_bytes());
    bytes
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

    #[test]
    fn archive_member_path_shapes_are_rejected_on_every_host(name in "[a-z]{1,8}") {
        for path in [
            format!("/{name}"),
            format!("//{name}"),
            format!("\\{name}"),
            format!("{name}\\child"),
            format!("C:{name}"),
            format!("C:/{name}"),
            format!("../{name}"),
            format!("{name}/../child"),
            format!("{name}/C:/child"),
        ] {
            let bytes = encode_path(&format!("fixture-0.1.0/{path}"));
            proptest::prop_assert!(
                decode_archive_members(&bytes, "fixture", "0.1.0").is_err(),
                "accepted hostile member: {:?}", path,
            );
        }
    }
}

#[test]
fn path_shape_oracle_detects_a_missing_root_guard() {
    use proptest::test_runner::{TestError, TestRunner};
    // Without RootDir rejection, Windows' non-absolute rooted path is admitted.
    let broken_accepts = |path: &str| {
        !path.contains(['\\', ':'])
            && !Path::new(path)
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
    };
    let result =
        TestRunner::default().run(&proptest::strategy::Just("/absolute".to_owned()), |path| {
            proptest::prop_assert!(
                !broken_accepts(&path),
                "accepted hostile member: {:?}",
                path
            );
            Ok(())
        });
    assert!(matches!(result, Err(TestError::Fail(_, _))));
    let bytes = encode_path("fixture-0.1.0//absolute");
    assert!(decode_archive_members(&bytes, "fixture", "0.1.0").is_err());
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
        let bytes = encode_path(name);
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
