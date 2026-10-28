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
        let bytes = encode(&members, compressed);
        let limits = ArchiveLimits::default();
        proptest::prop_assert_eq!(
            decode_archive_members(&bytes, "fixture", "0.1.0").unwrap(),
            super::archive_decoder_oracle::retained_archive_decoder(&bytes, "fixture", "0.1.0", limits).unwrap(),
        );
        proptest::prop_assert_eq!(decode_archive_members(&bytes, "fixture", "0.1.0").unwrap(), members);
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

#[test]
fn archive_admission_bounds_cover_input_members_and_complete_expansion() {
    let members = BTreeMap::from([("a".into(), vec![0; 4096]), ("b".into(), vec![1; 8])]);
    let plain = encode(&members, false);
    for compressed in [false, true] {
        let bytes = encode(&members, compressed);
        let exact = ArchiveLimits { compressed_bytes: bytes.len() as u64,
            expanded_bytes: plain.len() as u64, member_bytes: 4096, members: 2 };
        assert_eq!(decode_archive_members_with_limits(&bytes, "fixture", "0.1.0", exact).unwrap(), members);
        for (limits, refusal) in [
            (ArchiveLimits { compressed_bytes: exact.compressed_bytes - 1, ..exact }, "compressed-byte"),
            (ArchiveLimits { expanded_bytes: exact.expanded_bytes - 1, ..exact }, "expanded-byte"),
            (ArchiveLimits { member_bytes: 4095, ..exact }, "member-byte"),
            (ArchiveLimits { members: 1, ..exact }, "member-count"),
        ] {
            let error = decode_archive_members_with_limits(&bytes, "fixture", "0.1.0", limits).unwrap_err();
            assert!(error.contains(refusal), "{error}");
        }
    }
}


fn stage_bytes(bytes: &[u8], limits: ArchiveLimits) -> Result<super::super::archives::StagedArchive, String> {
    let input = tempfile::NamedTempFile::new().unwrap();
    fs::write(input.path(), bytes).unwrap();
    let release = RegistryVersion {
        package: "fixture".into(), num: "0.1.0".into(),
        checksum: crate::sha256_bytes(bytes).trim_start_matches("sha256:").into(), yanked: false,
    };
    super::super::archives::StagedArchive::read(&release, input.path(), limits)
}

#[test]
fn staged_archive_matches_retained_decoder_and_immutable_publication() {
    let members = BTreeMap::from([
        ("Cargo.toml".into(), b"[package]".to_vec()),
        ("a/./long".into(), vec![42; 128 * 1024]),
        (format!("src/{}", "long".repeat(40)), vec![255; 19]),
    ]);
    for compressed in [false, true] {
        let bytes = encode(&members, compressed);
        let original = super::archive_decoder_oracle::retained_archive_decoder(&bytes, "fixture", "0.1.0", ArchiveLimits::default()).unwrap();
        let staged = stage_bytes(&bytes, ArchiveLimits::default()).unwrap();
        assert_eq!(staged.files, original.iter().map(|(name, body)| (name.clone(), crate::sha256_bytes(body))).collect());
        let root = tempfile::tempdir().unwrap();
        staged.publish(root.path(), "fixture-0.1.0").unwrap();
        staged.publish(root.path(), "fixture-0.1.0").unwrap();
        for (name, body) in original {
            assert_eq!(fs::read(root.path().join("fixture-0.1.0").join(name)).unwrap(), body);
        }
        fs::write(root.path().join("fixture-0.1.0/Cargo.toml"), b"changed").unwrap();
        assert!(staged.publish(root.path(), "fixture-0.1.0").unwrap_err().contains("immutable"));
    }
}

#[test]
fn staged_archive_refuses_late_corruption_duplicates_and_every_limit_before_publication() {
    let members = BTreeMap::from([("a".into(), vec![0; 4096]), ("b".into(), vec![1; 8])]);
    let plain = encode(&members, false);
    for compressed in [false, true] {
        let bytes = encode(&members, compressed);
        let exact = ArchiveLimits { compressed_bytes: bytes.len() as u64, expanded_bytes: plain.len() as u64, member_bytes: 4096, members: 2 };
        assert!(stage_bytes(&bytes, exact).is_ok());
        for limits in [
            ArchiveLimits { compressed_bytes: exact.compressed_bytes - 1, ..exact },
            ArchiveLimits { expanded_bytes: exact.expanded_bytes - 1, ..exact },
            ArchiveLimits { member_bytes: 4095, ..exact }, ArchiveLimits { members: 1, ..exact },
        ] {
            assert!(stage_bytes(&bytes, limits).is_err());
        }
    }
    let first = encode(&BTreeMap::from([("same".into(), vec![1])]), false);
    let duplicate = [&first[..1024], &first[..1024], &first[1024..]].concat();
    let mut corrupt = encode(&members, true);
    let last = corrupt.len() - 1; corrupt[last] ^= 1;
    for invalid in [duplicate, corrupt, encode_path("fixture-0.1.0/../escape"), plain[..515].to_vec()] {
        assert!(super::archive_decoder_oracle::retained_archive_decoder(&invalid, "fixture", "0.1.0", ArchiveLimits::default()).is_err());
        let destination = tempfile::tempdir().unwrap();
        let output = destination.path().join("published");
        let result = stage_bytes(&invalid, ArchiveLimits::default()).and_then(|stage| stage.publish(&output, "fixture-0.1.0"));
        assert!(result.is_err());
        assert!(!output.exists(), "invalid input published an imported source");
    }
}

#[test]
fn staged_archive_retains_snapshot_and_detects_source_replacement() {
    let bytes = encode(&BTreeMap::from([("src/lib.rs".into(), vec![42])]), true);
    let input = tempfile::NamedTempFile::new().unwrap();
    fs::write(input.path(), &bytes).unwrap();
    let release = RegistryVersion { package: "fixture".into(), num: "0.1.0".into(),
        checksum: crate::sha256_bytes(&bytes).trim_start_matches("sha256:").into(), yanked: false };
    let staged = super::super::archives::StagedArchive::read(&release, input.path(), ArchiveLimits::default()).unwrap();
    fs::write(input.path(), b"replacement").unwrap();
    assert!(staged.check_source(&release, input.path()).unwrap_err().contains("changed"));
    assert_eq!(staged.files["src/lib.rs"], crate::sha256_bytes(&[42]));
}

#[test]
fn staged_dependency_verification_preserves_exact_inventory_bytes_and_refusals() {
    let bodies = BTreeMap::from([
        ("Cargo.toml".into(), b"[package]\nname='fixture'\nversion='0.1.0'\n".to_vec()),
        ("descriptor.bin".into(), vec![42; 128 * 1024]),
        (".cargo_vcs_info.json".into(), serde_json::to_vec(&serde_json::json!({
            "git": {"sha1": "a".repeat(40), "dirty": false}, "path_in_vcs": "original-owner"
        })).unwrap()),
    ]);
    for compressed in [false, true] {
        let bytes = encode(&bodies, compressed);
        let original = super::archive_decoder_oracle::retained_archive_decoder(
            &bytes, "fixture", "0.1.0", ArchiveLimits::default(),
        ).unwrap();
        let root = tempfile::tempdir().unwrap();
        let directory = "historical-fixture-0.1.0";
        let prefix = format!(".docs-producer/{directory}/");
        let archive_logical = format!("{prefix}registry-archives/fixture-0.1.0.crate");
        let archive = root.path().join(&archive_logical);
        fs::create_dir_all(archive.parent().unwrap()).unwrap();
        fs::write(&archive, &bytes).unwrap();
        let imported = root.path().join(format!("{prefix}dependencies/fixture-0.1.0"));
        fs::create_dir_all(&imported).unwrap();
        let mut expected = BTreeMap::from([(archive_logical, crate::sha256_bytes(&bytes))]);
        for (name, body) in &original {
            fs::write(imported.join(name), body).unwrap();
            expected.insert(format!("{prefix}dependencies/fixture-0.1.0/{name}"), crate::sha256_bytes(body));
        }
        let dependency = ProducerDependencyArchive {
            package: "fixture".into(), version: "0.1.0".into(),
            registry_checksum: crate::sha256_bytes(&bytes).trim_start_matches("sha256:").into(),
            yanked: true, publisher_vcs: archive_vcs(&original).unwrap(),
        };
        let retained = [dependency];
        let accepted = producer_dependencies(root.path(), directory, Some(&retained)).unwrap();
        assert_eq!(accepted.1, expected);
        assert_eq!(accepted.0[0].publisher_vcs, retained[0].publisher_vcs);
        assert!(accepted.0[0].yanked);
        let payload = imported.join("descriptor.bin");
        fs::write(&payload, vec![43; 128 * 1024]).unwrap();
        assert!(producer_dependencies(root.path(), directory, Some(&retained)).unwrap_err().contains("source differs"));
        fs::remove_file(&payload).unwrap();
        assert!(producer_dependencies(root.path(), directory, Some(&retained)).is_err());
        fs::write(&payload, &original["descriptor.bin"]).unwrap();
        #[cfg(unix)]
        {
            fs::remove_file(&payload).unwrap();
            let other = root.path().join("redirected.bin");
            fs::write(&other, &original["descriptor.bin"]).unwrap();
            std::os::unix::fs::symlink(&other, &payload).unwrap();
            assert!(producer_dependencies(root.path(), directory, Some(&retained)).unwrap_err().contains("redirects"));
            fs::remove_file(&payload).unwrap();
            fs::write(&payload, &original["descriptor.bin"]).unwrap();
        }
        let mut wrong_vcs = retained.clone();
        wrong_vcs[0].publisher_vcs = None;
        assert!(producer_dependencies(root.path(), directory, Some(&wrong_vcs)).unwrap_err().contains("publisher VCS differs"));
        fs::write(&archive, b"corrupted archive").unwrap();
        assert!(producer_dependencies(root.path(), directory, Some(&retained)).unwrap_err().contains("checksum differs"));
    }
}
