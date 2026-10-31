use super::read::Malformed;
use super::*;
use crate::kernel::{FileKind, FilePayload, InlineFileData};
use crate::model::FilesystemProfile;

fn record(number: u8) -> FileRecord {
    FileRecord {
        file_id: crate::FileId::from_bytes([number; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: crate::ObjectId {
            storage: None,
            kind: crate::ObjectKind::Metadata,
            digest: crate::Digest::from_bytes([number; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(b"x").unwrap()),
    }
}

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn named(config: VolumeConfig, names: &[(NameEncoding, Vec<u8>)]) -> NamespacePath {
    NamespacePath::new(
        names
            .iter()
            .map(|(encoding, bytes)| {
                LogicalName::new(
                    *encoding,
                    bytes.clone(),
                    config.limits.maximum_component_bytes,
                )
                .unwrap()
            })
            .collect(),
        config.limits,
    )
    .unwrap()
}

fn configs() -> [VolumeConfig; 3] {
    let portable = VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let mut windows = VolumeConfig {
        profile: FilesystemProfile::Windows,
        ..portable
    };
    windows.limits.maximum_component_bytes = 255 * 2;
    let folded = VolumeConfig {
        case_sensitivity: CaseSensitivity::ProfileFolded,
        ..portable
    };
    [portable, windows, folded]
}

/// Every path shape the producer meets, with whether its key carries it.
fn paths() -> Vec<(VolumeConfig, NamespacePath, bool)> {
    let [portable, windows, folded] = configs();
    let utf8 = |text: &str| (NameEncoding::Utf8, text.as_bytes().to_vec());
    let posix = |bytes: &[u8]| (NameEncoding::PosixBytes, bytes.to_vec());
    let wide = |text: &str| (NameEncoding::WindowsUtf16Le, utf16(text));
    let long = vec![utf8(&"x".repeat(240)); 5];
    // 4 * 255 names + 3 separators = 1023 bytes: one under the key bound.
    let longest_elided = vec![utf8(&"y".repeat(255)); 4];
    vec![
        (
            portable,
            named(
                portable,
                &[utf8(".system"), utf8("harness-uploads"), utf8("a.json")],
            ),
            true,
        ),
        (portable, named(portable, &[utf8("é"), utf8("日本")]), true),
        (portable, named(portable, &longest_elided), true),
        // 1200 bytes: a truncated key never carries the names.
        (portable, named(portable, &long), false),
        (
            portable,
            named(portable, &[posix(b"ok"), posix(b"\xff\xfe")]),
            false,
        ),
        (
            portable,
            named(portable, &[posix(b"same"), utf8("mixed")]),
            false,
        ),
        (
            portable,
            named(portable, &[posix(b"posix"), posix(b"text")]),
            true,
        ),
        (
            windows,
            named(windows, &[wide("Program Files"), wide("Ünïcode")]),
            true,
        ),
        // A lone surrogate has no Unicode text.
        (
            windows,
            named(
                windows,
                &[wide("ok"), (NameEncoding::WindowsUtf16Le, vec![0x00, 0xd8])],
            ),
            false,
        ),
        // A folded key differs from the names it stands for.
        (folded, named(folded, &[utf8("Mixed"), utf8("Case")]), false),
    ]
}

#[test]
fn every_entry_decodes_to_its_exact_path_and_only_untruncated_unicode_keys_carry_names() {
    let mut elided = 0;
    for (config, path, carries) in paths() {
        assert_eq!(names_from_key(&path, config).is_some(), carries, "{path:?}");
        for (before, after) in [
            (None, Some(record(1))),
            (Some(record(2)), None),
            (Some(record(3)), Some(record(4))),
        ] {
            let change = ChangedPath {
                path: path.clone(),
                before,
                after,
            };
            let key = key(&path, config);
            let mut metadata = Vec::new();
            encode(&change, config, &mut metadata);
            assert_eq!(metadata.len(), encoded_len(&change, config));
            let records = RECORD_BYTES
                * (usize::from(change.before.is_some()) + usize::from(change.after.is_some()));
            if carries {
                elided += 1;
                assert_eq!(
                    metadata.len(),
                    2 + records,
                    "the names are stored once, in the key"
                );
            }
            let facts =
                PublicationEntry::from_parts(key.slice(), operation(&change), &metadata, config)
                    .unwrap();
            assert_eq!(facts.before_present, change.before.is_some());
            assert_eq!(facts.after_present, change.after.is_some());
            let decoded = facts.path;
            assert_eq!(decoded, path);
            // The names the key carries are the key's exact text.
            if carries {
                let joined = decoded
                    .components()
                    .iter()
                    .map(|name| {
                        let mut text = String::new();
                        name.append_unicode(&mut text).unwrap();
                        text
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                assert_eq!(joined.as_bytes(), key.slice());
            }
        }
    }
    assert!(elided > 0);
}

#[test]
fn a_key_that_cannot_be_the_whole_path_never_stands_for_one() {
    let [portable, _, folded] = configs();
    let created = [0, 1]
        .into_iter()
        .chain([7; 16])
        .chain([1])
        .chain([8; 32])
        .collect::<Vec<u8>>();
    let carried = |config, key: &[u8]| super::path(key, "create", &created, config);
    // The negative control: an untruncated key in a case-preserving volume.
    assert!(carried(portable, b"a/b").is_ok());
    let truncated = vec![b'x'; acyclic_stream::MAX_RECORD_KEY_BYTES];
    assert_eq!(carried(portable, &truncated), Err(Malformed));
    assert_eq!(carried(folded, b"a/b"), Err(Malformed));
    assert_eq!(carried(portable, b""), Err(Malformed));
    assert_eq!(carried(portable, b"a//b"), Err(Malformed));
    assert_eq!(carried(portable, b"\xff"), Err(Malformed));
    // Records must match the operation, and nothing may trail them.
    assert_eq!(
        super::path(b"a", "remove", &created, portable).map(|_| ()),
        Ok(())
    );
    assert_eq!(
        super::path(b"a", "update", &created, portable),
        Err(Malformed)
    );
    assert_eq!(
        super::path(b"a", "rename", &created, portable),
        Err(Malformed)
    );
    let trailing = [created.as_slice(), &[0]].concat();
    assert_eq!(
        super::path(b"a", "create", &trailing, portable),
        Err(Malformed)
    );
    // A nonminimal varint is not the one encoding of the same names.
    let nonminimal = [&[0x80, 0x00][..], created.get(1..).unwrap()].concat();
    assert_eq!(
        super::path(b"a", "create", &nonminimal, portable),
        Err(Malformed)
    );
}
