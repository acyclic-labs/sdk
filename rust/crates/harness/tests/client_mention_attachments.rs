use acyclic_harness::{
    client::{CommandResolution, FileMention},
    conversation::{FileDescriptor, FileRef, VolumeRef},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};

#[path = "../../harness-fuzz/support/mentions.rs"]
mod oracle;

fn checked_input() -> CommandResolution {
    let mentions = [file(1, 1, 1, b"one"), file(2, 1, 1, b"two")]
        .into_iter()
        .enumerate()
        .map(|(index, file)| FileMention {
            start: index * 8,
            end: index * 8 + 7,
            file,
        })
        .collect();
    CommandResolution::Prompt {
        text: "@file-1 @file-2".into(),
        source: None,
        mentions,
    }
}

#[test]
fn fuzz_oracle_checks_every_seed() {
    for data in [
        include_bytes!("../../harness-fuzz/corpus/client_mentions/pinned-mentions").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/plain-prompt").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/view-selection").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/ordered-mentions").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/conflicting-metadata").as_slice(),
    ] {
        let input: CommandResolution = serde_json::from_slice(data).unwrap();
        oracle::assert_conversion(&input, &input.clone().into_model_input());
    }
}

#[test]
#[should_panic(expected = "conversion must preserve ordered unique pinned references")]
fn negative_control_dropped_attachment_fails_the_fuzz_oracle() {
    let input = checked_input();
    let mut content = input.clone().into_model_input().unwrap();
    let ModelContent::Parts(parts) = &mut content else {
        panic!("parts");
    };
    parts.remove(1);
    oracle::assert_conversion(&input, &Ok(content));
}

#[test]
#[should_panic(expected = "valid prompt conversion must succeed")]
fn negative_control_rejected_valid_prompt_fails_the_fuzz_oracle() {
    oracle::assert_conversion(
        &checked_input(),
        &Err(acyclic_harness::Error::Invalid("broken".into())),
    );
}

#[test]
#[should_panic(expected = "conflicting file metadata must be rejected")]
fn negative_control_accepted_metadata_conflict_fails_the_fuzz_oracle() {
    let mut input = checked_input();
    let CommandResolution::Prompt { mentions, .. } = &mut input else {
        unreachable!()
    };
    mentions.push(FileMention {
        start: 16,
        end: 23,
        file: file(1, 1, 1, b"changed"),
    });
    oracle::assert_conversion(&input, &Ok(ModelContent::Text("broken".into())));
}

fn file(id: u8, revision: u8, volume: u8, body: &[u8]) -> FileRef {
    let fixture: FileRef =
        serde_json::from_str(include_str!("../fixtures/v2/file-ref.json")).unwrap();
    let reference = fixture.volume();
    FileRef::new(
        VolumeRef::new(
            reference.provider().clone(),
            format!("volume-{volume}"),
            reference.class(),
            reference.owner().clone(),
            &mut None,
        )
        .unwrap(),
        format!("file-{id}"),
        format!("revision-{revision}"),
        FileDescriptor::from_bytes(body, "text/plain", &mut None).unwrap(),
        "file".into(),
        &mut None,
    )
    .unwrap()
}

#[test]
fn conflicting_metadata_for_one_pinned_identity_is_rejected() {
    let mentions = [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .enumerate()
        .map(|(index, body)| FileMention {
            start: index * 8,
            end: index * 8 + 7,
            file: file(1, 1, 1, body),
        })
        .collect();
    assert!(matches!(
        CommandResolution::Prompt {
            text: "@file-1 @file-1".into(),
            source: None,
            mentions,
        }
        .into_model_input(),
        Err(acyclic_harness::Error::Conflict(_))
    ));
}

proptest::proptest! {
    // Property: the shared fuzz oracle covers success and metadata-conflict
    // rejection for arbitrary repeated identities and file descriptors.
    #[test]
    fn fuzz_oracle_checks_arbitrary_metadata_conflicts(
        rows in proptest::collection::vec((0u8..4, proptest::collection::vec(proptest::prelude::any::<u8>(), 0..16)), 0..16),
    ) {
        let mentions = rows.into_iter().enumerate().map(|(index, (id, body))| FileMention {
            start: index * 8,
            end: index * 8 + 7,
            file: file(id, 1, 1, &body),
        }).collect();
        let input = CommandResolution::Prompt { text: "inspect files".into(), source: None, mentions };
        oracle::assert_conversion(&input, &input.clone().into_model_input());
    }

    // Property: attach each pinned (volume,path,version) exactly once, in first
    // mention order; preserve prompt text and the model-input codec, and reject
    // empty input when it contains no attachment.
    #[test]
    fn attachment_projection_preserves_pinned_identity_order_and_roundtrips(
        identities in proptest::collection::vec((0u8..8, 0u8..4, 0u8..4), 0..64),
        prefix in "[a-zA-Z ]{0,32}",
    ) {
        let mut text = prefix;
        let mut mentions = Vec::new();
        let mut expected = Vec::new();
        for (id, revision, volume) in identities {
            let path = format!("file-{id}");
            let identity = (format!("volume-{volume}"), path.clone(), format!("revision-{revision}"));
            text.push(' ');
            let start = text.len();
            text.push('@');
            text.push_str(&path);
            mentions.push(FileMention { start, end: text.len(), file: file(id, revision, volume, b"body") });
            if !expected.contains(&identity) {
                expected.push(identity);
            }
        }
        let original = text.clone();
        let content = CommandResolution::Prompt { text, source: None, mentions }.into_model_input();
        if original.is_empty() {
            proptest::prop_assert!(matches!(content, Err(acyclic_harness::Error::Invalid(_))));
            return Ok(());
        }
        let content = content.unwrap();
        match &content {
            ModelContent::Text(text) => {
                proptest::prop_assert!(expected.is_empty());
                proptest::prop_assert_eq!(text, &original);
            }
            ModelContent::Parts(parts) => {
                proptest::prop_assert_eq!(parts.len(), expected.len() + 1);
                proptest::prop_assert_eq!(&parts[0], &ModelContentPart::Text { text: original });
                for (part, (volume, path, version)) in parts[1..].iter().zip(expected) {
                    match part {
                        ModelContentPart::File { file, policy } => {
                            proptest::prop_assert_eq!(file.volume().id(), volume);
                            proptest::prop_assert_eq!(file.path(), path);
                            proptest::prop_assert_eq!(file.version(), version);
                            proptest::prop_assert_eq!(policy, &FileProjectionPolicy::BoundedFull);
                        }
                        _ => proptest::prop_assert!(false, "attachment must be a file"),
                    }
                }
            }
            _ => proptest::prop_assert!(false, "complete prompt required"),
        }
        let restored: ModelContent = serde_json::from_slice(&serde_json::to_vec(&content).unwrap()).unwrap();
        proptest::prop_assert_eq!(restored, content);
    }
}

#[path = "../../harness-fuzz/support/workspace_media.rs"]
mod workspace_media;

proptest::proptest! {
    // Classification preserves the full UTF-8 body, and codec round-trips preserve
    // the immutable descriptor. Invalid tails and NUL anywhere remain binary.
    #[test]
    fn workspace_media_matches_full_byte_and_unicode_roundtrips(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
        chars in proptest::collection::vec(proptest::prelude::any::<char>(), 0..256),
    ) {
        workspace_media::exercise(&bytes);
        workspace_media::exercise(chars.into_iter().collect::<String>().as_bytes());
    }
}

#[test]
fn media_oracle_checks_text_binary_and_invalid_tail_seeds() {
    for seed in [
        include_bytes!("../../harness-fuzz/corpus/client_mentions/utf8-workspace").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/binary-workspace").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/client_mentions/invalid-utf8-tail").as_slice(),
    ] {
        workspace_media::exercise(seed);
    }
}

#[test]
fn old_binary_default_fails_and_shrinks_the_same_media_property() {
    use proptest::prelude::*;
    use proptest::test_runner::{Config, TestError, TestRunner};
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&"[a-z]{1,32}", |text| {
        prop_assert!(workspace_media::matches(
            text.as_bytes(),
            "application/octet-stream"
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TestError::Fail(_, text)) if text == "a"));
}

#[test]
fn binary_as_text_mutant_fails_the_same_media_property() {
    assert!(!workspace_media::matches(b"\0binary", "text/plain"));
    assert!(!workspace_media::matches(b"bad\xff", "text/plain"));
}
