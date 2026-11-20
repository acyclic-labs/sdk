//! File mention projection and attachment policy against the shared parser oracle.
use acyclic_harness::{
    Error, Result,
    command_request::{CommandResolution, FileMention},
    conversation::{FileDescriptor, FileRef, VolumeRef},
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};

#[path = "../../../harness-fuzz/support/mentions.rs"]
mod oracle;

fn checked_input() -> Result<CommandResolution> {
    let mentions = [file(1, 1, 1, b"one")?, file(2, 1, 1, b"two")?]
        .into_iter()
        .enumerate()
        .map(|(index, file)| FileMention {
            start: index * 8,
            end: index * 8 + 7,
            file,
        })
        .collect();
    Ok(CommandResolution::Prompt {
        text: "@file-1 @file-2".into(),
        source: None,
        mentions,
    })
}

#[test]
fn fuzz_oracle_checks_every_seed() -> Result<()> {
    let fixtures = mention_oracle()?;
    for data in [
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/pinned-mentions").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/plain-prompt").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/view-selection").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/ordered-mentions").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/conflicting-metadata")
            .as_slice(),
    ] {
        let input: CommandResolution = serde_json::from_slice(data)
            .map_err(|error| Error::Invalid(format!("corpus input: {error}")))?;
        oracle::check(&input);
        fixtures.check(&input);
    }
    Ok(())
}

#[derive(Debug)]
enum Fault {
    DroppedAttachment,
    RejectedValid,
    AcceptedAction,
    AcceptedConflict,
    AcceptedInvalid,
}

impl Fault {
    fn tamper(
        &self,
        site: &str,
        actual: &mut acyclic_harness::Result<ModelContent>,
    ) -> acyclic_harness::Result<()> {
        match (self, site) {
            (Self::DroppedAttachment, "projection") => {
                let Ok(ModelContent::Parts(parts)) = actual else {
                    return Err(acyclic_harness::Error::Invalid(
                        "attachment control needs parts".into(),
                    ));
                };
                if parts.len() <= 1 {
                    return Err(acyclic_harness::Error::Invalid(
                        "attachment control needs an attachment".into(),
                    ));
                }
                parts.remove(1);
            }
            (Self::RejectedValid, "success") => {
                *actual = Err(acyclic_harness::Error::Invalid("broken".into()))
            }
            (Self::AcceptedAction, "actions")
            | (Self::AcceptedConflict, "conflict")
            | (Self::AcceptedInvalid, "invalid") => {
                *actual = Ok(ModelContent::Text("broken".into()))
            }
            _ => {}
        }
        Ok(())
    }
}

fn mention_oracle() -> Result<
    acyclic_test_support::differential::oracle::Oracle<CommandResolution, Fault, oracle::Seen>,
> {
    use acyclic_test_support::differential::oracle::Oracle;
    let mut builder = Oracle::declare(|input, fault: Option<&Fault>, see| {
        oracle::observe(
            input,
            |site, actual| {
                if let Some(fault) = fault {
                    let result = fault.tamper(site, actual);
                    assert!(result.is_ok(), "attachment control failed: {result:?}");
                }
            },
            |event| see.see(event),
        )
    });
    for (name, invariant) in oracle::INVARIANTS {
        builder = builder.invariant(name, invariant);
    }
    let mut conflict = checked_input()?;
    let CommandResolution::Prompt { mentions, .. } = &mut conflict else {
        unreachable!()
    };
    mentions.push(FileMention {
        start: 16,
        end: 23,
        file: file(1, 1, 1, b"changed")?,
    });
    Ok(builder
        .control("projection", checked_input()?, Fault::DroppedAttachment)
        .control("success", checked_input()?, Fault::RejectedValid)
        .control(
            "actions",
            CommandResolution::Sdk {
                command: "test".into(),
                arguments: String::new(),
            },
            Fault::AcceptedAction,
        )
        .control("conflict", conflict, Fault::AcceptedConflict)
        .control(
            "invalid",
            CommandResolution::Prompt {
                text: String::new(),
                source: None,
                mentions: Vec::new(),
            },
            Fault::AcceptedInvalid,
        )
        .build())
}

#[test]
fn every_conversion_invariant_detects_its_control_and_survives_deletion() -> Result<()> {
    mention_oracle()?.check(&checked_input()?);
    Ok(())
}

fn file(id: u8, revision: u8, volume: u8, body: &[u8]) -> Result<FileRef> {
    let fixture: FileRef =
        serde_json::from_str(include_str!("../../../harness/fixtures/v2/file-ref.json"))
            .map_err(|error| Error::Invalid(format!("file reference fixture: {error}")))?;
    let reference = fixture.volume();
    FileRef::new(
        VolumeRef::new(
            reference.provider().clone(),
            format!("volume-{volume}"),
            reference.class(),
            reference.owner().clone(),
            &mut None,
        )?,
        format!("file-{id}"),
        format!("revision-{revision}"),
        FileDescriptor::from_bytes(body, "text/plain", &mut None)?,
        "file".into(),
        &mut None,
    )
}

#[test]
fn conflicting_metadata_for_one_pinned_identity_is_rejected() -> Result<()> {
    let mentions = [b"first".as_slice(), b"second".as_slice()]
        .into_iter()
        .enumerate()
        .map(|(index, body)| {
            Ok(FileMention {
                start: index * 8,
                end: index * 8 + 7,
                file: file(1, 1, 1, body)?,
            })
        })
        .collect::<Result<_>>()?;
    assert!(matches!(
        CommandResolution::Prompt {
            text: "@file-1 @file-1".into(),
            source: None,
            mentions,
        }
        .into_model_input(),
        Err(acyclic_harness::Error::Conflict(_))
    ));
    Ok(())
}

proptest::proptest! {
    // Property: the shared fuzz oracle covers success and metadata-conflict
    // rejection for arbitrary repeated identities and file descriptors.
    #[test]
    fn fuzz_oracle_checks_arbitrary_metadata_conflicts(
        rows in proptest::collection::vec((0u8..4, proptest::collection::vec(proptest::prelude::any::<u8>(), 0..16)), 0..16),
    ) {
        let mentions = rows.into_iter().enumerate().map(|(index, (id, body))| Ok(FileMention {
            start: index * 8,
            end: index * 8 + 7,
            file: file(id, 1, 1, &body)?,
        })).collect::<Result<_>>()
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        let input = CommandResolution::Prompt { text: "inspect files".into(), source: None, mentions };
        mention_oracle()
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?
            .check(&input);
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
            let file = file(id, revision, volume, b"body")
                .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
            mentions.push(FileMention { start, end: text.len(), file });
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
                proptest::prop_assert_eq!(&parts[0], &ModelContentPart::text(original));
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

#[path = "../../../harness-fuzz/support/workspace_media.rs"]
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
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/utf8-workspace").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/binary-workspace").as_slice(),
        include_bytes!("../../../harness-fuzz/corpus/client_mentions/invalid-utf8-tail").as_slice(),
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
