//! Independent conversion oracle shared by fuzzing and negative controls.
use acyclic_harness::{
    Error, Result,
    client::CommandResolution,
    conversation::FileRef,
    model::{FileProjectionPolicy, ModelContent, ModelContentPart},
};

pub fn assert_conversion(input: &CommandResolution, actual: &Result<ModelContent>) {
    let CommandResolution::Prompt { text, mentions, .. } = input else {
        assert!(
            matches!(actual, Err(Error::Invalid(_))),
            "typed actions cannot become prompts"
        );
        return;
    };
    // Deliberately use an ordered linear specification, independently of the
    // production index/selection algorithm. Metadata must agree on duplicates.
    let mut unique = Vec::<&FileRef>::new();
    for mention in mentions {
        let file = &mention.file;
        if let Some(previous) = unique.iter().find(|previous| {
            previous.volume() == file.volume()
                && previous.path() == file.path()
                && previous.version() == file.version()
        }) {
            if *previous != file {
                assert!(
                    matches!(actual, Err(Error::Conflict(_))),
                    "conflicting file metadata must be rejected"
                );
                return;
            }
        } else {
            unique.push(file);
        }
    }
    if text.is_empty() || unique.iter().any(|file| file.validate(&mut None).is_err()) {
        assert!(actual.is_err(), "invalid prompt must be rejected");
        return;
    }
    let expected = if unique.is_empty() {
        ModelContent::Text(text.clone())
    } else {
        let mut parts = vec![ModelContentPart::Text { text: text.clone() }];
        parts.extend(unique.into_iter().map(|file| ModelContentPart::File {
            file: file.clone(),
            policy: FileProjectionPolicy::BoundedFull,
        }));
        ModelContent::Parts(parts)
    };
    let content = actual
        .as_ref()
        .expect("valid prompt conversion must succeed");
    assert_eq!(
        content, &expected,
        "conversion must preserve ordered unique pinned references"
    );
    content.validate_user_input(&mut None).unwrap();
    let json = serde_json::to_vec(content).unwrap();
    assert_eq!(
        serde_json::from_slice::<ModelContent>(&json).unwrap(),
        *content
    );
}
