//! Independent conversion reference and observations shared by fuzzing/tests.
use acyclic_harness::{
    Error, Result,
    client::{CommandResolution, FileMention},
    model::{
        FileProjectionPolicy, ImageDetail, ModelContent, ModelContentPart, NativeMediaIntent,
        NativeMediaPolicy,
    },
};

#[derive(Debug)]
pub struct Seen {
    site: &'static str,
    equal: bool,
}

fn judge(event: &Seen, site: &str) -> std::result::Result<(), String> {
    if event.site != site || event.equal {
        Ok(())
    } else {
        Err(site.into())
    }
}

type Invariant = (&'static str, fn(&Seen) -> std::result::Result<(), String>);
pub const INVARIANTS: [Invariant; 5] = [
    ("actions", |event| judge(event, "actions")),
    ("conflict", |event| judge(event, "conflict")),
    ("invalid", |event| judge(event, "invalid")),
    ("success", |event| judge(event, "success")),
    ("projection", |event| judge(event, "projection")),
];

fn reference(input: &CommandResolution) -> std::result::Result<ModelContent, &'static str> {
    let CommandResolution::Prompt { text, mentions, .. } = input else {
        return Err("actions");
    };
    // Ordered linear specification, independent of the production index.
    let mut unique = Vec::<&FileMention>::new();
    for mention in mentions {
        let file = &mention.file;
        if let Some(previous) = unique.iter().find(|previous| {
            previous.file.volume() == file.volume()
                && previous.file.path() == file.path()
                && previous.file.version() == file.version()
        }) {
            if previous.file != mention.file || previous.image_input != mention.image_input {
                return Err("conflict");
            }
        } else {
            unique.push(mention);
        }
    }
    if text.is_empty()
        || unique
            .iter()
            .any(|mention| mention.file.validate(&mut None).is_err())
    {
        return Err("invalid");
    }
    Ok(if unique.is_empty() {
        ModelContent::Text(text.clone())
    } else {
        let mut parts = vec![ModelContentPart::Text { text: text.clone() }];
        for mention in unique {
            let policy = if mention.file.descriptor().media_type().starts_with("image/") {
                let image = mention.image_input.as_ref().ok_or("invalid")?;
                image.validate().map_err(|_| "invalid")?;
                FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                    intent: NativeMediaIntent::Image {
                        detail: ImageDetail::Auto,
                    },
                    maximum_bytes: image.maximum_bytes,
                    maximum_work: u64::from(image.maximum_tokens),
                    configuration: None,
                }))
            } else {
                FileProjectionPolicy::BoundedFull
            };
            parts.push(ModelContentPart::File {
                file: mention.file.clone(),
                policy,
            });
        }
        let content = ModelContent::Parts(parts);
        content
            .validate_user_input(&mut None)
            .map_err(|_| "invalid")?;
        content
    })
}

pub fn observe<E>(
    input: &CommandResolution,
    tamper: impl Fn(&str, &mut Result<ModelContent>),
    mut see: impl FnMut(Seen) -> std::result::Result<(), E>,
) -> std::result::Result<(), E> {
    // Fix the independent expectation before executing the production path.
    let expected = reference(input);
    let mut actual = input.clone().into_model_input();
    let expected = match expected {
        Err(site) => {
            tamper(site, &mut actual);
            let equal = match site {
                "actions" => matches!(actual, Err(Error::Invalid(_))),
                "conflict" => matches!(actual, Err(Error::Conflict(_))),
                _ => actual.is_err(),
            };
            return see(Seen { site, equal });
        }
        Ok(expected) => expected,
    };
    tamper("success", &mut actual);
    see(Seen {
        site: "success",
        equal: actual.is_ok(),
    })?;
    if actual.is_err() {
        return Ok(());
    }
    tamper("projection", &mut actual);
    let equal = actual.as_ref().is_ok_and(|content| {
        content == &expected
            && content.validate_user_input(&mut None).is_ok()
            && serde_json::to_vec(content)
                .ok()
                .and_then(|json| serde_json::from_slice::<ModelContent>(&json).ok())
                .as_ref()
                == Some(content)
    });
    see(Seen {
        site: "projection",
        equal,
    })
}

pub fn check(input: &CommandResolution) {
    observe::<std::convert::Infallible>(
        input,
        |_, _| {},
        |event| {
            for (name, invariant) in INVARIANTS {
                if let Err(detail) = invariant(&event) {
                    panic!("{name}: {detail}");
                }
            }
            Ok(())
        },
    )
    .unwrap_or_else(|never| match never {});
}
