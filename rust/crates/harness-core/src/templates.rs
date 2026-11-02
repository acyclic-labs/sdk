//! Creation snapshots from the effective template tree. Selection reads one
//! file, never a roster; the returned reference preserves that exact generation.
use crate::{
    Error, Result,
    agent::{CreationConfig, SpawnContext},
    context::Context,
    conversation::{ContentAccess, ContentResidencyVerifier, FileRef, VolumeRef},
    model::{ModelContent, ModelMessage, ModelRole},
};
use serde::Deserialize;
use std::sync::Arc;

const ROOT: &str = include_str!("seed/templates/orchestrator/AGENT.md");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Creation {
    version: u32,
    context: SpawnContext,
    may_spawn: Vec<String>,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
}

fn parse(bytes: &[u8], template: Option<FileRef>) -> Result<(CreationConfig, usize)> {
    let (creation, body): (Creation, _) = crate::context::parse_frontmatter_with_body(bytes)?;
    if creation.version != 1 {
        return Err(Error::Invalid(
            "unsupported template version; use version 1".into(),
        ));
    }
    for name in &creation.may_spawn {
        name_component(name)?;
    }
    for names in [Some(&creation.may_spawn), creation.allowed_tools.as_ref()]
        .into_iter()
        .flatten()
    {
        let mut unique = std::collections::BTreeSet::new();
        for name in names {
            if name.trim().is_empty() {
                return Err(Error::Invalid("empty template capability".into()));
            }
            if !unique.insert(name) {
                return Err(Error::Invalid("duplicate template capability".into()));
            }
        }
    }
    Ok((
        CreationConfig {
            context: creation.context,
            template,
            may_spawn: creation.may_spawn,
            allowed_tools: creation.allowed_tools,
        },
        body,
    ))
}

fn name_component(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
    {
        return Err(Error::Invalid(
            "template names must be single path components".into(),
        ));
    }
    Ok(())
}

/// The default root's creation context. The root's model remains host-selected.
pub fn root() -> Result<CreationConfig> {
    let (root, _) = parse(ROOT.as_bytes(), None)?;
    if root.context != SpawnContext::TopLevel {
        return Err(Error::Invalid("the root default must be top_level".into()));
    }
    Ok(root)
}

/// Checks creation capabilities before authority issuance or storage IO.
pub(crate) fn admit_child(parent: &CreationConfig, name: &str) -> Result<()> {
    name_component(name)?;
    if !parent.may_spawn.iter().any(|allowed| allowed == name) {
        return Err(Error::Invalid(format!(
            "the parent template does not admit {name}"
        )));
    }
    Ok(())
}

/// Reads the selected effective template once through the original authorized
/// content reader. A missing home file falls through to the immutable seed;
/// a refusal or an off marker never does. No later head lookup is performed.
pub(crate) async fn select(
    reader: &dyn ContentResidencyVerifier,
    home: &VolumeRef,
    seed: &VolumeRef,
    name: &str,
    access: ContentAccess<'_, '_>,
) -> Result<CreationConfig> {
    let path = format!("templates/{name}/AGENT.md");
    let read = reader.read_private_path(
        home,
        "",
        &path,
        None,
        crate::conversation::MAX_EXACT_JS_INTEGER,
        ContentAccess {
            ceiling: access.ceiling,
            io: access.io,
        },
    )?;
    let (volume, selected) = match read.await {
        Err(error) if error.code() == crate::wire::ErrorCode::NotFound => (
            seed,
            reader
                .read_private_path(
                    seed,
                    "",
                    &path,
                    None,
                    crate::conversation::MAX_EXACT_JS_INTEGER,
                    ContentAccess {
                        ceiling: access.ceiling,
                        io: access.io,
                    },
                )?
                .await?,
        ),
        result => (home, result?),
    };
    let (file, bytes, _hold) = selected;
    if file.path() != path || file.volume() != volume {
        return Err(Error::Conflict(
            "template reader returned another selection".into(),
        ));
    }
    file.descriptor().verify(&bytes, &mut None)?;
    if crate::layers::is_off(&bytes) {
        return Err(Error::NotFound(format!("template {name} is off")));
    }
    let (creation, _) = parse(&bytes, Some(file))?;
    if creation.context == SpawnContext::TopLevel {
        return Err(Error::Invalid(
            "a top_level template cannot be spawned".into(),
        ));
    }
    Ok(creation)
}

/// Projects the immutable directive, under the turn's existing reader. The
/// creation record contains a reference, so prose is never copied into actor
/// checkpoints and later template edits cannot reinterpret an existing child.
pub(crate) async fn directive(
    creation: &CreationConfig,
    reader: &dyn ContentResidencyVerifier,
) -> Result<Arc<Context>> {
    let bytes;
    let source = match &creation.template {
        Some(file) => {
            bytes = reader
                .read(file, file.descriptor().byte_length(), None, None)?
                .await?;
            bytes.as_ref()
        }
        None => ROOT.as_bytes(),
    };
    let (_, _, body) = crate::context::frontmatter_end(source)?;
    let text = std::str::from_utf8(&source[body..])
        .map_err(|_| Error::Invalid("template directive is not UTF-8".into()))?
        .trim();
    Ok(Arc::new(Context {
        messages: (!text.is_empty())
            .then(|| ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text.to_owned()),
            })
            .into_iter()
            .collect(),
        ..Context::default()
    }))
}
