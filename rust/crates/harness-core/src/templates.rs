//! Creation snapshots from the effective template tree. Selection reads one
//! file, never a roster; the returned reference preserves that exact generation.
use crate::{
    Error, Result,
    agent::{CreationConfig, SpawnContext},
    context::Context,
    conversation::{ContentAccess, ContentResidencyVerifier, VolumeRef},
    model::{ModelContent, ModelMessage, ModelRole},
};
use serde::Deserialize;
use std::sync::Arc;

/// Select one model at admission: role policy wins, otherwise inherit the fallback.
#[must_use]
pub fn resolve_model<'a>(role: Option<&'a str>, fallback: Option<&'a str>) -> Option<&'a str> {
    role.or(fallback)
}

/// The model a root records when its composition serves `binding` and binds
/// other selections through `models`. Role policy wins at admission
/// ([`resolve_model`]), so a composition that cannot bind the root role's
/// model is refused here, when it is constructed, instead of recording a model
/// its turns cannot serve.
///
/// # Errors
/// [`Error::Invalid`] when the root role selects another model and `models` is
/// absent, or `models`' refusal of that model.
pub fn root_model(binding: &str, models: Option<&crate::model::ModelResolver>) -> Result<String> {
    let role = root()?.model;
    model_for_role(role.as_deref(), binding, models)
}

fn model_for_role(
    role: Option<&str>,
    binding: &str,
    models: Option<&crate::model::ModelResolver>,
) -> Result<String> {
    let selection = resolve_model(role, Some(binding)).unwrap_or(binding);
    if selection != binding {
        models
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "the root role selects {selection}, which the fixed binding {binding} does not serve; bind a catalog that resolves the role model"
                ))
            })?
            .resolve(selection)?;
    }
    Ok(selection.to_owned())
}

/// The model the root role selects, at `revision`: the binding of a provider
/// that serves any model, so its composition is never refused by
/// [`root_model`].
///
/// # Errors
/// When the root role selects no model.
pub fn root_role_model(revision: &str) -> Result<crate::model::Model> {
    let role = root()?
        .model
        .ok_or_else(|| Error::Invalid("the root role selects no model".into()))?;
    let (provider, name) = role
        .split_once('/')
        .ok_or_else(|| Error::Invalid("expected provider/model".into()))?;
    crate::model::Model::new(provider.into(), name.into(), revision.into())
}

/// The model the shipped `name` role template selects, if any.
///
/// # Errors
/// When no such template ships, or it does not parse.
pub fn seed_model(name: &str) -> Result<Option<String>> {
    let path = format!("templates/{name}/AGENT.md");
    let bytes = crate::seed::file(&path)
        .ok_or_else(|| Error::NotFound(format!("template {name} does not ship")))?;
    Ok(parse(bytes)?.model)
}

/// Every model a shipped role template selects.
///
/// # Errors
/// When a shipped template does not parse.
pub fn role_models() -> Result<std::collections::BTreeSet<String>> {
    let mut models = std::collections::BTreeSet::new();
    for (path, bytes) in crate::seed::tree() {
        if path.starts_with("templates/") && path.ends_with("/AGENT.md") {
            models.extend(parse(bytes)?.model);
        }
    }
    Ok(models)
}
const ROOT: &str = include_str!("seed/templates/orchestrator/AGENT.md");
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Creation {
    #[serde(default)]
    model: Option<String>,
    version: u32,
    context: SpawnContext,
    may_spawn: Vec<String>,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    question: bool,
}

fn parse(bytes: &[u8]) -> Result<Creation> {
    let (creation, body): (Creation, _) = crate::context::parse_frontmatter_with_body(bytes)?;
    std::str::from_utf8(&bytes[body..])
        .map_err(|_| Error::Invalid("template directive is not UTF-8".into()))?;
    if creation.version != 1 {
        return Err(Error::Invalid(
            "unsupported template version; use version 1".into(),
        ));
    }
    if let Some(model) = &creation.model {
        crate::model::selection::validate(model)?;
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
    Ok(creation)
}

pub(crate) fn name_component(name: &str) -> Result<()> {
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

/// The default root uses the orchestrator role's capabilities.
pub fn root() -> Result<CreationConfig> {
    root_with_topology(None)
}

pub(crate) fn root_with_topology(
    topology: Option<&crate::topology::Topology>,
) -> Result<CreationConfig> {
    let root = parse(ROOT.as_bytes())?;
    Ok(CreationConfig {
        question_plugin: false,
        topology: topology.cloned(),
        model: root.model,
        continuation: None,
        context: SpawnContext::TopLevel,
        template: None,
        peers: Default::default(),
    })
}

/// Checks capabilities from the parent's immutable creation source. Lists stay
/// in that file instead of growing every adoption and actor checkpoint.
pub(crate) async fn admit_child(
    parent: &CreationConfig,
    reader: &dyn ContentResidencyVerifier,
    name: &str,
    ceiling: Option<&crate::Capabilities>,
) -> Result<()> {
    name_component(name)?;
    let bytes;
    let source = match &parent.template {
        Some(file) => {
            bytes = reader
                .read(file, file.descriptor().byte_length(), ceiling, None)?
                .await?;
            bytes.as_ref()
        }
        None => ROOT.as_bytes(),
    };
    let parent = parse(source)?;
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
        None,
        ContentAccess {
            ceiling: access.ceiling,
            io: access.io,
        },
    )?;
    let (volume, selected) = match read.await? {
        None => (
            seed,
            reader
                .read_private_path(
                    seed,
                    "",
                    &path,
                    None,
                    None,
                    ContentAccess {
                        ceiling: access.ceiling,
                        io: access.io,
                    },
                )?
                .await?
                .ok_or_else(|| Error::NotFound(format!("template {name} does not exist")))?,
        ),
        Some(selected) => (home, selected),
    };
    let (file, bytes, _hold) = selected;
    if file.path() != path || file.volume() != volume {
        return Err(Error::Conflict(
            "template reader returned another selection".into(),
        ));
    }
    // The admitted content reader has already authenticated these exact bytes.
    if crate::layers::is_off(&bytes) {
        return Err(Error::NotFound(format!("template {name} is off")));
    }
    let creation = parse(&bytes)?;
    if creation.context == SpawnContext::TopLevel {
        return Err(Error::Invalid(
            "a top_level template cannot be spawned".into(),
        ));
    }
    Ok(CreationConfig {
        question_plugin: false,
        topology: None,
        model: creation.model,
        continuation: None,
        context: creation.context,
        template: Some(file),
        peers: Default::default(),
    })
}

/// Projects the immutable directive, under the turn's existing reader. The
/// creation record contains a reference, so prose is never copied into actor
/// checkpoints and later template edits cannot reinterpret an existing child.
pub(crate) struct Directive {
    pub digest: String,
    pub context: Arc<Context>,
    pub allowed_tools: Option<Vec<String>>,
    pub question: bool,
}

pub(crate) async fn directive(
    creation: &CreationConfig,
    reader: &dyn ContentResidencyVerifier,
) -> Result<Directive> {
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
    let capabilities = parse(source)?;
    let mut digest = blake3::Hasher::new();
    digest.update(source);
    if let Some(topology) = &creation.topology {
        digest.update(
            &serde_json::to_vec(topology).map_err(|error| Error::Invalid(error.to_string()))?,
        );
    }
    let (_, _, body) = crate::context::frontmatter_end(source)?;
    let text = std::str::from_utf8(&source[body..])
        .map_err(|_| Error::Invalid("template directive is not UTF-8".into()))?
        .trim();
    Ok(Directive {
        digest: digest.finalize().to_hex().to_string(),
        allowed_tools: capabilities.allowed_tools,
        question: capabilities.question,
        context: Arc::new(Context {
            messages: (!text.is_empty())
                .then(|| ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text(text.to_owned()),
                })
                .into_iter()
                .collect(),
            ..Context::default()
        }),
    })
}

#[cfg(test)]
mod binding_admission_tests {
    use super::*;

    #[test]
    fn explicit_role_models_require_a_serving_binding() -> Result<()> {
        assert_eq!(model_for_role(None, "fixture/fixed", None)?, "fixture/fixed");
        assert_eq!(
            model_for_role(Some("fixture/role"), "fixture/role", None)?,
            "fixture/role"
        );
        assert!(matches!(
            model_for_role(Some("fixture/role"), "fixture/fixed", None),
            Err(Error::Invalid(_))
        ));
        let refusing = crate::model::ModelResolver::new("fixture/fixed", |selection: &str| {
            Err(Error::NotFound(selection.to_owned()))
        });
        assert!(model_for_role(Some("fixture/role"), "fixture/fixed", Some(&refusing)).is_err());
        let binding: crate::model::ModelBinding = (
            crate::model::Model::new("fixture".into(), "role".into(), "1".into())?,
            Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new())),
        );
        let served = crate::model::ModelResolver::new("fixture/fixed", move |_: &str| {
            Ok(binding.clone())
        });
        assert_eq!(
            model_for_role(Some("fixture/role"), "fixture/fixed", Some(&served))?,
            "fixture/role"
        );
        Ok(())
    }
}
