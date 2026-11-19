use super::{ScriptedModelProvider, ScriptedTurn};
use crate::{
    Error, Result,
    agent::{AgentState, WorkspaceTable},
    conversation::{ContentAccess, validate_content_path},
    model::{ModelContent, ModelContentPart},
    runtime::ContentBindings,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, sync::Arc};

/// Immutable script selected within an agent's primary workspace, or its
/// private volume if it has no primary workspace.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeScriptSource {
    /// Workspace-relative content path.
    pub path: String,
    /// Lowercase SHA-256 of the exact file bytes.
    pub sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Script {
    version: u32,
    #[serde(default)]
    prompts: BTreeMap<String, Vec<ScriptedTurn>>,
    #[serde(default)]
    child_settled: Vec<ScriptedTurn>,
}

pub(super) enum VolumeScript {
    Source(VolumeScriptSource),
    Bound(Vec<ScriptedTurn>),
}

impl ScriptedModelProvider {
    /// Configures an immutable volume script. An authenticated workspace is
    /// bound when the harness opens the turn. No network or credentials are used.
    pub fn from_volume(source: VolumeScriptSource) -> Result<Self> {
        validate_content_path(&source.path, &mut None)?;
        if source.sha256.len() != 64
            || !source
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::Invalid(
                "scripted model requires a lowercase SHA-256".into(),
            ));
        }
        let mut provider = Self::new([]);
        provider.volume = Some(Arc::new(VolumeScript::Source(source)));
        Ok(provider)
    }
}

impl VolumeScript {
    pub(super) async fn bind(
        &self,
        agent: &AgentState,
        input: &ModelContent,
        content: &ContentBindings,
        maximum: Option<u64>,
    ) -> Result<Self> {
        let Self::Source(source) = self else {
            return Err(Error::Invalid(
                "scripted model workspace is already bound".into(),
            ));
        };
        let workspace = WorkspaceTable::primary_in(&agent.volumes);
        let (volume, root) = match workspace {
            Some(workspace) => (workspace.volume()?, workspace.root.as_str()),
            None => (&content.volume, ""),
        };
        let path = if root.is_empty() {
            source.path.clone()
        } else {
            format!("{root}/{}", source.path)
        };
        let (_, bytes, _) = content
            .reader
            .read_private_path(
                volume,
                root,
                &path,
                None,
                maximum,
                ContentAccess {
                    ceiling: None,
                    io: None,
                },
            )?
            .await?
            .ok_or_else(|| Error::Invalid("scripted model file is absent".into()))?;
        if hex::encode(Sha256::digest(bytes.as_slice())) != source.sha256 {
            return Err(Error::Conflict(
                "scripted model file SHA-256 mismatch".into(),
            ));
        }
        let mut script: Script = serde_json::from_slice(bytes.as_slice())
            .map_err(|_| Error::Invalid("invalid scripted model file".into()))?;
        if script.version != 1 {
            return Err(Error::Invalid(
                "unsupported scripted model file version".into(),
            ));
        }
        let replies = if input.notice_parts().is_some_and(|(notices, _)| {
            notices
                .iter()
                .any(|notice| notice.wake && notice.kind == "child.settled")
        }) {
            Some(script.child_settled)
        } else {
            match input {
                ModelContent::Text(text) => script.prompts.remove(text),
                input => match input.parts().first() {
                    Some(ModelContentPart::Text { text, .. }) => script.prompts.remove(text),
                    Some(ModelContentPart::File { file, .. }) => {
                        let prompt = content_prompt(content, file, maximum).await?;
                        script.prompts.remove(prompt.as_str())
                    }
                    _ => None,
                },
            }
        };
        Ok(Self::Bound(replies.unwrap_or_default()))
    }

    pub(super) fn select(&self, step: u32) -> Result<&ScriptedTurn> {
        let Self::Bound(replies) = self else {
            return Err(Error::Invalid(
                "scripted model workspace is not bound".into(),
            ));
        };
        replies
            .get(step as usize)
            .ok_or_else(|| Error::Invalid(format!("scripted model has no reply for step {step}")))
    }
}

async fn content_prompt(
    content: &ContentBindings,
    file: &crate::conversation::FileRef,
    maximum: Option<u64>,
) -> Result<String> {
    let length = file.descriptor().byte_length();
    let bytes = content
        .reader
        .read(file, maximum.unwrap_or(length), None, None)?
        .await?;
    std::str::from_utf8(bytes.as_slice())
        .map(str::to_owned)
        .map_err(|_| Error::Invalid("scripted model input is not UTF-8".into()))
}
