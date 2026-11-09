//! The frozen model prefix of one agent between compactions.
//!
//! Every file-derived model input (instructions, skills, hooks and settings,
//! conventions, tools, environment) renders into one [`ContextManifest`]. An
//! agent's requests render the manifest of its current epoch, staged once at
//! session start or at its last compaction, so the cached span (system block
//! and tools) is byte-identical until the next compaction. A later change to a
//! source is an appended delta, never a rewrite of the prefix.

use super::*;
use crate::{Error, OperationId, conversation::ContentAccess, tool::ToolDefinition};

/// One agent's frozen context: the immutable manifest every request of the
/// epoch renders as its prefix. Request assembly takes this, never a live
/// manifest; [`ContextEpoch::open`] renders sources only for an epoch that has
/// no manifest yet.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextEpoch {
    manifest: FileRef,
}

impl ContextEpoch {
    /// The staged identity of `agent`'s epoch that begins at its context
    /// `checkpoint` (`None`: session start) over the `tools` it declares. A
    /// different tool set (a new build or tool configuration across a restart)
    /// starts a new epoch, because a frozen tool array must bind to the
    /// executors it names.
    pub fn operation(
        agent: &str,
        checkpoint: Option<u64>,
        tools: &[ToolDefinition],
    ) -> Result<OperationId> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.context-epoch.v1");
        hash.update(&(agent.len() as u64).to_le_bytes());
        hash.update(agent.as_bytes());
        hash.update(&[u8::from(checkpoint.is_some())]);
        hash.update(&checkpoint.unwrap_or(0).to_le_bytes());
        hash.update(&crate::contract::canonical_json_bytes(&tools)?);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(OperationId::from_bytes(bytes))
    }

    /// The private path of the epoch manifest staged under `operation`.
    #[must_use]
    pub fn path(operation: OperationId) -> String {
        format!("inputs/epochs/{operation}.manifest.json")
    }

    /// The epoch of an already staged, immutable manifest.
    #[must_use]
    pub const fn from_manifest(manifest: FileRef) -> Self {
        Self { manifest }
    }

    /// The epoch staged under `operation` on `publisher`, or, when it has none
    /// yet, the one `render` produces from the live sources, staged once. A
    /// concurrent first opener's manifest stands.
    pub async fn open<F>(
        publisher: &dyn ContentPublisher,
        operation: OperationId,
        limits: Limits,
        render: impl FnOnce() -> F,
    ) -> Result<Self>
    where
        F: Future<Output = Result<ContextManifest>>,
    {
        let path = Self::path(operation);
        if path.len() > limits.path_bytes {
            return Err(Error::Invalid(
                "context epoch path exceeds path bound".into(),
            ));
        }
        if let Some(manifest) = Self::observe(publisher, operation, &path, limits).await? {
            return Ok(Self { manifest });
        }
        let bytes = render().await?.encode(limits)?;
        let staged = publisher
            .stage_once(
                operation,
                &path,
                &bytes,
                "application/json",
                "context-manifest.json",
                ContentAccess::default(),
            )?
            .await;
        match staged {
            Ok((manifest, _)) => Ok(Self { manifest }),
            Err(error) => Self::observe(publisher, operation, &path, limits)
                .await?
                .map(|manifest| Self { manifest })
                .ok_or(error),
        }
    }

    async fn observe(
        publisher: &dyn ContentPublisher,
        operation: OperationId,
        path: &str,
        limits: Limits,
    ) -> Result<Option<FileRef>> {
        let (reference, _) = publisher
            .observe_staged_once(
                operation,
                None,
                // A harness record bounded by what it renders, not admitted
                // content under the file bound.
                crate::conversation::MAX_EXACT_JS_INTEGER,
                ContentAccess::default(),
            )?
            .await?;
        if let Some(reference) = &reference {
            limits.validate_record(reference, &mut None)?;
            if reference.path() != path {
                return Err(Error::Conflict(
                    "retained context epoch has another path".into(),
                ));
            }
        }
        Ok(reference)
    }

    /// The immutable manifest this epoch renders.
    #[must_use]
    pub const fn manifest(&self) -> &FileRef {
        &self.manifest
    }
}
