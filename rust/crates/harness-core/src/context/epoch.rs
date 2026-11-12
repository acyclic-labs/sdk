//! The frozen model prefix of one agent between compactions.
//!
//! Every file-derived model input (instructions, skills, hooks and settings,
//! conventions, tools, environment) renders into one [`ContextManifest`]. An
//! agent's requests render the manifest of its current epoch, staged once at
//! session start or at its last compaction, so the cached span (system block
//! and tools) is byte-identical until the next compaction. A later change to a
//! source is an appended delta, never a rewrite of the prefix.

use super::*;
use crate::{Error, OperationId, conversation::ContentAccess};

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
    /// `checkpoint` (`None`: session start). Live tool edits append notices;
    /// only a different checkpoint starts a new prefix.
    pub fn operation(agent: &str, checkpoint: Option<u64>) -> Result<OperationId> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.context-epoch.v1");
        hash.update(&(agent.len() as u64).to_le_bytes());
        hash.update(agent.as_bytes());
        hash.update(&[u8::from(checkpoint.is_some())]);
        hash.update(&checkpoint.unwrap_or(0).to_le_bytes());
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
        if limits
            .path_bytes
            .is_some_and(|maximum| path.len() > maximum.get())
        {
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

    /// The model-visible sources of this epoch's manifest, as
    /// [`ContextManifest::digests`] keys them.
    pub async fn digests(
        &self,
        reader: &dyn ContentResidencyVerifier,
        limits: Limits,
    ) -> Result<BTreeMap<String, [u8; 32]>> {
        let (bytes, _hold) = crate::conversation::verified_content_bytes(
            reader,
            &self.manifest,
            limits.render_bytes,
            None,
            None,
        )
        .await?;
        ContextManifest::decode(&bytes, limits)?.digests()
    }

    /// The appended delta that brings a model told `told` up to `live`: one
    /// quiet notice per changed source, its body depending only on that
    /// source's current state, so it is valid whatever came before it and a
    /// coalesced run keeps only the latest. Tools and environment are fixed
    /// for the epoch and never announced.
    pub async fn changes(
        told: &BTreeMap<String, [u8; 32]>,
        live: &ContextManifest,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<Vec<ContextChange>> {
        let now = live.digests()?;
        let mut changes = Vec::new();
        let mut change = |source: &str, digest: Option<[u8; 32]>, body: Value| {
            changes.push(ContextChange {
                source: source.to_owned(),
                digest,
                notice: crate::conversation::wire::Notice {
                    source: "harness".into(),
                    kind: CONTEXT_CHANGED.into(),
                    body,
                    wake: false,
                    coalesce_key: Some(format!("{CONTEXT_CHANGED}:{source}")),
                },
            });
        };
        for instruction in &live.discovery.instructions {
            let key = instruction_key(instruction)?;
            if told.get(&key) == now.get(&key) {
                continue;
            }
            let source = instruction.source();
            let (volume, path) = (source.volume(), source.path());
            let length = instruction.descriptor().byte_length();
            let body = if length <= INLINE_TEXT_BYTES {
                let bytes = reader.read(instruction, length, None, None)?.await?;
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("project instructions are not UTF-8".into()))?;
                serde_json::json!({"version": 1, "volume": volume, "instructions": path, "text": text})
            } else {
                serde_json::json!({"version": 1, "volume": volume, "instructions": path, "bytes": length, "inline": false})
            };
            change(&key, now.get(&key).copied(), body);
        }
        for skill in &live.discovery.skills {
            let key = skill_key(skill)?;
            if told.get(&key) != now.get(&key) {
                change(
                    &key,
                    now.get(&key).copied(),
                    serde_json::json!({"version": 1, "skill": skill.summary()}),
                );
            }
        }
        if told.get("conventions") != now.get("conventions") {
            change(
                "conventions",
                now.get("conventions").copied(),
                serde_json::json!({"version": 1, "conventions": live.conventions}),
            );
        }
        for key in told.keys().filter(|key| !now.contains_key(*key)) {
            let body = match key.split_once(':') {
                Some(("skill", identity)) => {
                    let (name, _identity): (String, String) = serde_json::from_str(identity)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    serde_json::json!({"version": 1, "skill": {"name": name}, "removed": true})
                }
                Some(("instructions", identity)) => {
                    let (volume, path): (crate::conversation::VolumeRef, String) =
                        serde_json::from_str(identity)
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                    serde_json::json!({"version": 1, "volume": volume, "instructions": path, "removed": true})
                }
                _ => serde_json::json!({"version": 1, "source": key, "removed": true}),
            };
            change(key, None, body);
        }
        Ok(changes)
    }
}

/// The notice kind of an appended context change.
pub const CONTEXT_CHANGED: &str = "context.changed";

/// Instruction text up to this size travels in its notice; a larger file's
/// notice names it for the model to read, within one inbox record.
const INLINE_TEXT_BYTES: u64 = 32 * 1024;

/// One model-visible source that changed after an epoch froze.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextChange {
    /// The source, as [`ContextManifest::digests`] keys it.
    pub source: String,
    /// Its current digest; `None` once removed.
    pub digest: Option<[u8; 32]>,
    /// The quiet notice telling the model its current state.
    pub notice: crate::conversation::wire::Notice,
}

fn skill_key(skill: &SkillMetadata) -> Result<String> {
    let identity = serde_json::to_string(&(&skill.name, &skill.identity))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(format!("skill:{identity}"))
}
fn instruction_key(instruction: &crate::model::ProjectInstruction) -> Result<String> {
    let source = instruction.source();
    // Original source identity, never the retained body's storage location.
    // JSON framing keeps arbitrary volume labels and paths unambiguous.
    let identity = serde_json::to_string(&(source.volume(), source.path()))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(format!("instructions:{identity}"))
}

impl ContextManifest {
    /// The digest of each model-visible source, keyed by source: each
    /// instruction file by its original volume and path, each skill by name and original source, and the
    /// conventions. A changed digest is a change the model must be told.
    pub fn digests(&self) -> Result<BTreeMap<String, [u8; 32]>> {
        let mut digests = BTreeMap::new();
        for instruction in &self.discovery.instructions {
            digests.insert(
                instruction_key(instruction)?,
                *instruction.descriptor().sha256(),
            );
        }
        for skill in &self.discovery.skills {
            // A retained body can change without changing its path or
            // frontmatter. Its exact source identity is part of freshness.
            digests.insert(skill_key(skill)?, digest(skill)?);
        }
        digests.insert("conventions".into(), digest(&self.conventions)?);
        Ok(digests)
    }
}

fn digest<T: Serialize + ?Sized>(value: &T) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&crate::contract::canonical_json_bytes(value)?).as_bytes())
}
