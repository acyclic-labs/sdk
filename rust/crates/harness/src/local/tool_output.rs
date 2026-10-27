//! A live, read-only follow of one agent's tool output.
use super::{LocalHarness, storage};
use crate::{
    Error, Result,
    agent::AgentPath,
    conversation::{ContentAccess, ContentResidencyVerifier, VolumeRef},
    filesystem::{FilesystemContentVerifier, TOOL_OUTPUT, TurnAdmission, agent_authority},
};
use acyclic_stream::session::Client;
use std::sync::Arc;

/// One agent's tool output, followed through an in-process session over the
/// local filesystem's own change streams.
pub struct ToolOutput {
    /// Session pool serving the follows; register `credential` on it.
    pub client: Client,
    /// The `fs:/` resource of the agent's `tools/`; turn `t`'s calls are
    /// under `{resource}/{t}`.
    pub resource: String,
    /// Sealed credential to list and read under `resource`.
    pub credential: Vec<u8>,
    reader: Arc<dyn ContentResidencyVerifier>,
    volume: VolumeRef,
}

impl ToolOutput {
    /// Reads one output file named by a change follow: a path under
    /// `tools/` on the agent's private volume, at most `maximum_bytes`.
    ///
    /// # Errors
    /// Fails for a path outside `tools/`, a missing file or a larger one.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Vec<u8>> {
        let (_, bytes, _) = self
            .reader
            .read_private_path(
                &self.volume,
                TOOL_OUTPUT,
                path,
                None,
                maximum_bytes,
                ContentAccess::default(),
            )?
            .await?;
        Ok(bytes.to_vec())
    }
}

impl LocalHarness {
    /// A follow of `agent`'s tool output, derived from its own scope and
    /// bound to its audience.
    ///
    /// # Errors
    /// Fails for an embedded harness, an unknown agent, or a scope that does
    /// not grant reading its tool output.
    pub async fn tool_output(self: &Arc<Self>, agent: &AgentPath) -> Result<ToolOutput> {
        let (agents, _) = self.agents().await?;
        let state = agents.state(agent).await.map_err(storage)?;
        let issuer = self.root_authority.as_ref().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let stream = self
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Unsupported("an embedded harness has no agent stream".into()))?;
        let authority = agent_authority(
            issuer,
            &TurnAdmission::LocalRoot,
            &state.path,
            &state.volumes,
            &[],
        )?;
        let follow = self
            .files
            .tool_output_follow(&authority, &authority.private.volume)
            .await?;
        let changes = self.files.change_provider(Arc::clone(&stream))?;
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &authority.verifier,
            &authority.scope,
            self.limits.file_bytes,
            None,
        )?;
        Ok(ToolOutput {
            client: Client::in_process_with_resources(
                stream,
                follow.authority,
                Arc::new(changes),
                Arc::new(acyclic_stream::runtime::PlatformExecutor),
            ),
            resource: follow.resource,
            credential: follow.credential,
            reader,
            volume: authority.private.volume.clone(),
        })
    }
}

/// Bytes of the newest call's output a [`OutputTail`] retains.
const TAIL_BYTES: usize = 16 * 1024;
/// Largest output chunk a shell call stages.
pub(super) const CHUNK_BYTES: usize = 64 * 1024;

/// The live output of an agent's newest tool call: a follow of its whole
/// `tools/` directory that reads each new `stdout`/`stderr` chunk once.
pub struct OutputTail {
    output: ToolOutput,
    session: acyclic_harness_client::WireSession,
    /// Next record sequence not yet absorbed.
    next: u64,
    /// `tools/{turn}/{operation}` of the newest call.
    call: Option<String>,
    text: String,
}

impl ToolOutput {
    /// Follows this agent's tool output from its first record.
    ///
    /// # Errors
    /// Fails when the session refuses the credential or the follow.
    pub fn tail(self) -> Result<OutputTail> {
        use acyclic_stream::wire;
        let mut session = acyclic_harness_client::WireSession::new(
            self.client.clone(),
            acyclic_harness_client::DemandPolicy::default(),
        );
        session
            .register_credential(0, self.credential.clone().into())
            .map_err(storage)?;
        session
            .begin_resource(wire::SessionSubscription {
                credential: 0,
                filter: None,
                channel: None,
                resume: Some(wire::SessionCursor {
                    next: 0,
                    opaque: Default::default(),
                }),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: self.resource.clone(),
                        delivery: wire::SessionDelivery::All as i32,
                        ..Default::default()
                    },
                )),
            })
            .map_err(storage)?;
        Ok(OutputTail {
            output: self,
            session,
            next: 0,
            call: None,
            text: String::new(),
        })
    }
}

impl OutputTail {
    /// Completes when the follow delivers a frame; cancel-safe.
    ///
    /// # Errors
    /// Fails when the follow is refused or ends.
    pub async fn changed(&mut self) -> Result<()> {
        let resource = &self.output.resource;
        let session = &mut self.session;
        futures::future::poll_fn(|context| session.poll_resource(resource, context))
            .await
            .map_err(storage)?
            .map(drop)
            .ok_or_else(|| Error::Storage("tool output follow ended".into()))
    }

    /// Reads the output chunks the follow has announced since the last call.
    /// Only the newest call's chunks are read, so a reopened history costs
    /// one call's output, not every call's.
    ///
    /// # Errors
    /// Fails when an announced chunk cannot be read.
    pub async fn absorb(&mut self) -> Result<()> {
        let Some(view) = self.session.resource_view(&self.output.resource) else {
            return Ok(());
        };
        let mut chunks = Vec::new();
        for record in view.records().filter(|record| record.sequence >= self.next) {
            // Filesystem change records carry their paths in the compact
            // change list, not as wire keys.
            let changes = acyclic_stream::decode_changes(&record.value).map_err(storage)?;
            for entry in &changes.entries {
                if let Ok(path) = std::str::from_utf8(&entry.key)
                    && let Some((call, _)) = path
                        .split_once("/stdout/")
                        .or_else(|| path.split_once("/stderr/"))
                {
                    chunks.push((call.to_owned(), path.to_owned()));
                }
            }
        }
        let next = view.next_sequence();
        let Some((newest, _)) = chunks.last() else {
            self.next = next;
            return Ok(());
        };
        let newest = newest.clone();
        // Stage the display and commit it with the cursor only after every
        // announced read settles: a failed or cancelled absorb changes
        // nothing, so a retry reads the same announcements exactly once.
        let mut text = if self.call.as_ref() == Some(&newest) {
            self.text.clone()
        } else {
            String::new()
        };
        for (call, path) in chunks {
            if call == newest {
                let bytes = self.output.read(&path, CHUNK_BYTES as u64).await?;
                text.push_str(&String::from_utf8_lossy(&bytes));
                keep_tail(&mut text);
            }
        }
        self.next = next;
        self.call = Some(newest);
        self.text = text;
        Ok(())
    }

    /// The newest call's output, its last [`TAIL_BYTES`] at most.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Bounds the staged display to its last [`TAIL_BYTES`] on a char boundary.
fn keep_tail(text: &mut String) {
    if text.len() > TAIL_BYTES {
        let mut cut = text.len() - TAIL_BYTES;
        while !text.is_char_boundary(cut) {
            cut += 1;
        }
        text.drain(..cut);
    }
}
