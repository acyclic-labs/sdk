//! [`TurnGoal`] on the agent's [`GOAL_VOLUME`] binding: GOAL.md read once per
//! settled turn and rewritten through the ordinary content writer.

use super::FilesystemHost;
use crate::{
    Error, IdempotencyKey, Result,
    agent::{AgentState, TurnGoal},
    conversation::{ContentAccess, ContentGrant, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    goal::{GoalContinuationPolicy, GoalView},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Name of the volume binding that holds an agent's GOAL.md.
pub const GOAL_VOLUME: &str = "scratch";
const GOAL_PATH: &str = "GOAL.md";

/// The goal file of one agent turn.
pub(in crate::filesystem) struct FilesystemTurnGoal<A: 'static, O: 'static> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    read: ContentGrant,
    write: ContentGrant,
    maximum_bytes: u64,
    policy: GoalContinuationPolicy,
}

impl<A, O> FilesystemTurnGoal<A, O> {
    /// The goal of `agent` under `scope`; `None` when it binds no goal volume.
    pub(in crate::filesystem) fn bind(
        host: &Arc<FilesystemHost<A, O>>,
        agent: &AgentState,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
        policy: GoalContinuationPolicy,
    ) -> Result<Option<Self>> {
        let Some(binding) = agent
            .volumes
            .iter()
            .find(|binding| binding.name == GOAL_VOLUME)
        else {
            return Ok(None);
        };
        let volume = &binding.volume;
        Ok(Some(Self {
            host: Arc::clone(host),
            read: ContentGrant::verify_directory_read(verifier, scope, volume, "", &mut None)?,
            write: ContentGrant::verify(
                verifier,
                scope,
                volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )?,
            volume: volume.clone(),
            maximum_bytes,
            policy,
        }))
    }
}

impl<A, O> TurnGoal for FilesystemTurnGoal<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn policy(&self) -> GoalContinuationPolicy {
        self.policy
    }

    fn read(&self) -> BoxProviderFuture<'_, Result<Option<GoalView>>> {
        Box::pin(async move {
            let (source, bytes, _) = match self
                .host
                .read_private_path_indexed(
                    &self.volume,
                    &self.read,
                    GOAL_PATH,
                    None,
                    self.maximum_bytes,
                    ContentAccess::default(),
                )
                .await
            {
                Ok(file) => file,
                Err(Error::NotFound(_)) => return Ok(None),
                // An unusable goal (over the byte bound, or malformed) is no
                // goal: the turn settles as without one.
                Err(error) if error.code() == crate::wire::ErrorCode::Invalid => return Ok(None),
                Err(error) => return Err(error),
            };
            // A malformed goal is no goal: the turn settles as without one.
            match GoalView::from_file(source, &bytes, self.maximum_bytes) {
                Ok(goal) => Ok(Some(goal)),
                Err(Error::Invalid(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn rewrite<'a>(
        &'a self,
        goal: &'a GoalView,
        contents: &'a str,
        key: &'a str,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let key = IdempotencyKey::new(format!("{key}:{}", goal.source.version()))?;
            // Conditioned on the observed version: a concurrent edit wins and
            // the retried step decides again on the edited goal.
            self.host
                .put_content(
                    &self.volume,
                    &self.write,
                    GOAL_PATH,
                    contents.as_bytes(),
                    "text/markdown",
                    GOAL_PATH,
                    self.maximum_bytes,
                    &key,
                    Some(&self.host.file_generation(&goal.source, None)?),
                    ContentAccess::default(),
                )
                .await?;
            Ok(())
        })
    }
}
