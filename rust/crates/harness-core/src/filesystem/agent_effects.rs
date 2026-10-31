//! [`TurnEffects`] as internal files on the agent's private volume: one file
//! `effects/v3/<turn>/<segment>/<call>.json` per agent tool call, in the
//! internal interaction area, so no file tool can forge an effect past the
//! call's approval. Each segment's effects commit together, so a segment's
//! directory lists exactly the effects not yet committed.

use super::agent_internal::AgentInternalFiles;
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, TurnEffect, TurnEffects},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use std::sync::Arc;

/// The effects of one agent turn.
pub(in crate::filesystem) struct FilesystemTurnEffects<P: ?Sized, A: 'static, O: 'static> {
    files: Arc<AgentInternalFiles<A, O>>,
    stream: Arc<P>,
    agent: AgentPath,
    directory: String,
    maximum_bytes: u64,
}

impl<P: ?Sized, A, O> FilesystemTurnEffects<P, A, O> {
    pub(in crate::filesystem) fn new(
        files: Arc<AgentInternalFiles<A, O>>,
        stream: Arc<P>,
        agent: AgentPath,
        (turn, segment): (OperationId, u32),
        maximum_bytes: u64,
    ) -> Self {
        Self {
            files,
            stream,
            agent,
            directory: format!("effects/v3/{turn}/{segment}"),
            maximum_bytes,
        }
    }
}

/// A saved effect of any version but [`crate::agent::RECORD_VERSION`] is
/// refused whole.
fn decode(bytes: &[u8]) -> Result<TurnEffect> {
    crate::agent::records::open(bytes).map_err(|unopened| match unopened {
        crate::agent::records::Unopened::Version(version) => {
            Error::UnsupportedRecordVersion(version)
        }
        crate::agent::records::Unopened::Malformed => {
            Error::Storage("recorded agent effect is malformed".into())
        }
    })
}

impl<P, A, O> TurnEffects for FilesystemTurnEffects<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn record<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
    ) -> BoxProviderFuture<'a, Result<TurnEffect>> {
        Box::pin(async move {
            if call.is_empty()
                || !call
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            {
                return Err(Error::Invalid(
                    "agent effect call key is not a file name".into(),
                ));
            }
            // The call key already orders effects by model step. The file's
            // create-once identity supplies replay without a directory index.
            let path = format!("{}/{call}.json", self.directory);
            if let Some(existing) = self.files.read(&path, self.maximum_bytes).await? {
                return decode(&existing);
            }
            // A spawn names a new child: a name already taken would make the
            // step's fork conflict on every retry.
            if let TurnEffect::Spawn { intent, .. } = effect {
                let child = TurnEffect::child(&self.agent, intent)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                match crate::agent::inbox_tail(&*self.stream, &child).await {
                    Err(crate::agent::Error::NoSuchAgent) => {}
                    Ok(_) => return Err(Error::Conflict("an agent with this name exists".into())),
                    Err(error) => return Err(Error::Storage(error.to_string())),
                }
            }
            let bytes = crate::agent::records::seal(effect)?;
            match self.files.create(&path, &bytes, self.maximum_bytes).await? {
                Some(existing) => decode(&existing),
                None => Ok(effect.clone()),
            }
        })
    }

    fn recorded(&self) -> BoxProviderFuture<'_, Result<Vec<TurnEffect>>> {
        Box::pin(async move {
            let mut recorded = Vec::new();
            for name in self.files.list(&self.directory).await? {
                let bytes = self
                    .files
                    .read(&format!("{}/{name}", self.directory), self.maximum_bytes)
                    .await?
                    .ok_or_else(|| Error::Storage("listed agent effect is missing".into()))?;
                recorded.push(decode(&bytes)?);
            }
            Ok(recorded)
        })
    }

    fn holds(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<bool>> {
        Box::pin(async move {
            Ok(self.files.list(&self.directory).await?.iter().any(|name| {
                !name
                    .strip_suffix(".json")
                    .is_some_and(|call| crate::agent::is_call_of(call, operation))
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        agent::IntentId,
        conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityRoot},
        filesystem::FilesystemHost,
        resources::ProviderRef,
    };

    #[tokio::test]
    async fn call_files_replay_the_first_effect_of_this_record_version_by_key() -> Result<()> {
        let provider =
            ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([3; 16]));
        let owner = AgentId::from_bytes(agent.id().to_bytes());
        let volume = VolumeRef::new(
            provider,
            "effects-test".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityRoot::new(
            "effects",
            [4; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "effects".into(),
            },
        );
        let scope = issuer.bootstrap_scope(
            "owner",
            &Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]),
            Some(owner),
            &mut None,
        )?;
        let write = ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let files = Arc::new(AgentInternalFiles::new(host, volume.clone(), write));
        let effects = FilesystemTurnEffects::new(
            files.clone(),
            Arc::new(acyclic_stream::MemoryStream::default()),
            agent,
            (OperationId::from_bytes([5; 16]), 0),
            4096,
        );
        let effect = |name: &str| -> Result<TurnEffect> {
            Ok(TurnEffect::Spawn {
                profile: crate::agent::AgentProfilePin {
                    role: "worker".into(),
                    model: "test/worker".into(),
                    digest: [1; 32],
                    context: crate::agent::SpawnContext::Fork,
                },
                intent: IntentId::try_from(name)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                brief: Box::new(crate::conversation::FileRef::new(
                    volume.clone(),
                    format!("briefs/{name}.md"),
                    "version-1".into(),
                    crate::conversation::FileDescriptor::from_bytes(
                        name.as_bytes(),
                        "text/markdown",
                        &mut None,
                    )?,
                    name.into(),
                    &mut None,
                )?),
            })
        };
        let first = effect("first")?;
        let later = effect("later")?;
        assert_eq!(effects.record("0000000002-b", &later).await?, later);
        assert_eq!(effects.record("0000000001-a", &first).await?, first);
        assert_eq!(effects.record("0000000001-a", &later).await?, first);
        assert_eq!(
            effects.recorded().await?,
            vec![first.clone(), later.clone()]
        );
        // Effects of other calls hold a call until they commit; its own never do.
        let own = OperationId::from_bytes([9; 16]);
        assert!(effects.holds(own).await?);
        let alone = FilesystemTurnEffects::new(
            files.clone(),
            Arc::new(acyclic_stream::MemoryStream::default()),
            AgentPath::root(&crate::agent::AgentId::from_bytes([3; 16])),
            (OperationId::from_bytes([5; 16]), 1),
            4096,
        );
        assert!(!alone.holds(own).await?);
        alone
            .record(&crate::agent::call_key(3, own), &effect("own")?)
            .await?;
        assert!(!alone.holds(own).await?);
        assert!(alone.holds(OperationId::from_bytes([8; 16])).await?);
        assert_eq!(
            files.list(&effects.directory).await?,
            ["0000000001-a.json", "0000000002-b.json"]
        );
        for call in ["", "../escape", "slash/name", "dot.name"] {
            assert!(matches!(
                effects.record(call, &effect("invalid")?).await,
                Err(Error::Invalid(_))
            ));
        }
        // A saved effect of another version is refused whole, both by replay,
        // which would otherwise resume a spawn of another format, and by the
        // listing. Negative control: this version's `first`, saved under the
        // same call key of segment 0 above, replays.
        let old = FilesystemTurnEffects::new(
            files.clone(),
            Arc::new(acyclic_stream::MemoryStream::default()),
            AgentPath::root(&crate::agent::AgentId::from_bytes([3; 16])),
            (OperationId::from_bytes([5; 16]), 2),
            4096,
        );
        // A spawn effect of the headerless version 1, its brief a string.
        let saved = br#"{"spawn":{"brief":"review the diff","intent":"first"}}"#;
        assert_eq!(
            files
                .create(&format!("{}/0000000001-a.json", old.directory), saved, 4096)
                .await?,
            None
        );
        let refusal = crate::agent::UnsupportedRecordVersion {
            found: 1,
            expected: crate::agent::RECORD_VERSION,
        };
        assert!(matches!(
            old.record("0000000001-a", &first).await,
            Err(Error::UnsupportedRecordVersion(version)) if version == refusal
        ));
        assert!(matches!(
            old.recorded().await,
            Err(Error::UnsupportedRecordVersion(version)) if version == refusal
        ));
        assert_eq!(effects.record("0000000001-a", &later).await?, first);
        Ok(())
    }
}
