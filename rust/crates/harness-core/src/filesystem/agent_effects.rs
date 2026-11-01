//! [`TurnEffects`] as internal files on the agent's private volume: one file
//! `effects/v3/<turn>/<segment>/<call>.json` per agent tool call, in the
//! internal interaction area, so no file tool can forge an effect past the
//! call's approval. Each segment's effects commit together, so a segment's
//! directory lists exactly the effects not yet committed.

use super::agent_internal::{AgentInternalFiles, InternalFileCreation, filename};
use crate::{
    Error, OperationId, Result,
    agent::{AgentPath, TurnEffect, TurnEffects},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken};
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
            let vacant = match self.files.prepare_create(&path, self.maximum_bytes).await? {
                InternalFileCreation::Existing(existing) => return decode(&existing),
                InternalFileCreation::Vacant(file) => file,
            };
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
            match vacant.create(&bytes).await? {
                Some(existing) => decode(&existing),
                None => Ok(effect.clone()),
            }
        })
    }

    fn recorded(&self) -> BoxProviderFuture<'_, Result<Vec<TurnEffect>>> {
        Box::pin(async move {
            let reader = self.files.reader().await?;
            let mut recorded = Vec::new();
            let mut pages = reader.directory(&self.directory);
            let cancellation = CancellationToken::new();
            while let Some(page) = pages.next(&cancellation).await? {
                for entry in &page.entries {
                    let name = filename(entry.name.as_bytes())?;
                    let bytes = reader
                        .read(&format!("{}/{name}", self.directory), self.maximum_bytes)
                        .await?
                        .ok_or_else(|| Error::Storage("listed agent effect is missing".into()))?;
                    recorded.push(decode(&bytes)?);
                }
            }
            Ok(recorded)
        })
    }

    fn holds(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<bool>> {
        Box::pin(async move {
            let reader = self.files.reader().await?;
            let mut pages = reader.directory(&self.directory);
            let cancellation = CancellationToken::new();
            while let Some(page) = pages.next(&cancellation).await? {
                for entry in &page.entries {
                    if !filename(entry.name.as_bytes())?
                        .strip_suffix(".json")
                        .is_some_and(|call| crate::agent::is_call_of(call, operation))
                    {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
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
        let files = Arc::new(AgentInternalFiles::new(host.clone(), volume.clone(), write));
        let effects = FilesystemTurnEffects::new(
            files.clone(),
            Arc::new(acyclic_stream::MemoryStream::default()),
            agent,
            (OperationId::from_bytes([5; 16]), 0),
            4096,
        );
        let effect = |name: &str| -> Result<TurnEffect> {
            Ok(TurnEffect::Spawn {
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

        // Preparing a body does not reserve the name. Two original absent
        // observations still arbitrate through the same create-once identity;
        // a stale observation cannot replace the first creator's effect.
        let raced_path = "create-observation/race.json";
        let InternalFileCreation::Vacant(first_creation) =
            files.prepare_create(raced_path, 4096).await?
        else {
            panic!("new file already exists");
        };
        let InternalFileCreation::Vacant(later_creation) =
            files.prepare_create(raced_path, 4096).await?
        else {
            panic!("preparation created a file");
        };
        let first_bytes = crate::agent::records::seal(&first)?;
        let later_bytes = crate::agent::records::seal(&later)?;
        assert!(first_creation.create(&first_bytes).await?.is_none());
        let replayed = later_creation
            .create(&later_bytes)
            .await?
            .expect("original winner");
        assert_eq!(decode(&replayed)?, first);
        let InternalFileCreation::Existing(existing) =
            files.prepare_create(raced_path, 4096).await?
        else {
            panic!("published original is missing");
        };
        assert_eq!(decode(&existing)?, first);
        let abandoned_path = "create-observation/abandoned.json";
        {
            let InternalFileCreation::Vacant(_abandoned) =
                files.prepare_create(abandoned_path, 4096).await?
            else {
                panic!("new file already exists");
            };
        }
        assert!(files.read(abandoned_path, 4096).await?.is_none());

        // Removing a file after selection changes a fresh read, but cannot
        // remove a listed effect from the retained original generation.
        let reader = files.reader().await?;
        let selected = reader.list(&effects.directory).await?;
        let removed = format!("{}/0000000001-a.json", effects.directory);
        let workspace = host
            .filesystem
            .open_workspace(
                volume.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(|failure| super::super::map_error(failure.error))?
            .value;
        workspace
            .remove(&format!(
                "/{}{removed}",
                super::super::InternalContentClass::Interaction.prefix(),
            ))
            .await
            .map_err(super::super::map_error)?;
        assert!(files.read(&removed, 4096).await?.is_none());
        assert_eq!(reader.list(&effects.directory).await?, selected);
        let mut retained = Vec::new();
        for name in selected {
            let bytes = reader
                .read(&format!("{}/{name}", effects.directory), 4096)
                .await?
                .ok_or_else(|| Error::Storage("selected effect disappeared".into()))?;
            retained.push(decode(&bytes)?);
        }
        assert_eq!(retained, [first, later.clone()]);
        assert_eq!(effects.recorded().await?, [later]);

        #[cfg(not(target_arch = "wasm32"))]
        {
            // One real atomic batch crosses a directory page. The foreign call
            // is beyond the first page, so a truncated hold check misses it.
            let paged = FilesystemTurnEffects::new(
                files.clone(),
                Arc::new(acyclic_stream::MemoryStream::default()),
                AgentPath::root(&crate::agent::AgentId::from_bytes([3; 16])),
                (OperationId::from_bytes([5; 16]), 3),
                4096,
            );
            let prefix = format!(
                "/{}{}",
                super::super::InternalContentClass::Interaction.prefix(),
                paged.directory,
            );
            let foreign = OperationId::from_bytes([8; 16]);
            let paths: Vec<_> = (0..257)
                .map(|step| {
                    format!(
                        "{prefix}/{}.json",
                        crate::agent::call_key(step, if step == 256 { foreign } else { own }),
                    )
                })
                .collect();
            let saved = effect("paged")?;
            let bytes = acyclic_stream::OwnedBytes::from(crate::agent::records::seal(&saved)?);
            let reply = format!("/{}/{own}/0001/reply", super::super::TOOL_OUTPUT);
            let reply_paths: Vec<_> = (0..257)
                .map(|chunk| format!("{reply}/{chunk:010}"))
                .collect();
            let reply_bytes: Vec<_> = (0..257)
                .map(|chunk| {
                    acyclic_stream::OwnedBytes::from(match chunk {
                        255 => vec![0xe2],
                        256 => vec![0x82, 0xac],
                        _ => vec![b'a'],
                    })
                })
                .collect();
            let older = format!("/{}/{own}/0000/reply", super::super::TOOL_OUTPUT);
            let older_path = format!("{older}/0000000000");
            let empty = format!("/{}/{own}/0002", super::super::TOOL_OUTPUT);
            let malformed = format!("/{}/{foreign}/0001/reply", super::super::TOOL_OUTPUT);
            let malformed_path = format!("{malformed}/0000000000");
            let mut batch: Vec<_> = paths
                .iter()
                .map(|path| (path.as_str(), bytes.clone()))
                .collect();
            batch.extend(
                reply_paths
                    .iter()
                    .zip(&reply_bytes)
                    .map(|(path, bytes)| (path.as_str(), bytes.clone())),
            );
            batch.push((&older_path, acyclic_stream::OwnedBytes::from(&b"older"[..])));
            batch.push((
                &malformed_path,
                acyclic_stream::OwnedBytes::from(vec![0xff]),
            ));
            let cancellation = CancellationToken::new();
            let mut transaction = workspace
                .begin_transaction(
                    acyclic_fs::IdempotencyKey::new(),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| super::super::map_error(failure.error))?
                .value;
            transaction
                .write_batch(
                    &[&prefix, &reply, &older, &empty, &malformed],
                    &batch,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| super::super::map_error(failure.error))?;
            let committed = transaction
                .commit::<acyclic_fs::WorkspaceError, _, _>(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                    |_| async {
                        Ok(acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::default(),
                        })
                    },
                )
                .await
                .map_err(|failure| super::super::map_error(failure.error))?;
            assert!(matches!(
                committed.value,
                acyclic_fs::TransactionCommit::Committed(_)
            ));
            // Chunk names cross the page boundary; UTF-8 crosses it too. An
            // empty newer dispatch cannot hide the selected complete reply.
            assert_eq!(
                crate::agent::TurnOutputs::partial(&*files, own).await?,
                format!("{}€", "a".repeat(255)),
            );
            assert!(matches!(
                crate::agent::TurnOutputs::partial(&*files, foreign).await,
                Err(Error::Storage(_))
            ));
            assert!(
                crate::agent::TurnOutputs::partial(&*files, OperationId::from_bytes([10; 16]))
                    .await?
                    .is_empty()
            );
            assert_eq!(paged.recorded().await?, vec![saved; paths.len()]);
            assert!(paged.holds(own).await?);
            assert!(paged.holds(foreign).await?);

            let reader = files.reader().await?;
            let mut pages = reader.directory(&paged.directory);
            let page = pages.next(&cancellation).await?.expect("first page");
            assert!(page.has_more);
            assert!(page.entries.iter().all(|entry| {
                filename(entry.name.as_bytes())
                    .unwrap()
                    .strip_suffix(".json")
                    .is_some_and(|call| crate::agent::is_call_of(call, own))
            }));
            workspace
                .remove(paths.last().unwrap())
                .await
                .map_err(super::super::map_error)?;
            let page = pages
                .next(&cancellation)
                .await?
                .expect("retained final page");
            assert!(!page.has_more);
            assert_eq!(page.entries.len(), 1);
            assert_eq!(
                filename(page.entries[0].name.as_bytes())?,
                format!("{}.json", crate::agent::call_key(256, foreign)),
            );
            // A completed cursor performs no further source work, even if the
            // next read would fail cancellation. An interrupted cursor is terminal.
            cancellation.cancel();
            assert!(pages.next(&cancellation).await?.is_none());
            let cancellation = CancellationToken::new();
            let mut interrupted = reader.directory(&paged.directory);
            assert!(interrupted.next(&cancellation).await?.is_some());
            cancellation.cancel();
            assert!(matches!(
                interrupted.next(&cancellation).await,
                Err(Error::Cancelled)
            ));
            assert!(interrupted.next(&CancellationToken::new()).await?.is_none());
            assert!(!paged.holds(own).await?);
        }
        Ok(())
    }
}
