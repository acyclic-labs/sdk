//! [`TurnEffects`] as internal files on the agent's private volume: one file
//! `effects/v4/<turn>/<segment>/<call>.json` per agent tool call, in the
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
            directory: format!("effects/v4/{turn}/{segment}"),
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
    fn recorded_call_with_io<'a>(
        &'a self,
        call: &'a str,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<TurnEffect>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let Some(io) = io else {
                return self.recorded_call(call).await;
            };
            valid_call(call, io)?;
            let (path, _path) =
                super::agent_internal::original_text(&[&self.directory, "/", call, ".json"], io)?;
            self.files
                .read_with_io(&path, self.maximum_bytes, io)
                .await?
                .as_deref()
                .map(|bytes| crate::agent::records::open_with_io(bytes, io))
                .transpose()
        })
    }

    fn record_with_io<'a>(
        &'a self,
        call: &'a str,
        effect: &'a TurnEffect,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, TurnEffect>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let Some(io) = io else {
                return self.record(call, effect).await;
            };
            valid_call(call, io)?;
            let (path, _path) =
                super::agent_internal::original_text(&[&self.directory, "/", call, ".json"], io)?;
            if let Some(bytes) = self
                .files
                .read_with_io(&path, self.maximum_bytes, io)
                .await?
            {
                return crate::agent::records::open_with_io(&bytes, io);
            }
            if let TurnEffect::Spawn { intent, .. } = effect {
                let child = self
                    .agent
                    .child_for_intent(intent.as_str(), &mut Some(&mut io.history_io()))?;
                let tail = crate::store::operations::tail_optional_with_work(
                    &self.stream,
                    &crate::agent::stream_parts_with_io(&[child.as_str()], io)?,
                    &mut Some(&mut io.history_io()),
                )
                .await?;
                if tail.is_some() {
                    return Err(io.invalid("an agent with this name exists").tool_input());
                }
            }
            #[derive(serde::Serialize)]
            struct Sealed<'a> {
                record: &'a TurnEffect,
                version: u32,
            }
            let bytes = io.encode(
                &Sealed {
                    record: effect,
                    version: crate::agent::RECORD_VERSION,
                },
                self.maximum_bytes
                    .min(crate::agent::MAX_RECORD_BYTES as u64),
                true,
            )?;
            match self
                .files
                .create_with_io(&path, &bytes, self.maximum_bytes, io)
                .await?
            {
                Some(existing) => crate::agent::records::open_with_io(&existing, io),
                None => crate::agent::records::open_with_io(&bytes, io),
            }
        })
    }

    fn recorded_with_io<'a>(
        &'a self,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<TurnEffect>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let Some(io) = io else {
                return self.recorded().await;
            };
            let names = self.files.list_with_io(&self.directory, io).await?;
            let capacity = crate::core::owned_buffer::<TurnEffect>(names.len())?;
            io.retain_owned(capacity)?;
            io.allocation_attempt(capacity)?;
            let mut effects = Vec::new();
            effects
                .try_reserve_exact(names.len())
                .map_err(Error::Allocation)?;
            for name in names {
                let (path, _path) =
                    super::agent_internal::original_text(&[&self.directory, "/", &name], io)?;
                let bytes = self
                    .files
                    .read_with_io(&path, self.maximum_bytes, io)
                    .await?
                    .ok_or_else(|| io.invalid("listed agent effect is missing"))?;
                effects.push(crate::agent::records::open_with_io(&bytes, io)?);
            }
            Ok(effects)
        })
    }

    fn recorded_call<'a>(
        &'a self,
        call: &'a str,
    ) -> BoxProviderFuture<'a, Result<Option<TurnEffect>>> {
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
            self.files
                .read(
                    &format!("{}/{call}.json", self.directory),
                    self.maximum_bytes,
                )
                .await?
                .as_deref()
                .map(decode)
                .transpose()
        })
    }

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
                    Ok(_) => return Err(Error::tool_rejected("an agent with this name exists")),
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

    fn holds_with_io<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let Some(io) = io else {
                return self.holds(operation).await;
            };
            let names = self.files.list_with_io(&self.directory, io).await?;
            for name in names {
                io.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                if !name
                    .strip_suffix(".json")
                    .is_some_and(|call| crate::agent::is_call_of(call, operation))
                {
                    return Ok(true);
                }
            }
            Ok(false)
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

fn valid_call(call: &str, io: &super::PreparationIo) -> Result<()> {
    if call.is_empty()
        || !call
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(io.invalid("agent effect call key is not a file name"));
    }
    Ok(())
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
        let publisher = super::super::FilesystemContentPublisher::new(
            host.clone(),
            &volume,
            &issuer.verifier(&mut None)?,
            &scope,
            4096,
            None,
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
            effects.recorded_call("0000000001-a").await?,
            Some(first.clone())
        );
        assert_eq!(effects.recorded_call("0000000003-c").await?, None);
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
        // The actual tool, private effect files and create-once publisher share
        // replay identity. A different call may not reuse a pending child name.
        {
            use crate::{
                agent::{AgentState, ForkAdopted},
                profile::{ModelProfile, Profiles, ProjectLayer, RoleFile},
                tool::{ToolInvocation, ToolRegistry},
            };
            const EMPTY: &str = "---\nversion: 1\n---\n";
            let profiles = Arc::new(Profiles::new(
                [ModelProfile::parse(
                    "---\nversion: 1\ntools: []\nreasoning: {mode: disabled}\ncache: automatic\nwatermark_tokens: 700\n---\n",
                    "---\nversion: 1\nkey: test/worker\n---\n",
                    crate::context::ModelContextCapacity {
                        context_tokens: 1000,
                        output_tokens: Some(200),
                    },
                    "test-renderer-1",
                )?],
                [RoleFile::parse(
                    "---\nversion: 1\nname: worker\nmodel: test/worker\ncontext: fork\nmay_spawn: [worker]\n---\n",
                )?],
                ProjectLayer::parse(EMPTY)?,
                EMPTY,
                crate::approvals::ApprovalPolicy::default().bind()?,
            )?);
            let mut state = AgentState::adopted(
                effects.agent.clone(),
                &ForkAdopted {
                    agent: effects.agent.id(),
                    parent: None,
                    profile: Some(profiles.role_pin("worker")?),
                    forked_at: 0,
                    conversation_tail: 0,
                    checkpoint: None,
                    volumes: Vec::new(),
                    brief: None,
                    policy: None,
                },
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            state.open_children.insert(
                crate::tools::spawned_child(&effects.agent, "existing")
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            let pending = Arc::new(FilesystemTurnEffects::new(
                files.clone(),
                effects.stream.clone(),
                effects.agent.clone(),
                (OperationId::from_bytes([5; 16]), 3),
                4096,
            ));
            let mut tools = ToolRegistry::default();
            crate::tools::bind_agent_tools(
                &mut tools,
                &state,
                (pending.clone(), None),
                publisher,
                4096,
                (&["spawn".into()], Some(profiles)),
            )?;
            let tool = tools
                .get("spawn", None, None)?
                .ok_or_else(|| Error::Storage("spawn declaration was not bound".into()))?;
            let invoke = |operation, name: &str, task: &str| ToolInvocation {
                operation_id: OperationId::from_bytes([operation; 16]),
                call_id: format!("spawn-{operation}"),
                name: "spawn".into(),
                arguments: serde_json::json!({"name": name, "task": task, "role": "worker"}),
            };
            let original = invoke(21, "candidate", "first brief");
            let (first, replay) = futures::join!(
                tool.executor.execute(original.clone(), None),
                tool.executor.execute(original, None),
            );
            assert_eq!(first?.value, replay?.value);
            for invocation in [
                invoke(22, "candidate", "first brief"),
                invoke(23, "candidate", "changed brief"),
                invoke(24, "existing", "replacement brief"),
            ] {
                assert!(matches!(
                    tool.executor.execute(invocation, None).await,
                    Err(Error::ToolRejected { .. })
                ));
            }
            tool.executor
                .execute(invoke(25, "independent", "another child"), None)
                .await?;
            let recorded = pending.recorded().await?;
            assert_eq!(recorded.len(), 2);
            let TurnEffect::Spawn { brief, .. } = &recorded[0] else {
                return Err(Error::Storage("original spawn effect was replaced".into()));
            };
            assert_eq!(
                brief.descriptor(),
                &crate::conversation::FileDescriptor::from_bytes(
                    b"first brief",
                    "text/markdown",
                    &mut None,
                )?
            );
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let original = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 32 * 1024 * 1024),
                &cancellation,
            )
            .map_err(|(cause, _)| Error::from(cause))?;
            let io = super::super::PreparationIo::from_native_work(&original)?;
            assert_eq!(
                effects
                    .recorded_call_with_io("0000000001-a", Some(&io))?
                    .await?,
                Some(first.clone())
            );
            let native = TurnEffect::Archive {
                target: effects.agent.clone(),
                id: IntentId::try_from("native-original")
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            };
            assert_eq!(
                effects
                    .record_with_io("0000000004-d", &native, Some(&io))?
                    .await?,
                native
            );
            assert_eq!(
                effects
                    .record_with_io("0000000004-d", &later, Some(&io))?
                    .await?,
                native
            );
            assert_eq!(
                effects
                    .recorded_call_with_io("0000000004-d", Some(&io))?
                    .await?,
                Some(native.clone())
            );

            assert!(effects.holds_with_io(own, Some(&io))?.await?);
            assert!(!alone.holds_with_io(own, Some(&io))?.await?);
            let policy = Arc::new(crate::approvals::AgentApprovalPolicy::new(
                crate::approvals::ApprovalPolicy::accept_all("rules"),
                None,
                Default::default(),
                crate::conversation::Limits::default(),
            )?);
            let approvals = crate::approvals::AgentApprovals::new(files.clone(), policy);
            let interaction = crate::interaction::Interaction::approval(
                "review".into(),
                own,
                [7; 32],
                crate::interaction::ApprovalSubject::Opaque {},
            )?;
            let approval_id = crate::InteractionId::from_bytes([11; 16]);
            approvals
                .open_with_io(approval_id, &interaction, Some(&io))
                .await?;
            assert_eq!(
                approvals.dispatch_with_io(approval_id, Some(&io)).await?,
                Some(crate::interaction::InteractionOutcome::Approved {})
            );
            // Reopening retains exactly the original request; changed action bytes
            // cannot be accepted as an acknowledgement or replace its decision.
            approvals
                .open_with_io(approval_id, &interaction, Some(&io))
                .await?;
            let changed = crate::interaction::Interaction::approval(
                "different action".into(),
                own,
                [8; 32],
                crate::interaction::ApprovalSubject::Opaque {},
            )?;
            let mismatch = approvals
                .open_with_io(approval_id, &changed, Some(&io))
                .await;
            assert!(matches!(
                mismatch,
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    ..
                })
            ));
            let human = crate::approvals::AgentApprovals::new(
                files.clone(),
                Arc::new(crate::approvals::AgentApprovalPolicy::new(
                    Default::default(),
                    None,
                    Default::default(),
                    crate::conversation::Limits::default(),
                )?),
            );
            let human_id = crate::InteractionId::from_bytes([12; 16]);
            human
                .open_with_io(human_id, &interaction, Some(&io))
                .await?;
            assert_eq!(human.dispatch_with_io(human_id, Some(&io)).await?, None);
            human
                .decide(
                    human_id,
                    crate::approvals::ApprovalResponder::Human("human"),
                    true,
                    None,
                )
                .await?;
            assert_eq!(
                human.dispatch_with_io(human_id, Some(&io)).await?,
                Some(crate::interaction::InteractionOutcome::Approved {})
            );
            let receipt = original.receipt();
            assert!(receipt.backend_read_operations > 0);
            assert!(receipt.backend_write_operations > 0);
            assert!(receipt.bytes_encoded > 0);
            assert!(
                matches!(old.recorded_call_with_io("0000000001-a", Some(&io))?.await,
                Err(Error::UnsupportedRecordVersion(version)) if version == refusal)
            );
            // Cancelling the original operation ends this invocation at its
            // cancellation event; no receipt or replacement is made.
            use futures::FutureExt as _;
            let pending =
                crate::approvals::until_stopped(Some(&io), std::future::pending::<Result<()>>());
            futures::pin_mut!(pending);
            assert!(pending.as_mut().now_or_never().is_none());
            cancellation.cancel();
            assert!(matches!(
                pending.await,
                Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    ..
                })
            ));
            let cancelled_approval = approvals
                .open_with_io(
                    crate::InteractionId::from_bytes([13; 16]),
                    &interaction,
                    Some(&io),
                )
                .await;
            assert!(matches!(
                cancelled_approval,
                Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    ..
                })
            ));
            let cancelled = match effects.record_with_io("0000000005-e", &native, Some(&io)) {
                Ok(operation) => operation.await,
                Err(error) => Err(error),
            };
            assert!(cancelled.is_err());
            assert_eq!(effects.recorded_call("0000000005-e").await?, None);
            assert_eq!(effects.recorded_call("0000000004-d").await?, Some(native));
        }
        Ok(())
    }
}
