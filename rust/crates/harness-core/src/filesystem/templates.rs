//! The filesystem admission for template selection, shared by both spawn entrances.
use super::{FilesystemContentVerifier, FilesystemHost, TurnAdmission, agent_authority};
use crate::{
    Result,
    agent::{AgentPath, CreationConfig, SpawnTemplates, VolumeBinding},
    core::RuntimeAuthority,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Reads templates under the same original authority as an agent's turns.
pub struct Templates<A: 'static, O: 'static> {
    host: Arc<FilesystemHost<A, O>>,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{AgentId, AgentPath, SpawnContext, VolumeBinding},
        conversation::{
            ContentAccess, ContentPublisher, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
        },
        core::{AggregateKind, Authority, AuthorityRoot},
        resources::ProviderRef,
    };

    #[tokio::test]
    async fn creation_keeps_the_selected_template_after_home_changes() -> Result<()> {
        let provider = ProviderRef::new(
            "templates".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
        let (seed, _) = host.seed_layer().await?;
        let path = AgentPath::root(&AgentId::from_bytes([91; 16]));
        let owner = crate::AgentId::from_bytes(path.id().to_bytes());
        let private = super::super::private_volume(provider.clone(), &path.id())?;
        let home = VolumeRef::new(
            provider,
            "home".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        host.create_volume(&private).await?;
        host.create_volume(&home).await?;
        let volumes = [
            VolumeBinding {
                name: super::super::PRIVATE_VOLUME.into(),
                volume: private,
            },
            VolumeBinding {
                name: super::super::HOME_VOLUME.into(),
                volume: home.clone(),
            },
        ];
        let issuer = RuntimeAuthority::from(AuthorityRoot::new(
            "templates",
            [92; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "templates".into(),
            },
        ));
        let templates = Arc::new(Templates::new(
            Arc::clone(&host),
            issuer.clone(),
            TurnAdmission::LocalRoot,
        ));
        let parent = crate::templates::root()?;
        let original = templates
            .child(&parent, &path, &volumes, "worker", None)
            .await?;
        assert_eq!(original.context, SpawnContext::Fork);
        assert_eq!(original.template.as_ref().unwrap().volume(), &seed);
        let admission = TurnAdmission::LocalRoot;
        let authority = agent_authority(&issuer, &admission, &path, &volumes, &[]).await?;
        let writer = super::super::FilesystemContentPublisher::new(
            Arc::clone(&host),
            &home,
            &authority.verifier,
            &authority.scope,
            Some(crate::conversation::MAX_EXACT_JS_INTEGER),
            None,
        )?;
        let put = |path: &'static str, bytes: &'static [u8]| {
            writer.stage(
                crate::OperationId::new(),
                path,
                bytes,
                "text/markdown",
                "AGENT.md",
                ContentAccess::default(),
            )
        };
        const FRESH: &[u8] = b"---\nversion: 1\ncontext: fresh\nmay_spawn: [worker]\n---\nRetain this exact directive.\n";
        put("templates/worker/AGENT.md", FRESH)?.await?;
        // An unrelated malformed template must not be discovered or parsed.
        put("templates/unrelated/AGENT.md", b"not valid frontmatter")?.await?;
        let selected = templates
            .child(&parent, &path, &volumes, "worker", None)
            .await?;
        assert_eq!(selected.context, SpawnContext::Fresh);
        let selected_file = selected.template.as_ref().unwrap();
        assert_eq!(selected_file.volume(), &home);
        put(
            "templates/worker/AGENT.md",
            b"---\nversion: 1\ncontext: fork\nmay_spawn: []\n---\nA later directive.\n",
        )?
        .await?;
        let later = templates
            .child(&parent, &path, &volumes, "worker", None)
            .await?;
        assert_eq!(later.context, SpawnContext::Fork);
        assert_ne!(later.template, selected.template);
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&host),
            &authority.verifier,
            &authority.scope,
            Some(crate::conversation::MAX_EXACT_JS_INTEGER),
            None,
        )?;
        let directive = crate::templates::directive(&selected, reader.as_ref()).await?;
        assert_eq!(
            directive.messages[0].content,
            crate::model::ModelContent::Text("Retain this exact directive.".into())
        );
        // A child reads its original parent-home template by the same exact
        // external-file grant its turn uses, without gaining that whole volume.
        let child = path
            .child(&AgentId::from_bytes([93; 16]))
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let child_private = super::super::private_volume(home.provider().clone(), &child.id())?;
        host.create_volume(&child_private).await?;
        let child_volumes = [VolumeBinding {
            name: super::super::PRIVATE_VOLUME.into(),
            volume: child_private,
        }];
        templates
            .child(&selected, &child, &child_volumes, "worker", None)
            .await?;
        // Capability lists remain in the pinned source even when much larger
        // than a stream frame; only its bounded reference enters records.
        let large = format!(
            "---\nversion: 1\ncontext: fork\nmay_spawn: [reviewer, {}]\nallowed_tools: [{}]\n---\nLarge capabilities.\n",
            "a".repeat(70 * 1024),
            "b".repeat(70 * 1024),
        );
        writer
            .stage(
                crate::OperationId::new(),
                "templates/worker/AGENT.md",
                large.as_bytes(),
                "text/markdown",
                "AGENT.md",
                ContentAccess::default(),
            )?
            .await?;
        let large_creation = templates
            .child(&parent, &path, &volumes, "worker", None)
            .await?;
        templates
            .child(&large_creation, &path, &volumes, "reviewer", None)
            .await?;
        use crate::agent::{AgentRecord, AgentState, ForkAdopted, TurnEffects};
        let adopted = ForkAdopted {
            agent: path.id(),
            parent: None,
            profile: Some(large_creation),
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: volumes.to_vec(),
            brief: None,
            policy: None,
        };
        let encoded = crate::agent::InboxRecord::ForkAdopted(Box::new(adopted.clone()))
            .encode()
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        assert!(encoded.len() < crate::agent::MAX_RECORD_BYTES);
        // A source change cannot widen an existing parent's creation rights.
        assert!(
            templates
                .child(&selected, &path, &volumes, "reviewer", None)
                .await
                .is_err()
        );

        // Simulate interruption after the original tool effect is durable but
        // before its result is saved. Recovery must not select a template again.
        let grant = crate::conversation::ContentGrant::verify(
            &authority.verifier,
            &authority.scope,
            &home,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let files = Arc::new(super::super::agent_internal::AgentInternalFiles::new(
            host.clone(),
            home.clone(),
            grant,
        ));
        let effects = Arc::new(super::super::agent_effects::FilesystemTurnEffects::new(
            files,
            Arc::new(acyclic_stream::MemoryStream::default()),
            path.clone(),
            (crate::OperationId::new(), 0),
            Some(crate::conversation::MAX_EXACT_JS_INTEGER),
        ));
        let mut root_adoption = adopted;
        root_adoption.profile = Some(parent.clone());
        let state = AgentState::adopted(path.clone(), &root_adoption)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let mut registry = crate::tool::ToolRegistry::default();
        crate::tools::bind_agent_tools(
            &mut registry,
            &state,
            (effects.clone(), None),
            writer.clone(),
            Some(crate::conversation::MAX_EXACT_JS_INTEGER),
            (&["spawn".into()], None, Some(templates.clone())),
        )?;
        let tool = registry
            .get("spawn", None, None)?
            .expect("registered spawn");
        let invocation = crate::tool::ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "create".into(),
            name: "spawn".into(),
            arguments: serde_json::json!({"name":"child", "task":"Own the task", "role":"worker"}),
        };
        let result = tool.executor.execute(invocation.clone(), None).await?;
        assert!(result.value.get("agent").is_some(), "{result:?}");
        let original_effects = effects.recorded().await?;
        assert_eq!(original_effects.len(), 1);
        assert!(
            crate::agent::records::seal(&original_effects[0])?.len()
                < crate::agent::MAX_RECORD_BYTES
        );
        // Off, malformed, traversal, top-level, capability and authority negatives
        // all end in typed refusal; none silently falls back to the seed.
        put("templates/worker/AGENT.md", b"off: true")?.await?;
        assert_eq!(
            tool.executor
                .reconcile(invocation.clone(), None)
                .await?
                .unwrap()
                .value,
            result.value,
        );
        assert_eq!(effects.recorded().await?, original_effects);
        let mut fresh_invocation = invocation.clone();
        fresh_invocation.operation_id = crate::OperationId::new();
        assert!(
            tool.executor
                .execute(fresh_invocation, None)
                .await?
                .value
                .get("agent")
                .is_none()
        );
        assert_eq!(
            templates
                .child(&parent, &path, &volumes, "worker", None)
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::NotFound
        );
        for malformed in [
            b"---\nversion: 2\ncontext: fork\nmay_spawn: []\n---\n".as_slice(),
            b"---\nversion: 1\ncontext: fork\nmay_spawn: []\n---\nInvalid body: \xff",
        ] {
            put("templates/worker/AGENT.md", malformed)?.await?;
            assert_eq!(
                tool.executor
                    .reconcile(invocation.clone(), None)
                    .await?
                    .unwrap()
                    .value,
                result.value,
            );
            assert!(matches!(
                templates
                    .child(&parent, &path, &volumes, "worker", None)
                    .await,
                Err(crate::Error::Invalid(_))
            ));
            let mut fresh = invocation.clone();
            fresh.operation_id = crate::OperationId::new();
            assert!(
                tool.executor
                    .execute(fresh, None)
                    .await?
                    .value
                    .get("agent")
                    .is_none()
            );
            assert_eq!(effects.recorded().await?, original_effects);
        }
        assert!(
            templates
                .child(&parent, &path, &volumes, "../worker", None)
                .await
                .is_err()
        );
        assert!(
            templates
                .child(&parent, &path, &volumes, "orchestrator", None)
                .await
                .is_err()
        );
        assert!(
            templates
                .child(&selected, &path, &volumes, "reviewer", None)
                .await
                .is_err()
        );
        let ceiling =
            crate::Capabilities::new([seed.capability(VolumeOperation::Read, &mut None)?]);
        assert!(
            templates
                .child(&parent, &path, &volumes, "worker", Some(&ceiling))
                .await
                .is_err()
        );
        assert!(
            templates
                .child(&parent, &path, &[], "worker", None)
                .await
                .is_err()
        );
        Ok(())
    }
}

impl<A, O> Templates<A, O> {
    /// Binds the host's verified admission; template text confers no authority.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
    ) -> Self {
        Self {
            host,
            issuer,
            admission,
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> SpawnTemplates
    for Templates<A, O>
{
    fn child<'a>(
        &'a self,
        creation: &'a CreationConfig,
        path: &'a AgentPath,
        volumes: &'a [VolumeBinding],
        name: &'a str,
        ceiling: Option<&'a crate::Capabilities>,
    ) -> BoxProviderFuture<'a, Result<CreationConfig>> {
        Box::pin(async move {
            crate::templates::name_component(name)?;
            let authority = agent_authority(
                &self.issuer,
                &self.admission,
                path,
                volumes,
                creation.template.as_slice(),
            )
            .await?;
            let home = volumes
                .iter()
                .find(|binding| binding.name == super::HOME_VOLUME)
                .or_else(|| {
                    volumes
                        .iter()
                        .find(|binding| binding.name == super::PRIVATE_VOLUME)
                })
                .ok_or_else(|| crate::Error::Invalid("parent has no home volume".into()))?;
            let seed = crate::seed::volume(home.volume.provider())?;
            let reader = FilesystemContentVerifier::new(
                Arc::clone(&self.host),
                &authority.verifier,
                &authority.scope,
                Some(crate::conversation::MAX_EXACT_JS_INTEGER),
                None,
            )?;
            crate::templates::admit_child(creation, reader.as_ref(), name, ceiling).await?;
            crate::templates::select(
                reader.as_ref(),
                &home.volume,
                &seed,
                name,
                crate::conversation::ContentAccess { ceiling, io: None },
            )
            .await
        })
    }
}
