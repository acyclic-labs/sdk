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
        let templates = Templates::new(Arc::clone(&host), issuer.clone(), TurnAdmission::LocalRoot);
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
            crate::conversation::MAX_EXACT_JS_INTEGER,
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
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        let directive = crate::templates::directive(&selected, reader.as_ref()).await?;
        assert_eq!(
            directive.messages[0].content,
            crate::model::ModelContent::Text("Retain this exact directive.".into())
        );
        // Off, malformed, traversal, top-level, capability and authority negatives
        // all end in typed refusal; none silently falls back to the seed.
        put("templates/worker/AGENT.md", b"off: true")?.await?;
        assert_eq!(
            templates
                .child(&parent, &path, &volumes, "worker", None)
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::NotFound
        );
        put(
            "templates/worker/AGENT.md",
            b"---\nversion: 2\ncontext: fork\nmay_spawn: []\n---\n",
        )?
        .await?;
        assert!(
            templates
                .child(&parent, &path, &volumes, "worker", None)
                .await
                .is_err()
        );
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
            crate::templates::admit_child(creation, name)?;
            let authority =
                agent_authority(&self.issuer, &self.admission, path, volumes, &[]).await?;
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
                crate::conversation::MAX_EXACT_JS_INTEGER,
                None,
            )?;
            crate::templates::child(
                reader.as_ref(),
                &home.volume,
                &seed,
                creation,
                name,
                crate::conversation::ContentAccess { ceiling, io: None },
            )
            .await
        })
    }
}
