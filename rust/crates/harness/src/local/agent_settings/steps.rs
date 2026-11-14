//! One local source of journal-retained per-step model and plugin selections.
use super::*;
use crate::{
    agent::{AgentState, WorkspaceRole},
    conversation::ContentResidencyVerifier,
    executor::{ExecutionJournal, PreparationIo, StepBinding, StepConfiguration, StockExecutor},
    filesystem::TurnAuthority,
};
use std::sync::{Mutex, MutexGuard, Weak};

type Current = Option<(blake3::Hash, Arc<StepBinding>)>;

pub(in crate::local) struct Steps {
    local: Weak<LocalHarness>,
    agent: AgentId,
    home: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    reader: Arc<dyn ContentResidencyVerifier>,
    template: StepBinding,
    current: Mutex<Current>,
}

impl Steps {
    pub(in crate::local) fn new(
        local: &Arc<LocalHarness>,
        executor: &StockExecutor,
        agent: &AgentState,
        authority: &TurnAuthority,
    ) -> Result<Self> {
        let volume = |role| {
            agent
                .volumes
                .iter()
                .find(|entry| entry.role == role)
                .and_then(|entry| entry.as_volume())
                .cloned()
                .ok_or_else(|| Error::Invalid("configuration source has no bound volume".into()))
        };
        Ok(Self {
            local: Arc::downgrade(local),
            agent: AgentId::from_bytes(agent.path.id_bytes()),
            home: volume(WorkspaceRole::Home)?,
            verifier: authority.issuer.verifier(&mut None)?,
            scope: authority.scope.copy(&mut None)?,
            reader: Arc::clone(&authority.content.reader),
            template: executor.step_template(),
            current: Mutex::new(None),
        })
    }

    fn current(&self) -> Result<MutexGuard<'_, Current>> {
        self.current.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "step configuration lock poisoned",
        })
    }
}

impl StepConfiguration for Steps {
    fn select<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Arc<StepBinding>>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let local = self.local.upgrade().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local harness is closed",
            })?;
            let volumes = Volumes {
                decisions: crate::config::SettingsAdmissions::new(
                    local.agent_stream.clone().ok_or_else(|| {
                        Error::Unsupported("settings needs its owner stream".into())
                    })?,
                ),
                files: &local.files,
                verifier: &self.verifier,
                signed: &self.scope,
                home: &self.home,
                seed: (&local.seed.0, &local.seed.1),
                configuration: local.harness.issuer_configuration(),
                maximum_bytes: local.limits.file_bytes,
            };
            let tree = Tree::for_step(&volumes, journal, operation, step, io).await?;
            let cut = tree.cut()?;
            {
                let current = self.current()?;
                if let Some((previous, binding)) = current.as_ref()
                    && *previous == cut
                {
                    return Ok(Some(Arc::clone(binding)));
                }
            }
            let resolver = local
                .models
                .as_deref()
                .map(|models| move |selection: &str| models.resolve_shared(selection));
            let resolve: Resolve<'_> = resolver
                .as_ref()
                .map(|resolve| resolve as &(dyn Fn(&str) -> Result<Arc<ResolvedModel>> + Sync));
            let settings = admit(
                &tree,
                self.agent,
                Some(Arc::clone(self.template.model_binding())),
                resolve,
                None,
                local.clock.as_ref(),
            )
            .await?;
            let sampling = settings.sampling.and_then(Sampling::model_sampling);
            let plugins = plugins(
                local.question_plugin,
                local.feedback_plugin,
                &tree,
                self.agent,
                local.clock.as_ref(),
            )
            .await?
            .into_shared()?;
            let binding = self.template.bind_configuration(
                (settings.model, sampling, settings.effort),
                plugins,
                Arc::clone(&self.reader),
                local.limits.file_bytes,
                io,
            )?;
            *self.current()? = Some((cut, Arc::clone(&binding)));
            Ok(Some(binding))
        })
    }
}
