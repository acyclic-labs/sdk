use super::execution;
use acyclic_harness::{
    Result,
    context::{
        ContextDiscovery, ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextPlacement,
        ContextReloadPolicy, ContextRoot, InstructionScope, SourceStage,
    },
    filesystem::MemoryHarnessStorage,
};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discovery {
    #[serde(default)]
    instructions: Vec<Root>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    policy: ContextDiscoveryPolicy,
    #[serde(default)]
    limits: ContextDiscoveryLimits,
    #[serde(default)]
    after_capture: BTreeMap<String, String>,
    reload: Option<ContextReloadPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    #[serde(default)]
    directory: String,
    #[serde(default)]
    active_directory: String,
}

impl Discovery {
    pub(super) async fn stage<
        P: acyclic_stream::StreamProvider,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    >(
        &self,
        storage: &MemoryHarnessStorage<P, A, O>,
    ) -> Result<SourceStage> {
        let root = |directory: &str| ContextRoot {
            volume: storage.volume().clone(),
            directory: directory.into(),
        };
        let declaration = ContextDiscovery {
            instructions: self
                .instructions
                .iter()
                .map(|scope| InstructionScope {
                    root: root(&scope.directory),
                    active_directory: scope.active_directory.clone(),
                })
                .collect(),
            skills: self
                .skills
                .iter()
                .map(|directory| root(directory))
                .collect(),
            policy: self.policy.clone(),
            limits: self.limits,
        };
        let mut captured = declaration.capture(storage.content_reader()).await?;
        execution::publish(storage, &self.after_capture).await?;
        if let Some(reload) = self.reload {
            captured = declaration
                .for_request(storage.content_reader(), &captured, reload)
                .await?;
        }
        captured.stage(ContextPlacement::Prepend)
    }
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
