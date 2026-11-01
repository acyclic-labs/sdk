use crate::{AlloySpec, AlloyTurn, ModelFeatures, Registry, ResolvedModel, invalid};
use acyclic_harness_core::{Result, context::ModelContextCapacity};
use std::sync::Arc;

/// A validated immutable policy and providers. Configuration files remain authoritative.
pub struct ResolvedAlloy {
    spec: AlloySpec,
    models: Vec<Arc<ResolvedModel>>,
    capacity: ModelContextCapacity,
}

impl Registry {
    /// Resolves every member and required feature before any turn is dispatched.
    /// Every selected provider uses the alloy's minimum context/output capacity.
    pub fn resolve_alloy(&self, spec: AlloySpec, required: ModelFeatures) -> Result<ResolvedAlloy> {
        spec.validate()?;
        let mut models = Vec::with_capacity(spec.models.len());
        let mut capacity = ModelContextCapacity {
            context_tokens: u32::MAX,
            output_tokens: None,
        };
        for selection in &spec.models {
            let (provider, name) = selection.split_once('/').expect("validated model identity");
            let entry = self
                .catalog()
                .get(provider)
                .and_then(|provider| provider.models.get(name))
                .ok_or_else(|| invalid("alloy model is absent from the provider catalog"))?;
            if !entry.features.contains(required) {
                return Err(invalid("alloy model lacks a required declared feature"));
            }
            let model = self.resolve(selection)?;
            capacity.context_tokens = capacity.context_tokens.min(model.capacity.context_tokens);
            capacity.output_tokens = capacity
                .output_tokens
                .into_iter()
                .chain(model.capacity.output_tokens)
                .min();
            models.push(model);
        }
        capacity.output_tokens = capacity
            .output_tokens
            .map(|limit| limit.min(capacity.context_tokens));
        capacity.validate()?;
        let models = models
            .into_iter()
            .map(|mut model| {
                model.selection_capacity = Some(capacity);
                let mut revision = blake3::Hasher::new();
                revision.update(b"acyclic.alloy-selection-bound.v1\0");
                serde_json::to_writer(&mut revision, &(&model.model.revision, capacity))
                    .map_err(|_| invalid("cannot encode alloy selection bound"))?;
                model.model.revision = format!("alloy-v1:{}", revision.finalize().to_hex());
                Ok(Arc::new(model))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ResolvedAlloy {
            spec,
            models,
            capacity,
        })
    }
}

impl ResolvedAlloy {
    /// Binds every member at its original hard capacity once for this role.
    /// The existing common selection reservation remains an additional bound.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn bind_profiles(
        &self,
        profiles: &acyclic_harness_core::profile::Profiles,
        pin: &acyclic_harness_core::agent::AgentProfilePin,
    ) -> Result<Self> {
        let models = self
            .spec
            .models
            .iter()
            .zip(&self.models)
            .map(|(key, model)| {
                let profile = profiles.for_model(pin, key)?;
                Ok(Arc::new(model.as_ref().clone().with_profile(&profile)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            spec: self.spec.clone(),
            models,
            capacity: self.capacity,
        })
    }

    /// Capacity used by compaction, independent of which member is selected.
    pub fn capacity(&self) -> ModelContextCapacity {
        self.capacity
    }

    /// Selects without I/O or mutation. Bind this provider and its real model
    /// identity into the turn; preserve the same key for retries/tool follow-ups.
    pub fn select(&self, turn: AlloyTurn<'_>) -> &Arc<ResolvedModel> {
        &self.models[self.spec.index(turn)]
    }
}
