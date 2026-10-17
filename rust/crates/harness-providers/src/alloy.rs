use crate::{AlloySpec, AlloyTurn, ModelFeatures, Registry, ResolvedModel, invalid};
use acyclic_harness::{Result, context::ModelContextCapacity};
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
            output_tokens: u32::MAX,
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
            capacity.output_tokens = capacity.output_tokens.min(model.capacity.output_tokens);
            models.push(model);
        }
        capacity.validate()?;
        let models = models
            .into_iter()
            .map(|mut model| {
                model.capacity = capacity;
                Arc::new(model)
            })
            .collect();
        Ok(ResolvedAlloy {
            spec,
            models,
            capacity,
        })
    }
}

impl ResolvedAlloy {
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
