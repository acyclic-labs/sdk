use crate::{AlloySpec, AlloyTurn, ModelFeatures, Registry, ResolvedModel, invalid};
use acyclic_harness_core::{Result, context::ModelContextCapacity};
use std::sync::Arc;
#[path = "alloy_provider.rs"]
mod provider;

/// A validated immutable policy and providers. Configuration files remain authoritative.
pub struct ResolvedAlloy {
    spec: AlloySpec,
    models: Vec<Arc<ResolvedModel>>,
    capacity: ModelContextCapacity,
    model: acyclic_harness_core::model::Model,
}

impl Registry {
    /// Resolves a named policy or a one-member policy for a concrete model.
    pub fn resolve_selection(
        &self,
        selection: &str,
        required: ModelFeatures,
    ) -> Result<ResolvedAlloy> {
        let spec = self
            .alloys()
            .get(selection)
            .cloned()
            .unwrap_or_else(|| AlloySpec {
                models: vec![selection.into()],
                select: crate::AlloySelection::First,
            });
        let mut resolved = self.resolve_alloy(spec, required)?;
        resolved.model = if self.alloys().contains_key(selection) {
            acyclic_harness_core::model::Model::new(
                "alloy".into(),
                selection.into(),
                "alloy-v1".into(),
                serde_json::to_value(&resolved.spec)
                    .map_err(|_| invalid("invalid alloy identity"))?,
            )?
        } else {
            resolved
                .models
                .first()
                .ok_or_else(|| invalid("alloy has no members"))?
                .model()
                .clone()
        };
        Ok(resolved)
    }
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
            let model = self.resolve(selection)?;
            let (provider, name) = selection.split_once('/').expect("validated model identity");
            let entry = self
                .catalog()
                .get(provider)
                .and_then(|provider| provider.models.get(name))
                .ok_or_else(|| invalid("alloy model is absent from the provider catalog"))?;
            if !entry.features.contains(required) {
                return Err(invalid("alloy model lacks a required declared feature"));
            }
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
                model.capacity = capacity;
                Arc::new(model)
            })
            .collect();
        Ok(ResolvedAlloy {
            model: acyclic_harness_core::model::Model::new(
                "alloy".into(),
                "anonymous".into(),
                "alloy-v1".into(),
                serde_json::to_value(&spec).map_err(|_| invalid("invalid alloy identity"))?,
            )?,
            spec,
            models,
            capacity,
        })
    }
}

impl ResolvedAlloy {
    /// Immutable policy identity used to bind a task before its first step.
    pub fn model(&self) -> &acyclic_harness_core::model::Model {
        &self.model
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
