//! Shared provider-side measurement for local swarm integration fixtures.
//!
//! The production executor only accepts budgeted providers that bind the
//! journal dispatch identity to measurements made at the provider boundary.
//! These helpers keep the test providers honest: the source is owned by the
//! provider, keyed by the authenticated operation/dispatch pair, and updated
//! from the events actually emitted by the fixture stream.

use acyclic_harness::{
    Error, IdempotencyKey, OperationId,
    contract::canonical_json_bytes,
    model::{ModelEvent, ProviderDispatchContext},
    swarm_budget::{SwarmUsage, SwarmUsageSource},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Default)]
pub struct FixtureUsage {
    provider: String,
    usage: Mutex<BTreeMap<(OperationId, String), SwarmUsage>>,
}

impl FixtureUsage {
    pub fn new(provider: &'static str) -> Arc<Self> {
        Arc::new(Self {
            provider: provider.into(),
            usage: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn begin(&self, dispatch: &ProviderDispatchContext) {
        if let Ok(mut usage) = self.usage.lock() {
            usage
                .entry((dispatch.operation_id, dispatch.dispatch_id.0.clone()))
                .or_default()
                .model_steps = 1;
        }
    }

    pub fn record(&self, dispatch: &ProviderDispatchContext, started: Instant, event: &ModelEvent) {
        let bytes = canonical_json_bytes(event)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        if let Ok(mut usage) = self.usage.lock() {
            let entry = usage
                .entry((dispatch.operation_id, dispatch.dispatch_id.0.clone()))
                .or_default();
            entry.output_bytes = entry.output_bytes.saturating_add(bytes);
            entry.execution_time_ms = entry
                .execution_time_ms
                .max(started.elapsed().as_millis() as u64);
        }
    }

    pub fn record_events(
        &self,
        dispatch: &ProviderDispatchContext,
        started: Instant,
        events: &[ModelEvent],
    ) {
        for event in events {
            self.record(dispatch, started, event);
        }
    }
}

impl SwarmUsageSource for FixtureUsage {
    fn provider_identity(&self) -> &str {
        &self.provider
    }

    fn cumulative_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
    ) -> acyclic_harness::Result<SwarmUsage> {
        self.usage
            .lock()
            .map_err(|_| Error::Conflict("fixture usage lock poisoned".into()))
            .map(|usage| {
                usage
                    .get(&(operation_id, dispatch_id.0.clone()))
                    .copied()
                    .unwrap_or_default()
            })
    }
}

pub fn record_result(
    usage: &FixtureUsage,
    dispatch: &ProviderDispatchContext,
    started: Instant,
    event: &acyclic_harness::Result<ModelEvent>,
) {
    if let Ok(event) = event {
        usage.record(dispatch, started, event);
    }
}

/// Adds the provider boundary methods required by a budgeted fixture whose
/// actual `generate`/`reconcile` implementation is in the surrounding test.
#[macro_export]
macro_rules! fixture_budget_methods {
    () => {
        fn supports_dispatch_context(&self) -> bool {
            true
        }

        fn swarm_usage_source(
            &self,
        ) -> Option<Arc<dyn acyclic_harness::swarm_budget::SwarmUsageSource>> {
            Some(self.usage.clone())
        }

        fn generate_with_dispatch<'a>(
            &'a self,
            prepared: acyclic_harness::model_input::PreparedModelInput,
            dispatch: ProviderDispatchContext,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            self.usage.begin(&dispatch);
            let started = std::time::Instant::now();
            let stream = self.generate(prepared);
            let usage = self.usage.clone();
            Box::pin(stream.map(move |event| {
                swarm_provider_support::record_result(&usage, &dispatch, started, &event);
                event
            }))
        }

        fn reconcile_admitted_with_dispatch<'a>(
            &'a self,
            prepared: acyclic_harness::model_input::PreparedModelInput,
            attempt: acyclic_harness::model::ModelAttempt,
            dispatch: ProviderDispatchContext,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            let usage = self.usage.clone();
            Box::pin(async move {
                if dispatch.operation_id != attempt.operation_id
                    || dispatch.request_digest != attempt.request_digest
                {
                    return Err(Error::Conflict(
                        "fixture dispatch context does not match admitted attempt".into(),
                    ));
                }
                let started = std::time::Instant::now();
                let events = self.reconcile_admitted(prepared, attempt).await?;
                if let Some(events) = &events {
                    usage.record_events(&dispatch, started, events);
                }
                Ok(events)
            })
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_is_keyed_by_the_authenticated_dispatch() {
        let source = FixtureUsage::new("harness.test.provider");
        let first = ProviderDispatchContext {
            operation_id: OperationId::from_bytes([1; 16]),
            step: 0,
            request_digest: [2; 32],
            dispatch_id: IdempotencyKey::new("dispatch-a").expect("dispatch id"),
        };
        let second = ProviderDispatchContext {
            operation_id: first.operation_id,
            step: 1,
            request_digest: [3; 32],
            dispatch_id: IdempotencyKey::new("dispatch-b").expect("dispatch id"),
        };
        source.begin(&first);
        source.record(
            &first,
            Instant::now(),
            &ModelEvent::Content {
                delta: "measured".into(),
            },
        );
        let observed = source
            .cumulative_usage(first.operation_id, &first.dispatch_id)
            .expect("first receipt");
        assert_eq!(observed.model_steps, 1);
        assert!(observed.output_bytes > 0);
        assert_eq!(
            source
                .cumulative_usage(second.operation_id, &second.dispatch_id)
                .expect("unseen dispatch receipt"),
            SwarmUsage::default()
        );
    }
}
