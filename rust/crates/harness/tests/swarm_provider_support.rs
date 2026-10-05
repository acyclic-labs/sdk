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
    usage: Mutex<BTreeMap<(OperationId, String), FixtureMeasurement>>,
}

#[derive(Default, Clone, Copy)]
struct FixtureMeasurement {
    usage: SwarmUsage,
    elapsed_sample_ms: u64,
}

impl FixtureUsage {
    pub fn new(provider: &'static str) -> Arc<Self> {
        Arc::new(Self {
            provider: provider.into(),
            usage: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn begin(&self, dispatch: &ProviderDispatchContext) -> acyclic_harness::Result<()> {
        let key = (dispatch.operation_id, dispatch.dispatch_id.0.clone());
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Conflict("fixture usage lock poisoned".into()))?;
        let entry = usage.entry(key.clone()).or_default();
        entry.usage.model_steps = entry
            .usage
            .model_steps
            .checked_add(1)
            .ok_or_else(|| Error::Conflict("fixture model-step measurement overflow".into()))?;
        entry.elapsed_sample_ms = 0;
        Ok(())
    }

    pub fn record(
        &self,
        dispatch: &ProviderDispatchContext,
        started: Instant,
        event: &ModelEvent,
    ) -> acyclic_harness::Result<()> {
        let bytes = canonical_json_bytes(event)?.len() as u64;
        let key = (dispatch.operation_id, dispatch.dispatch_id.0.clone());
        let elapsed = u64::try_from(started.elapsed().as_millis())
            .map_err(|_| Error::Conflict("fixture elapsed measurement overflow".into()))?;
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Conflict("fixture usage lock poisoned".into()))?;
        let entry = usage.entry(key).or_default();
        entry.usage.output_bytes = entry
            .usage
            .output_bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::Conflict("fixture output measurement overflow".into()))?;
        let delta = elapsed
            .checked_sub(entry.elapsed_sample_ms)
            .ok_or_else(|| Error::Conflict("fixture elapsed measurement regressed".into()))?;
        entry.elapsed_sample_ms = elapsed;
        entry.usage.execution_time_ms = entry
            .usage
            .execution_time_ms
            .checked_add(delta)
            .ok_or_else(|| Error::Conflict("fixture time measurement overflow".into()))?;
        Ok(())
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
        let usage = self
            .usage
            .lock()
            .map_err(|_| Error::Conflict("fixture usage lock poisoned".into()))?;
        usage
            .get(&(operation_id, dispatch_id.0.clone()))
            .map(|measurement| measurement.usage)
            .ok_or_else(|| Error::NotFound("fixture usage receipt".into()))
    }
}

pub fn record_result(
    usage: &FixtureUsage,
    dispatch: &ProviderDispatchContext,
    started: Instant,
    event: &acyclic_harness::Result<ModelEvent>,
) -> acyclic_harness::Result<()> {
    if let Ok(event) = event {
        usage.record(dispatch, started, event)?;
    }
    Ok(())
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
            if let Err(error) = self.usage.begin(&dispatch) {
                return Box::pin(futures::stream::once(async move { Err(error) }));
            }
            let started = std::time::Instant::now();
            let stream = self.generate(prepared);
            let usage = self.usage.clone();
            Box::pin(stream.map(move |event| {
                if let Err(error) =
                    swarm_provider_support::record_result(&usage, &dispatch, started, &event)
                {
                    return Err(error);
                }
                event
            }))
        }

        fn reconcile_admitted_with_dispatch<'a>(
            &'a self,
            prepared: acyclic_harness::model_input::PreparedModelInput,
            attempt: acyclic_harness::model::ModelAttempt,
            dispatch: ProviderDispatchContext,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async move {
                if dispatch.operation_id != attempt.operation_id
                    || dispatch.request_digest != attempt.request_digest
                {
                    return Err(Error::Conflict(
                        "fixture dispatch context does not match admitted attempt".into(),
                    ));
                }
                let events = self.reconcile_admitted(prepared, attempt).await?;
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
        source.begin(&first).expect("begin measurement");
        source
            .record(
                &first,
                Instant::now(),
                &ModelEvent::Content {
                    delta: "measured".into(),
                },
            )
            .expect("record measurement");
        source.begin(&first).expect("begin retry measurement");
        source
            .record(
                &first,
                Instant::now(),
                &ModelEvent::Content {
                    delta: "again".into(),
                },
            )
            .expect("record retry measurement");
        let observed = source
            .cumulative_usage(first.operation_id, &first.dispatch_id)
            .expect("first receipt");
        assert_eq!(observed.model_steps, 2);
        assert!(observed.output_bytes > 0);
        assert!(matches!(
            source.cumulative_usage(second.operation_id, &second.dispatch_id),
            Err(Error::NotFound(_))
        ));
    }
}
