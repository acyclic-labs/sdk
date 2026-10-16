//! Independently count durable admissions, terminal facts and external effects.
use super::*;
use crate::{model::ModelDispatch, scheduler::SchedulerEvent};

#[derive(Clone)]
pub(super) struct Observations {
    root: OperationId,
    admissions: Vec<OperationId>,
    completions: Vec<OperationId>,
    models: Vec<ModelDispatch>,
    tools: Vec<(OperationId, String)>,
}

pub(super) async fn observe(
    harness: &Harness,
    fixture: &Fixture,
    root: TaskId,
) -> Result<Observations> {
    let mut observed = Observations {
        root: OperationId::from_bytes(root.into_bytes()),
        admissions: vec![],
        completions: vec![],
        models: fixture.provider.dispatches(),
        tools: fixture
            .effect
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| (call.operation_id, call.call_id.clone()))
            .collect(),
    };
    let stream = harness.runtime.task_host().workflow_stream();
    let mut after = 0;
    loop {
        let page = crate::distributed::read_coordinator_event_page(&stream, after, 128).await?;
        if page.is_empty() {
            break;
        }
        after += page.len() as u64;
        for event in page {
            match event {
                // Task admission publishes one immutable declaration, regardless
                // of how often its worker capacity is released and reclaimed.
                SchedulerEvent::Declared { spec } => observed.admissions.push(spec.operation_id),
                SchedulerEvent::Completed {
                    operation_id,
                    outcome: crate::Outcome::Succeeded(_),
                    ..
                } => observed.completions.push(operation_id),
                _ => {}
            }
        }
    }
    Ok(observed)
}

impl Observations {
    pub(super) fn check(&self, scenario: Scenario) -> std::result::Result<(), &'static str> {
        let (tasks, models, tools) = match scenario {
            Scenario::Text => (1, 1, 0),
            Scenario::EffectAndContinuation => (2, 3, 1),
            Scenario::Compaction => (1, 2, 0),
        };
        if self.admissions.len() != tasks
            || self
                .admissions
                .iter()
                .filter(|operation| **operation == self.root)
                .count()
                != 1
            || !unique(&self.admissions)
        {
            return Err("missing or duplicate task admission");
        }
        if self.completions.len() != tasks
            || !unique(&self.completions)
            || self
                .admissions
                .iter()
                .any(|operation| !self.completions.contains(operation))
        {
            return Err("lost or duplicate task completion");
        }
        if self.models.len() != models || !unique(&self.models) {
            return Err("missing or duplicate provider submission");
        }
        if self.tools.len() != tools || !unique(&self.tools) {
            return Err("missing or duplicate tool effect");
        }
        Ok(())
    }
}

fn unique<T: PartialEq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .all(|(index, value)| !values[..index].contains(value))
}

#[tokio::test]
async fn broken_recovery_records_are_rejected_by_the_actual_sweep_checks() -> Result<()> {
    for scenario in [
        Scenario::Text,
        Scenario::EffectAndContinuation,
        Scenario::Compaction,
    ] {
        let good = case(scenario, None, &[], None).await?.observations;
        good.check(scenario).expect("actual recovery must pass");
        let mut duplicate_admission = good.clone();
        duplicate_admission.admissions.push(good.admissions[0]);
        assert_eq!(
            duplicate_admission.check(scenario),
            Err("missing or duplicate task admission")
        );
        let mut silent_admission = good.clone();
        silent_admission
            .admissions
            .push(OperationId::from_bytes([99; 16]));
        assert_eq!(
            silent_admission.check(scenario),
            Err("missing or duplicate task admission")
        );
        let mut duplicate_provider = good.clone();
        duplicate_provider.models.push(good.models[0]);
        assert_eq!(
            duplicate_provider.check(scenario),
            Err("missing or duplicate provider submission")
        );
        let mut lost_completion = good;
        lost_completion.completions.pop();
        assert_eq!(
            lost_completion.check(scenario),
            Err("lost or duplicate task completion")
        );
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn admission_and_completion_checks_require_a_bijection(ids in proptest::collection::vec(proptest::prelude::any::<[u8; 16]>(), 1..3)) {
        let admissions: Vec<_> = ids.into_iter().map(OperationId::from_bytes).collect();
        let scenario = if admissions.len() == 1 { Scenario::Text } else { Scenario::EffectAndContinuation };
        let mut observed = Observations { root: admissions[0], completions: admissions.clone(), admissions,
            models: vec![], tools: vec![] };
        // The first independent invariant always rejects a duplicate, even if
        // that task never reaches provider dispatch or terminal publication.
        observed.admissions.push(observed.root);
        proptest::prop_assert_eq!(observed.check(scenario), Err("missing or duplicate task admission"));
    }
}
