#![forbid(unsafe_code)]
//! Transport-independent customer messages and validation.

use sha2::{Digest, Sha256};
use zeroize::Zeroize;

/// Maximum immutable candidates admitted by one evaluation.
pub const MAXIMUM_EVALUATION_CANDIDATES: usize = 256;
/// Maximum ordered cases admitted by one evaluation suite.
pub const MAXIMUM_EVALUATION_CASES: usize = 4_096;
/// Maximum metric identities admitted by one evaluation.
pub const MAXIMUM_EVALUATION_METRICS: usize = 64;
/// Maximum materialized candidate/case results in one evaluation.
pub const MAXIMUM_EVALUATION_RESULTS: u64 = 65_536;

/// Generated, transport-independent customer messages.
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/inference.customer.v1.rs"));
}

impl Drop for wire::Item {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::Replace {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::RunEvent {
    fn drop(&mut self) {
        scrub_run_event(self);
    }
}

impl Drop for wire::RunResult {
    fn drop(&mut self) {
        scrub_run_result(self);
    }
}

/// Zeroize a run event's output bytes before releasing it.
pub fn scrub_run_event(event: &mut wire::RunEvent) {
    if let Some(wire::run_event::Event::Output(output)) = event.event.as_mut() {
        output.zeroize();
    }
}

/// Zeroize a run result's output bytes before releasing it.
pub fn scrub_run_result(result: &mut wire::RunResult) {
    result.output.zeroize();
}

/// Contract validation failure with a stable diagnostic.
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// A customer message violates its generated or cross-field contract.
    #[error("invalid customer contract: {0}")]
    Invalid(&'static str),
}

impl ValidationError {
    /// The contract diagnostic without a transport-specific prefix.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        match self {
            Self::Invalid(message) => message,
        }
    }
}

#[allow(missing_docs)]
mod validation {
    use super::{
        Digest, MAXIMUM_EVALUATION_CANDIDATES, MAXIMUM_EVALUATION_CASES,
        MAXIMUM_EVALUATION_METRICS, MAXIMUM_EVALUATION_RESULTS, Sha256, ValidationError, wire,
    };
    pub fn nonzero<const N: usize>(value: &[u8; N]) -> Result<(), ValidationError> {
        if *value == [0; N] {
            return Err(ValidationError::Invalid("zero identity"));
        }
        Ok(())
    }

    pub fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], ValidationError> {
        let bytes = value
            .try_into()
            .map_err(|_| ValidationError::Invalid("identity length differs"))?;
        nonzero(&bytes)?;
        Ok(bytes)
    }

    pub fn validate_model_capabilities(
        response: &wire::ListModelsResponse,
    ) -> Result<(), ValidationError> {
        if response.models.is_empty() || response.models.len() > 4_096 {
            return Err(ValidationError::Invalid(
                "model capability count is invalid",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for model in &response.models {
            let mut retention_profiles = std::collections::BTreeSet::new();
            if model.model.is_empty()
                || model.model.len() > 256
                || !names.insert(model.model.as_str())
                || fixed::<32>(&model.execution_profile).is_err()
                || model.maximum_context == 0
                || model.maximum_output == 0
                || model.features.is_empty()
                || model.features.len() > 64
                || model
                    .features
                    .iter()
                    .any(|feature| feature.is_empty() || feature.len() > 64)
                || model.retention_profiles.len() > 64
                || model.retention_profiles.iter().any(|profile| {
                    fixed::<32>(&profile.profile).is_err()
                        || profile.minimum_duration_ms == 0
                        || profile.maximum_duration_ms < profile.minimum_duration_ms
                        || !retention_profiles.insert(profile.profile.as_slice())
                })
            {
                return Err(ValidationError::Invalid("model capability is invalid"));
            }
        }
        Ok(())
    }

    pub fn validate_evaluation_spec(spec: &wire::EvaluationSpec) -> Result<(), ValidationError> {
        if spec.candidates.is_empty() || spec.candidates.len() > MAXIMUM_EVALUATION_CANDIDATES {
            return Err(ValidationError::Invalid(
                "evaluation candidate count is invalid",
            ));
        }
        let mut candidate_digests = std::collections::BTreeSet::new();
        for candidate in &spec.candidates {
            let digest = fixed::<32>(&candidate.digest)?;
            if candidate.media_type.is_empty()
                || candidate.media_type.len() > 256
                || candidate.logical_size == 0
                || !candidate_digests.insert(digest)
            {
                return Err(ValidationError::Invalid("evaluation candidate is invalid"));
            }
        }

        let suite = spec
            .suite
            .as_ref()
            .ok_or(ValidationError::Invalid("evaluation suite is absent"))?;
        if suite.identity.is_empty()
            || suite.identity.len() > 256
            || suite.cases.is_empty()
            || suite.cases.len() > MAXIMUM_EVALUATION_CASES
        {
            return Err(ValidationError::Invalid("evaluation suite is invalid"));
        }
        fixed::<32>(&suite.digest)?;
        let mut case_ids = std::collections::BTreeSet::new();
        for case in &suite.cases {
            let case_id = fixed::<16>(&case.case_id)?;
            let inline = !case.input.is_empty();
            let artifact = case.input_artifact_digest.as_ref();
            if !case_ids.insert(case_id) || inline == artifact.is_some() {
                return Err(ValidationError::Invalid("evaluation case is invalid"));
            }
            if let Some(digest) = artifact {
                fixed::<32>(digest)?;
            }
        }

        let grader = spec
            .grader
            .as_ref()
            .ok_or(ValidationError::Invalid("evaluation grader is absent"))?;
        if grader.handle.is_empty() || grader.handle.len() > 4_096 {
            return Err(ValidationError::Invalid("evaluation grader is invalid"));
        }
        fixed::<32>(&grader.artifact_digest)?;

        if spec.metrics.is_empty() || spec.metrics.len() > MAXIMUM_EVALUATION_METRICS {
            return Err(ValidationError::Invalid(
                "evaluation metric count is invalid",
            ));
        }
        let mut metric_ids = std::collections::BTreeSet::new();
        for metric in &spec.metrics {
            if metric.identity.is_empty()
                || metric.identity.len() > 256
                || !metric_ids.insert(metric.identity.as_str())
                || wire::EvaluationAggregation::try_from(metric.aggregation)
                    .unwrap_or(wire::EvaluationAggregation::Unspecified)
                    == wire::EvaluationAggregation::Unspecified
            {
                return Err(ValidationError::Invalid("evaluation metric is invalid"));
            }
        }

        let possible_results = u64::try_from(spec.candidates.len())
            .ok()
            .and_then(|candidates| {
                u64::try_from(suite.cases.len())
                    .ok()
                    .and_then(|cases| candidates.checked_mul(cases))
            })
            .ok_or(ValidationError::Invalid("evaluation result count overflow"))?;
        if spec.maximum_case_results == 0
            || spec.maximum_case_results > possible_results
            || spec.maximum_case_results > MAXIMUM_EVALUATION_RESULTS
        {
            return Err(ValidationError::Invalid(
                "evaluation result bound is invalid",
            ));
        }
        fixed::<32>(&spec.spec_digest)?;
        Ok(())
    }

    fn validate_exact_rational(value: Option<&wire::ExactRational>) -> Result<(), ValidationError> {
        if value.is_none_or(|value| value.denominator == 0) {
            return Err(ValidationError::Invalid("evaluation rational is invalid"));
        }
        Ok(())
    }

    pub fn evaluation_observation_binding(native: &[u8; 32], observation: &[u8; 32]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"acyclic.inference.grader-observation.v1\0");
        digest.update(native);
        digest.update(observation);
        digest.finalize().into()
    }

    pub fn validate_evaluation_admission(
        view: &wire::EvaluationView,
        expected: [u8; 16],
        expected_spec: &wire::EvaluationSpec,
    ) -> Result<(), ValidationError> {
        if view.spec.as_ref() != Some(expected_spec) {
            return Err(ValidationError::Invalid(
                "evaluation admission spec differs",
            ));
        }
        validate_evaluation_view(view, expected)
    }

    pub fn validate_evaluation_view(
        view: &wire::EvaluationView,
        expected: [u8; 16],
    ) -> Result<(), ValidationError> {
        if fixed::<16>(&view.evaluation_id)? != expected || view.sequence == 0 {
            return Err(ValidationError::Invalid("evaluation identity differs"));
        }
        let spec = view
            .spec
            .as_ref()
            .ok_or(ValidationError::Invalid("evaluation spec is absent"))?;
        validate_evaluation_spec(spec)?;
        let state = wire::EvaluationState::try_from(view.state)
            .unwrap_or(wire::EvaluationState::Unspecified);
        if state == wire::EvaluationState::Unspecified
            || (state == wire::EvaluationState::Completed) != view.result.is_some()
        {
            return Err(ValidationError::Invalid("evaluation state is invalid"));
        }
        let Some(result) = &view.result else {
            return Ok(());
        };
        validate_evaluation_result(spec, result)
    }

    fn validate_evaluation_result(
        spec: &wire::EvaluationSpec,
        result: &wire::EvaluationResult,
    ) -> Result<(), ValidationError> {
        if fixed::<32>(&result.spec_digest)? != fixed::<32>(&spec.spec_digest)?
            || result.case_results.is_empty()
            || u64::try_from(result.case_results.len()).unwrap_or(u64::MAX)
                != spec.maximum_case_results
        {
            return Err(ValidationError::Invalid("evaluation result is invalid"));
        }
        fixed::<32>(&result.result_digest)?;

        let suite = spec
            .suite
            .as_ref()
            .ok_or(ValidationError::Invalid("evaluation suite is absent"))?;
        let candidates: std::collections::BTreeSet<_> = spec
            .candidates
            .iter()
            .map(|candidate| candidate.digest.as_slice())
            .collect();
        let cases: std::collections::BTreeSet<_> = suite
            .cases
            .iter()
            .map(|case| case.case_id.as_slice())
            .collect();
        let metrics: std::collections::BTreeSet<_> = spec
            .metrics
            .iter()
            .map(|metric| metric.identity.as_str())
            .collect();
        let mut observations = std::collections::BTreeSet::new();
        for case in &result.case_results {
            validate_evaluation_case(case, &candidates, &cases, &metrics, &mut observations)?;
        }
        validate_evaluation_aggregates(spec, result, &candidates, &metrics)
    }

    fn validate_evaluation_case<'a>(
        case: &'a wire::EvaluationCaseResult,
        candidates: &std::collections::BTreeSet<&[u8]>,
        cases: &std::collections::BTreeSet<&[u8]>,
        metrics: &std::collections::BTreeSet<&str>,
        observations: &mut std::collections::BTreeSet<(&'a [u8], &'a [u8])>,
    ) -> Result<(), ValidationError> {
        let observation = case.observation.as_ref().ok_or(ValidationError::Invalid(
            "evaluation grader observation is absent",
        ))?;
        let native_output_digest = fixed::<32>(&observation.native_output_digest)?;
        let grader_observation_digest = fixed::<32>(&observation.observation_digest)?;
        if fixed::<32>(&observation.binding_digest)?
            != evaluation_observation_binding(&native_output_digest, &grader_observation_digest)
        {
            return Err(ValidationError::Invalid(
                "evaluation grader observation binding differs",
            ));
        }
        let outcome = wire::EvaluationCaseOutcome::try_from(case.outcome)
            .unwrap_or(wire::EvaluationCaseOutcome::Unspecified);
        if !candidates.contains(case.candidate_digest.as_slice())
            || !cases.contains(case.case_id.as_slice())
            || !observations.insert((case.candidate_digest.as_slice(), case.case_id.as_slice()))
            || outcome == wire::EvaluationCaseOutcome::Unspecified
        {
            return Err(ValidationError::Invalid(
                "evaluation case result is invalid",
            ));
        }
        let mut observed_metrics = std::collections::BTreeSet::new();
        for metric in &case.metrics {
            if !metrics.contains(metric.metric_identity.as_str())
                || !observed_metrics.insert(metric.metric_identity.as_str())
            {
                return Err(ValidationError::Invalid(
                    "evaluation case metric is invalid",
                ));
            }
            validate_exact_rational(metric.value.as_ref())?;
        }
        if (outcome == wire::EvaluationCaseOutcome::Scored
            && observed_metrics.len() != metrics.len())
            || (outcome != wire::EvaluationCaseOutcome::Scored && !observed_metrics.is_empty())
        {
            return Err(ValidationError::Invalid(
                "evaluation case metric coverage is invalid",
            ));
        }
        Ok(())
    }

    fn validate_evaluation_aggregates(
        spec: &wire::EvaluationSpec,
        result: &wire::EvaluationResult,
        candidates: &std::collections::BTreeSet<&[u8]>,
        metrics: &std::collections::BTreeSet<&str>,
    ) -> Result<(), ValidationError> {
        let mut aggregates = std::collections::BTreeSet::new();
        for aggregate in &result.aggregates {
            let Some(metric) = spec
                .metrics
                .iter()
                .find(|metric| metric.identity == aggregate.metric_identity)
            else {
                return Err(ValidationError::Invalid(
                    "evaluation aggregate metric is invalid",
                ));
            };
            if !candidates.contains(aggregate.candidate_digest.as_slice())
                || metric.aggregation != aggregate.aggregation
                || !aggregates.insert((
                    aggregate.candidate_digest.as_slice(),
                    aggregate.metric_identity.as_str(),
                ))
            {
                return Err(ValidationError::Invalid("evaluation aggregate is invalid"));
            }
            validate_exact_rational(aggregate.value.as_ref())?;
        }
        if aggregates.len() != candidates.len().saturating_mul(metrics.len()) {
            return Err(ValidationError::Invalid(
                "evaluation aggregate coverage is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_run_view(
        view: &wire::RunView,
        expected: [u8; 16],
    ) -> Result<(), ValidationError> {
        if fixed::<16>(&view.run_id)? != expected || fixed::<32>(&view.input).is_err() {
            return Err(ValidationError::Invalid("Run identity differs"));
        }
        if view.model.is_empty() || view.model.len() > 256 {
            return Err(ValidationError::Invalid("Run model is invalid"));
        }
        if let Some(result) = &view.result {
            let terminal = wire::RunTerminal::try_from(result.terminal)
                .unwrap_or(wire::RunTerminal::Unspecified);
            if terminal == wire::RunTerminal::Unspecified {
                return Err(ValidationError::Invalid("Run result terminal is invalid"));
            }
            if let Some(context) = &result.context {
                validate_context_view(context, None)?;
            }
            if let Some(receipt) = &result.receipt {
                fixed::<32>(&receipt.receipt_id)?;
                fixed::<32>(&receipt.model_profile)?;
                fixed::<32>(&receipt.meter_revision)?;
                fixed::<32>(&receipt.rate_card_revision)?;
                if receipt.usage.is_none() {
                    return Err(ValidationError::Invalid("Run usage is absent"));
                }
            }
        }
        Ok(())
    }

    /// Validate the complete shape of a context observation.
    ///
    /// The optional expected revision is supplied by operations that requested
    /// a particular context. Nested run continuations have no caller-supplied
    /// revision, but still need the same field, provenance, and identity
    /// checks before they are exposed to the customer.
    pub fn validate_context_view(
        view: &wire::ContextView,
        expected_revision: Option<[u8; 32]>,
    ) -> Result<(), ValidationError> {
        let revision = fixed::<32>(&view.revision)?;
        if expected_revision.is_some_and(|expected| expected != revision) {
            return Err(ValidationError::Invalid("revision differs"));
        }
        fixed::<32>(&view.lineage)?;
        fixed::<32>(&view.execution_profile)?;
        fixed::<32>(&view.content_digest)?;
        if let Some(parent) = &view.parent {
            fixed::<32>(parent)?;
        }
        if view.model.is_empty() || view.model.len() > 256 {
            return Err(ValidationError::Invalid("Context model is invalid"));
        }
        validate_provenance(
            view.provenance
                .as_ref()
                .ok_or(ValidationError::Invalid("Context provenance is absent"))?,
        )?;
        Ok(())
    }

    pub fn validate_warm_view(
        view: &wire::WarmView,
        expected_context: Option<[u8; 32]>,
        expected_commitment: Option<[u8; 32]>,
    ) -> Result<(), ValidationError> {
        let commitment = fixed::<32>(&view.commitment)?;
        let context = fixed::<32>(&view.context)?;
        fixed::<32>(&view.model_profile)?;
        fixed::<32>(&view.latency_profile)?;
        fixed::<32>(&view.evidence_digest)?;
        fixed::<32>(&view.admission_receipt_id)?;
        let state = wire::WarmState::try_from(view.state).unwrap_or(wire::WarmState::Unspecified);
        if expected_context.is_some_and(|expected| expected != context)
            || expected_commitment.is_some_and(|expected| expected != commitment)
            || view.expires_at_ms == 0
            || view.sequence == 0
            || state == wire::WarmState::Unspecified
        {
            return Err(ValidationError::Invalid("warm commitment shape differs"));
        }
        Ok(())
    }

    pub fn validate_provenance(value: &wire::ContextProvenance) -> Result<(), ValidationError> {
        use wire::context_provenance::Origin;
        match value
            .origin
            .as_ref()
            .ok_or(ValidationError::Invalid("Context provenance is absent"))?
        {
            Origin::Created(_) => Ok(()),
            Origin::Derived(value) | Origin::Forked(value) => {
                fixed::<32>(&value.source).map(|_| ())
            }
            Origin::Transferred(value) => fixed::<32>(&value.source).map(|_| ()),
            Origin::Generated(value) => {
                fixed::<16>(&value.run_id)?;
                fixed::<32>(&value.terminal_receipt_digest).map(|_| ())
            }
            Origin::RunInput(value) => {
                fixed::<32>(&value.source)?;
                fixed::<16>(&value.run_id)?;
                if value.maximum_output == 0 {
                    return Err(ValidationError::Invalid("Run input output bound is zero"));
                }
                Ok(())
            }
        }
    }

    pub fn validate_receipt(receipt: &wire::MutationReceipt) -> Result<(), ValidationError> {
        fixed::<32>(&receipt.revision)?;
        fixed::<32>(&receipt.command_digest)?;
        if receipt.sequence == 0 {
            return Err(ValidationError::Invalid("missing publication sequence"));
        }
        Ok(())
    }
}

pub use validation::{
    evaluation_observation_binding, fixed, nonzero, validate_context_view,
    validate_evaluation_admission, validate_evaluation_spec, validate_evaluation_view,
    validate_model_capabilities, validate_provenance, validate_receipt, validate_run_view,
    validate_warm_view,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn context_view() -> wire::ContextView {
        wire::ContextView {
            revision: vec![1; 32],
            lineage: vec![2; 32],
            execution_profile: vec![3; 32],
            content_digest: vec![4; 32],
            model: "model".to_owned(),
            provenance: Some(wire::ContextProvenance {
                origin: Some(wire::context_provenance::Origin::Created(wire::Empty {})),
            }),
            ..wire::ContextView::default()
        }
    }

    #[test]
    fn context_view_validation_covers_nested_identity_and_provenance() {
        let valid = context_view();
        assert!(validate_context_view(&valid, Some([1; 32])).is_ok());

        for invalid in [
            wire::ContextView {
                lineage: vec![0; 32],
                ..valid.clone()
            },
            wire::ContextView {
                execution_profile: vec![3],
                ..valid.clone()
            },
            wire::ContextView {
                content_digest: Vec::new(),
                ..valid.clone()
            },
            wire::ContextView {
                provenance: Some(wire::ContextProvenance {
                    origin: Some(wire::context_provenance::Origin::RunInput(
                        wire::RunInputProvenance {
                            source: vec![5],
                            run_id: vec![6; 16],
                            maximum_output: 1,
                            seed: None,
                        },
                    )),
                }),
                ..valid
            },
        ] {
            assert!(validate_context_view(&invalid, None).is_err());
        }
    }

    #[test]
    fn run_validation_rejects_malformed_nested_context() {
        let mut context = context_view();
        context.lineage = vec![0; 32];
        let view = wire::RunView {
            run_id: vec![7; 16],
            input: vec![8; 32],
            model: "model".to_owned(),
            result: Some(wire::RunResult {
                output: Vec::new(),
                context: Some(context),
                terminal: wire::RunTerminal::Completed.into(),
                receipt: None,
            }),
            ..wire::RunView::default()
        };
        assert!(validate_run_view(&view, [7; 16]).is_err());
    }
}
