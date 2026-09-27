use sha2::{Digest, Sha256};

use crate::wire;

/// Shared inference transport ceiling.
pub const MAXIMUM_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
/// HTTP JSON/NDJSON ceiling, sized above ordinary base64 expansion of the wire
/// limit while intentionally bounding escaped JSON strings separately.
pub const MAXIMUM_HTTP_JSON_BYTES: usize = 16 * 1024 * 1024;
/// Largest admitted candidate set.
pub const MAXIMUM_EVALUATION_CANDIDATES: usize = 256;
/// Largest admitted case set.
pub const MAXIMUM_EVALUATION_CASES: usize = 4_096;
/// Largest admitted metric set.
pub const MAXIMUM_EVALUATION_METRICS: usize = 64;
/// Largest materialized result set.
pub const MAXIMUM_EVALUATION_RESULTS: u64 = 65_536;

#[derive(Clone, Copy)]
pub(crate) enum Error {
    Invalid(&'static str),
}

#[cfg(feature = "host")]
impl Error {
    pub(crate) fn message(&self) -> &'static str {
        let Self::Invalid(message) = self;
        message
    }
}

fn nonzero<const N: usize>(value: &[u8; N]) -> Result<(), Error> {
    if *value == [0; N] {
        return Err(Error::Invalid("zero identity"));
    }
    Ok(())
}

fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], Error> {
    let bytes = value
        .try_into()
        .map_err(|_| Error::Invalid("identity length differs"))?;
    nonzero(&bytes)?;
    Ok(bytes)
}

pub(crate) fn validate_evaluation_spec(spec: &wire::EvaluationSpec) -> Result<(), Error> {
    if spec.candidates.is_empty() || spec.candidates.len() > MAXIMUM_EVALUATION_CANDIDATES {
        return Err(Error::Invalid("evaluation candidate count is invalid"));
    }
    let mut candidate_digests = std::collections::BTreeSet::new();
    for candidate in &spec.candidates {
        let digest = fixed::<32>(&candidate.digest)?;
        if candidate.media_type.is_empty()
            || candidate.media_type.len() > 256
            || candidate.logical_size == 0
            || !candidate_digests.insert(digest)
        {
            return Err(Error::Invalid("evaluation candidate is invalid"));
        }
    }

    let suite = spec
        .suite
        .as_ref()
        .ok_or(Error::Invalid("evaluation suite is absent"))?;
    if suite.identity.is_empty()
        || suite.identity.len() > 256
        || suite.cases.is_empty()
        || suite.cases.len() > MAXIMUM_EVALUATION_CASES
    {
        return Err(Error::Invalid("evaluation suite is invalid"));
    }
    fixed::<32>(&suite.digest)?;
    let mut case_ids = std::collections::BTreeSet::new();
    for case in &suite.cases {
        let case_id = fixed::<16>(&case.case_id)?;
        let inline = !case.input.is_empty();
        let artifact = case.input_artifact_digest.as_ref();
        if !case_ids.insert(case_id) || inline == artifact.is_some() {
            return Err(Error::Invalid("evaluation case is invalid"));
        }
        if let Some(digest) = artifact {
            fixed::<32>(digest)?;
        }
    }

    let grader = spec
        .grader
        .as_ref()
        .ok_or(Error::Invalid("evaluation grader is absent"))?;
    if grader.handle.is_empty() || grader.handle.len() > 4_096 {
        return Err(Error::Invalid("evaluation grader is invalid"));
    }
    fixed::<32>(&grader.artifact_digest)?;

    if spec.metrics.is_empty() || spec.metrics.len() > MAXIMUM_EVALUATION_METRICS {
        return Err(Error::Invalid("evaluation metric count is invalid"));
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
            return Err(Error::Invalid("evaluation metric is invalid"));
        }
    }

    let possible_results = u64::try_from(spec.candidates.len())
        .ok()
        .and_then(|candidates| {
            u64::try_from(suite.cases.len())
                .ok()
                .and_then(|cases| candidates.checked_mul(cases))
        })
        .ok_or(Error::Invalid("evaluation result count overflow"))?;
    if spec.maximum_case_results == 0
        || spec.maximum_case_results > possible_results
        || spec.maximum_case_results > MAXIMUM_EVALUATION_RESULTS
    {
        return Err(Error::Invalid("evaluation result bound is invalid"));
    }
    fixed::<32>(&spec.spec_digest)?;
    Ok(())
}

fn validate_exact_rational(value: Option<&wire::ExactRational>) -> Result<(), Error> {
    if value.is_none_or(|value| value.denominator == 0) {
        return Err(Error::Invalid("evaluation rational is invalid"));
    }
    Ok(())
}

pub(crate) fn evaluation_observation_binding(
    native: &[u8; 32],
    observation: &[u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"acyclic.inference.grader-observation.v1\0");
    digest.update(native);
    digest.update(observation);
    digest.finalize().into()
}

pub(crate) fn validate_evaluation_admission(
    view: &wire::EvaluationView,
    expected: [u8; 16],
    expected_spec: &wire::EvaluationSpec,
) -> Result<(), Error> {
    if view.spec.as_ref() != Some(expected_spec) {
        return Err(Error::Invalid("evaluation admission spec differs"));
    }
    validate_evaluation_view(view, expected)
}

pub(crate) fn validate_evaluation_view(
    view: &wire::EvaluationView,
    expected: [u8; 16],
) -> Result<(), Error> {
    if fixed::<16>(&view.evaluation_id)? != expected || view.sequence == 0 {
        return Err(Error::Invalid("evaluation identity differs"));
    }
    let spec = view
        .spec
        .as_ref()
        .ok_or(Error::Invalid("evaluation spec is absent"))?;
    validate_evaluation_spec(spec)?;
    let state =
        wire::EvaluationState::try_from(view.state).unwrap_or(wire::EvaluationState::Unspecified);
    if state == wire::EvaluationState::Unspecified
        || (state == wire::EvaluationState::Completed) != view.result.is_some()
    {
        return Err(Error::Invalid("evaluation state is invalid"));
    }
    let Some(result) = &view.result else {
        return Ok(());
    };
    validate_evaluation_result(spec, result)
}

fn validate_evaluation_result(
    spec: &wire::EvaluationSpec,
    result: &wire::EvaluationResult,
) -> Result<(), Error> {
    if fixed::<32>(&result.spec_digest)? != fixed::<32>(&spec.spec_digest)?
        || result.case_results.is_empty()
        || u64::try_from(result.case_results.len()).unwrap_or(u64::MAX) != spec.maximum_case_results
    {
        return Err(Error::Invalid("evaluation result is invalid"));
    }
    fixed::<32>(&result.result_digest)?;

    let suite = spec
        .suite
        .as_ref()
        .ok_or(Error::Invalid("evaluation suite is absent"))?;
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
) -> Result<(), Error> {
    let observation = case
        .observation
        .as_ref()
        .ok_or(Error::Invalid("evaluation grader observation is absent"))?;
    let native_output_digest = fixed::<32>(&observation.native_output_digest)?;
    let grader_observation_digest = fixed::<32>(&observation.observation_digest)?;
    if fixed::<32>(&observation.binding_digest)?
        != evaluation_observation_binding(&native_output_digest, &grader_observation_digest)
    {
        return Err(Error::Invalid(
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
        return Err(Error::Invalid("evaluation case result is invalid"));
    }
    let mut observed_metrics = std::collections::BTreeSet::new();
    for metric in &case.metrics {
        if !metrics.contains(metric.metric_identity.as_str())
            || !observed_metrics.insert(metric.metric_identity.as_str())
        {
            return Err(Error::Invalid("evaluation case metric is invalid"));
        }
        validate_exact_rational(metric.value.as_ref())?;
    }
    if (outcome == wire::EvaluationCaseOutcome::Scored && observed_metrics.len() != metrics.len())
        || (outcome != wire::EvaluationCaseOutcome::Scored && !observed_metrics.is_empty())
    {
        return Err(Error::Invalid("evaluation case metric coverage is invalid"));
    }
    Ok(())
}

fn validate_evaluation_aggregates(
    spec: &wire::EvaluationSpec,
    result: &wire::EvaluationResult,
    candidates: &std::collections::BTreeSet<&[u8]>,
    metrics: &std::collections::BTreeSet<&str>,
) -> Result<(), Error> {
    let mut aggregates = std::collections::BTreeSet::new();
    for aggregate in &result.aggregates {
        let Some(metric) = spec
            .metrics
            .iter()
            .find(|metric| metric.identity == aggregate.metric_identity)
        else {
            return Err(Error::Invalid("evaluation aggregate metric is invalid"));
        };
        if !candidates.contains(aggregate.candidate_digest.as_slice())
            || metric.aggregation != aggregate.aggregation
            || !aggregates.insert((
                aggregate.candidate_digest.as_slice(),
                aggregate.metric_identity.as_str(),
            ))
        {
            return Err(Error::Invalid("evaluation aggregate is invalid"));
        }
        validate_exact_rational(aggregate.value.as_ref())?;
    }
    if aggregates.len() != candidates.len().saturating_mul(metrics.len()) {
        return Err(Error::Invalid("evaluation aggregate coverage is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_run_view(view: &wire::RunView, expected: [u8; 16]) -> Result<(), Error> {
    if fixed::<16>(&view.run_id)? != expected || fixed::<32>(&view.input).is_err() {
        return Err(Error::Invalid("Run identity differs"));
    }
    if view.model.is_empty() || view.model.len() > 256 {
        return Err(Error::Invalid("Run model is invalid"));
    }
    if let Some(result) = &view.result {
        let terminal =
            wire::RunTerminal::try_from(result.terminal).unwrap_or(wire::RunTerminal::Unspecified);
        if terminal == wire::RunTerminal::Unspecified {
            return Err(Error::Invalid("Run result terminal is invalid"));
        }
        if let Some(context) = &result.context {
            fixed::<32>(&context.revision)?;
        }
        if let Some(receipt) = &result.receipt {
            fixed::<32>(&receipt.receipt_id)?;
            fixed::<32>(&receipt.model_profile)?;
            fixed::<32>(&receipt.meter_revision)?;
            fixed::<32>(&receipt.rate_card_revision)?;
            if receipt.usage.is_none() {
                return Err(Error::Invalid("Run usage is absent"));
            }
        }
    }
    Ok(())
}

/// Canonical state carried while consuming an ordered Run event stream.
///
/// The state is intentionally opaque to language bindings. They pass it back
/// to [`WatchRunState::advance_wire`] for every decoded event and to
/// [`WatchRunState::finish`] when the stream closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchRunState {
    expected: u64,
    terminal: bool,
}

impl WatchRunState {
    /// Whether the stream has already observed its terminal event.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        self.terminal
    }

    /// Validate and consume one encoded Run event in place.
    ///
    /// # Errors
    /// Rejects malformed protobuf, a gap or duplicate, post-terminal data,
    /// an invalid terminal enum, or cursor exhaustion.
    pub fn advance_wire(&mut self, message: &[u8]) -> Result<(), &'static str> {
        use prost::Message;
        if message.len() > MAXIMUM_MESSAGE_BYTES {
            return Err("message exceeds transport ceiling");
        }
        let event = wire::RunEvent::decode(message).map_err(|_| "malformed protobuf message")?;
        watch_run_event(self, &event).map_err(|Error::Invalid(message)| message)
    }

    /// Confirm that a stream closed only after its terminal event.
    ///
    /// # Errors
    /// Returns an error when the stream ended before terminal evidence.
    pub fn finish(self) -> Result<(), &'static str> {
        if self.terminal {
            Ok(())
        } else {
            Err("run stream ended before terminal")
        }
    }
}

pub(crate) fn watch_run_start(
    view: &wire::RunView,
    from_sequence: u64,
) -> Result<WatchRunState, Error> {
    let terminal = if view.result.is_none() {
        false
    } else {
        let end = view
            .last_sequence
            .checked_add(1)
            .ok_or(Error::Invalid("Run sequence exhausted"))?;
        if from_sequence > end {
            return Err(Error::Invalid("Run cursor exceeds retained events"));
        }
        from_sequence == end
    };
    Ok(WatchRunState {
        expected: from_sequence,
        terminal,
    })
}

pub(crate) fn watch_run_event(
    state: &mut WatchRunState,
    event: &wire::RunEvent,
) -> Result<(), Error> {
    if state.terminal || event.sequence != state.expected {
        return Err(Error::Invalid("run event order or shape differs"));
    }
    validate_run_event(event)?;
    state.expected = state
        .expected
        .checked_add(1)
        .ok_or(Error::Invalid("Run sequence exhausted"))?;
    if matches!(event.event, Some(wire::run_event::Event::Terminal(_))) {
        state.terminal = true;
    }
    Ok(())
}

fn validate_run_event(event: &wire::RunEvent) -> Result<(), Error> {
    match event.event.as_ref() {
        None => Err(Error::Invalid("run event is absent")),
        Some(wire::run_event::Event::Terminal(value))
            if wire::RunTerminal::try_from(*value).unwrap_or(wire::RunTerminal::Unspecified)
                == wire::RunTerminal::Unspecified =>
        {
            Err(Error::Invalid("run terminal is invalid"))
        }
        Some(_) => Ok(()),
    }
}

/// Start the canonical watch state from a validated encoded Run view.
///
/// # Errors
/// Rejects a malformed view, mismatched identity, or invalid cursor.
pub fn watch_run_start_state_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<WatchRunState, &'static str> {
    use prost::Message;
    let inner = || -> Result<WatchRunState, Error> {
        if message.len() > MAXIMUM_MESSAGE_BYTES {
            return Err(Error::Invalid("message exceeds transport ceiling"));
        }
        let view = wire::RunView::decode(message)
            .map_err(|_| Error::Invalid("malformed protobuf message"))?;
        validate_run_view(&view, fixed::<16>(expected)?)?;
        let cursor = from_sequence
            .parse::<u64>()
            .map_err(|_| Error::Invalid("Run cursor is invalid"))?;
        watch_run_start(&view, cursor)
    };
    inner().map_err(|Error::Invalid(message)| message)
}

/// Decide whether a completed Run has any events left at the requested cursor.
/// The view and cursor are validated by the same contract used by the Rust host.
///
/// # Errors
/// Rejects a malformed view, mismatched identity, or invalid cursor.
pub fn watch_run_start_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<bool, &'static str> {
    use prost::Message;
    let inner = || -> Result<bool, Error> {
        if message.len() > MAXIMUM_MESSAGE_BYTES {
            return Err(Error::Invalid("message exceeds transport ceiling"));
        }
        let view = wire::RunView::decode(message)
            .map_err(|_| Error::Invalid("malformed protobuf message"))?;
        validate_run_view(&view, fixed::<16>(expected)?)?;
        let cursor = from_sequence
            .parse::<u64>()
            .map_err(|_| Error::Invalid("Run cursor is invalid"))?;
        Ok(watch_run_start(&view, cursor)?.is_terminal())
    };
    inner().map_err(|Error::Invalid(message)| message)
}

pub(crate) fn validate_generated_run_view(
    view: &wire::RunView,
    expected: [u8; 16],
    context: [u8; 32],
) -> Result<(), Error> {
    validate_run_view(view, expected)?;
    if fixed::<32>(&view.input)? != context {
        return Err(Error::Invalid("Run input differs"));
    }
    Ok(())
}

pub(crate) fn validate_context_view(
    view: &wire::ContextView,
    expected: [u8; 32],
) -> Result<(), Error> {
    if fixed::<32>(&view.revision)? != expected {
        return Err(Error::Invalid("revision differs"));
    }
    fixed::<32>(&view.lineage)?;
    fixed::<32>(&view.execution_profile)?;
    fixed::<32>(&view.content_digest)?;
    if let Some(parent) = &view.parent {
        fixed::<32>(parent)?;
    }
    if view.model.is_empty() || view.model.len() > 256 {
        return Err(Error::Invalid("Context model is invalid"));
    }
    validate_provenance(
        view.provenance
            .as_ref()
            .ok_or(Error::Invalid("Context provenance is absent"))?,
    )
}

pub(crate) fn validate_warm_view(
    view: &wire::WarmView,
    expected_context: Option<[u8; 32]>,
    expected_commitment: Option<[u8; 32]>,
) -> Result<(), Error> {
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
        return Err(Error::Invalid("warm commitment shape differs"));
    }
    Ok(())
}

fn validate_provenance(value: &wire::ContextProvenance) -> Result<(), Error> {
    use wire::context_provenance::Origin;
    match value
        .origin
        .as_ref()
        .ok_or(Error::Invalid("Context provenance is absent"))?
    {
        Origin::Created(_) => Ok(()),
        Origin::Derived(value) | Origin::Forked(value) => fixed::<32>(&value.source).map(|_| ()),
        Origin::Transferred(value) => fixed::<32>(&value.source).map(|_| ()),
        Origin::Generated(value) => {
            fixed::<16>(&value.run_id)?;
            fixed::<32>(&value.terminal_receipt_digest).map(|_| ())
        }
        Origin::RunInput(value) => {
            fixed::<32>(&value.source)?;
            fixed::<16>(&value.run_id)?;
            if value.maximum_output == 0 {
                return Err(Error::Invalid("Run input output bound is zero"));
            }
            Ok(())
        }
    }
}

pub(crate) fn validate_receipt(receipt: &wire::MutationReceipt) -> Result<(), Error> {
    fixed::<32>(&receipt.revision)?;
    fixed::<32>(&receipt.command_digest)?;
    if receipt.sequence == 0 {
        return Err(Error::Invalid("missing publication sequence"));
    }
    Ok(())
}

/// Validate a generated protobuf message at a language binding boundary.
/// `expected` is the requested identity; `related` is the requested context
/// revision for a run, or the admitted spec bytes for an evaluation.
///
/// # Errors
/// Rejects unknown message kinds, malformed protobuf, and contract violations.
fn validate_customer_wire_inner(
    kind: &str,
    message: &[u8],
    expected: &[u8],
    related: &[u8],
) -> Result<(), Error> {
    use prost::Message;
    if message.len() > MAXIMUM_MESSAGE_BYTES || related.len() > MAXIMUM_MESSAGE_BYTES {
        return Err(Error::Invalid("message exceeds transport ceiling"));
    }
    macro_rules! decode {
        ($message:ty) => {
            <$message>::decode(message).map_err(|_| Error::Invalid("malformed protobuf message"))?
        };
    }
    match kind {
        "mutation_receipt" => validate_receipt(&decode!(wire::MutationReceipt)),
        "context_view" => {
            validate_context_view(&decode!(wire::ContextView), fixed::<32>(expected)?)
        }
        "warm_context" => {
            validate_warm_view(&decode!(wire::WarmView), Some(fixed::<32>(expected)?), None)
        }
        "warm_view" => validate_warm_view(&decode!(wire::WarmView), None, None),
        "warm_commitment" => {
            validate_warm_view(&decode!(wire::WarmView), None, Some(fixed::<32>(expected)?))
        }
        "run_view" | "generated_run_view" => {
            let view = decode!(wire::RunView);
            if kind == "generated_run_view" {
                validate_generated_run_view(&view, fixed::<16>(expected)?, fixed::<32>(related)?)
            } else {
                validate_run_view(&view, fixed::<16>(expected)?)
            }
        }
        "evaluation_spec" => validate_evaluation_spec(&decode!(wire::EvaluationSpec)),
        "evaluation_view" => {
            let view = decode!(wire::EvaluationView);
            let expected = fixed::<16>(expected)?;
            if related.is_empty() {
                validate_evaluation_view(&view, expected)
            } else {
                let spec = wire::EvaluationSpec::decode(related)
                    .map_err(|_| Error::Invalid("malformed evaluation spec"))?;
                validate_evaluation_admission(&view, expected, &spec)
            }
        }
        "run_event" => {
            let event = decode!(wire::RunEvent);
            validate_run_event(&event)
        }
        _ => Err(Error::Invalid("unknown inference message kind")),
    }
}

/// Validate one customer protobuf message and its caller-bound identity.
///
/// # Errors
/// Returns a stable contract violation message.
pub fn validate_customer_wire(
    kind: &str,
    message: &[u8],
    expected: &[u8],
    related: &[u8],
) -> Result<(), &'static str> {
    validate_customer_wire_inner(kind, message, expected, related)
        .map_err(|Error::Invalid(message)| message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn generated_run_is_bound_to_the_requested_context() {
        let view = wire::RunView {
            run_id: vec![2; 16],
            input: vec![3; 32],
            model: "model".to_owned(),
            ..Default::default()
        };
        let bytes = view.encode_to_vec();
        assert!(validate_generated_run_view(&view, [2; 16], [4; 32]).is_err());
        assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[]).is_err());
        assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[4; 32]).is_err());
        assert!(validate_customer_wire("generated_run_view", &bytes, &[2; 16], &[3; 32]).is_ok());
    }

    #[test]
    fn completed_run_watch_start_is_shared_with_the_host() {
        let mut result = wire::RunResult::default();
        result.terminal = wire::RunTerminal::Completed.into();
        let view = wire::RunView {
            run_id: vec![2; 16],
            input: vec![3; 32],
            model: "model".to_owned(),
            last_sequence: 7,
            result: Some(result),
            ..Default::default()
        };
        let bytes = view.encode_to_vec();
        assert_eq!(watch_run_start_wire(&bytes, &[2; 16], "7"), Ok(false));
        assert_eq!(watch_run_start_wire(&bytes, &[2; 16], "8"), Ok(true));
        assert_eq!(
            watch_run_start_wire(&bytes, &[2; 16], "9"),
            Err("Run cursor exceeds retained events")
        );
        assert_eq!(
            watch_run_start_wire(&bytes, &[3; 16], "8"),
            Err("Run identity differs")
        );
        assert_eq!(
            watch_run_start_wire(&bytes, &[2; 16], "-1"),
            Err("Run cursor is invalid")
        );
        assert_eq!(
            watch_run_start_wire(&bytes, &[2; 16], "18446744073709551616"),
            Err("Run cursor is invalid")
        );
        let mut exhausted = view;
        exhausted.last_sequence = u64::MAX;
        assert_eq!(
            watch_run_start_wire(&exhausted.encode_to_vec(), &[2; 16], "0"),
            Err("Run sequence exhausted")
        );
    }

    #[test]
    fn watch_state_owns_order_terminal_eof_and_overflow_rules() -> Result<(), &'static str> {
        let view = wire::RunView {
            run_id: vec![2; 16],
            input: vec![3; 32],
            model: "model".to_owned(),
            last_sequence: 0,
            ..Default::default()
        };
        let mut state = watch_run_start_state_wire(&view.encode_to_vec(), &[2; 16], "0")?;
        let progress = |sequence| wire::RunEvent {
            sequence,
            event: Some(wire::run_event::Event::Progress(wire::RunProgress {
                kind: "queued".to_owned(),
            })),
        };
        state.advance_wire(&progress(0).encode_to_vec())?;
        assert!(!state.is_terminal());
        assert_eq!(
            state.advance_wire(&progress(0).encode_to_vec()),
            Err("run event order or shape differs")
        );
        assert_eq!(
            state.advance_wire(&progress(2).encode_to_vec()),
            Err("run event order or shape differs")
        );
        assert_eq!(state.finish(), Err("run stream ended before terminal"));

        let terminal = wire::RunEvent {
            sequence: 1,
            event: Some(wire::run_event::Event::Terminal(
                wire::RunTerminal::Completed.into(),
            )),
        };
        state.advance_wire(&terminal.encode_to_vec())?;
        assert!(state.is_terminal());
        assert_eq!(state.finish(), Ok(()));
        assert_eq!(
            state.advance_wire(&progress(2).encode_to_vec()),
            Err("run event order or shape differs")
        );

        let mut max_state = WatchRunState {
            expected: u64::MAX,
            terminal: false,
        };
        assert_eq!(
            max_state.advance_wire(
                &wire::RunEvent {
                    sequence: u64::MAX,
                    event: Some(wire::run_event::Event::Terminal(
                        wire::RunTerminal::Completed.into(),
                    )),
                }
                .encode_to_vec()
            ),
            Err("Run sequence exhausted")
        );
        Ok(())
    }
}
