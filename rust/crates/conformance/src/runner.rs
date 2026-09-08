//! Validation and deterministic receipts for cross-language conformance runners.

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, MapAccess, SeqAccess, Visitor},
};
use std::collections::BTreeSet;
use std::fmt::{self, Display};

/// Stable runner protocol identifier.
pub const RUNNER_PROTOCOL: &str = "acyclic.conformance.runner.v1";
/// Stable qualification receipt identifier.
pub const RECEIPT_PROTOCOL: &str = "acyclic.conformance.receipt.v1";

#[derive(Deserialize)]
struct Suite {
    version: u32,
    cases: Vec<SuiteCase>,
}

#[derive(Deserialize)]
struct SuiteCase {
    name: String,
    family: String,
}

/// The implementation being qualified.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Subject {
    /// Public package or implementation name.
    pub name: String,
    /// Exact package or implementation version.
    pub version: String,
    /// Immutable source revision, normally a Git commit.
    pub source_revision: String,
    /// BLAKE3 digest of the exact package or executable bytes under test.
    pub artifact_digest: String,
}

/// Identity of the runner which executed the suite.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerIdentity {
    /// Language/runtime used by the runner.
    pub language: String,
    /// Public runner package or binary name.
    pub name: String,
    /// Exact runner version.
    pub version: String,
}

/// Runtime wire identity exercised by the runner.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolIdentity {
    /// Harness wire protocol version.
    pub version: String,
    /// Lowercase hexadecimal digest of the descriptor bytes.
    pub descriptor_digest: String,
}

/// Outcome reported for one canonical case.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    /// The assertion completed successfully.
    Passed,
    /// The assertion completed and contradicted the contract.
    Failed,
    /// The runner did not execute the assertion.
    Skipped,
}

/// One runner-produced case result.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CaseResult {
    /// Canonical case name from the locked suite.
    pub name: String,
    /// Execution outcome.
    pub status: CaseStatus,
    /// BLAKE3 digest of stable runner evidence for this case.
    pub evidence_digest: String,
}

/// Cross-language report consumed by the receipt validator.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerReport {
    /// Must equal [`RUNNER_PROTOCOL`].
    pub protocol: String,
    /// Must be `harness` for the Harness v1 suite.
    pub family: String,
    /// Version of the locked suite manifest.
    pub suite_version: u32,
    /// BLAKE3 digest of the exact suite manifest bytes.
    pub suite_digest: String,
    /// Implementation under test.
    pub subject: Subject,
    /// Runner which executed the cases.
    pub runner: RunnerIdentity,
    /// Wire contract exercised by the cases.
    pub protocol_identity: ProtocolIdentity,
    /// Sorted, unique capability profile exercised by the run.
    pub capability_profile: Vec<String>,
    /// Exactly one result for every Harness case, in suite order.
    pub cases: Vec<CaseResult>,
}

/// Deterministic qualification summary emitted after structural validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QualificationReceipt {
    /// Stable receipt format identifier.
    pub protocol: &'static str,
    /// Qualified family.
    pub family: &'static str,
    /// Locked suite version.
    pub suite_version: u32,
    /// Digest of the exact suite bytes.
    pub suite_digest: String,
    /// Implementation under test.
    pub subject: Subject,
    /// Runner which executed the cases.
    pub runner: RunnerIdentity,
    /// Wire identity exercised by the run.
    pub protocol_identity: ProtocolIdentity,
    /// Exact capability profile exercised by the run.
    pub capability_profile: Vec<String>,
    /// True only when every canonical case passed.
    pub qualified: bool,
    /// Number of passed cases.
    pub passed: usize,
    /// Total number of canonical cases.
    pub total: usize,
    /// Digest binding all validated report fields and case results.
    pub report_digest: String,
    /// Validated results in canonical suite order.
    pub cases: Vec<CaseResult>,
}

/// Structural failure which prevents a report from becoming a receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportError(String);

impl Display for ReportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReportError {}

/// Returns the digest that all Harness runner reports must bind.
#[must_use]
pub fn harness_suite_digest() -> String {
    digest(crate::HARNESS_SUITE)
}

/// Returns the exact Harness wire identity that a qualifying report must bind.
#[must_use]
pub fn harness_protocol_identity() -> ProtocolIdentity {
    let identity = acyclic_harness::wire_api::current_protocol();
    ProtocolIdentity {
        version: identity.version,
        descriptor_digest: format!("blake3:{}", identity.descriptor_digest),
    }
}

/// Validates exact suite coverage and emits a deterministic receipt.
pub fn validate_harness_report(report_bytes: &[u8]) -> Result<QualificationReceipt, ReportError> {
    let report = parse_report(report_bytes)?;
    let suite: Suite = serde_json::from_slice(crate::HARNESS_SUITE)
        .map_err(|error| ReportError(format!("invalid embedded Harness suite: {error}")))?;

    if report.protocol != RUNNER_PROTOCOL {
        return Err(ReportError("runner protocol is not supported".into()));
    }
    if report.family != "harness" {
        return Err(ReportError("runner family is not harness".into()));
    }
    if report.suite_version != suite.version || report.suite_digest != harness_suite_digest() {
        return Err(ReportError(
            "runner suite identity does not match the locked suite".into(),
        ));
    }
    validate_identity(&report.subject, &report.runner, &report.protocol_identity)?;
    validate_capabilities(&report.capability_profile)?;

    let expected = suite
        .cases
        .iter()
        .filter(|case| case.family == "harness")
        .map(|case| case.name.as_str())
        .collect::<Vec<_>>();
    if report.cases.len() != expected.len() {
        return Err(ReportError(
            "runner did not return exactly one result per Harness case".into(),
        ));
    }
    for (result, expected_name) in report.cases.iter().zip(expected) {
        if result.name != expected_name {
            return Err(ReportError(format!(
                "runner case order or identity changed at {}",
                result.name
            )));
        }
        validate_digest("case evidence", &result.evidence_digest)?;
    }

    let report_digest = digest(report_bytes);
    let passed = report
        .cases
        .iter()
        .filter(|result| result.status == CaseStatus::Passed)
        .count();
    let total = report.cases.len();
    Ok(QualificationReceipt {
        protocol: RECEIPT_PROTOCOL,
        family: "harness",
        suite_version: report.suite_version,
        suite_digest: report.suite_digest,
        subject: report.subject,
        runner: report.runner,
        protocol_identity: report.protocol_identity,
        capability_profile: report.capability_profile,
        qualified: passed == total,
        passed,
        total,
        report_digest,
        cases: report.cases,
    })
}

fn parse_report(report_bytes: &[u8]) -> Result<RunnerReport, ReportError> {
    let mut deserializer = serde_json::Deserializer::from_slice(report_bytes);
    let value = UniqueValue::deserialize(&mut deserializer)
        .map_err(|error| ReportError(format!("invalid runner report JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| ReportError(format!("invalid runner report JSON: {error}")))?;
    serde_json::from_value(value.0)
        .map_err(|error| ReportError(format!("invalid runner report JSON: {error}")))
}

struct UniqueValue(serde_json::Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object keys")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(value.into()))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(value.into()))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(value.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.visit_string(value.to_owned())
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(value.into()))
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(serde_json::Value::Null))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(serde_json::Value::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        UniqueValue::deserialize(deserializer)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueValue>()? {
            values.push(value.0);
        }
        Ok(UniqueValue(values.into()))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some((key, value)) = map.next_entry::<String, UniqueValue>()? {
            if values.insert(key.clone(), value.0).is_some() {
                return Err(A::Error::custom(format!("duplicate object key {key:?}")));
            }
        }
        Ok(UniqueValue(values.into()))
    }
}

fn validate_identity(
    subject: &Subject,
    runner: &RunnerIdentity,
    protocol: &ProtocolIdentity,
) -> Result<(), ReportError> {
    for (name, value) in [
        ("subject name", subject.name.as_str()),
        ("subject version", subject.version.as_str()),
        ("source revision", subject.source_revision.as_str()),
        ("runner language", runner.language.as_str()),
        ("runner name", runner.name.as_str()),
        ("runner version", runner.version.as_str()),
        ("wire protocol version", protocol.version.as_str()),
    ] {
        if value.is_empty() {
            return Err(ReportError(format!("{name} is empty")));
        }
    }
    if !is_lower_hex(&subject.source_revision) || !matches!(subject.source_revision.len(), 40 | 64)
    {
        return Err(ReportError(
            "source revision is not an immutable 40- or 64-character hexadecimal identity".into(),
        ));
    }
    validate_digest("subject artifact", &subject.artifact_digest)?;
    validate_digest("descriptor", &protocol.descriptor_digest)?;
    if protocol != &harness_protocol_identity() {
        return Err(ReportError(
            "wire protocol identity does not match the compiled Harness descriptor".into(),
        ));
    }
    Ok(())
}

fn validate_capabilities(capabilities: &[String]) -> Result<(), ReportError> {
    if capabilities.is_empty() {
        return Err(ReportError("capability profile is empty".into()));
    }
    let mut previous = None;
    let mut unique = BTreeSet::new();
    for capability in capabilities {
        if capability.is_empty() || !unique.insert(capability) {
            return Err(ReportError(
                "capability profile contains an empty or duplicate value".into(),
            ));
        }
        if previous.is_some_and(|value: &String| value >= capability) {
            return Err(ReportError(
                "capability profile is not strictly sorted".into(),
            ));
        }
        previous = Some(capability);
    }
    Ok(())
}

fn validate_digest(name: &str, value: &str) -> Result<(), ReportError> {
    let Some(hex) = value.strip_prefix("blake3:") else {
        return Err(ReportError(format!("{name} digest must use blake3")));
    };
    if hex.len() != 64 || !is_lower_hex(hex) {
        return Err(ReportError(format!(
            "{name} digest is not lowercase 32-byte hexadecimal"
        )));
    }
    Ok(())
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn digest(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> RunnerReport {
        let suite: Suite = serde_json::from_slice(crate::HARNESS_SUITE).unwrap_or_else(|error| {
            unreachable!("embedded suite must parse in its own tests: {error}")
        });
        RunnerReport {
            protocol: RUNNER_PROTOCOL.into(),
            family: "harness".into(),
            suite_version: suite.version,
            suite_digest: harness_suite_digest(),
            subject: Subject {
                name: "@acyclic/harness".into(),
                version: "0.1.0-rc.1".into(),
                source_revision: "0123456789abcdef0123456789abcdef01234567".into(),
                artifact_digest: digest(b"artifact"),
            },
            runner: RunnerIdentity {
                language: "typescript".into(),
                name: "@acyclic/harness/conformance".into(),
                version: "0.1.0-rc.1".into(),
            },
            protocol_identity: harness_protocol_identity(),
            capability_profile: vec!["durable-local".into(), "wasm-reducer".into()],
            cases: suite
                .cases
                .into_iter()
                .filter(|case| case.family == "harness")
                .map(|case| CaseResult {
                    evidence_digest: digest(case.name.as_bytes()),
                    name: case.name,
                    status: CaseStatus::Passed,
                })
                .collect(),
        }
    }

    fn encode(report: &RunnerReport) -> Vec<u8> {
        serde_json::to_vec(report)
            .unwrap_or_else(|error| unreachable!("test runner report must serialize: {error}"))
    }

    #[test]
    fn complete_exact_report_produces_a_bound_qualification_receipt() -> Result<(), ReportError> {
        let bytes = encode(&report());
        let receipt = validate_harness_report(&bytes)?;
        assert!(receipt.qualified);
        assert_eq!(receipt.passed, 17);
        assert_eq!(receipt.total, 17);
        assert_eq!(receipt.report_digest, digest(&bytes));
        Ok(())
    }

    #[test]
    fn report_digest_binds_the_exact_unambiguous_input_bytes() -> Result<(), ReportError> {
        let report = report();
        let compact = validate_harness_report(&encode(&report))?;
        let pretty = serde_json::to_vec_pretty(&report)
            .unwrap_or_else(|error| unreachable!("test runner report must serialize: {error}"));
        let pretty = validate_harness_report(&pretty)?;
        assert_ne!(compact.report_digest, pretty.report_digest);
        Ok(())
    }

    #[test]
    fn failures_are_receipted_but_do_not_qualify() -> Result<(), ReportError> {
        let mut report = report();
        report.cases[0].status = CaseStatus::Failed;
        let receipt = validate_harness_report(&encode(&report))?;
        assert!(!receipt.qualified);
        assert_eq!(receipt.passed, receipt.total - 1);
        Ok(())
    }

    #[test]
    fn missing_reordered_and_duplicate_cases_fail_closed() {
        let mut missing = report();
        missing.cases.pop();
        assert!(validate_harness_report(&encode(&missing)).is_err());

        let mut reordered = report();
        reordered.cases.swap(0, 1);
        assert!(validate_harness_report(&encode(&reordered)).is_err());

        let mut duplicate = report();
        duplicate.cases[1] = duplicate.cases[0].clone();
        assert!(validate_harness_report(&encode(&duplicate)).is_err());
    }

    #[test]
    fn unknown_fields_and_unbound_digests_fail_closed() {
        let mut value = serde_json::to_value(report())
            .unwrap_or_else(|error| unreachable!("test runner report must serialize: {error}"));
        value["untrusted"] = serde_json::json!(true);
        let bytes = serde_json::to_vec(&value)
            .unwrap_or_else(|error| unreachable!("test JSON value must serialize: {error}"));
        assert!(validate_harness_report(&bytes).is_err());

        let duplicate = br#"{"protocol":"acyclic.conformance.runner.v1","protocol":"acyclic.conformance.runner.v1"}"#;
        assert!(validate_harness_report(duplicate).is_err());

        let mut wrong_suite = report();
        wrong_suite.suite_digest = digest(b"another suite");
        assert!(validate_harness_report(&encode(&wrong_suite)).is_err());

        let mut wrong_protocol = report();
        wrong_protocol.protocol_identity.descriptor_digest = digest(b"another descriptor");
        assert!(validate_harness_report(&encode(&wrong_protocol)).is_err());

        let mut malformed_artifact = report();
        malformed_artifact.subject.artifact_digest = "blake3:00".into();
        assert!(validate_harness_report(&encode(&malformed_artifact)).is_err());
    }
}
