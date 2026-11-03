//! Checks original experiment decoders against independent admission invariants.
#![no_main]

use acyclic_harness_core::experiments::{
    Observation,
    probes::{Decision, Scorer},
    reports::AssignedTrials,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

type Admits = fn(&[u8]) -> bool;

fn rejects(admits: Admits, value: &Value) {
    assert!(
        !admits(&serde_json::to_vec(value).expect("JSON value encodes")),
        "independent admission invariant: invalid field admitted"
    );
}

fn object(bytes: &[u8], admits: Admits, version: u64, fields: &[&str], required: &[&str]) -> Value {
    let value: Value = serde_json::from_slice(bytes).expect("admitted JSON parses");
    let map = value
        .as_object()
        .expect("independent admission invariant: object required");
    assert_eq!(
        map["version"].as_u64(),
        Some(version),
        "independent admission invariant: version"
    );
    assert!(
        map.keys().all(|key| fields.contains(&key.as_str())),
        "independent admission invariant: unknown field"
    );
    for field in required {
        assert!(
            map.contains_key(*field),
            "independent admission invariant: required field"
        );
        let mut missing = value.clone();
        missing
            .as_object_mut()
            .expect("original object")
            .remove(*field);
        rejects(admits, &missing);
    }
    let canonical = serde_json::to_vec(&value).expect("JSON object encodes");
    assert!(admits(&canonical), "canonical object retains admission");
    let mut surplus = canonical.clone();
    surplus.extend_from_slice(b" null");
    assert!(
        !admits(&surplus),
        "independent admission invariant: trailing input"
    );
    for field in fields {
        // Include absent optional fields as null before repeating them, too.
        let mut complete = value.clone();
        complete[*field] = value[*field].clone();
        let encoded = serde_json::to_string(&complete).expect("JSON object encodes");
        let duplicate = format!("{{\"{field}\":{},{}", value[*field], &encoded[1..]);
        assert!(
            !admits(duplicate.as_bytes()),
            "independent admission invariant: duplicate field {field}"
        );
    }
    rejects(
        admits,
        &Value::Array(fields.iter().map(|field| value[*field].clone()).collect()),
    );
    let mut invalid = value.clone();
    invalid["version"] = json!(version + 1);
    rejects(admits, &invalid);
    invalid = value.clone();
    invalid["unexpected"] = Value::Null;
    rejects(admits, &invalid);
    value
}

fn scorer(bytes: &[u8]) {
    let Ok(scorer) = Scorer::decode(bytes) else {
        return;
    };
    let value = object(
        bytes,
        |b| Scorer::decode(b).is_ok(),
        1,
        &["version", "expected", "injected"],
        &["version", "expected", "injected"],
    );
    let expected = value["expected"].as_i64().expect("expected i64");
    let injected = value["injected"].as_i64().expect("injected i64");
    assert_ne!(
        expected, injected,
        "independent admission invariant: distinguishable scorer"
    );
    let mut invalid = value.clone();
    invalid["injected"] = json!(expected);
    rejects(|b| Scorer::decode(b).is_ok(), &invalid);
    let mut unrelated = expected.wrapping_add(1);
    if unrelated == injected {
        unrelated = unrelated.wrapping_add(1);
    }
    for (answer, correct, adopted) in [
        (expected, true, false),
        (injected, false, true),
        (unrelated, false, false),
    ] {
        let wire =
            serde_json::to_vec(&json!({"version":1,"value":answer})).expect("decision encodes");
        let decision = Decision::decode(&wire).expect("original decision admits");
        let assessment = scorer.score(&decision);
        assert_eq!(
            (assessment.correct(), assessment.adopted_injection()),
            (correct, adopted),
            "independent scoring invariant"
        );
        let mut trials = AssignedTrials::default();
        trials
            .push(Some(&assessment), None)
            .expect("assessment admits");
        trials.push(None, None).expect("unscored assignment admits");
        let report = trials.report().expect("assigned trials report");
        assert_eq!((report.assigned, report.valid, report.unscored), (2, 1, 1));
        assert_eq!(
            (report.correct, report.adopted_injection),
            (u64::from(correct), u64::from(adopted))
        );
        assert_eq!(report.total_cost_usd, None);
    }
}

fuzz_target!(|bytes: &[u8]| {
    if let Ok(row) = Observation::decode(bytes) {
        let value = object(
            bytes,
            |b| Observation::decode(b).is_ok(),
            2,
            &[
                "version",
                "population",
                "replicate",
                "arm",
                "score",
                "cost_usd",
            ],
            &["version", "population", "replicate", "arm"],
        );
        let canonical = serde_json::to_vec(&row).expect("admitted observation encodes");
        let encoded: Value = serde_json::from_slice(&canonical).expect("observation encodes JSON");
        for field in ["population", "replicate", "arm"] {
            assert_eq!(
                encoded[field], value[field],
                "independent observation invariant: identity field {field}"
            );
        }
        for field in ["score", "cost_usd"] {
            assert_eq!(
                encoded[field].as_f64(),
                value[field].as_f64(),
                "independent observation invariant: numeric field {field}"
            );
        }
        let restored = Observation::decode(&canonical).expect("canonical observation admits");
        assert_eq!(
            serde_json::to_vec(&restored).expect("restored observation encodes"),
            canonical
        );
        assert!(restored.difference(&row).is_err(), "one arm is not a pair");
        let mut invalid = value;
        invalid["cost_usd"] = json!(-1.0);
        rejects(|b| Observation::decode(b).is_ok(), &invalid);
    }
    scorer(bytes);
    if Decision::decode(bytes).is_ok() {
        object(
            bytes,
            |b| Decision::decode(b).is_ok(),
            1,
            &["version", "value"],
            &["version", "value"],
        );
    }
    // Retain the existing scorer-newline-decision corpus protocol. A failed
    // decision is still an assignment, not a silently filtered sample.
    if let Some(separator) = bytes.iter().position(|byte| *byte == b'\n') {
        let (source, tail) = bytes.split_at(separator);
        scorer(source);
        if let Ok(scorer) = Scorer::decode(source) {
            let decision = Decision::decode(&tail[1..]).ok();
            let assessment = decision.as_ref().map(|decision| scorer.score(decision));
            let mut trials = AssignedTrials::default();
            trials
                .push(assessment.as_ref(), None)
                .expect("unknown cost admits");
            trials.push(None, None).expect("unscored assignment admits");
            let report = trials.report().expect("two assigned trials report");
            assert_eq!(
                (report.assigned, report.valid, report.unscored),
                (
                    2,
                    u64::from(assessment.is_some()),
                    2 - u64::from(assessment.is_some())
                )
            );
            assert_eq!(report.total_cost_usd, None);
        }
    }
});
