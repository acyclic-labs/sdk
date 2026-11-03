//! Checks original experiment decoders against independent admission invariants.
#![no_main]

use acyclic_harness_core::experiments::{
    Observation,
    probes::{Decision, Scorer},
    reports::AssignedTrials,
    stats::{Cluster, PairedStats},
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

type Admits = fn(&[u8]) -> bool;

fn valid_files(bytes: &[u8]) -> (Scorer, i64) {
    let population: [u8; 32] =
        std::array::from_fn(|index| bytes.get(index).copied().unwrap_or_default());
    let word = |start| u64::from_le_bytes(population[start..start + 8].try_into().unwrap());
    let expected = word(0) as i64;
    let score = f64::from_bits(word(24));
    let score = score.is_finite().then_some(score);
    let cost = score.map(f64::abs);
    let wire = serde_json::to_vec(&json!({"version":2,"population":population,
        "replicate":word(8),"arm":word(16),"score":score,"cost_usd":cost}))
    .expect("valid observation encodes");
    let row =
        Observation::decode(&wire).expect("independent admission invariant: valid observation");
    // Compare with the original wire numbers, including JSON decimal rounding.
    let measurements: Value = serde_json::from_slice(&wire).expect("generated JSON parses");
    pairs(
        &row,
        population,
        word(8),
        word(16),
        measurements["score"].as_f64(),
        measurements["cost_usd"].as_f64(),
    );
    let wire =
        serde_json::to_vec(&json!({"version":1,"value":expected})).expect("valid decision encodes");
    assert!(
        Decision::decode(&wire).is_ok(),
        "independent admission invariant: valid decision"
    );
    let wire = serde_json::to_vec(
        &json!({"version":1,"expected":expected,"injected":expected.wrapping_add(1)}),
    )
    .expect("valid scorer encodes");
    (
        Scorer::decode(&wire).expect("independent admission invariant: valid scorer"),
        expected,
    )
}

fn pairs(
    row: &Observation,
    population: [u8; 32],
    replicate: u64,
    arm: u64,
    score: Option<f64>,
    cost: Option<f64>,
) {
    let mut foreign = population;
    foreign[0] ^= 1;
    for mask in 0..4 {
        let baseline_score = (mask & 1 != 0).then_some(0.0);
        let baseline_cost = (mask & 2 != 0).then_some(0.0);
        let baseline = Observation::new(
            population,
            replicate,
            arm.wrapping_add(1),
            baseline_score,
            baseline_cost,
        )
        .expect("matched observation admits");
        let expected_score = baseline_score.and(score);
        let expected_cost = baseline_cost.and(cost);
        assert_eq!(
            (
                row.difference(&baseline).expect("matched scores compare"),
                baseline
                    .difference(row)
                    .expect("reverse matched scores compare"),
                row.cost_difference(&baseline)
                    .expect("matched costs compare"),
                baseline
                    .cost_difference(row)
                    .expect("reverse matched costs compare")
            ),
            (
                expected_score,
                expected_score.map(|value| -value),
                expected_cost,
                expected_cost.map(|value| -value)
            ),
            "independent pairing invariant: matched measurements"
        );
        for (population, replicate, arm) in [
            (foreign, replicate, arm.wrapping_add(1)),
            (population, replicate.wrapping_add(1), arm.wrapping_add(1)),
            (population, replicate, arm),
        ] {
            let invalid =
                Observation::new(population, replicate, arm, baseline_score, baseline_cost)
                    .expect("individually valid observation admits");
            assert!(
                row.difference(&invalid).is_err()
                    && invalid.difference(row).is_err()
                    && row.cost_difference(&invalid).is_err()
                    && invalid.cost_difference(row).is_err(),
                "independent pairing invariant: foreign identity refusal"
            );
        }
    }
}

fn trials(bytes: &[u8], scorer: &Scorer, expected: i64) {
    let assessments =
        [expected, expected.wrapping_add(1), expected.wrapping_add(2)].map(|answer| {
            let wire = serde_json::to_vec(&json!({"version":1,"value":answer}))
                .expect("trial decision encodes");
            scorer.score(&Decision::decode(&wire).expect("trial decision admits"))
        });
    let mut trials = AssignedTrials::default();
    let mut refused = AssignedTrials::default();
    let mut counts = [0_u64; 4];
    let mut priced = 0_u64;
    let mut known_cost = 0_u128;
    // Integer prices give an independent exact batch sum. The suffix supplies
    // all four outcomes and multiple priced trials, including failed work.
    for (index, byte) in bytes.iter().copied().chain([12, 21, 30, 39]).enumerate() {
        let kind = usize::from(byte & 3);
        let cost = byte >> 3;
        let price = (byte & 4 != 0).then_some(f64::from(cost));
        let assessment = assessments.get(kind);
        trials
            .push(assessment, price)
            .expect("valid trial assignment admits");
        refused
            .push(assessment, price)
            .expect("refusal history admits the same valid trial");
        assert!(
            refused
                .push(assessment, Some([-1.0, f64::NAN, f64::INFINITY][index % 3]))
                .is_err(),
            "independent report invariant: invalid cost refusal"
        );
        counts[kind] += 1;
        priced += u64::from(price.is_some());
        known_cost += u128::from(cost) * u128::from(price.is_some());
    }
    let assigned: u64 = counts.iter().sum();
    let valid = assigned - counts[3];
    let report = trials.report().expect("assigned history reports");
    assert_eq!(
        refused.report().expect("refused history still reports"),
        report,
        "independent report invariant: refusal preserves report"
    );
    assert_eq!(
        (
            report.assigned,
            report.valid,
            report.unscored,
            report.correct,
            report.adopted_injection
        ),
        (assigned, valid, counts[3], counts[0], counts[1]),
        "independent report invariant: assignment counts"
    );
    assert_eq!(
        (
            report.correct_per_assigned,
            report.correct_per_valid,
            report.adoption_per_assigned_lower_bound,
            report.unscored_rate
        ),
        (
            counts[0] as f64 / assigned as f64,
            Some(counts[0] as f64 / valid as f64),
            counts[1] as f64 / assigned as f64,
            counts[3] as f64 / assigned as f64
        ),
        "independent report invariant: rates"
    );
    let cost = known_cost as f64;
    let total = (priced == assigned).then_some(cost);
    assert_eq!(
        report.priced_trials, priced,
        "independent report invariant: priced count"
    );
    for (actual, expected) in [
        (Some(report.known_cost_usd), Some(cost)),
        (report.total_cost_usd, total),
        (
            report.cost_per_assigned_usd,
            total.map(|cost| cost / assigned as f64),
        ),
    ] {
        assert_eq!(
            actual.is_some(),
            expected.is_some(),
            "independent report invariant: cost completeness"
        );
        if let (Some(actual), Some(expected)) = (actual, expected) {
            let tolerance = 64.0 * assigned as f64 * f64::EPSILON;
            assert!(
                (actual - expected).abs() <= tolerance * expected.abs().max(1.0),
                "independent report invariant: cost sum; actual={actual}, expected={expected}"
            );
        }
    }
}

fn statistics(bytes: &[u8]) {
    // The original unequal-cluster example is the empty-input case. Remaining
    // bytes add bounded integer observations, grouped without retaining rows.
    let groups = || {
        [&[1, 3][..], &[5][..], &[254, 0, 2][..]]
            .into_iter()
            .chain(bytes.chunks(3))
    };
    let value = |byte: &u8| f64::from(*byte as i8);
    let mut original = PairedStats::default();
    let mut refused = PairedStats::default();
    for group in groups() {
        let mut cluster = Cluster::default();
        for difference in group.iter().map(value) {
            cluster
                .push(difference)
                .expect("integer observation admits");
        }
        let mut after_refusal = cluster;
        assert!(
            after_refusal.push(f64::NAN).is_err(),
            "independent statistics invariant: nonfinite refusal"
        );
        original.push(cluster).expect("integer cluster admits");
        refused
            .push(after_refusal)
            .expect("independent statistics invariant: refused cluster retains admission");
    }
    let estimate = original.estimate().expect("independent clusters estimate");
    assert!(
        refused.push(Cluster::default()).is_err(),
        "independent statistics invariant: empty cluster refusal"
    );
    let after = refused.estimate().expect("refused history still estimates");
    assert_eq!(
        (
            after.pairs,
            after.clusters,
            after.difference,
            after.paired_se,
            after.clustered_se
        ),
        (
            estimate.pairs,
            estimate.clusters,
            estimate.difference,
            estimate.paired_se,
            estimate.clustered_se
        ),
        "independent statistics invariant: refusal preserves estimates"
    );

    let pairs = bytes.len() as u64 + 6;
    let clusters = bytes.chunks(3).len() as u64 + 3;
    assert_eq!(
        (estimate.pairs, estimate.clusters),
        (pairs, clusters),
        "independent statistics invariant: independent sample counts"
    );
    // Batch sums/residuals are independent of the production online QR update.
    // Integer sums fit i128 for any addressable slice; no rounding accumulation
    // enters the reference mean. Variances use direct centered residuals.
    let sum: i128 = groups().flatten().map(|byte| i128::from(*byte as i8)).sum();
    let n = pairs as f64;
    let g = clusters as f64;
    let mean = sum as f64 / n;
    let paired = groups()
        .flatten()
        .map(|byte| (value(byte) - mean).powi(2))
        .sum::<f64>();
    let clustered = groups()
        .map(|group| {
            let sum: i128 = group.iter().map(|byte| i128::from(*byte as i8)).sum();
            (sum as f64 - group.len() as f64 * mean).powi(2)
        })
        .sum::<f64>();
    // Compare numerical projections at a precision budget proportional to the
    // current input's arithmetic work, without a second online state machine.
    let tolerance = 64.0 * n * f64::EPSILON;
    for (actual, expected) in [
        (estimate.difference, mean),
        (estimate.paired_se, (paired / (n - 1.0) / n).sqrt()),
        (
            estimate.clustered_se,
            (g / (g - 1.0) * clustered / n / n).sqrt(),
        ),
    ] {
        assert!(
            (actual - expected).abs() <= tolerance * expected.abs().max(1.0),
            "independent statistics invariant: batch projection; actual={actual}, expected={expected}"
        );
    }
}

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
    }
}

fuzz_target!(|bytes: &[u8]| {
    let (valid_scorer, expected) = valid_files(bytes);
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
    trials(bytes, &valid_scorer, expected);
    statistics(bytes);
});
