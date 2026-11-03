//! Checks original experiment decoders against independent admission invariants.
#![no_main]

use acyclic_harness_core::experiments::{
    Error, Observation,
    plan::PairAssignment,
    probes::{Decision, Scorer},
    reports::AssignedTrials,
    stats::{Cluster, PairedStats, required_clusters},
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

type Admits = fn(&[u8]) -> bool;

fn valid_files(bytes: &[u8]) -> (Scorer, i64) {
    let population: [u8; 32] =
        std::array::from_fn(|index| bytes.get(index).copied().unwrap_or_default());
    let word = |start| u64::from_le_bytes(population[start..start + 8].try_into().unwrap());
    let expected = word(0) as i64;
    let measurement = f64::from_bits(word(24));
    let measurement = measurement.is_finite().then_some(measurement);
    let score = measurement.filter(|_| population[0] & 2 != 0);
    let cost = measurement.filter(|_| population[0] & 4 != 0).map(f64::abs);
    let wire = serde_json::to_vec(&json!({"version":2,"population":population,
        "replicate":word(8),"arm":word(16),"score":score,"cost_usd":cost}))
    .expect("valid observation encodes");
    let row =
        Observation::decode(&wire).expect("independent admission invariant: valid observation");
    // Compare with the original wire numbers, including JSON decimal rounding.
    let measurements: Value = serde_json::from_slice(&wire).expect("generated JSON parses");
    let encoded = serde_json::to_value(&row).expect("admitted observation projects");
    identities(&encoded, &measurements);
    pairs(
        &row,
        population,
        word(8),
        word(16),
        measurements["score"].as_f64(),
        measurements["cost_usd"].as_f64(),
        0.0,
    );
    // Bounded integer subtraction is an exact reference for nonzero baselines.
    let baseline = i16::from(population[1]) + 1;
    let score = (population[0] & 2 != 0).then_some(i16::from(population[0] as i8));
    let cost = (population[0] & 4 != 0).then_some(i16::from(population[2]) + 1);
    let value = json!({"version":2,"population":population,
        "replicate":word(8),"arm":word(16),"score":score,"cost_usd":cost});
    let wire = serde_json::to_vec(&value).expect("integer observation encodes");
    let row = Observation::decode(&wire).expect("integer observation admits");
    identities(
        &serde_json::to_value(&row).expect("integer observation projects"),
        &value,
    );
    pairs(
        &row,
        population,
        word(8),
        word(16),
        score.map(|value| f64::from(value - baseline)),
        cost.map(|value| f64::from(value - baseline)),
        f64::from(baseline),
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

fn identities(encoded: &Value, original: &Value) {
    for field in ["population", "replicate", "arm"] {
        assert_eq!(
            encoded[field], original[field],
            "independent observation invariant: identity field {field}"
        );
    }
}

fn pairs(
    row: &Observation,
    population: [u8; 32],
    replicate: u64,
    arm: u64,
    score_difference: Option<f64>,
    cost_difference: Option<f64>,
    baseline: f64,
) {
    let mut foreign = population;
    foreign[0] ^= 1;
    for mask in 0..4 {
        let baseline_score = (mask & 1 != 0).then_some(baseline);
        let baseline_cost = (mask & 2 != 0).then_some(baseline);
        let baseline = Observation::new(
            population,
            replicate,
            arm.wrapping_add(1),
            baseline_score,
            baseline_cost,
        )
        .expect("matched observation admits");
        let expected_score = baseline_score.and(score_difference);
        let expected_cost = baseline_cost.and(cost_difference);
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

fn insufficient_clusters(stats: &PairedStats) {
    assert!(
        matches!(
            stats.estimate(),
            Err(Error::Statistics(
                "at least two independent clusters are required"
            ))
        ),
        "independent statistics invariant: independent cluster admission"
    );
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
    insufficient_clusters(&original);
    for (index, group) in groups().enumerate() {
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
        if index == 0 {
            insufficient_clusters(&original);
        }
        refused
            .push(after_refusal)
            .expect("independent statistics invariant: refused cluster retains admission");
    }
    let estimate = original.estimate().expect("independent clusters estimate");
    assert!(
        refused.push(Cluster::default()).is_err(),
        "independent statistics invariant: empty cluster refusal"
    );
    let mut extreme = Cluster::default();
    extreme.push(f64::MAX).expect("finite singleton admits");
    assert!(
        refused.push(extreme).is_err(),
        "independent statistics invariant: overflow refusal"
    );
    let after = refused
        .estimate()
        .expect("independent statistics invariant: refusal preserves estimates");
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

fn planning(bytes: &[u8]) {
    let word = |index| u128::from(bytes.get(index).copied().unwrap_or_default());
    let sd = word(0);
    let effect = word(1) + 1;
    let alpha = word(2) + 1;
    let power = word(3) + 1;
    // Exact rational ceiling; no floating-point planning algorithm is copied.
    let numerator = ((alpha + power) * sd).pow(2);
    let denominator = effect.pow(2);
    let expected = numerator.div_ceil(denominator).max(2) as u64;
    let inputs = [sd as f64, effect as f64, alpha as f64, power as f64];
    assert_eq!(
        required_clusters(inputs[0], inputs[1], inputs[2], inputs[3])
            .expect("bounded planning inputs admit"),
        expected,
        "independent planning invariant: rational ceiling"
    );
    for index in 0..4 {
        for invalid in [f64::NAN, f64::INFINITY, -1.0]
            .into_iter()
            .chain((index != 0).then_some(0.0))
        {
            let mut refused = inputs;
            refused[index] = invalid;
            assert!(
                required_clusters(refused[0], refused[1], refused[2], refused[3]).is_err(),
                "independent planning invariant: invalid input refusal"
            );
        }
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

fn decision(bytes: &[u8]) {
    if Decision::decode(bytes).is_ok() {
        object(
            bytes,
            |b| Decision::decode(b).is_ok(),
            1,
            &["version", "value"],
            &["version", "value"],
        );
    }
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
        identities(&encoded, &value);
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
    decision(bytes);
    if let Ok(assignment) = serde_json::from_slice::<PairAssignment>(bytes) {
        object(
            bytes,
            |input| serde_json::from_slice::<PairAssignment>(input).is_ok(),
            1,
            &["version", "population", "replicate"],
            &["version", "population", "replicate"],
        );
        let canonical = serde_json::to_vec(&assignment).expect("admitted assignment encodes");
        assert_eq!(
            serde_json::from_slice::<PairAssignment>(&canonical)
                .expect("canonical assignment admits"),
            assignment
        );
    }
    // Retain both original files in the scorer-newline-decision corpus protocol.
    if let Some(separator) = bytes.iter().position(|byte| *byte == b'\n') {
        let (source, tail) = bytes.split_at(separator);
        scorer(source);
        decision(&tail[1..]);
    }
    trials(bytes, &valid_scorer, expected);
    statistics(bytes);
    planning(bytes);
});
