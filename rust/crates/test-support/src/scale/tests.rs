//! Production report-oracle properties and adversarial observation controls.
use super::*;
use proptest::prelude::*;

const RECOVERY: &str = "cargo test -p acyclic-test-support scale::tests";
const OPERATIONS: [Operation; 1] = [Operation::ObjectHead];

// These observations are inputs to the shipped report checker, not a model of
// provider operations or a claim about their physical work and progress.
fn observed(values: &[Vec<u64>; 3], metric: usize, operation: Operation) -> Report {
    Report {
        points: POPULATIONS
            .into_iter()
            .zip(values)
            .map(|(agents, values)| {
                let mut sample = Sample::new(operation);
                for &value in values {
                    let mut work = Work([Some(0); METRICS.len()]);
                    work.0[metric] = Some(value);
                    sample.record(work, RECOVERY).expect("bounded observations");
                }
                Point {
                    agents,
                    samples: vec![sample],
                    idle: Idle {
                        bytes: Some(agents * 64),
                        threads: Some(0),
                        timers: Some(0),
                    },
                }
            })
            .collect(),
    }
}

fn flat() -> Report {
    let mut report = observed(&[vec![1; 4], vec![1; 4], vec![1; 4]], 0, OPERATIONS[0]);
    for point in &mut report.points {
        let sample = &mut point.samples[0];
        let counter = sample.work[0];
        sample.work.fill(counter);
    }
    report
}

proptest! {
    #[test]
    fn recorded_work_matches_exact_mean_and_tail_oracle(
        values in proptest::array::uniform3(proptest::collection::vec(any::<u64>(), 1..17)),
        metric in 0..METRICS.len(),
        operation in 0..CORE_OPERATIONS.len(),
    ) {
        let operation = CORE_OPERATIONS[operation];
        let report = observed(&values, metric, operation);
        let totals = values.each_ref().map(|values| values.iter().copied().map(u128::from).sum::<u128>());
        let maxima = values.each_ref().map(|values| *values.iter().max().unwrap());
        for (index, point) in report.points.iter().enumerate() {
            let sample = &point.samples[0];
            prop_assert_eq!(sample.observations, values[index].len() as u64);
            prop_assert_eq!(sample.work[metric], Counter {
                total: totals[index], maximum: maxima[index], missing: 0,
            });
        }
        // At most sixteen u64 observations per cohort: both independent cross
        // products fit u128. Population sizes must not be the denominators.
        let grows = (1..values.len()).any(|index| {
            totals[index] * values[index - 1].len() as u128
                > totals[index - 1] * values[index].len() as u128
                || maxima[index] > maxima[index - 1]
        });
        let findings = report.qualification_findings(&[operation], RECOVERY);
        prop_assert!(findings.iter().all(|finding| finding.step == "constant work"));
        prop_assert_eq!(!findings.is_empty(), grows);
    }
}

#[test]
fn rejects_missing_coverage_hidden_tail_work_memory_growth_and_forged_reports() {
    let original = flat();
    for (metric, name) in METRICS.iter().enumerate() {
        let mut missing = original.clone();
        missing.points[1].samples[0].work[metric].missing = 1;
        assert!(
            missing
                .findings(&OPERATIONS, RECOVERY)
                .iter()
                .any(|failure| failure.step == "counter coverage"),
            "missing {name} escaped qualification",
        );
        let mut tail = original.clone();
        let counter = &mut tail.points[2].samples[0].work[metric];
        // The mean falls below the smaller point's mean, but one op becomes dearer.
        counter.total = 2;
        counter.maximum = 2;
        assert!(
            tail.findings(&OPERATIONS, RECOVERY)
                .iter()
                .any(|failure| failure.step == "constant work"),
            "extra {name} escaped qualification",
        );
    }
    let mut memory = original.clone();
    memory.points[2].idle.bytes = Some(POPULATIONS[2] * 64 + 1);
    assert!(
        memory
            .findings(&OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "constant idle memory")
    );
    for idle in [
        Idle {
            threads: Some(1),
            ..original.points[0].idle
        },
        Idle {
            timers: Some(1),
            ..original.points[0].idle
        },
        Idle::default(),
    ] {
        let mut incomplete = original.clone();
        incomplete.points[0].idle = idle;
        assert!(!incomplete.findings(&OPERATIONS, RECOVERY).is_empty());
    }
    let mut partial = original.clone();
    partial.points.pop();
    assert_eq!(
        partial.qualify(&OPERATIONS, RECOVERY).unwrap_err().step,
        "population coverage"
    );
    let mut forged = original;
    forged.points[1].samples.pop();
    assert!(
        forged
            .findings(&OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "report shape")
    );
    assert_eq!(
        Report::default().findings(&OPERATIONS, RECOVERY)[0].step,
        "report shape"
    );
    // Maximum-width counts must not overflow a ratio comparison or round away
    // one extra unit of work at a large population.
    let n = u64::MAX;
    let total = u128::from(n) * u128::from(n);
    assert!(mean_grows(total - 1, n, total, n));
    assert!(!mean_grows(total, n, total - 1, n));
}

#[test]
fn fixed_sample_counts_use_observations_instead_of_population_as_denominator() {
    // Time and work share a fixed sample count at each resident population.
    // Dividing these counts by population would conceal the larger sample cost.
    let operations = [Operation::ObjectHead];
    flat()
        .qualify(&operations, RECOVERY)
        .expect("constant sampled work");
    let mut report = observed(&[vec![0, 2], vec![2, 2], vec![2, 2]], 5, operations[0]);
    assert!(
        report
            .findings(&operations, RECOVERY)
            .iter()
            .any(|finding| finding.step == "constant work"),
        "growing mean with an unchanged maximum escaped qualification",
    );
    // Retain all original refusals once. The former reporting/admission pair
    // is an independent allocation negative on this same counted report.
    let mut reused = None;
    let single = crate::measure(|| {
        reused = Some(report.qualification_findings(&operations, RECOVERY));
    });
    let mut repeated = None;
    let duplicate = crate::measure(|| {
        repeated = Some((
            report.findings(&operations, RECOVERY),
            report.qualify(&operations, RECOVERY),
        ));
    });
    let reused = reused.unwrap();
    let (original, qualified) = repeated.unwrap();
    assert_eq!(reused, original);
    assert_eq!(qualified.unwrap_err(), reused[0]);
    assert!(single.count_total < duplicate.count_total);
    assert!(single.bytes_total < duplicate.bytes_total);
    report.points[1].samples[0] = Sample::new(operations[0]);
    assert!(
        report
            .findings(&operations, RECOVERY)
            .iter()
            .any(|finding| finding.step == "report shape")
    );
    assert!(
        flat()
            .findings(&[Operation::ObjectHead, Operation::ObjectHead], RECOVERY)
            .iter()
            .any(|finding| finding.step == "report shape")
    );
}
