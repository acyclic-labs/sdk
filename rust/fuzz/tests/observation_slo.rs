//! Replay production-writer captures and mutations through the shared fuzz oracle.

#[path = "../support/observation_slo.rs"]
mod oracle;

#[test]
fn histogram_projection_matches_exact_endpoint_cohorts() -> Result<(), Box<dyn std::error::Error>> {
    use proptest::{
        prelude::*,
        test_runner::{TestCaseError, TestRunner},
    };
    let fraction = || prop_oneof![Just(0_u32), Just(1_000_000), 0_u32..=1_000_000];
    let input = (
        prop::collection::vec((0_u32..=94, 0_u8..3), 0..17),
        any::<u64>(),
        0_u8..5,
        fraction(),
        fraction(),
    );
    TestRunner::deterministic().run(
        &input,
        |(buckets, arbitrary, threshold, success, target)| {
            let fail = |error: Box<dyn std::error::Error>| TestCaseError::fail(error.to_string());
            let [low, high] =
                oracle::latency_bounds(buckets.first().map_or(0, |&(bucket, _)| bucket))
                    .map_err(fail)?;
            let low = u64::try_from(low.as_nanos()).unwrap_or(u64::MAX);
            let high = u64::try_from(high.as_nanos()).unwrap_or(u64::MAX);
            let maximum = match threshold {
                0 => low.saturating_sub(1),
                1 => low,
                2 => high.saturating_sub(1),
                3 => high,
                _ => arbitrary,
            };
            let mut slo: acyclic_observability::Slo =
                serde_json::from_slice(include_bytes!("../corpus/observation_slo/zero"))
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
            slo.success_ppm = success;
            slo.latency = Some(acyclic_observability::LatencySlo {
                maximum_ns: maximum,
                target_ppm: target,
            });
            let bytes =
                serde_json::to_vec(&slo).map_err(|error| TestCaseError::fail(error.to_string()))?;
            let decoded = serde_json::from_slice(&bytes)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            oracle::histogram(&decoded, &buckets).map_err(fail)
        },
    )?;
    Ok(())
}

#[test]
fn independent_cohorts_preserve_inclusive_slo_thresholds() -> Result<(), Box<dyn std::error::Error>>
{
    use proptest::{
        prelude::*,
        test_runner::{TestCaseError, TestRunner},
    };

    let count = || prop_oneof![Just(0_u64), Just(u64::MAX), any::<u64>()];
    let fraction = || prop_oneof![Just(0_u32), Just(1_000_000), 0_u32..=1_000_000];
    let input = (count(), count(), count(), fraction(), fraction(), 0_u64..=1);
    TestRunner::deterministic().run(
        &input,
        |(ok, err, incomplete, success, target, maximum)| {
            let err = err.min(u64::MAX - ok);
            let incomplete = incomplete.min(u64::MAX - ok - err);
            let error = |error: serde_json::Error| TestCaseError::fail(error.to_string());
            let mut slo: acyclic_observability::Slo =
                serde_json::from_slice(include_bytes!("../corpus/observation_slo/zero"))
                    .map_err(error)?;
            slo.success_ppm = success;
            slo.latency = Some(acyclic_observability::LatencySlo {
                maximum_ns: maximum,
                target_ppm: target,
            });
            let bytes = serde_json::to_vec(&slo).map_err(error)?;
            let decoded = serde_json::from_slice(&bytes).map_err(error)?;
            oracle::evidence(&decoded, [ok, err, incomplete])
                .map_err(|error| TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}

#[test]
fn persisted_slo_corpus_and_mutations() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = oracle::corpus()?;
    let retained = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/observation_slo");
    for (name, bytes) in &corpus {
        assert_eq!(std::fs::read(retained.join(name))?, *bytes);
        // Generated admitted seeds must reach the real decoder and evaluator.
        let slo: acyclic_observability::Slo = serde_json::from_slice(bytes)?;
        if matches!(*name, "zero" | "threshold" | "maximum" | "rpc") {
            slo.validate()?;
        } else {
            assert!(slo.validate().is_err());
        }
        oracle::check(bytes)?;
        for cut in 0..bytes.len() {
            oracle::check(&bytes[..cut])?;
            for bit in 0..8 {
                let mut mutated = bytes.clone();
                mutated[cut] ^= 1 << bit;
                oracle::check(&mutated)?;
            }
        }
    }
    // Optional capture uses the actual public writer above, never hand-written JSON.
    if let Some(directory) = std::env::var_os("OBSERVATION_SLO_CORPUS") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        for (name, bytes) in corpus {
            std::fs::write(directory.join(name), bytes)?;
        }
    }
    Ok(())
}
