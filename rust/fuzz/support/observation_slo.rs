#[cfg(test)]
use acyclic_observability::LatencySlo;
use acyclic_observability::{Slo, SloError, Snapshot, Verdict};

/// Use the public persisted decoder, writer and evaluator, including refusals.
/// Each successful decode is exercised even when its semantic admission fails.
pub(super) fn check(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let Ok(slo) = serde_json::from_slice::<Slo>(bytes) else {
        return Ok(());
    };
    let canonical = serde_json::to_vec(&slo)?;
    assert_eq!(serde_json::from_slice::<Slo>(&canonical)?, slo);
    let snapshot = Snapshot {
        operation: slo.operation.clone(),
        rpc: slo.rpc.clone(),
        ..Snapshot::default()
    };
    // Check both acceptance and refusal against the public file contract.
    // An oracle conditional on successful admission would miss over-refusal.
    let operation = slo
        .operation
        .strip_prefix("acyclic.")
        .is_some_and(|suffix| !suffix.is_empty() && !suffix.as_bytes().contains(&0));
    let rpc = slo
        .rpc
        .as_ref()
        .is_none_or(|rpc| !rpc.is_empty() && !rpc.as_bytes().contains(&0));
    let fractions = [
        slo.success_ppm,
        slo.latency.as_ref().map_or(0, |latency| latency.target_ppm),
    ]
    .into_iter()
    .all(|fraction| fraction <= 1_000_000);
    let admission = match (slo.version == 1, operation && rpc, fractions) {
        (false, _, _) => Err(SloError::Version),
        (_, false, _) => Err(SloError::Operation),
        (_, _, false) => Err(SloError::Fraction),
        _ => Ok(()),
    };
    assert_eq!(slo.validate(), admission);
    if let Err(error) = admission {
        assert_eq!(slo.evaluate(&snapshot), Err(error));
        return Ok(());
    }

    assert_eq!(
        slo.evaluate(&snapshot)?,
        Verdict {
            success: None,
            latency: None
        }
    );

    // Exact observations at 0 ns and 1 ns avoid a second histogram model.
    // Three fixed-width counts exercise wide arithmetic without input-length work.
    let mut counts = [0_u64; 3];
    for (count, byte) in counts.iter_mut().zip(bytes) {
        *count = u64::from(*byte) * (u64::MAX / 3 / 255);
    }
    // Include the counter endpoints for every admitted file. Ordinary JSON
    // framing cannot generate either an empty-success or an all-success cohort.
    for counts in [counts, [0, u64::MAX, 0], [u64::MAX, 0, 0]] {
        evidence(&slo, counts)?;
    }
    let bucket = slo.latency.as_ref().map_or(0, |objective| {
        objective
            .maximum_ns
            .checked_ilog2()
            .map_or(0, |bit| bit + 1)
    });
    histogram(&slo, &[(0, 0), (1, 1), (bucket, 2)])?;

    let mut unknown: serde_json::Value = serde_json::from_slice(&canonical)?;
    if slo.latency.is_some() {
        let mut nested = unknown.clone();
        nested
            .get_mut("latency")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("original SLO writer omitted its latency object")?
            .insert("unknown".into(), true.into());
        assert!(serde_json::from_value::<Slo>(nested).is_err());
    }
    unknown
        .as_object_mut()
        .ok_or("original SLO writer omitted its file object")?
        .insert("unknown".into(), true.into());
    assert!(serde_json::from_value::<Slo>(unknown).is_err());
    Ok(())
}

/// Check admitted cohorts independently of persisted file framing. Counts must
/// fit one histogram bucket, as required by the production snapshot writer.
pub(super) fn evidence(slo: &Slo, counts: [u64; 3]) -> Result<(), SloError> {
    let mut snapshot = Snapshot {
        operation: slo.operation.clone(),
        rpc: slo.rpc.clone(),
        ..Snapshot::default()
    };
    snapshot.ok = counts[0];
    snapshot.err = counts[1];
    snapshot.incomplete = counts[2];
    let total = counts.iter().map(|count| u128::from(*count)).sum::<u128>();
    let availability = (total != 0)
        .then(|| u128::from(snapshot.ok) * 1_000_000 >= total * u128::from(slo.success_ppm));
    for duration in 0..=1 {
        snapshot.latency_ns = if duration == 0 {
            vec![counts.iter().sum()]
        } else {
            vec![0, counts.iter().sum()]
        };
        let latency = slo.latency.as_ref().and_then(|objective| {
            (total != 0).then_some(duration <= objective.maximum_ns || objective.target_ppm == 0)
        });
        assert_eq!(
            slo.evaluate(&snapshot)?,
            Verdict {
                success: availability,
                latency
            },
            "SLO cohort verdict differs from exact observations"
        );
    }
    snapshot.overflowed = true;
    assert_eq!(
        slo.evaluate(&snapshot)?,
        Verdict {
            success: None,
            latency: None
        }
    );
    snapshot.rpc.get_or_insert_default().push('\0');
    assert_eq!(
        slo.evaluate(&snapshot),
        Err(SloError::WrongOperation),
        "SLO must refuse evidence from a different RPC selector"
    );
    snapshot.rpc.clone_from(&slo.rpc);
    snapshot.operation.push('\0');
    assert_eq!(slo.evaluate(&snapshot), Err(SloError::WrongOperation));

    Ok(())
}

/// Endpoints of the public base-two bucket, clipped to real Duration capacity.
pub(super) fn latency_bounds(
    bucket: u32,
) -> Result<[std::time::Duration; 2], Box<dyn std::error::Error>> {
    use std::time::Duration;
    let (low, high) = if bucket == 0 {
        (0, 0)
    } else {
        (
            1_u128
                .checked_shl(bucket - 1)
                .ok_or("invalid latency bucket")?,
            1_u128.checked_shl(bucket).ok_or("invalid latency bucket")? - 1,
        )
    };
    let duration = |nanos: u128| -> Result<Duration, Box<dyn std::error::Error>> {
        Ok(Duration::new(
            u64::try_from(nanos / 1_000_000_000)?,
            u32::try_from(nanos % 1_000_000_000)?,
        ))
    };
    Ok([
        duration(low)?,
        duration(high.min(Duration::MAX.as_nanos()))?,
    ])
}

/// Differential conformance to real Metrics recording and live SLO projection.
/// Endpoint substitutions must produce identical histograms; their exact sample
/// verdicts bound every duration the public histogram can represent between them.
pub(super) fn histogram(
    slo: &Slo,
    buckets: &[(u32, u8)],
) -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_observability::{__tracing as tracing, Metrics, Outcome, Sample, Sink};
    use tracing::callsite::Callsite;
    let callsite = tracing::callsite! {
        name: "acyclic.test.operation", kind: tracing::metadata::Kind::SPAN, fields:
    };
    let metadata = callsite.metadata();
    let mut selected = slo.clone();
    selected.operation = metadata.name().into();
    let records = [Metrics::default(), Metrics::default()];
    let queries = [
        records[0].query_slo(selected.clone())?,
        records[1].query_slo(selected.clone())?,
    ];
    let id = tracing::Id::from_u64(1);
    let mut exact = [0_u128; 2];
    let mut ok = 0_u128;
    for &(bucket, outcome) in buckets {
        let outcome = match outcome {
            0 => Outcome::Ok,
            1 => Outcome::Err,
            _ => Outcome::Incomplete,
        };
        ok += u128::from(outcome == Outcome::Ok);
        for ((record, exact), elapsed) in
            records.iter().zip(&mut exact).zip(latency_bounds(bucket)?)
        {
            record.record(&Sample {
                span_id: &id,
                parent_id: None,
                durability: None,
                native_task: None,
                capture_retirement: None,
                work: None,
                publication: None,
                binding: None,
                metadata,
                rpc: selected.rpc.as_deref(),
                elapsed,
                interval: None,
                outcome,
                error_kind: None,
            });
            if let Some(objective) = &selected.latency {
                *exact += u128::from(elapsed.as_nanos() <= u128::from(objective.maximum_ns));
            }
        }
    }
    let views = queries.map(|query| query.snapshot());
    assert_eq!(
        views[0].evidence, views[1].evidence,
        "latency endpoints must share the production histogram"
    );
    let total = u128::try_from(buckets.len())?;
    assert_eq!(
        views[0]
            .evidence
            .latency_ns
            .iter()
            .map(|&n| u128::from(n))
            .sum::<u128>(),
        total
    );
    let latency = selected.latency.as_ref().and_then(|objective| {
        let meets =
            exact.map(|count| count * 1_000_000 >= total * u128::from(objective.target_ppm));
        assert!(exact[1] <= exact[0], "latency endpoints reversed");
        match (total != 0, meets) {
            (true, [true, true]) => Some(true),
            (true, [false, false]) => Some(false),
            _ => None,
        }
    });
    let success = (total != 0).then(|| ok * 1_000_000 >= total * u128::from(selected.success_ppm));
    for view in views {
        assert_eq!(
            view.verdict,
            Verdict { success, latency },
            "histogram verdict must match exact endpoint bounds"
        );
        assert_eq!(selected.evaluate(&view.evidence)?, view.verdict);
    }
    Ok(())
}

/// Capture the production writer's complete v1 shape, then explicit corruptions.
#[cfg(test)]
pub(super) fn corpus() -> Result<Vec<(&'static str, Vec<u8>)>, serde_json::Error> {
    let mut slo = Slo {
        version: 1,
        operation: "acyclic.test.operation".into(),
        rpc: None,
        success_ppm: 990_000,
        latency: Some(LatencySlo {
            maximum_ns: 0,
            target_ppm: 990_000,
        }),
    };
    let mut entries = Vec::new();
    for (name, success_ppm, maximum_ns, target_ppm) in [
        ("zero", 0, 0, 0),
        ("threshold", 990_000, 0, 990_000),
        ("maximum", 1_000_000, u64::MAX, 1_000_000),
    ] {
        slo.success_ppm = success_ppm;
        slo.latency = Some(LatencySlo {
            maximum_ns,
            target_ppm,
        });
        entries.push((name, serde_json::to_vec(&slo)?));
    }
    slo.rpc = Some("/acyclic.Service/Call".into());
    slo.latency = None;
    entries.push(("rpc", serde_json::to_vec(&slo)?));
    slo.version = 2;
    entries.push(("version", serde_json::to_vec(&slo)?));
    slo.version = 1;
    slo.success_ppm = 1_000_001;
    entries.push(("fraction", serde_json::to_vec(&slo)?));
    slo.success_ppm = 0;
    slo.rpc = Some(String::new());
    entries.push(("empty_rpc", serde_json::to_vec(&slo)?));
    slo.rpc = None;
    slo.operation.push('\0');
    entries.push(("nul_operation", serde_json::to_vec(&slo)?));
    Ok(entries)
}
