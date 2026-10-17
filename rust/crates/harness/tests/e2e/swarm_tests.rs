use super::{
    TestResult, scenario, swarm,
    swarm_case::{Expectations, Layout, SwarmCase},
    swarm_wire::Fault,
};
use proptest::prelude::*;
use std::path::Path;

fn small(agents: usize, before: String, gap: String, live: String, promote: bool) -> SwarmCase {
    let snapshots = if promote { 2 } else { 3 };
    SwarmCase {
        agents,
        attention_every: 2,
        layouts: vec![
            Layout {
                visible: [0, 1],
                prefetch: [1, 2],
            },
            Layout {
                visible: [1, 2],
                prefetch: if promote { [0, 1] } else { [2, 3] },
            },
        ],
        turns: vec![
            vec![before.clone(), gap.clone(), live.clone()],
            vec![gap.clone(), live.clone()],
        ],
        expect: Expectations {
            text: format!("{before}{gap}{live}{gap}{live}"),
            wire: serde_json::json!({
            "sessions":1,"children":1,"actor_channels":0,"status_follows":agents,"content_follows":snapshots,
            "removed":usize::from(!promote),"peak_full":2,"snapshots":snapshots,"peak_heads":2,
            "retained_full":2,"visible_updates":6,"fold_items":24,"full_records":8,"statuses":agents,"attention":agents.div_ceil(2)*2}),
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Snapshot plus guarded follow must equal the full Unicode history,
    // independent of swarm size and whether prefetch is promoted or replaced.
    #[test]
    fn selected_queries_equal_the_complete_history_across_tier_changes(
        agents in 3usize..12, before in ".{0,10}", gap in ".{0,10}", live in ".{0,10}", promote in any::<bool>(), overview in any::<bool>(),
    ) {
        let mut case = small(agents, before, gap, live, promote);
        if overview {
            for layout in &mut case.layouts {
                layout.visible = [0, 0];
                layout.prefetch = [0, 0];
            }
            for field in ["content_follows", "removed", "peak_full", "snapshots", "peak_heads", "retained_full", "visible_updates", "fold_items", "full_records"] {
                case.expect.wire[field] = 0.into();
            }
        }
        let encoded = serde_json::to_vec(&case)?;
        let decoded: SwarmCase = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(&case, &decoded);
        prop_assert!(case.validate().is_ok());
        futures::executor::block_on(swarm::run(decoded)).map_err(|e| TestCaseError::fail(e.to_string()))?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Count opened provider streams, including their lifetime through cancellation.
    // The maximum must depend on selected views, independent of swarm size/history.
    #[test]
    fn live_server_follows_stay_within_the_selected_bound(
        width in 1usize..5, shift in 0usize..5, extra in 0usize..20,
        before in ".{0,6}", live in ".{0,6}",
    ) {
        let shift = shift.min(width);
        let mut case = small(3 * width + extra, before, "gap".into(), live, false);
        case.layouts = vec![
            Layout { visible: [0, width], prefetch: [width, 2 * width] },
            Layout { visible: [shift, width + shift], prefetch: [width + shift, 2 * width + shift] },
        ];
        case.expect.wire = serde_json::json!({});
        futures::executor::block_on(swarm::run(case)).map_err(|e| TestCaseError::fail(e.to_string()))?;
    }
}

#[tokio::test]
async fn retained_server_follows_and_corrupt_retired_snapshots_break_unchanged_expectations()
-> TestResult {
    let case = small(3, "before".into(), "gap".into(), "live".into(), false);
    swarm::run(case.clone()).await?;
    for (fault, reason) in [
        (
            Fault::IgnoreRemoval,
            "live server follows exceeded selected bound",
        ),
        (Fault::CorruptSnapshot, "selected agent 0 has a tier gap"),
        (
            Fault::ScanRetainedViews,
            "file fold examined 4 items instead of 3",
        ),
    ] {
        let error = swarm::run_with_fault(case.clone(), fault, false)
            .await
            .expect_err("accepted injected fault");
        assert!(error.to_string().contains(reason), "{fault:?}: {error}");
    }
    Ok(())
}

#[tokio::test]
async fn dropped_attention_and_missing_boundary_break_unchanged_expectations() -> TestResult {
    let case = small(3, "before".into(), "gap".into(), "live".into(), false);
    swarm::run(case.clone()).await?;
    for fault in [Fault::DropAttention, Fault::DropBoundary] {
        assert!(
            swarm::run_with_fault(case.clone(), fault, false)
                .await
                .is_err(),
            "accepted {fault:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_session_delivery_is_rejected_before_another_fold() -> TestResult {
    let case = small(3, "before".into(), "gap".into(), "live".into(), false);
    let error = swarm::run_with_fault(case, Fault::DuplicateContent, false)
        .await
        .expect_err("accepted duplicate in one ALL generation");
    assert!(error.to_string().contains("stream unavailable"), "{error}");
    Ok(())
}

#[test]
fn checked_swarm_cases_round_trip_and_reject_out_of_bounds_layouts() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut paths = Vec::new();
    super::scenario_data::discover(&root.join("swarm"), &mut paths)?;
    for path in paths {
        let mut value = scenario::load_value(&root, &path)?;
        value.as_object_mut().ok_or("case")?.remove("driver");
        let case: SwarmCase = serde_json::from_value(value)?;
        case.validate()?;
        assert_eq!(
            serde_json::from_slice::<SwarmCase>(&serde_json::to_vec(&case)?)?,
            case
        );
        let mut broken = case;
        broken.layouts[0].visible[1] = broken.agents + 1;
        assert!(broken.validate().is_err());
    }
    Ok(())
}
