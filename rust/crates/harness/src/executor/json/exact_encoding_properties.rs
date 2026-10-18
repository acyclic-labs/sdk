// Fixed 64-bit production allocation totals, including the four-slot trace buffer.
// A canonical encoding decodes its own output, so its retained, released and
// peak bytes include one decoder diagnostic allowance, whose size depends on
// the enabled error variants; it is added symbolically as `D`.
// Fixtures cover a scalar, array indexing/object decode, and output growth.
use super::*;

fn values() -> [Value; 3] {
    [
        Value::Null,
        serde_json::json!({"z": ["a", "b"], "a": 7}),
        serde_json::json!({"text": "x".repeat(512)}),
    ]
}

const D: u64 = sdk_json_diagnostic_bytes();

const CODEC: [[([u64; 9], [i128; 6]); 2]; 3] = [
    [
        ([4, 1, 4, 0, 0, 1, 64, 0, 4], [2, 2, 2, 160, 160, 160]),
        (
            [8, 1, 8, 0, 0, 1, 472 + D, 408 + D, 4],
            [2, 2, 2, 160, 160, 160],
        ),
    ],
    [
        ([21, 7, 21, 0, 0, 1, 64, 0, 1], [2, 2, 2, 160, 160, 160]),
        (
            [42, 7, 70, 0, 7, 10, 2932 + D, 2868 + D, 29],
            [11, 2, 10, 1076, 160, 1012],
        ),
    ],
    [
        (
            [523, 3, 1053, 0, 0, 3, 1627, 585, 1044],
            [4, 2, 3, 1723, 1138, 1659],
        ),
        (
            [1046, 3, 2092, 0, 0, 7, 4911 + D, 3869 + D, 1560],
            [8, 2, 6, 2935, 1138, 2350],
        ),
    ],
];

const LEDGER: [[(u64, u64, u64, u64, u64); 2]; 3] = [
    [(4, 4, 1, 1, 64), (8, 8, 1, 1, 472 + D)],
    [(21, 21, 7, 1, 64), (70, 42, 14, 10, 2932 + D)],
    [(1053, 523, 3, 3, 1563), (2092, 1046, 3, 7, 4326 + D)],
];

fn check_encoder(index: usize, canonical: bool, repeat: bool) -> TestCaseResult {
    let input = values().into_iter().nth(index).unwrap();
    let bounds = JsonBounds {
        maximum_bytes: 65536,
        maximum_items: 65536,
        maximum_depth: 128,
    };
    let [(old, _), (new, _)] = differential::pair::<_, _, (&'static str, u64)>(
        || Source(input.clone()),
        |state, old| run(state, old, canonical, bounds, !old && repeat),
    );
    let charges = [
        new.1.encoded,
        new.1.items,
        new.1.copied,
        new.1.read,
        new.1.inspected,
        new.1.allocations,
        new.1.retained,
        new.1.released,
        new.1.admitted_copy,
    ];
    let physical = [
        i128::from(new.2.count_total),
        i128::from(new.2.count_current),
        i128::from(new.2.count_max),
        i128::from(new.2.bytes_total),
        i128::from(new.2.bytes_current),
        i128::from(new.2.bytes_max),
    ];
    prop_assert!(
        old.0 == new.0 && (charges, physical) == CODEC[index][usize::from(canonical)],
        "exact encoding work/allocation differs: charges={charges:?}, physical={physical:?}"
    );
    eprintln!(
        "ENCODER {index}/{canonical}: old={:?}/{:?}, new={:?}/{:?}",
        old.1, old.2, new.1, new.2
    );
    Ok(())
}

fn encode_with_ledger(
    value: &Value,
    io: &crate::filesystem::PreparationIo,
    old: bool,
    canonical: bool,
) -> Result<acyclic_stream::OwnedBytes> {
    let bounds = JsonBounds {
        maximum_bytes: 65536,
        maximum_items: 65536,
        maximum_depth: 128,
    };
    let code = Some(crate::wire::ErrorCode::Invalid);
    let mut charge = |event| io.codec_charge(event, true);
    let bytes = match (old, canonical) {
        (true, true) => {
            baseline::serialization::canonical_json_bytes_bounded(value, bounds, code, &mut charge)
        }
        (true, false) => {
            baseline::serialization::json_bytes_bounded(value, bounds, code, &mut charge)
        }
        (false, true) => canonical_json_bytes_bounded(value, bounds, code, &mut charge),
        (false, false) => json_bytes_bounded(value, bounds, code, &mut charge),
    }?;
    crate::executor::with_history_io(Some(io), |work| {
        crate::store::operations::owned_bytes_from_retained_vec(bytes, work).map(|(bytes, _)| bytes)
    })
}

// Live bytes a fresh original native ledger holds before any work, under the
// same budget `accounted` uses.
fn original_ledger_bytes() -> u64 {
    let cancellation = acyclic_fs::CancellationToken::new();
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 64_000_000),
        &cancellation,
    )
    .map_err(|(cause, _)| cause.to_string())
    .unwrap()
    .live_allocation_bytes()
}

fn check_ledger(index: usize, canonical: bool, native: bool, repeat: bool) -> TestCaseResult {
    use acyclic_fs::WorkCounters;
    let value = values().into_iter().nth(index).unwrap();
    let [(old, _), (new, trace)] = differential::pair::<_, _, (&'static str, u64)>(
        || value.clone(),
        |value, old| {
            crate::test_support::accounted::accounted(native, 64_000_000, false, |io| {
                let bytes = encode_with_ledger(&value, io, old, canonical);
                if !old && repeat {
                    drop(encode_with_ledger(&value, io, false, canonical));
                }
                bytes
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| format!("{error:?}"))
            })
        },
    );
    let old = old.unwrap();
    let new = new.unwrap();
    let (copied, encoded, items, operations, peak) = LEDGER[index][usize::from(canonical)];
    let capacity =
        CODEC[index][usize::from(canonical)].0[6] - CODEC[index][usize::from(canonical)].0[7];
    // The original ledger owns its own fixed allocations, and native output
    // custody adds two allocations and the guard header beside the existing
    // output. Both sizes are layout- and feature-dependent, so they come from
    // the real types rather than one build's constants.
    let ledger_bytes = if native { original_ledger_bytes() } else { 0 };
    let header =
        acyclic_stream::OwnedBytes::vec_guard_header_bytes::<acyclic_fs::NativeAllocation>();
    let expected = WorkCounters {
        bytes_copied: copied,
        bytes_encoded: encoded,
        items_examined: items,
        allocation_operations: operations + if native { 2 } else { 0 },
        peak_allocation_bytes: if native {
            ledger_bytes + peak.max(capacity + header)
        } else {
            peak
        },
        ..WorkCounters::UNCHARGED
    };
    let mut expected_trace = Vec::new();
    if canonical {
        expected_trace.push(("decode document", old.0.as_ref().unwrap().len() as u64));
        expected_trace.push(("decode index", if index == 1 { 24 } else { 0 }));
    }
    if native {
        expected_trace.push(("custody-take", capacity));
    }
    prop_assert!(
        old.0 == new.0
            && new.1 == expected
            && new.2 == if native { 0 } else { capacity }
            && new.3
                == if native {
                    expected
                } else {
                    WorkCounters::UNCHARGED
                }
            && new.4 == ledger_bytes
            && new.5 == ledger_bytes
            && trace == expected_trace,
        "exact encoding ledger/trace differs: actual={new:?}, expected={expected:?}, trace={trace:?}, expected_trace={expected_trace:?}"
    );
    eprintln!(
        "LEDGER {index}/{canonical}/{native}: old={:?}, new={:?}",
        old.1, new.1
    );
    Ok(())
}

#[test]
fn encoding_has_exact_production_work_physical_allocations_and_original_custody() {
    for index in 0..3 {
        for canonical in [false, true] {
            check_encoder(index, canonical, false).unwrap();
            for native in [false, true] {
                check_ledger(index, canonical, native, false).unwrap();
            }
        }
    }
}

#[test]
fn adding_back_real_encoding_fails_the_exact_guards() {
    for index in 0..3 {
        for canonical in [false, true] {
            let error = check_encoder(index, canonical, true).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("exact encoding work/allocation differs")
            );
            for native in [false, true] {
                let error = check_ledger(index, canonical, native, true).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("exact encoding ledger/trace differs")
                );
            }
        }
    }
}
