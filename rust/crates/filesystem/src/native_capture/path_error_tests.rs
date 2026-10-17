use super::*;
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static OLD_APPEND: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn restore_old_append() -> bool {
    OLD_APPEND.get()
}

pub(super) struct OldAppend(bool);
impl OldAppend {
    pub(super) fn enter() -> Self {
        Self(OLD_APPEND.replace(true))
    }
}
impl Drop for OldAppend {
    fn drop(&mut self) {
        OLD_APPEND.set(self.0);
    }
}

// Frozen before this change; the control does not call the candidate.
pub(super) fn old_append(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &NativeMaterializationWork,
) -> Result<NamespacePath, CaptureError> {
    meter.clone().check().map_err(capture_demand_error)?;
    NamespacePath::validate_component_lengths(
        parent
            .components()
            .iter()
            .chain(std::iter::once(&name))
            .map(|name| name.as_bytes().len()),
        limits,
    )
    .map_err(CaptureError::from)?;
    let depth = parent
        .depth()
        .checked_add(1)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    capture_charge(
        meter,
        WorkCounters {
            source_path_components: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        },
    )?;
    let mut components = capture_vec(depth, meter)?;
    for component in parent.components() {
        components.push(clone_capture_name(component, meter)?);
    }
    components.push(name);
    NamespacePath::new(components, limits).map_err(CaptureError::from)
}

fn name(length: u8, encoding: NameEncoding) -> LogicalName {
    let bytes = match encoding {
        NameEncoding::Utf8 => vec![b'a'; length.into()],
        NameEncoding::PosixBytes => vec![0xff; length.into()],
        NameEncoding::WindowsUtf16Le => {
            (0..length).flat_map(|_| 0xd800_u16.to_le_bytes()).collect()
        }
    };
    LogicalName::new(encoding, bytes, u32::MAX).unwrap()
}

// Each variant constructs its own names, ledger, cancellation and capture custody.
fn check(
    lengths: &[u8],
    leaf_length: u8,
    encoding: NameEncoding,
    bound: u8,
    denied: u8,
    cancelled: bool,
) -> Result<(), TestCaseError> {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let mut outcomes = Vec::new();
    for old in [false, true] {
        let mut limits = crate::model::VolumeLimits::default();
        let parent = NamespacePath::new(
            lengths
                .iter()
                .map(|length| name(*length, encoding))
                .collect(),
            limits,
        )
        .unwrap();
        let leaf = name(leaf_length, encoding);
        let leaf_bytes = leaf.as_bytes().len() as u64;
        let copied = parent
            .components()
            .iter()
            .map(|n| n.as_bytes().len() as u64)
            .sum::<u64>();
        let width = ((parent.depth() + 1) * std::mem::size_of::<LogicalName>()) as u64;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: if denied == 1 { 3 } else { u64::MAX },
            bytes_copied: if denied == 2 { leaf_bytes } else { u64::MAX },
            items_examined: if denied == 3 { 1 } else { u64::MAX },
            ..WorkBudget::UNBOUNDED
        };
        let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let scope = CaptureWorkScope::new(&work).unwrap();
        let admitted_leaf = clone_capture_name(&leaf, &scope.native).unwrap();
        let live = work.live_allocation_bytes();
        let before = WorkCounters {
            allocation_operations: 3,
            source_path_components: 1,
            items_examined: 1,
            source_bytes_read: leaf_bytes,
            bytes_copied: leaf_bytes,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
                + std::mem::size_of::<crate::demand::CaptureTransientCustody>() as u64
                + leaf_bytes,
            ..WorkCounters::UNCHARGED
        };
        prop_assert_eq!(work.receipt(), before);
        match bound {
            1 => limits.maximum_path_depth = parent.depth() as u16,
            2 => limits.maximum_component_bytes = 0,
            3 => limits.maximum_path_bytes = 1,
            _ => {}
        }
        if cancelled {
            cancellation.cancel();
        }
        let mut result = None;
        let heap = acyclic_test_support::measure(|| {
            result = Some(if old {
                old_append(&parent, admitted_leaf, limits, &scope.native)
            } else {
                append_path_measured(&parent, admitted_leaf, limits, &scope.native)
            });
        });
        let result = result.unwrap().map_err(|error| format!("{error:?}"));
        let reached = !cancelled && bound == 0;
        let success = reached && denied != 1 && denied != 3 && (denied != 2 || lengths.is_empty());
        prop_assert_eq!(result.is_ok(), success);
        let mut delta = WorkCounters::UNCHARGED;
        let mut allocations = 0;
        let mut retained = 0;
        if reached && denied != 3 {
            delta.source_path_components = 1;
            delta.items_examined = 1;
            if denied != 1 {
                allocations = 1;
                retained = width;
                let cloned = if success { lengths.len() as u64 } else { 1 };
                delta.source_path_components += cloned;
                delta.items_examined += cloned;
                if success {
                    allocations += lengths.len() as u64;
                    retained += copied;
                    delta.source_bytes_read = copied;
                    delta.bytes_copied = copied;
                }
            }
        }
        delta.allocation_operations = allocations;
        delta.peak_allocation_bytes = live + retained;
        prop_assert_eq!(work.receipt(), before.checked_add(delta).unwrap());
        prop_assert_eq!(work.live_allocation_bytes(), live + retained);
        // A component vector plus one byte vector per borrowed parent name.
        // Failures allocate their real error once. Success has no error box.
        let expected_heap = allocations + u64::from(!success);
        let no_unused_error = |count| count == expected_heap;
        prop_assert_eq!(heap.count_total, expected_heap + u64::from(old && reached));
        prop_assert_eq!(no_unused_error(heap.count_total), !(old && reached));
        if let Ok(path) = &result {
            let expected = NamespacePath::new(
                lengths
                    .iter()
                    .map(|n| name(*n, encoding))
                    .chain([leaf])
                    .collect(),
                limits,
            )
            .unwrap();
            prop_assert_eq!(path, &expected);
            prop_assert_eq!(path.encoded_bytes(), expected.encoded_bytes());
        }
        outcomes.push((result, work.receipt(), heap));
        drop(scope);
        prop_assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }
    prop_assert_eq!(&outcomes[0].0, &outcomes[1].0);
    prop_assert_eq!(outcomes[0].1, outcomes[1].1);
    let extra = u64::from(!cancelled && bound == 0);
    prop_assert_eq!(
        outcomes[1].2.bytes_total,
        outcomes[0].2.bytes_total + extra * std::mem::size_of::<crate::FsError>() as u64
    );
    Ok(())
}

#[test]
fn appending_paths_has_exact_heap_and_work_counts() {
    for encoding in [
        NameEncoding::Utf8,
        NameEncoding::PosixBytes,
        NameEncoding::WindowsUtf16Le,
    ] {
        for lengths in [vec![], vec![1, 7, 3]] {
            for bound in 0..4 {
                for denied in 0..4 {
                    for cancelled in [false, true] {
                        check(&lengths, 5, encoding, bound, denied, cancelled).unwrap();
                    }
                }
            }
        }
    }
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner
        .run(
            &(
                prop::collection::vec(1_u8..16, 0..5),
                1_u8..16,
                0_u8..3,
                0_u8..4,
                0_u8..4,
                any::<bool>(),
            ),
            |(lengths, leaf, encoding, bound, denied, cancelled)| {
                check(
                    &lengths,
                    leaf,
                    [
                        NameEncoding::Utf8,
                        NameEncoding::PosixBytes,
                        NameEncoding::WindowsUtf16Le,
                    ][encoding as usize],
                    bound,
                    denied,
                    cancelled,
                )
            },
        )
        .unwrap();
}
