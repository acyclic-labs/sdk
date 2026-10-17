use super::*;
use crate::conversation::Limits;
use crate::store::HistoryReadLimits;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

struct Input {
    sizes: [u8; 2],
    payloads: Vec<u8>,
    references: Vec<u8>,
    file_limit: u8,
    render_limit: u8,
}

struct Fixture {
    seed: ForkSeed,
    capture: SummaryForkCapture,
    inherited: Vec<FileRef>,
    foreign: VolumeRef,
}

impl Input {
    fn fixture(&self) -> Fixture {
        let mut seed = seed().unwrap();
        let child = &seed.child_private_volume;
        let foreign = VolumeRef::new(
            child.provider().clone(),
            "foreign-summary".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([55; 16])),
            &mut None,
        )
        .unwrap();
        let sized = |volume: &VolumeRef, path: &str, size: u8| {
            file(
                volume,
                path,
                &vec![b'x'; usize::from(size)],
                "application/json",
            )
            .unwrap()
        };
        let context = sized(
            child,
            ".system/inherited-conversation/summary-context.json",
            self.sizes[1],
        );
        let payloads = self
            .payloads
            .iter()
            .enumerate()
            .map(|(index, size)| {
                sized(
                    child,
                    &format!(".system/inherited-conversation/summary-payloads/{index}.json"),
                    *size,
                )
            })
            .collect::<Vec<_>>();
        // Capture expected order before constructing the production input.
        let inherited = std::iter::once(context.clone())
            .chain(payloads.iter().cloned())
            .collect::<Vec<_>>();
        let capture = SummaryForkCapture {
            selection: SummaryForkSelection {
                checkpoint: sized(&foreign, "checkpoint.json", self.sizes[0]),
                limits: Limits {
                    file_bytes: u64::from(self.file_limit),
                    render_bytes: u64::from(self.render_limit),
                    path_bytes: 1024,
                    ..Limits::default()
                },
                history_limits: HistoryReadLimits {
                    maximum_events: 1,
                    maximum_bytes: 1,
                },
            },
            context,
            payloads,
            references: self
                .references
                .iter()
                .enumerate()
                .map(|(index, size)| sized(&foreign, &format!("references/{index}.json"), *size))
                .collect(),
        };
        seed.inherited_through_sequence = 1;
        seed.inherited_context = vec![
            file(
                child,
                ".system/inherited-conversation/prefix.json",
                b"prefix",
                "application/vnd.acyclic.harness.inherited-conversation+json",
            )
            .unwrap(),
        ];
        seed.inherited_context.extend(inherited.iter().cloned());
        seed.summary = Some(capture.clone());
        Fixture {
            seed,
            capture,
            inherited,
            foreign,
        }
    }

    fn check_sizes(&self, fault: SizeFault) -> TestCaseResult {
        let fixture = self.fixture();
        let mut candidate = fixture.capture;
        match fault {
            SizeFault::FileBudget => candidate.selection.limits.file_bytes = 255,
            SizeFault::RenderBudget => candidate.selection.limits.render_bytes = 255,
            _ => {}
        }
        let actual = candidate
            .validate(&fixture.seed.child_private_volume)
            .is_ok()
            && !matches!(fault, SizeFault::RejectAll);
        let expected = self
            .sizes
            .iter()
            .chain(&self.payloads)
            .chain(&self.references)
            .all(|size| *size <= self.file_limit)
            && self.sizes.iter().all(|size| *size <= self.render_limit);
        prop_assert_eq!(
            actual,
            expected,
            "summary size acceptance differs from input lengths"
        );
        Ok(())
    }
}

fn healthy(payloads: Vec<u8>, references: Vec<u8>) -> Input {
    Input {
        sizes: [1, 1],
        payloads,
        references,
        file_limit: 8,
        render_limit: 8,
    }
}

#[derive(Clone, Copy)]
enum SizeFault {
    None,
    FileBudget,
    RenderBudget,
    RejectAll,
}

#[derive(Clone, Copy, Debug)]
enum Violation {
    CheckpointMedia,
    ZeroEvents,
    ZeroHistoryBytes,
    ForeignContext,
    ContextPath,
    ContextMedia,
    ForeignPayload,
    PayloadPath,
    DuplicatePayloadPath,
    DuplicateReference,
    InternalReference,
}
const VIOLATIONS: [Violation; 11] = [
    Violation::CheckpointMedia,
    Violation::ZeroEvents,
    Violation::ZeroHistoryBytes,
    Violation::ForeignContext,
    Violation::ContextPath,
    Violation::ContextMedia,
    Violation::ForeignPayload,
    Violation::PayloadPath,
    Violation::DuplicatePayloadPath,
    Violation::DuplicateReference,
    Violation::InternalReference,
];

impl Fixture {
    fn check_metadata(&self, violation: Option<Violation>, original: bool) -> TestCaseResult {
        let mut candidate = self.capture.clone();
        let child = &self.seed.child_private_volume;
        let payload = |volume, path| file(volume, path, b"x", "application/json").unwrap();
        match violation {
            Some(Violation::CheckpointMedia) => {
                candidate.selection.checkpoint =
                    file(&self.foreign, "checkpoint.json", b"x", "text/plain").unwrap()
            }
            Some(Violation::ZeroEvents) => candidate.selection.history_limits.maximum_events = 0,
            Some(Violation::ZeroHistoryBytes) => {
                candidate.selection.history_limits.maximum_bytes = 0
            }
            Some(Violation::ForeignContext) => {
                candidate.context = payload(&self.foreign, self.capture.context.path())
            }
            Some(Violation::ContextPath) => {
                candidate.context = payload(child, ".system/inherited-conversation/other.json")
            }
            Some(Violation::ContextMedia) => {
                candidate.context =
                    file(child, self.capture.context.path(), b"x", "text/plain").unwrap()
            }
            Some(Violation::ForeignPayload) => candidate.payloads.push(payload(
                &self.foreign,
                ".system/inherited-conversation/summary-payloads/foreign.json",
            )),
            Some(Violation::PayloadPath) => candidate.payloads.push(payload(child, "outside.json")),
            Some(Violation::DuplicatePayloadPath) => {
                // Same path with distinct descriptors must still reject.
                let path = ".system/inherited-conversation/summary-payloads/duplicate.json";
                candidate.payloads.extend([
                    payload(child, path),
                    file(child, path, b"xx", "application/json").unwrap(),
                ]);
            }
            Some(Violation::DuplicateReference) => {
                let reference = payload(&self.foreign, "duplicate-reference.json");
                candidate.references.extend([reference.clone(), reference]);
            }
            Some(Violation::InternalReference) => candidate
                .references
                .push(payload(&self.foreign, ".system/execution/secret.json")),
            None => {}
        }
        let submitted = if original { &self.capture } else { &candidate };
        let accepted = submitted.validate(child).is_ok();
        prop_assert_eq!(
            accepted,
            violation.is_none(),
            "summary metadata acceptance mismatch: {:?}",
            violation
        );
        Ok(())
    }

    fn check_order(&self, fault: OrderFault) -> TestCaseResult {
        let mut actual = self.capture.inherited_files().cloned().collect::<Vec<_>>();
        match fault {
            OrderFault::DropContext => {
                actual.remove(0);
            }
            OrderFault::DropPayload => {
                actual.remove(1);
            }
            OrderFault::DuplicatePayload => actual.push(actual[1].clone()),
            OrderFault::ReversePayloads => actual[1..].reverse(),
            OrderFault::LeakReference => actual.push(self.capture.references[0].clone()),
            OrderFault::None => {}
        }
        prop_assert_eq!(
            &actual,
            &self.inherited,
            "summary inherited file order differs from inputs"
        );
        Ok(())
    }

    fn check_seed_order(&self, violation: Option<SeedViolation>, original: bool) -> TestCaseResult {
        let mut candidate = self.seed.clone();
        match violation {
            Some(SeedViolation::DropContext) => {
                candidate.inherited_context.remove(1);
            }
            Some(SeedViolation::DropPrefix) => {
                candidate.inherited_context.remove(0);
            }
            Some(SeedViolation::ReversePayloads) => candidate.inherited_context[2..].reverse(),
            Some(SeedViolation::ExtraFile) => candidate.inherited_context.push(
                file(
                    &candidate.child_private_volume,
                    ".system/inherited-conversation/extra.json",
                    b"x",
                    "application/json",
                )
                .unwrap(),
            ),
            Some(SeedViolation::PrefixLast) => candidate.inherited_context.rotate_left(1),
            None => {}
        }
        let submitted = if original { &self.seed } else { &candidate };
        let accepted = submitted.validate(&mut None).is_ok();
        prop_assert_eq!(
            accepted,
            violation.is_none(),
            "seed summary file list mismatch: {:?}",
            violation
        );
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum OrderFault {
    None,
    DropContext,
    DropPayload,
    DuplicatePayload,
    ReversePayloads,
    LeakReference,
}
#[derive(Clone, Copy, Debug)]
enum SeedViolation {
    DropContext,
    DropPrefix,
    ReversePayloads,
    ExtraFile,
    PrefixLast,
}
const SEED_VIOLATIONS: [SeedViolation; 5] = [
    SeedViolation::DropContext,
    SeedViolation::DropPrefix,
    SeedViolation::ReversePayloads,
    SeedViolation::ExtraFile,
    SeedViolation::PrefixLast,
];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn summary_sizes_match_independent_byte_limits(
        sizes in proptest::array::uniform2(0..8_u8), payloads in prop::collection::vec(0..8_u8,0..4),
        references in prop::collection::vec(0..8_u8,0..4), file_limit in 1..9_u8, render_limit in 1..9_u8,
    ) { Input { sizes,payloads,references,file_limit,render_limit }.check_sizes(SizeFault::None)?; }

    #[test]
    fn summary_metadata_rejects_foreign_and_duplicate_files(
        payloads in prop::collection::vec(0..8_u8,0..4), references in prop::collection::vec(0..8_u8,0..4),
    ) {
        let fixture = healthy(payloads,references).fixture();
        fixture.check_metadata(None,false)?;
        for violation in VIOLATIONS { fixture.check_metadata(Some(violation),false)?; }
    }

    #[test]
    fn summary_inherited_file_order_matches_inputs(
        payloads in prop::collection::vec(0..8_u8,0..4), references in prop::collection::vec(0..8_u8,0..4),
    ) { healthy(payloads,references).fixture().check_order(OrderFault::None)?; }

    #[test]
    fn seed_summary_requires_the_complete_ordered_file_list(
        payloads in prop::collection::vec(0..8_u8,2..4), references in prop::collection::vec(0..8_u8,0..4),
    ) {
        let fixture = healthy(payloads,references).fixture();
        fixture.check_seed_order(None,false)?;
        for violation in SEED_VIOLATIONS { fixture.check_seed_order(Some(violation),false)?; }
    }
}

fn expect_fault(result: TestCaseResult, target: &str) {
    let failure = result.unwrap_err();
    assert!(
        failure.to_string().contains(target),
        "wrong observation: {failure}"
    );
}

#[test]
fn negative_controls_ignore_summary_byte_budgets() {
    for index in 0..4 {
        let mut input = Input {
            sizes: [1, 1],
            payloads: vec![1],
            references: vec![1],
            file_limit: 1,
            render_limit: 8,
        };
        match index {
            0 | 1 => input.sizes[index] = 2,
            2 => input.payloads[0] = 2,
            _ => input.references[0] = 2,
        }
        expect_fault(
            input.check_sizes(SizeFault::FileBudget),
            "summary size acceptance differs from input lengths",
        );
    }
    for index in 0..2 {
        let mut input = Input {
            sizes: [1, 1],
            payloads: vec![],
            references: vec![],
            file_limit: 8,
            render_limit: 1,
        };
        input.sizes[index] = 2;
        expect_fault(
            input.check_sizes(SizeFault::RenderBudget),
            "summary size acceptance differs from input lengths",
        );
    }
    expect_fault(
        healthy(vec![], vec![]).check_sizes(SizeFault::RejectAll),
        "summary size acceptance differs from input lengths",
    );
}

#[test]
fn negative_control_validates_healthy_summary_instead_of_candidate() {
    let fixture = healthy(vec![], vec![]).fixture();
    for violation in VIOLATIONS {
        expect_fault(
            fixture.check_metadata(Some(violation), true),
            "summary metadata acceptance mismatch",
        );
    }
}

#[test]
fn negative_controls_change_summary_inherited_files() {
    let fixture = healthy(vec![1, 2], vec![1]).fixture();
    for fault in [
        OrderFault::DropContext,
        OrderFault::DropPayload,
        OrderFault::DuplicatePayload,
        OrderFault::ReversePayloads,
        OrderFault::LeakReference,
    ] {
        expect_fault(
            fixture.check_order(fault),
            "summary inherited file order differs from inputs",
        );
    }
}

#[test]
fn negative_control_validates_healthy_seed_file_list() {
    let fixture = healthy(vec![1, 2], vec![]).fixture();
    for violation in SEED_VIOLATIONS {
        expect_fault(
            fixture.check_seed_order(Some(violation), true),
            "seed summary file list mismatch",
        );
    }
}

#[test]
fn summary_boundary_and_reference_identity_cases() {
    for input in [
        Input {
            sizes: [3, 3],
            payloads: vec![3],
            references: vec![3],
            file_limit: 3,
            render_limit: 3,
        },
        Input {
            sizes: [0, 0],
            payloads: vec![],
            references: vec![],
            file_limit: 1,
            render_limit: 1,
        },
        Input {
            sizes: [3, 3],
            payloads: vec![3],
            references: vec![3],
            file_limit: 2,
            render_limit: 3,
        },
        Input {
            sizes: [3, 3],
            payloads: vec![3],
            references: vec![3],
            file_limit: 3,
            render_limit: 2,
        },
    ] {
        input.check_sizes(SizeFault::None).unwrap();
    }
    let mut fixture = healthy(vec![], vec![]).fixture();
    // Distinct pinned contents at one path are distinct references; inherited files are allowed.
    fixture.capture.references = vec![
        file(&fixture.foreign, "same.json", b"x", "application/json").unwrap(),
        file(&fixture.foreign, "same.json", b"xx", "application/json").unwrap(),
        file(
            &fixture.foreign,
            ".system/inherited-conversation/kept.json",
            b"x",
            "application/json",
        )
        .unwrap(),
    ];
    fixture.check_metadata(None, false).unwrap();
}
