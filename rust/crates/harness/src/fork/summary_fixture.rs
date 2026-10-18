use super::*;

pub(super) struct Fixture {
    pub(super) seed: ForkSeed,
    pub(super) capture: SummaryForkCapture,
    pub(super) inherited: Vec<FileRef>,
    pub(super) foreign: VolumeRef,
}

impl Input {
    pub(super) fn fixture(&self) -> Fixture {
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
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Violation {
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
pub(super) const VIOLATIONS: [Violation; 11] = [
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

#[derive(Clone, Copy, Debug)]
pub(super) enum SeedViolation {
    DropContext,
    DropPrefix,
    ReversePayloads,
    ExtraFile,
    PrefixLast,
}
pub(super) const SEED_VIOLATIONS: [SeedViolation; 5] = [
    SeedViolation::DropContext,
    SeedViolation::DropPrefix,
    SeedViolation::ReversePayloads,
    SeedViolation::ExtraFile,
    SeedViolation::PrefixLast,
];

impl Fixture {
    pub(super) fn metadata(&self, violation: Option<Violation>) -> SummaryForkCapture {
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
        candidate
    }

    pub(super) fn seed_order(&self, violation: Option<SeedViolation>) -> ForkSeed {
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
        candidate
    }

    pub(super) fn reference_identity_cases(&mut self) {
        self.capture.references = vec![
            file(&self.foreign, "same.json", b"x", "application/json").unwrap(),
            file(&self.foreign, "same.json", b"xx", "application/json").unwrap(),
            file(
                &self.foreign,
                ".system/inherited-conversation/kept.json",
                b"x",
                "application/json",
            )
            .unwrap(),
        ];
    }
}
