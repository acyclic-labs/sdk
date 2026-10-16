use super::*;
use crate::core::{AggregateKind, Authority};
use crate::fork::InheritedConversationPrefix;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[derive(Clone)]
struct Input {
    agent: AgentId,
    parent: Authority,
    revision: u64,
    readers: Vec<AgentId>,
    messages: Vec<ConversationMessage>,
    checkpoint: FileRef,
    other_checkpoint: FileRef,
}

impl Input {
    fn new(salt: u8, reader_bits: u8) -> Self {
        let agent = AgentId::from_bytes([salt + 1; 16]);
        let mut readers = (0..4_u8)
            .filter(|index| reader_bits & (1 << index) != 0)
            .map(|index| AgentId::from_bytes([30 + index; 16]))
            .collect::<Vec<_>>();
        if salt % 2 == 1 {
            readers.reverse();
        }
        Self {
            agent,
            parent: Authority {
                kind: AggregateKind::Conversation,
                id: format!("prefix-{salt}"),
            },
            revision: 100 + u64::from(salt),
            readers,
            messages: Vec::new(),
            checkpoint: file(agent, "checkpoint.json").unwrap(),
            other_checkpoint: file(agent, "other-checkpoint.json").unwrap(),
        }
    }

    fn message(&self) -> ConversationMessage {
        let sequence = self.messages.len() as u64 + 1;
        ConversationMessage {
            kind: MessageKind::Interaction,
            reply_to: None,
            ..message(
                &file(self.agent, &format!("message-{sequence}.txt")).unwrap(),
                sequence,
            )
        }
    }

    fn empty_state(&self) -> ConversationState {
        let mut state = ConversationState::default();
        state.bind(self.agent).unwrap();
        state
    }

    fn populated(count: u8, salt: u8, readers: u8) -> (Self, ConversationState) {
        let mut input = Self::new(salt, readers);
        let mut state = input.empty_state();
        for _ in 0..count {
            input.append(&mut state);
        }
        (input, state)
    }

    fn append(&mut self, state: &mut ConversationState) {
        let message = self.message();
        self.messages.push(message.clone());
        state.append(message, &mut None).unwrap();
    }

    // Fold the v3 protocol from caller inputs, never the state's digest cache.
    // Canonical encoding is shared; prefix selection, caching and retirement are not.
    fn digests(&self) -> Vec<[u8; 32]> {
        let mut digests = vec![[0; 32]];
        for message in &self.messages {
            let encoded = crate::contract::canonical_json_bytes(message).unwrap();
            let record = blake3::hash(&encoded);
            let mut bytes = b"harness:conversation-prefix:v3\0".to_vec();
            bytes.extend_from_slice(digests.last().unwrap());
            bytes.extend_from_slice(record.as_bytes());
            digests.push(*blake3::hash(&bytes).as_bytes());
        }
        digests
    }

    fn select(
        &self,
        state: &ConversationState,
        through: u64,
        readers: &[AgentId],
    ) -> Result<InheritedConversationPrefix> {
        InheritedConversationPrefix::select(
            self.parent.clone(),
            self.revision,
            through,
            readers,
            state,
            &mut None,
        )
    }

    fn check(&self, state: &ConversationState, base: usize, fault: Fault) -> TestCaseResult {
        let digests = self.digests();
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for through in base..=self.messages.len() {
            let mut selected = self.select(state, through as u64, &self.readers).ok();
            if let Some(prefix) = &mut selected {
                match fault {
                    Fault::HeadDigest => {
                        prefix.message_digest = state
                            .history_prefix_digest(state.logical_revision(), &mut None)
                            .unwrap()
                    }
                    Fault::WrongParent => prefix.parent.id.push_str("-foreign"),
                    Fault::WrongRevision => prefix.parent_revision += 1,
                    Fault::SortReaders => prefix.attached_agents.sort(),
                    Fault::WrongFormat => prefix.format_version = 2,
                    Fault::WrongCut => prefix.through_sequence += 1,
                    _ => {}
                }
            }
            if matches!(fault, Fault::RejectAll) {
                selected = None;
            }
            actual.push(selected);
            expected.push(Some(InheritedConversationPrefix {
                format_version: 3,
                parent: self.parent.clone(),
                parent_revision: self.revision,
                parent_agent: self.agent,
                through_sequence: through as u64,
                attached_agents: self.readers.clone(),
                message_digest: digests[through],
            }));
        }
        let archive = state.archived_prefix.as_ref().map(|prefix| {
            (
                prefix.through_sequence,
                prefix.history_digest,
                &prefix.checkpoint,
            )
        });
        let expected_archive = (base > 0).then(|| (base as u64, digests[base], &self.checkpoint));
        let coverage = (
            state.checkpoint_covers_archived_prefix(&self.checkpoint, base as u64),
            state.checkpoint_covers_archived_prefix(&self.other_checkpoint, base as u64),
            state.checkpoint_covers_archived_prefix(&self.checkpoint, base as u64 + 1),
        );
        prop_assert_eq!(
            (
                state.agent,
                state.logical_revision(),
                state.resident_after_sequence(),
                state.messages(),
                archive,
                coverage,
                actual
            ),
            (
                Some(self.agent),
                self.messages.len() as u64,
                base as u64,
                &self.messages[base..],
                expected_archive,
                (base > 0, false, false),
                expected
            ),
            "inherited prefix differs from input history"
        );
        Ok(())
    }

    fn retire(&self, state: &ConversationState, base: usize) -> ConversationState {
        let selection = ModelContextSelection {
            checkpoint: Some(self.checkpoint.clone()),
            conversation_revision: base as u64,
            message_ids: Vec::new(),
        };
        state
            .checkpointed_suffix(&selection, &mut None, None)
            .unwrap()
            .0
    }

    fn rejected_append(
        &self,
        state: &mut ConversationState,
        violation: AppendViolation,
        fault: Fault,
    ) -> TestCaseResult {
        let before = state.copy(&mut None, None).unwrap().0;
        let mut candidate = self.message();
        if !matches!(fault, Fault::RepairDuplicate) {
            match violation {
                AppendViolation::DuplicateId => candidate.id = self.messages[0].id,
                AppendViolation::Rewind => candidate.sequence -= 1,
                AppendViolation::Gap => candidate.sequence += 1,
            }
        }
        let result = state.append(candidate, &mut None);
        if matches!(fault, Fault::MutateRejected) {
            state.prefix_digests.push([7; 32]);
        }
        prop_assert_eq!(
            (result.is_err(), &*state == &before),
            (true, true),
            "rejected append changed conversation state"
        );
        Ok(())
    }

    fn invalid_selection(
        &self,
        state: &ConversationState,
        violation: Violation,
        check_healthy: bool,
    ) -> TestCaseResult {
        let mut candidate = state.copy(&mut None, None).unwrap().0;
        let mut through = 0;
        let mut readers = self.readers.clone();
        match violation {
            Violation::Unbound => candidate.agent = None,
            Violation::BeyondHead => through = self.messages.len() as u64 + 1,
            Violation::DuplicateReaders => readers.extend([AgentId::from_bytes([30; 16]); 2]),
            Violation::Sparse => {
                candidate = ConversationState::from_wire(
                    ConversationStateWire {
                        agent: Some(self.agent),
                        archived_prefix: None,
                        messages: vec![self.messages.last().unwrap().clone()],
                    },
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    &mut None,
                )
                .unwrap()
                .0;
            }
            Violation::Retired => {
                candidate = self.retire(state, 2);
                through = 1;
            }
        }
        let accepted = if check_healthy {
            self.select(state, 0, &self.readers).is_ok()
        } else {
            self.select(&candidate, through, &readers).is_ok()
        };
        prop_assert_eq!(
            accepted,
            false,
            "invalid prefix selection was accepted: {:?}",
            violation
        );
        Ok(())
    }
}

fn reopen(state: &ConversationState) -> ConversationState {
    serde_json::from_slice(&crate::contract::canonical_json_bytes(state).unwrap()).unwrap()
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    HeadDigest,
    WrongParent,
    WrongRevision,
    SortReaders,
    WrongFormat,
    WrongCut,
    RejectAll,
    DropCache,
    ExtraAppend,
    EmptyRebuild,
    DropArchive,
    WrongCheckpoint,
    MutateRejected,
    RepairDuplicate,
}
#[derive(Clone, Copy, Debug)]
enum Violation {
    Unbound,
    BeyondHead,
    DuplicateReaders,
    Sparse,
    Retired,
}
const VIOLATIONS: [Violation; 5] = [
    Violation::Unbound,
    Violation::BeyondHead,
    Violation::DuplicateReaders,
    Violation::Sparse,
    Violation::Retired,
];

#[derive(Clone, Copy)]
enum AppendViolation {
    DuplicateId,
    Rewind,
    Gap,
}
const APPEND_VIOLATIONS: [AppendViolation; 3] = [
    AppendViolation::DuplicateId,
    AppendViolation::Rewind,
    AppendViolation::Gap,
];

fn history(count: u8, salt: u8, readers: u8) -> TestCaseResult {
    let mut input = Input::new(salt, readers);
    let mut state = input.empty_state();
    input.check(&state, 0, Fault::None)?;
    for _ in 0..count {
        input.append(&mut state);
        input.check(&state, 0, Fault::None)?;
        input.check(&reopen(&state), 0, Fault::None)?;
        for violation in APPEND_VIOLATIONS {
            input.rejected_append(&mut state, violation, Fault::None)?;
        }
    }
    for base in 0..=usize::from(count) {
        let archived = input.retire(&state, base);
        input.check(&archived, base, Fault::None)?;
        let mut restored = reopen(&archived);
        input.check(&restored, base, Fault::None)?;
        let mut extended = input.clone();
        extended.append(&mut restored);
        extended.check(&restored, base, Fault::None)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn inherited_prefixes_match_independent_message_history(count in 0..7_u8, salt in 0..16_u8, readers in 0..16_u8) {
        history(count, salt, readers)?;
    }

    #[test]
    fn invalid_inherited_prefixes_are_rejected(count in 3..7_u8, salt in 0..16_u8, readers in 0..16_u8) {
        let (input, state) = Input::populated(count, salt, readers);
        input.check(&state, 0, Fault::None)?;
        for violation in VIOLATIONS { input.invalid_selection(&state, violation, false)?; }
    }
}

#[test]
fn empty_and_complete_prefix_boundaries() {
    for count in [0, 1, 6] {
        for readers in [0, 15] {
            history(count, 1, readers).unwrap();
        }
    }
}

fn expect_fault(result: TestCaseResult, target: &str) {
    let failure = result.unwrap_err();
    assert!(
        failure.to_string().contains(target),
        "wrong observation: {failure}"
    );
}

fn history_control(fault: Fault) {
    let (input, mut state) = Input::populated(3, 1, 3);
    let mut base = 0;
    match fault {
        Fault::DropCache => state.prefix_digests.clear(),
        Fault::ExtraAppend => state.append(input.message(), &mut None).unwrap(),
        Fault::EmptyRebuild => state = reopen(&input.empty_state()),
        Fault::DropArchive => {
            state = input.retire(&state, 2);
            base = 2;
            state.archived_prefix = None;
        }
        Fault::WrongCheckpoint => {
            state = input.retire(&state, 2);
            base = 2;
            state.archived_prefix.as_mut().unwrap().checkpoint = input.other_checkpoint.clone();
        }
        _ => {}
    }
    expect_fault(
        input.check(&state, base, fault),
        "inherited prefix differs from input history",
    );
}

macro_rules! history_control {
    ($name:ident, $fault:ident) => {
        #[test]
        fn $name() {
            history_control(Fault::$fault);
        }
    };
}
history_control!(negative_control_uses_head_digest, HeadDigest);
history_control!(negative_control_changes_parent, WrongParent);
history_control!(negative_control_changes_parent_revision, WrongRevision);
history_control!(negative_control_sorts_readers, SortReaders);
history_control!(negative_control_downgrades_format, WrongFormat);
history_control!(negative_control_changes_cut, WrongCut);
history_control!(negative_control_rejects_all_prefixes, RejectAll);
history_control!(negative_control_drops_digest_cache, DropCache);
history_control!(negative_control_appends_an_extra_record, ExtraAppend);
history_control!(negative_control_rebuilds_empty_history, EmptyRebuild);
history_control!(negative_control_drops_archive, DropArchive);
history_control!(negative_control_changes_checkpoint, WrongCheckpoint);

#[test]
fn negative_control_selects_healthy_history_for_invalid_input() {
    let (input, state) = Input::populated(3, 1, 3);
    for violation in VIOLATIONS {
        expect_fault(
            input.invalid_selection(&state, violation, true),
            "invalid prefix selection was accepted",
        );
    }
}

#[test]
fn negative_control_mutates_after_rejected_append() {
    for violation in APPEND_VIOLATIONS {
        let (input, mut state) = Input::populated(3, 1, 3);
        expect_fault(
            input.rejected_append(&mut state, violation, Fault::MutateRejected),
            "rejected append changed conversation state",
        );
    }
}

#[test]
fn negative_control_repairs_a_duplicate_append() {
    for violation in APPEND_VIOLATIONS {
        let (input, mut state) = Input::populated(3, 1, 3);
        expect_fault(
            input.rejected_append(&mut state, violation, Fault::RepairDuplicate),
            "rejected append changed conversation state",
        );
    }
}
