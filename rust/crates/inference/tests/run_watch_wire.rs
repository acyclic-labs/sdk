//! Cross-language wire fixtures for the bounded Run watch contract.
//!
//! The byte vectors in this test are intentionally authored as protobuf wire
//! data. They exercise the same bytes a non-Rust binding emits, while the
//! public Rust validator supplies the semantic checks for presence, ordering,
//! cancellation, and terminal recovery.

use acyclic_inference::{WatchRunState, validate_customer_wire, watch_run_start_state_wire, wire};
use prost::Message;
use std::fmt::Display;

const RUN_ID: [u8; 16] = [2; 16];
const INPUT: [u8; 32] = [3; 32];

fn cancelled_view() -> wire::RunView {
    wire::RunView {
        run_id: RUN_ID.to_vec(),
        input: INPUT.to_vec(),
        model: "model.example.v1".to_owned(),
        cancellation_requested: true,
        ..Default::default()
    }
}

fn event(sequence: u64, terminal: Option<wire::RunTerminal>) -> wire::RunEvent {
    wire::RunEvent {
        sequence,
        event: terminal.map(|value| wire::run_event::Event::Terminal(value.into())),
    }
}

fn fixture<T, E: Display>(result: Result<T, E>, context: &str) -> Result<T, String> {
    result.map_err(|error| format!("{context}: {error}"))
}

#[test]
fn cross_language_fixture_preserves_presence_and_signed_values() -> Result<(), String> {
    // ExactRational(-42, 7): sint64 zig-zag encodes -42 as 83 (0x53).
    let rational = fixture(
        wire::ExactRational::decode([0x08, 0x53, 0x10, 0x07].as_slice()),
        "signed ExactRational fixture",
    )?;
    assert_eq!(rational.numerator, -42);
    assert_eq!(rational.denominator, 7);
    assert_eq!(rational.encode_to_vec(), [0x08, 0x53, 0x10, 0x07]);

    let view = cancelled_view();
    let bytes = view.encode_to_vec();
    let decoded = fixture(
        wire::RunView::decode(bytes.as_slice()),
        "cancelled RunView fixture",
    )?;
    assert!(decoded.cancellation_requested);
    assert!(
        decoded.result.is_none(),
        "optional result must remain absent"
    );
    assert_eq!(decoded.run_id, RUN_ID.to_vec());
    assert_eq!(decoded.input, INPUT.to_vec());
    fixture(
        validate_customer_wire("run_view", &bytes, &RUN_ID, &[]),
        "cancelled RunView satisfies the wire contract",
    )?;
    Ok(())
}

#[test]
fn event_ordering_cancellation_and_terminal_recovery_are_bounded() -> Result<(), String> {
    let view = cancelled_view();
    let view_bytes = view.encode_to_vec();
    let mut state = fixture(
        watch_run_start_state_wire(&view_bytes, &RUN_ID, "0"),
        "watch starts from a cancelled nonterminal view",
    )?;

    let progress = wire::RunEvent {
        sequence: 0,
        event: Some(wire::run_event::Event::Progress(wire::RunProgress {
            kind: "cancellation-requested".to_owned(),
        })),
    };
    fixture(
        state.advance_wire(&progress.encode_to_vec()),
        "progress sequence is accepted",
    )?;
    let terminal = event(1, Some(wire::RunTerminal::Cancelled));
    fixture(
        state.advance_wire(&terminal.encode_to_vec()),
        "cancellation terminal is accepted",
    )?;
    assert!(state.is_terminal());
    fixture(state.finish(), "cancelled watch reaches terminal state")?;

    // A terminal stream rejects all later data, including a duplicate terminal.
    assert_eq!(
        state.advance_wire(&terminal.encode_to_vec()),
        Err("run event order or shape differs")
    );

    // Reconnect after a terminal view: the cursor is at last_sequence + 1 and
    // the recovered watcher is already terminal, so no duplicate event is read.
    let mut recovered_result = wire::RunResult::default();
    recovered_result.terminal = wire::RunTerminal::Cancelled.into();
    let mut recovered_view = view;
    recovered_view.last_sequence = 1;
    recovered_view.result = Some(recovered_result);
    let recovered = fixture(
        watch_run_start_state_wire(&recovered_view.encode_to_vec(), &RUN_ID, "2"),
        "terminal recovery starts at the exclusive next cursor",
    )?;
    assert!(recovered.is_terminal());
    fixture(recovered.finish(), "recovered watch reaches terminal state")?;

    let mut reordered: WatchRunState = fixture(
        watch_run_start_state_wire(&view_bytes, &RUN_ID, "0"),
        "watch starts for ordering check",
    )?;
    assert_eq!(
        reordered.advance_wire(&event(1, None).encode_to_vec()),
        Err("run event order or shape differs")
    );
    assert_eq!(reordered.finish(), Err("run stream ended before terminal"));
    Ok(())
}
