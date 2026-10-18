#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::task_volume::{ChildRequest, ChildStatus, Control, FileKey, FileValue, Query};
fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(event) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::executor::ExecutionEvent>,
    ) {
        let encoded = serde_json::to_vec(&event).unwrap();
        let _ = fuzz_support::round_trip(
            &(event),
            serde_json::from_slice::<acyclic_harness::executor::ExecutionEvent>(&encoded),
        );
    }
    if let Some(value) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<serde_json::Value>)
    {
        use acyclic_harness::workflow::{MachineStatus, ResumableMachine};
        let machine = acyclic_harness::filesystem::ChildAgentMachine::new();
        let initial = value.get("initial").unwrap_or(&value);
        if let Ok(state) =
            acyclic_harness::workflow::ResumableMachine::initialize(&machine, initial)
        {
            let encoded = serde_json::to_vec(initial).unwrap();
            let _ = fuzz_support::round_trip(
                &(state),
                machine.initialize(&serde_json::from_slice(&encoded).unwrap()),
            );
            let waiting = machine
                .transition(&state, &serde_json::Value::Null)
                .unwrap();
            assert_eq!(waiting.status, MachineStatus::Suspended);
            if let Some(input) = value.get("input")
                && let Ok(active) = acyclic_harness::workflow::ResumableMachine::transition(
                    &machine,
                    &waiting.state,
                    input,
                )
            {
                assert_eq!(active.status, MachineStatus::Suspended);
                if let Some(result) = value.get("model_result")
                    && let Ok(next) = acyclic_harness::workflow::ResumableMachine::transition(
                        &machine,
                        &active.state,
                        result,
                    )
                {
                    assert_eq!(next.status, MachineStatus::Suspended);
                    let encoded = serde_json::to_vec(&next.state).unwrap();
                    let _ = fuzz_support::round_trip(
                        &(next.state),
                        serde_json::from_slice::<serde_json::Value>(&encoded),
                    );
                }
            }
        }
    }
    if let Some(Some(file)) = fuzz_support::FuzzInput::parse(fuzz_input, |bytes| {
        let record = acyclic_stream::Record {
            sequence: 1,
            value: bytes.to_vec().into(),
            commit_id: acyclic_stream::CommitId::from_bytes([0; 32]),
            committed_at_micros: 0,
        };
        FileValue::from_record(&record)
    }) {
        let encoded = serde_json::to_vec(&file).unwrap();
        let _ = fuzz_support::round_trip(&(file), serde_json::from_slice::<FileValue>(&encoded));
    }
    if let Some(request) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::fork::ForkRequest>,
    ) && request.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded: acyclic_harness::fork::ForkRequest =
            fuzz_support::round_trip(&request, serde_json::from_slice(&encoded));
        decoded.validate(&mut None).unwrap();
    }
    if let Some(seed) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::fork::ForkSeed>,
    ) && seed.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&seed).unwrap();
        let decoded: acyclic_harness::fork::ForkSeed =
            fuzz_support::round_trip(&seed, serde_json::from_slice(&encoded));
        decoded.validate(&mut None).unwrap();
    }
    macro_rules! tool_input {
        ($ty:ty) => {
            if let Some(input) =
                fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<$ty>)
            {
                let _ = fuzz_support::round_trip(
                    &(input),
                    serde_json::from_slice::<$ty>(&serde_json::to_vec(&input).unwrap()),
                );
            }
        };
    }
    tool_input!(acyclic_harness::tools::SpawnInput);
    tool_input!(acyclic_harness::tools::SendInput);
    tool_input!(acyclic_harness::tools::WaitInput);
    tool_input!(acyclic_harness::tools::ArchiveInput);
    if let Some(result) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        acyclic_harness::task_volume::ChildTurnResult::parse,
    ) {
        let _ = fuzz_support::round_trip(
            &(result),
            acyclic_harness::task_volume::ChildTurnResult::parse(
                &serde_json::to_vec(&result).unwrap(),
            ),
        );
    }
    if let Some(status) = fuzz_support::FuzzInput::parse(fuzz_input, ChildStatus::parse) {
        let _ = fuzz_support::round_trip(
            &(status),
            ChildStatus::parse(&serde_json::to_vec(&status).unwrap()),
        );
    }
    if let Some(request) =
        fuzz_support::FuzzInput::parse_bounded(fuzz_input, 65536, |raw_input, context0| {
            ChildRequest::parse(raw_input, context0)
        })
    {
        let encoded = request.encode();
        let _ = fuzz_support::round_trip(
            &(request),
            ChildRequest::parse(&encoded, encoded.len() as u64),
        );
    }
    if let Some(control) = fuzz_support::FuzzInput::parse(fuzz_input, Control::parse) {
        assert_eq!(control.bytes(), bytes);
    }
    if let Some(path) = fuzz_input.text()
        && let Some(key) = fuzz_support::FuzzInput::parse_text(fuzz_input, FileKey::parse)
    {
        assert_eq!(key.path(), path);
        let _ = fuzz_support::round_trip(&(key), FileKey::parse(&key.path()));
        assert!(Query::Root.matches(&key));
    }
});
