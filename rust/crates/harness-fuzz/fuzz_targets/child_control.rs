#![no_main]
use acyclic_harness::task_volume::{ChildRequest, ChildStatus, Control, FileKey, FileValue, Query};
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(event) = serde_json::from_slice::<acyclic_harness::executor::ExecutionEvent>(bytes) {
        let encoded = serde_json::to_vec(&event).unwrap();
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::executor::ExecutionEvent>(&encoded).unwrap(),
            event
        );
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        use acyclic_harness::workflow::{MachineStatus, ResumableMachine};
        let machine = acyclic_harness::filesystem::ChildAgentMachine::new();
        let initial = value.get("initial").unwrap_or(&value);
        if let Ok(state) = machine.initialize(initial) {
            let encoded = serde_json::to_vec(initial).unwrap();
            assert_eq!(
                machine
                    .initialize(&serde_json::from_slice(&encoded).unwrap())
                    .unwrap(),
                state
            );
            let waiting = machine
                .transition(&state, &serde_json::Value::Null)
                .unwrap();
            assert_eq!(waiting.status, MachineStatus::Suspended);
            if let Some(input) = value.get("input")
                && let Ok(active) = machine.transition(&waiting.state, input)
            {
                assert_eq!(active.status, MachineStatus::Suspended);
                if let Some(result) = value.get("model_result")
                    && let Ok(next) = machine.transition(&active.state, result)
                {
                    assert_eq!(next.status, MachineStatus::Suspended);
                    let encoded = serde_json::to_vec(&next.state).unwrap();
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
                        next.state
                    );
                }
            }
        }
    }
    let record = acyclic_stream::Record {
        sequence: 1,
        value: bytes.to_vec().into(),
        commit_id: acyclic_stream::CommitId::from_bytes([0; 32]),
        committed_at_micros: 0,
    };
    if let Ok(Some(file)) = FileValue::from_record(&record) {
        let encoded = serde_json::to_vec(&file).unwrap();
        assert_eq!(serde_json::from_slice::<FileValue>(&encoded).unwrap(), file);
    }
    if let Ok(request) = serde_json::from_slice::<acyclic_harness::fork::ForkRequest>(bytes)
        && request.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded: acyclic_harness::fork::ForkRequest = serde_json::from_slice(&encoded).unwrap();
        decoded.validate(&mut None).unwrap();
        assert_eq!(decoded, request);
    }
    if let Ok(seed) = serde_json::from_slice::<acyclic_harness::fork::ForkSeed>(bytes)
        && seed.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&seed).unwrap();
        let decoded: acyclic_harness::fork::ForkSeed = serde_json::from_slice(&encoded).unwrap();
        decoded.validate(&mut None).unwrap();
        assert_eq!(decoded, seed);
    }
    macro_rules! tool_input {
        ($ty:ty) => {
            if let Ok(input) = serde_json::from_slice::<$ty>(bytes) {
                assert_eq!(
                    serde_json::from_slice::<$ty>(&serde_json::to_vec(&input).unwrap()).unwrap(),
                    input
                );
            }
        };
    }
    tool_input!(acyclic_harness::tools::SpawnInput);
    tool_input!(acyclic_harness::tools::SendInput);
    tool_input!(acyclic_harness::tools::WaitInput);
    tool_input!(acyclic_harness::tools::ArchiveInput);
    if let Ok(result) = acyclic_harness::task_volume::ChildTurnResult::parse(bytes) {
        assert_eq!(
            acyclic_harness::task_volume::ChildTurnResult::parse(
                &serde_json::to_vec(&result).unwrap()
            )
            .unwrap(),
            result
        );
    }
    if let Ok(status) = ChildStatus::parse(bytes) {
        assert_eq!(
            ChildStatus::parse(&serde_json::to_vec(&status).unwrap()).unwrap(),
            status
        );
    }
    if let Ok(request) = ChildRequest::parse(bytes, 65536) {
        let encoded = request.encode();
        assert_eq!(
            ChildRequest::parse(&encoded, encoded.len() as u64).unwrap(),
            request
        );
    }
    if let Ok(control) = Control::parse(bytes) {
        assert_eq!(control.bytes(), bytes);
    }
    if let Ok(path) = std::str::from_utf8(bytes) {
        if let Ok(key) = FileKey::parse(path) {
            assert_eq!(key.path(), path);
            assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
            assert!(Query::Root.matches(&key));
        }
    }
});
