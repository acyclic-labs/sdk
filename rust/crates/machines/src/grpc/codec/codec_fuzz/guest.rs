use super::*;
use crate::grpc::{files, recovery};

pub(super) fn check(name: &str, bytes: &[u8]) {
    let machine = MachineId::from_digest([1; 32]);
    let foreign = MachineId::from_digest([9; 32]);
    match name {
        "acyclic.machines.v1.ExecOutput" => {
            for terminal in [false, true] {
                let original = project::<wire::ExecOutput, _>(bytes, |value| {
                    let mut exited = terminal;
                    let result = decode_exec_output(value, machine, &mut exited);
                    if result.is_err() {
                        assert_eq!(exited, terminal, "refusal cannot consume terminal state");
                    }
                    result.map(|output| (output, exited))
                });
                if let Some((output, exited)) = original {
                    assert!(!terminal);
                    assert_eq!(
                        exited,
                        matches!(output, ExecOutput::ExitCode(_) | ExecOutput::Stopped(_))
                    );
                    let value = wire::ExecOutput::decode(bytes).unwrap();
                    let mut foreign_terminal = false;
                    assert!(
                        decode_exec_output(value.clone(), foreign, &mut foreign_terminal).is_err()
                    );
                    assert!(!foreign_terminal);
                    if exited {
                        let mut after_terminal = true;
                        assert!(decode_exec_output(value, machine, &mut after_terminal).is_err());
                        assert!(after_terminal);
                    }
                }
            }
        }
        "acyclic.machines.v1.MachineOutput" => {
            for offset in [0, u64::MAX] {
                for limit in [1, MAX_OUTPUT_READ_BYTES] {
                    if let Some(output) = project::<wire::MachineOutput, _>(bytes, |value| {
                        decode_machine_output(value, machine, offset, limit)
                    }) {
                        assert!(output.offset >= offset);
                        assert!(
                            output
                                .offset
                                .checked_add(output.data.len() as u64)
                                .is_some()
                        );
                        assert!(
                            decode_machine_output(
                                wire::MachineOutput::decode(bytes).unwrap(),
                                foreign,
                                offset,
                                limit
                            )
                            .is_err()
                        );
                    }
                }
            }
        }
        "acyclic.machines.v1.ReadFileResponse" => {
            for offset in [0, u64::MAX - u64::from(MAX_OUTPUT_READ_BYTES)] {
                for limit in [1, MAX_OUTPUT_READ_BYTES] {
                    if let Some(result) = project::<wire::ReadFileResponse, _>(bytes, |value| {
                        files::decode_read_response(value, machine, offset, limit)
                    }) {
                        let value = wire::ReadFileResponse::decode(bytes).unwrap();
                        assert!(
                            files::decode_read_response(value.clone(), foreign, offset, limit)
                                .is_err()
                        );
                        assert!(
                            files::decode_read_response(value.clone(), machine, offset ^ 1, limit)
                                .is_err()
                        );
                        if let FileRead::Data(data) = result {
                            if data.len() > 1 {
                                let short = u32::try_from(data.len() - 1).unwrap();
                                assert!(
                                    files::decode_read_response(value, machine, offset, short)
                                        .is_err()
                                );
                            }
                        }
                    }
                }
            }
        }
        "acyclic.machines.v1.WriteFileResponse" => {
            for offset in [0, u64::MAX - u64::from(MAX_OUTPUT_READ_BYTES)] {
                for length in [0, 1, MAX_OUTPUT_READ_BYTES as usize] {
                    if let Some(result) = project::<wire::WriteFileResponse, _>(bytes, |value| {
                        files::decode_write_response(value, machine, offset, length)
                    }) {
                        let value = wire::WriteFileResponse::decode(bytes).unwrap();
                        assert_eq!(
                            files::decode_write_response(value.clone(), foreign, offset, length),
                            Err(ProviderError::FileIndeterminate(foreign))
                        );
                        assert_eq!(
                            files::decode_write_response(
                                value.clone(),
                                machine,
                                offset ^ 1,
                                length
                            ),
                            Err(ProviderError::FileIndeterminate(machine))
                        );
                        if let FileWrite::Written(written) = result {
                            if written > 0 {
                                assert_eq!(
                                    files::decode_write_response(
                                        value,
                                        machine,
                                        offset,
                                        written as usize - 1
                                    ),
                                    Err(ProviderError::FileIndeterminate(machine))
                                );
                            }
                        }
                    }
                }
            }
        }
        "acyclic.machines.v1.MachinePage" => {
            for after in [None, Some(machine)] {
                for limit in [1, MAX_PAGE_SIZE] {
                    if let Some(page) = project::<wire::MachinePage, _>(bytes, |value| {
                        decode_machine_page(value, after, limit)
                    }) {
                        assert!(page.machines.len() <= limit as usize);
                        if let Some(last) = page.machines.last() {
                            assert!(
                                decode_machine_page(
                                    wire::MachinePage::decode(bytes).unwrap(),
                                    Some(last.id),
                                    limit
                                )
                                .is_err()
                            );
                        }
                        let mut value = wire::MachinePage::decode(bytes).unwrap();
                        value.next = Some(encode_machine(foreign));
                        if page.machines.last().map(|value| value.id) == Some(foreign) {
                            value.next = Some(encode_machine(machine));
                        }
                        assert!(decode_machine_page(value, after, limit).is_err());
                    }
                }
            }
        }
        "acyclic.machines.v1.RecoveredAdmission" => {
            if let Some(operation) = project::<wire::RecoveredAdmission, _>(bytes, |value| {
                recovery::validate_recovered_admission(&value)
            }) {
                let mut value = wire::RecoveredAdmission::decode(bytes).unwrap();
                let foreign = if operation == OperationId::from_digest([9; 32]) {
                    OperationId::from_digest([8; 32])
                } else {
                    OperationId::from_digest([9; 32])
                };
                value.operation = operation_request(foreign).operation;
                assert!(recovery::validate_recovered_admission(&value).is_err());
            }
        }
        "acyclic.machines.v1.MachineAdmissions" => {
            if let Some((operation, _, _)) = project::<wire::MachineAdmissions, _>(bytes, |value| {
                recovery::decode_fork_admissions(&value)
                    .map(|value| (value.operation, value.provenance, value.children))
            }) {
                let mut value = wire::MachineAdmissions::decode(bytes).unwrap();
                let foreign = if operation == OperationId::from_digest([9; 32]) {
                    OperationId::from_digest([8; 32])
                } else {
                    OperationId::from_digest([9; 32])
                };
                value.operation = operation_request(foreign).operation;
                assert!(recovery::decode_fork_admissions(&value).is_err());
            }
        }
        _ => {}
    }
}
