use super::*;

pub(super) fn check(name: &str, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::from_digest([1; 32]);
    let foreign = MachineId::from_digest([9; 32]);
    match name {
        "acyclic.machines.v1.ExecOutput" => exec_output(bytes, machine, foreign)?,
        "acyclic.machines.v1.MachineOutput" => machine_output(bytes, machine, foreign)?,
        "acyclic.machines.v1.ReadFileResponse" => read_file_response(bytes, machine, foreign)?,
        "acyclic.machines.v1.WriteFileResponse" => write_file_response(bytes, machine, foreign)?,
        "acyclic.machines.v1.RecoveredAdmission" => {
            if let Some(operation) = project::<wire::RecoveredAdmission, _>(bytes, |value| {
                validate_recovered_admission(&value)
            })? {
                let mut value = wire::RecoveredAdmission::decode(bytes)?;
                let foreign = super::foreign(&operation, OperationId::from_digest);
                value.operation = operation_request(foreign).operation;
                assert!(validate_recovered_admission(&value).is_err());
            }
        }
        "acyclic.machines.v1.MachineAdmissions" => {
            if project::<wire::MachineAdmissions, _>(bytes, |value| {
                decode_fork_admissions(&value)
                    .map(|value| (value.operation, value.provenance, value.count))
            })?
            .is_some()
            {
                let mut value = wire::MachineAdmissions::decode(bytes)?;
                value.operation = None;
                assert!(decode_fork_admissions(&value).is_err());
            }
        }
        "acyclic.machines.v1.ResultCut" => {
            if let Some(cut) =
                project::<wire::ResultCut, _>(bytes, |value| decode_result_cut(Some(&value)))?
            {
                assert_eq!(decode_result_cut(Some(&cut.into())), Ok(cut));
                let mut value = wire::ResultCut::from(cut);
                value.settlement = None;
                assert!(decode_result_cut(Some(&value)).is_err());
            }
        }
        "acyclic.machines.v1.ResultCursor" => {
            if let Some(cursor) =
                project::<wire::ResultCursor, _>(bytes, |value| decode_result_cursor(Some(&value)))?
            {
                assert_eq!(decode_result_cursor(Some(&cursor.into())), Ok(cursor));
                let mut value = wire::ResultCursor::from(cursor);
                value.cut = None;
                assert!(decode_result_cursor(Some(&value)).is_err());
            }
        }
        "acyclic.machines.v1.ResultPage" => {
            let Ok(wire) = wire::ResultPage::decode(bytes) else {
                return Ok(());
            };
            let Ok(cursor) = decode_result_cursor(wire.cursor.as_ref()) else {
                return Ok(());
            };
            for credit in [NonZeroU32::MIN, NonZeroU32::MAX] {
                if let Some(page) = project::<wire::ResultPage, _>(bytes, |value| {
                    decode_result_page(value, cursor, credit)
                })? {
                    assert_eq!(page.cursor, cursor);
                    assert!(page.machines.len() as u64 <= u64::from(credit.get()));
                    let mut foreign = cursor;
                    foreign.cut.operation =
                        super::foreign(&cursor.cut.operation, OperationId::from_digest);
                    assert!(decode_result_page(wire.clone(), foreign, credit).is_err());
                    let mut substituted = wire.clone();
                    substituted.next = Some(cursor.into());
                    assert!(decode_result_page(substituted, cursor, credit).is_err());
                    if page.machines.len() > 1 {
                        let short = NonZeroU32::new(u32::try_from(page.machines.len() - 1)?)
                            .ok_or("short page credit is zero")?;
                        assert!(decode_result_page(wire.clone(), cursor, short).is_err());
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn exec_output(
    bytes: &[u8],
    machine: MachineId,
    foreign: MachineId,
) -> Result<(), Box<dyn std::error::Error>> {
    for terminal in [false, true] {
        let original = project::<wire::ExecOutput, _>(bytes, |value| {
            let mut exited = terminal;
            let result = decode_exec_output(value, machine, &mut exited);
            if result.is_err() {
                assert_eq!(exited, terminal, "refusal cannot consume terminal state");
            }
            result.map(|output| (output, exited))
        })?;
        if let Some((output, exited)) = original {
            assert!(!terminal);
            assert_eq!(
                exited,
                matches!(output, ExecOutput::ExitCode(_) | ExecOutput::Stopped(_))
            );
            let value = wire::ExecOutput::decode(bytes)?;
            let mut foreign_terminal = false;
            assert!(decode_exec_output(value.clone(), foreign, &mut foreign_terminal).is_err());
            assert!(!foreign_terminal);
            if exited {
                let mut after_terminal = true;
                assert!(decode_exec_output(value, machine, &mut after_terminal).is_err());
                assert!(after_terminal);
            }
        }
    }
    Ok(())
}

fn machine_output(
    bytes: &[u8],
    machine: MachineId,
    foreign: MachineId,
) -> Result<(), Box<dyn std::error::Error>> {
    for offset in [0, u64::MAX] {
        for limit in [1, MAX_OUTPUT_READ_BYTES] {
            if let Some(output) = project::<wire::MachineOutput, _>(bytes, |value| {
                decode_machine_output(value, machine, offset, limit)
            })? {
                assert!(output.offset >= offset);
                assert!(
                    output
                        .offset
                        .checked_add(output.data.len() as u64)
                        .is_some()
                );
                assert!(
                    decode_machine_output(
                        wire::MachineOutput::decode(bytes)?,
                        foreign,
                        offset,
                        limit
                    )
                    .is_err()
                );
            }
        }
    }
    Ok(())
}

fn read_file_response(
    bytes: &[u8],
    machine: MachineId,
    foreign: MachineId,
) -> Result<(), Box<dyn std::error::Error>> {
    for offset in [0, u64::MAX - u64::from(MAX_OUTPUT_READ_BYTES)] {
        for limit in [1, MAX_OUTPUT_READ_BYTES] {
            if let Some(result) = project::<wire::ReadFileResponse, _>(bytes, |value| {
                decode_read_response(value, machine, offset, limit)
            })? {
                let value = wire::ReadFileResponse::decode(bytes)?;
                assert!(decode_read_response(value.clone(), foreign, offset, limit).is_err());
                assert!(decode_read_response(value.clone(), machine, offset ^ 1, limit).is_err());
                if let FileRead::Data(data) = result
                    && data.len() > 1
                {
                    let short = u32::try_from(data.len() - 1)?;
                    assert!(decode_read_response(value, machine, offset, short).is_err());
                }
            }
        }
    }
    Ok(())
}

fn write_file_response(
    bytes: &[u8],
    machine: MachineId,
    foreign: MachineId,
) -> Result<(), Box<dyn std::error::Error>> {
    for offset in [0, u64::MAX - u64::from(MAX_OUTPUT_READ_BYTES)] {
        for length in [0, 1, MAX_OUTPUT_READ_BYTES as usize] {
            if let Some(result) = project::<wire::WriteFileResponse, _>(bytes, |value| {
                decode_write_response(&value, machine, offset, length)
            })? {
                let value = wire::WriteFileResponse::decode(bytes)?;
                assert_eq!(
                    decode_write_response(&value, foreign, offset, length),
                    Err(ProviderError::FileIndeterminate(foreign))
                );
                assert_eq!(
                    decode_write_response(&value, machine, offset ^ 1, length),
                    Err(ProviderError::FileIndeterminate(machine))
                );
                if let FileWrite::Written(written) = result
                    && written > 0
                {
                    assert_eq!(
                        decode_write_response(&value, machine, offset, written as usize - 1),
                        Err(ProviderError::FileIndeterminate(machine))
                    );
                }
            }
        }
    }
    Ok(())
}
