use super::*;

fn creation_service(interrupted: bool) -> (OperationService, MachineId) {
    let operation =
        OperationId::parse("00000000-0000-0000-0000-000000000042").expect("fixture operation");
    let terminal = WatchReply::Items(vec![WatchItem::State(operation_state(
        operation,
        wire::OperationStatus::Succeeded,
    ))]);
    let watches = if interrupted {
        vec![WatchReply::Reject(Code::Unavailable), terminal]
    } else {
        vec![terminal]
    };
    let (mut service, _, machine) = scripted_operation_service(
        watches,
        vec![Ok(operation_state(
            operation,
            wire::OperationStatus::Pending,
        ))],
    );
    service.recovered = wire::RecoveredAdmission {
        operation: operation_request(operation).operation,
        result: Some(wire::recovered_admission::Result::Create(
            wire::MachineAdmission {
                machine: Some(encode_machine(machine)),
                operation: operation_request(operation).operation,
                contract: Some(codecs::wire_contract(&[])),
            },
        )),
    };
    service.guest = Some(Arc::new(guest_io::GuestIo::default()));
    (service, machine)
}

fn create_request(key: IdempotencyKey) -> Result<CreateMachine, ProviderError> {
    let mut request = CreateMachine::new(key, Image::Custom(ImageDigest::new([7; 32])?), [8; 32]);
    request.suspension = SuspensionPolicy::Manual;
    Ok(request)
}

#[tokio::test]
#[ignore = "measurement: seven bounded create/exec round trips; run once before and after a client change"]
async fn create_exec_round_trip_measurement() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    for interrupted in [false, true] {
        let mut creates = Vec::new();
        let mut execs = Vec::new();
        for _ in 0..7 {
            let (service, expected) = creation_service(interrupted);
            let key = service.expected_key;
            let io = service.guest.clone().expect("guest fixture");
            let (provider, shutdown, task) = serve_operation_service(service).await?;
            let machines = Machines::new(Arc::new(provider));
            let start = std::time::Instant::now();
            let machine = machines.create(create_request(key)?).await?;
            creates.push(start.elapsed().as_micros());
            assert_eq!(machine.id(), expected);
            let start = std::time::Instant::now();
            let mut output = machine
                .exec(ExecCommand::new(vec!["echo".into(), "hi".into()], 1000)?)
                .await?;
            let sender = io.sender();
            for value in [
                wire::exec_output::Output::Data(b"hi\n".to_vec()),
                wire::exec_output::Output::ExitCode(0),
            ] {
                sender
                    .send(Ok(wire::ExecOutput {
                        machine: Some(encode_machine(expected)),
                        output: Some(value),
                    }))
                    .await?;
            }
            drop(sender);
            assert_eq!(
                output.next().await.transpose()?,
                Some(ExecOutput::Data(b"hi\n".to_vec()))
            );
            assert_eq!(
                output.next().await.transpose()?,
                Some(ExecOutput::ExitCode(0))
            );
            assert!(output.next().await.is_none());
            execs.push(start.elapsed().as_micros());
            let _ = shutdown.send(());
            task.await??;
        }
        creates.sort_unstable();
        execs.sort_unstable();
        eprintln!(
            "{}",
            serde_json::json!({"case": if interrupted {"interrupted_watch"} else {"healthy"}, "samples":7,
            "create_us":creates,"exec_us":execs,"create_median_us":creates[3],"exec_median_us":execs[3]})
        );
    }
    Ok(())
}
