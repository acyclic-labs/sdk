#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "fake-server assertions fail the test loudly"
)]

use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type Sender = mpsc::Sender<Result<wire::ExecOutput, Status>>;
type ServerTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

#[derive(Default)]
pub(super) struct GuestIo {
    senders: Mutex<VecDeque<Sender>>,
    commands: Mutex<Vec<wire::ExecRequest>>,
    reads: Mutex<Vec<wire::ReadOutputRequest>>,
    output: Mutex<wire::MachineOutput>,
}

impl GuestIo {
    pub(super) fn exec(&self, request: Request<wire::ExecRequest>) -> Response<ExecStreamWire> {
        self.commands.lock().unwrap().push(request.into_inner());
        let (sender, receiver) = mpsc::channel(4);
        self.senders.lock().unwrap().push_back(sender);
        Response::new(Box::pin(ReceiverStream::new(receiver)))
    }

    pub(super) fn read_output(
        &self,
        request: Request<wire::ReadOutputRequest>,
    ) -> Response<wire::MachineOutput> {
        self.reads.lock().unwrap().push(request.into_inner());
        Response::new(self.output.lock().unwrap().clone())
    }

    fn sender(&self) -> Sender {
        self.senders.lock().unwrap().pop_front().unwrap()
    }
}

type ExecStreamWire = Pin<Box<dyn Stream<Item = Result<wire::ExecOutput, Status>> + Send>>;

async fn fixture() -> (
    Machine,
    Arc<GuestIo>,
    tokio::sync::oneshot::Sender<()>,
    ServerTask,
) {
    let machine = MachineId::parse("00000000-0000-4000-8000-000000000001").unwrap();
    let io = Arc::new(GuestIo::default());
    let service = OperationService {
        guest: Some(io.clone()),
        expected_key: IdempotencyKey::parse("00000000-0000-4000-8000-000000000002").unwrap(),
        expected_operation: OperationId::parse("00000000-0000-4000-8000-000000000003").unwrap(),
        recovered: wire::RecoveredAdmission::default(),
        inspected: wire::OperationState::default(),
        cancelled: wire::OperationState::default(),
        watch: WatchReply::Items(vec![]),
    };
    let (machines, shutdown, task) = serve_operation_service(service).await.unwrap();
    (Machine::new(machines, machine), io, shutdown, task)
}

async fn stop(shutdown: tokio::sync::oneshot::Sender<()>, task: ServerTask) {
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn command(timeout_ms: u32) -> ExecCommand {
    ExecCommand::new(vec!["printf".into(), "hello".into()], timeout_ms).unwrap()
}

fn frame(machine: MachineId, output: wire::exec_output::Output) -> wire::ExecOutput {
    wire::ExecOutput {
        machine: Some(encode_machine(machine)),
        output: Some(output),
    }
}

#[test]
fn command_bounds_are_checked_before_dispatch() {
    for (argv, timeout) in [
        (vec![], 1),
        (vec!["".into()], 1),
        (vec!["a\0b".into()], 1),
        (vec!["a".repeat(4097)], 1),
        (vec!["a".into(); 65], 1),
        (vec!["a".repeat(4096); 9], 1),
        (vec!["a".into()], 0),
        (vec!["a".into()], 60_001),
    ] {
        assert!(matches!(
            ExecCommand::new(argv, timeout),
            Err(ProviderError::Invalid(_))
        ));
    }
    let bounded = ExecCommand::new(vec!["é".repeat(2048); 8], 60_000).unwrap();
    assert_eq!(bounded.argv().len(), 8);
    assert_eq!(bounded.timeout_ms(), 60_000);
    assert!(ExecCommand::new(vec!["a".into(), "".into()], 1).is_ok());
}

#[tokio::test]
async fn exec_stream_preserves_values_and_completed_eof_after_deadline() {
    let (machine, io, shutdown, task) = fixture().await;
    let mut output = machine.exec(command(1)).await.unwrap();
    let sender = io.sender();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::Data(vec![0, 255]),
        )))
        .await
        .unwrap();
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        ExecOutput::Data(vec![0, 255])
    );
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::ExitCode(-1),
        )))
        .await
        .unwrap();
    drop(sender);
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        ExecOutput::ExitCode(-1)
    );
    tokio::time::sleep(Duration::from_millis(1010)).await;
    assert!(output.next().await.is_none());
    {
        let commands = io.commands.lock().unwrap();
        assert_eq!(commands.len(), 1);
        let command = commands.first().unwrap();
        assert_eq!(command.machine, Some(encode_machine(machine.id())));
        assert_eq!(command.argv, ["printf", "hello"]);
        assert_eq!(command.timeout_ms, 1);
        assert_eq!(command.protocol, Some(protocol()));
    }
    drop(output);
    stop(shutdown, task).await;
}

#[tokio::test]
async fn exec_drop_and_bad_frame_release_only_their_rpc() {
    let (machine, io, shutdown, task) = fixture().await;
    let active = machine.exec(command(60_000)).await.unwrap();
    let active_sender = io.sender();
    let stopped = machine.exec(command(60_000)).await.unwrap();
    let stopped_sender = io.sender();
    drop(stopped);
    tokio::time::timeout(Duration::from_secs(1), stopped_sender.closed())
        .await
        .unwrap();
    for invalid in [
        wire::ExecOutput {
            machine: None,
            output: Some(wire::exec_output::Output::ExitCode(0)),
        },
        wire::ExecOutput {
            machine: Some(encode_machine(machine.id())),
            output: None,
        },
        frame(machine.id(), wire::exec_output::Output::Data(vec![])),
        frame(
            machine.id(),
            wire::exec_output::Output::Data(vec![0; 65_537]),
        ),
        frame(
            MachineId::parse("00000000-0000-4000-8000-000000000004").unwrap(),
            wire::exec_output::Output::ExitCode(0),
        ),
    ] {
        let mut rejected = machine.exec(command(60_000)).await.unwrap();
        let sender = io.sender();
        sender.send(Ok(invalid)).await.unwrap();
        assert!(matches!(
            rejected.next().await.unwrap(),
            Err(ProviderError::Rejected(_))
        ));
        tokio::time::timeout(Duration::from_secs(1), sender.closed())
            .await
            .unwrap();
        assert!(rejected.next().await.is_none());
        assert!(!active_sender.is_closed());
    }
    drop(active);
    tokio::time::timeout(Duration::from_secs(1), active_sender.closed())
        .await
        .unwrap();
    stop(shutdown, task).await;
}

#[tokio::test]
async fn exec_rejects_extra_frames_and_reports_lost_outcomes_without_retry() {
    let (machine, io, shutdown, task) = fixture().await;
    let mut extra = machine.exec(command(60_000)).await.unwrap();
    let sender = io.sender();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::ExitCode(0),
        )))
        .await
        .unwrap();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::Data(vec![1]),
        )))
        .await
        .unwrap();
    assert_eq!(
        extra.next().await.unwrap().unwrap(),
        ExecOutput::ExitCode(0)
    );
    assert!(matches!(
        extra.next().await.unwrap(),
        Err(ProviderError::Rejected(_))
    ));
    tokio::time::timeout(Duration::from_secs(1), sender.closed())
        .await
        .unwrap();
    for error in [None, Some(Code::Unavailable), Some(Code::DeadlineExceeded)] {
        let mut output = machine.exec(command(60_000)).await.unwrap();
        let sender = io.sender();
        if let Some(code) = error {
            sender
                .send(Err(Status::new(code, "owner unavailable")))
                .await
                .unwrap();
        }
        drop(sender);
        assert_eq!(
            output.next().await.unwrap().unwrap_err(),
            ProviderError::CommandIndeterminate(machine.id())
        );
        assert!(output.next().await.is_none());
    }
    assert_eq!(io.commands.lock().unwrap().len(), 4);
    assert_eq!(
        ProviderError::CommandIndeterminate(machine.id()).kind(),
        "command_indeterminate"
    );
    stop(shutdown, task).await;
}

#[tokio::test]
async fn exec_stream_errors_distinguish_unknown_and_completed_commands() {
    let (machine, io, shutdown, task) = fixture().await;
    for exited in [false, true] {
        for code in [
            Code::ResourceExhausted,
            Code::Unavailable,
            Code::PermissionDenied,
        ] {
            let mut output = machine.exec(command(60_000)).await.unwrap();
            let sender = io.sender();
            let (wire_output, expected) = if exited {
                (
                    wire::exec_output::Output::ExitCode(7),
                    ExecOutput::ExitCode(7),
                )
            } else {
                (
                    wire::exec_output::Output::Data(vec![1]),
                    ExecOutput::Data(vec![1]),
                )
            };
            sender
                .send(Ok(frame(machine.id(), wire_output)))
                .await
                .unwrap();
            assert_eq!(output.next().await.unwrap().unwrap(), expected);
            sender
                .send(Err(Status::new(code, "stream failed")))
                .await
                .unwrap();
            drop(sender);
            let error = output.next().await.unwrap().unwrap_err();
            if exited {
                assert!(matches!(error, ProviderError::Rejected(_)));
            } else {
                assert_eq!(error, ProviderError::CommandIndeterminate(machine.id()));
            }
            assert!(output.next().await.is_none());
        }
    }
    assert_eq!(io.commands.lock().unwrap().len(), 6);
    stop(shutdown, task).await;
}

#[tokio::test]
async fn read_output_preserves_wide_offsets_and_validates_evidence() {
    let (machine, io, shutdown, task) = fixture().await;
    let offset = 9_007_199_254_740_993;
    for limit in [0, 65_537] {
        assert!(matches!(
            machine.read_output(offset, limit).await,
            Err(ProviderError::Invalid(_))
        ));
    }
    assert!(io.reads.lock().unwrap().is_empty());
    for data in [vec![0, 255], vec![]] {
        *io.output.lock().unwrap() = wire::MachineOutput {
            machine: Some(encode_machine(machine.id())),
            offset: offset + 4,
            data: data.clone(),
        };
        let output = machine.read_output(offset, 2).await.unwrap();
        assert_eq!(
            output,
            MachineOutput {
                machine: machine.id(),
                offset: offset + 4,
                data
            }
        );
    }
    for invalid in [
        wire::MachineOutput {
            machine: None,
            offset,
            data: vec![],
        },
        wire::MachineOutput {
            machine: Some(encode_machine(machine.id())),
            offset: offset - 1,
            data: vec![],
        },
        wire::MachineOutput {
            machine: Some(encode_machine(machine.id())),
            offset,
            data: vec![0; 3],
        },
        wire::MachineOutput {
            machine: Some(encode_machine(machine.id())),
            offset: u64::MAX,
            data: vec![1],
        },
    ] {
        *io.output.lock().unwrap() = invalid;
        assert!(matches!(
            machine.read_output(offset, 2).await,
            Err(ProviderError::Rejected(_))
        ));
    }
    assert!(io.reads.lock().unwrap().iter().all(|read| read.machine
        == Some(encode_machine(machine.id()))
        && read.offset == offset
        && read.limit == 2
        && read.protocol == Some(protocol())));
    stop(shutdown, task).await;
}

#[tokio::test]
async fn simulator_rejects_guest_io_instead_of_fabricating_output() {
    let machines = Machines::new(Arc::new(SimulatedMachines::default()));
    let machine = Machine::new(
        machines,
        MachineId::parse("00000000-0000-4000-8000-000000000001").unwrap(),
    );
    assert!(matches!(
        machine.exec(command(1)).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        machine.read_output(0, 1).await,
        Err(ProviderError::Unsupported(_))
    ));
}
