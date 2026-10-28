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
    file_reads: Mutex<Vec<wire::ReadFileRequest>>,
    file_writes: Mutex<Vec<wire::WriteFileRequest>>,
    file_read: Mutex<wire::ReadFileResponse>,
    file_write: Mutex<wire::WriteFileResponse>,
    file_write_error: Mutex<Option<Code>>,
}

impl GuestIo {
    pub(super) fn read_file(
        &self,
        request: Request<wire::ReadFileRequest>,
    ) -> Result<Response<wire::ReadFileResponse>, Status> {
        self.file_reads.lock().unwrap().push(request.into_inner());
        Ok(Response::new(self.file_read.lock().unwrap().clone()))
    }
    pub(super) fn write_file(
        &self,
        request: Request<wire::WriteFileRequest>,
    ) -> Result<Response<wire::WriteFileResponse>, Status> {
        self.file_writes.lock().unwrap().push(request.into_inner());
        if let Some(code) = *self.file_write_error.lock().unwrap() {
            return Err(Status::new(code, "native completion unavailable"));
        }
        Ok(Response::new(self.file_write.lock().unwrap().clone()))
    }
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
    (
        Machine::new(Machines::new(Arc::new(machines)), machine),
        io,
        shutdown,
        task,
    )
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
    for timeout in [60_001, 120_000, 300_000, u32::MAX] {
        assert_eq!(command(timeout).timeout_ms(), timeout);
    }
}

#[tokio::test]
async fn exec_preserves_stdin_cwd_authority_and_both_output_channels() {
    let (machine, io, shutdown, task) = fixture().await;
    let command = command(300_000)
        .with_stdin(vec![0, 255, 10])
        .with_cwd("/workspace/sub directory".into())
        .unwrap()
        .with_expiry(4_102_444_800_000)
        .with_output_limit(4);
    let output = machine.exec(command).await.unwrap();
    let sender = io.sender();
    for value in [
        wire::exec_output::Output::Data(vec![0, 255]),
        wire::exec_output::Output::Stderr(vec![254, 10]),
        wire::exec_output::Output::ExitCode(7),
    ] {
        sender.send(Ok(frame(machine.id(), value))).await.unwrap();
    }
    drop(sender);
    let captured = capture_exec(output, 4).await.unwrap();
    assert_eq!(captured.stdout, [0, 255]);
    assert_eq!(captured.stderr, [254, 10]);
    assert_eq!(captured.exit_code, Some(7));
    assert!(captured.stop.is_none());
    let request = io.commands.lock().unwrap().first().unwrap().clone();
    assert_eq!(request.stdin, [0, 255, 10]);
    assert_eq!(request.cwd.as_deref(), Some("/workspace/sub directory"));
    assert_eq!(request.expiry_unix_ms, Some(4_102_444_800_000));
    assert_eq!(request.maximum_output_bytes, Some(4));
    assert_eq!(request.timeout_ms, 300_000);
    stop(shutdown, task).await;
}

#[tokio::test]
async fn native_files_preserve_binary_ranges_errno_and_lost_write_identity() {
    use wire::{read_file_response::Result as Read, write_file_response::Result as Write};
    let (machine, io, shutdown, task) = fixture().await;
    let path = "/workspace/$(printf literal) ' \" file".to_string();
    let access = FileAccess::new(path.clone(), 0, 4_102_444_800_000).unwrap();
    *io.file_write.lock().unwrap() = wire::WriteFileResponse {
        machine: Some(encode_machine(machine.id())),
        offset: 0,
        result: Some(Write::Written(2)),
    };
    assert_eq!(
        machine
            .write_file(access.clone(), vec![0, 255], true)
            .await
            .unwrap(),
        FileWrite::Written(2)
    );
    let written = io.file_writes.lock().unwrap().first().unwrap().clone();
    assert_eq!(written.data, [0, 255]);
    assert!(written.truncate);
    let admitted = written.access.unwrap();
    assert_eq!(admitted.path, path);
    assert_eq!(admitted.deadline_unix_ms, access.deadline_unix_ms());
    assert_eq!(admitted.machine, Some(encode_machine(machine.id())));
    let offset = 9_007_199_254_740_993;
    let range = FileAccess::new(path.clone(), offset, access.deadline_unix_ms()).unwrap();
    for data in [vec![0, 255], vec![]] {
        *io.file_read.lock().unwrap() = wire::ReadFileResponse {
            machine: Some(encode_machine(machine.id())),
            offset,
            result: Some(Read::Data(data.clone())),
        };
        assert_eq!(
            machine.read_file(range.clone(), 2).await.unwrap(),
            FileRead::Data(data)
        );
    }
    for errno in [2, 13, 28] {
        io.file_read.lock().unwrap().result = Some(Read::Errno(errno));
        assert_eq!(
            machine.read_file(range.clone(), 2).await.unwrap(),
            FileRead::Errno(NonZeroU32::new(errno).unwrap())
        );
        io.file_write.lock().unwrap().result = Some(Write::Errno(errno));
        assert_eq!(
            machine
                .write_file(access.clone(), vec![0, 255], true)
                .await
                .unwrap(),
            FileWrite::Errno(NonZeroU32::new(errno).unwrap())
        );
    }
    for invalid in [None, Some(Read::Errno(0)), Some(Read::Data(vec![1; 3]))] {
        io.file_read.lock().unwrap().result = invalid;
        assert!(matches!(
            machine.read_file(range.clone(), 2).await,
            Err(ProviderError::Rejected(_))
        ));
    }
    io.file_read.lock().unwrap().machine = Some(encode_machine(
        MachineId::parse("00000000-0000-4000-8000-000000000099").unwrap(),
    ));
    assert!(matches!(
        machine.read_file(range.clone(), 2).await,
        Err(ProviderError::Rejected(_))
    ));
    for path in ["", "relative", "/a\0b"] {
        assert!(FileAccess::new(path.into(), 0, access.deadline_unix_ms()).is_err());
    }
    assert!(machine.read_file(access.clone(), 0).await.is_err());
    assert!(machine.read_file(access.clone(), 65_537).await.is_err());
    assert!(machine.write_file(range, vec![1], true).await.is_err());
    assert!(
        machine
            .write_file(access.clone(), vec![0; 65_537], false)
            .await
            .is_err()
    );
    let overflowing = FileAccess::new(path, u64::MAX, access.deadline_unix_ms()).unwrap();
    assert!(machine.read_file(overflowing, 1).await.is_err());
    let expired = FileAccess::new("/expired".into(), 0, 1).unwrap();
    assert!(matches!(
        machine.write_file(expired, vec![], true).await,
        Err(ProviderError::Rejected(_))
    ));
    let before = io.file_writes.lock().unwrap().len();
    *io.file_write_error.lock().unwrap() = Some(Code::Unavailable);
    assert_eq!(
        machine.write_file(access, vec![1], true).await.unwrap_err(),
        ProviderError::FileIndeterminate(machine.id())
    );
    assert_eq!(io.file_writes.lock().unwrap().len(), before + 1);
    assert!(io.commands.lock().unwrap().is_empty());
    stop(shutdown, task).await;
}

#[tokio::test]
async fn exec_expiry_wakes_a_silent_stream_and_closes_only_that_rpc() {
    let (machine, io, shutdown, task) = fixture().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let expiry = u64::try_from(now.as_millis()).unwrap() + 100;
    let mut output = machine
        .exec(command(300_000).with_expiry(expiry))
        .await
        .unwrap();
    let sender = io.sender();
    assert_eq!(
        output.next().await.unwrap().unwrap_err(),
        ProviderError::CommandIndeterminate(machine.id())
    );
    tokio::time::timeout(Duration::from_secs(1), sender.closed())
        .await
        .unwrap();
    assert!(output.next().await.is_none());
    assert!(matches!(
        machine.exec(command(300_000).with_expiry(1)).await,
        Err(ProviderError::Rejected(_))
    ));
    assert_eq!(io.commands.lock().unwrap().len(), 1);
    stop(shutdown, task).await;
}

#[tokio::test]
async fn stopped_capture_retains_reason_and_physical_cleanup_receipt() {
    let (machine, io, shutdown, task) = fixture().await;
    let output = machine.exec(command(300_000)).await.unwrap();
    let sender = io.sender();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::Stopped(wire::ExecStop {
                kind: wire::ExecStopKind::Expired.into(),
                message: "scope elapsed".into(),
                cleanup_completed: false,
            }),
        )))
        .await
        .unwrap();
    drop(sender);
    let captured = capture_exec(output, 0).await.unwrap();
    assert_eq!(captured.exit_code, None);
    assert_eq!(
        captured.stop,
        Some(ExecStop {
            kind: ExecStopKind::Expired,
            message: "scope elapsed".into(),
            cleanup_completed: false
        })
    );
    stop(shutdown, task).await;
}

#[tokio::test]
async fn combined_capture_allowance_refuses_without_draining_or_redispatch() {
    let (machine, io, shutdown, task) = fixture().await;
    let output = machine
        .exec(command(300_000).with_output_limit(1))
        .await
        .unwrap();
    let sender = io.sender();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::Data(vec![1]),
        )))
        .await
        .unwrap();
    sender
        .send(Ok(frame(
            machine.id(),
            wire::exec_output::Output::Stderr(vec![2]),
        )))
        .await
        .unwrap();
    assert!(matches!(
        capture_exec(output, 1).await,
        Err(ProviderError::Rejected(_))
    ));
    tokio::time::timeout(Duration::from_secs(1), sender.closed())
        .await
        .unwrap();
    assert_eq!(io.commands.lock().unwrap().len(), 1);
    stop(shutdown, task).await;
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
        frame(machine.id(), wire::exec_output::Output::Stderr(vec![])),
        frame(
            machine.id(),
            wire::exec_output::Output::Stopped(wire::ExecStop::default()),
        ),
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
async fn exec_terminal_event_completes_without_waiting_for_eof_and_lost_outcomes_are_not_retried() {
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
    assert_eq!(
        extra.next().await.unwrap().unwrap(),
        ExecOutput::ExitCode(0)
    );
    assert!(extra.next().await.is_none());
    let mut exited = true;
    assert!(matches!(
        decode_exec_output(
            frame(machine.id(), wire::exec_output::Output::Data(vec![1])),
            machine.id(),
            &mut exited
        ),
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
            if exited {
                assert!(output.next().await.is_none());
                assert!(matches!(
                    command_stream_error(machine.id(), true, &Status::new(code, "stream failed")),
                    ProviderError::Rejected(_)
                ));
                continue;
            }
            sender
                .send(Err(Status::new(code, "stream failed")))
                .await
                .unwrap();
            drop(sender);
            let error = output.next().await.unwrap().unwrap_err();
            assert_eq!(error, ProviderError::CommandIndeterminate(machine.id()));
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
