use super::*;

// Inspect the concrete state machines without opening stores or running a command.
fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}

type Commands<'a> = FilesystemTaskCommands<
    'a,
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

#[test]
fn model_command_future_stays_bounded() {
    let arguments: Option<(&Commands<'_>, CommandInvocation<'_>)> = None;
    let bytes = future_bytes(|| {
        let (commands, invocation) = arguments.unwrap();
        commands.model_command(invocation, Value::Null)
    });
    eprintln!("model command future: {bytes} bytes");
    assert!(bytes <= 32_768, "model command future is {bytes} bytes");
}

#[test]
fn command_dispatch_future_stays_bounded() {
    let commands: Option<&Commands<'_>> = None;
    let arguments: Option<(Arc<TaskContext>, LeaseFence, WorkflowCommand)> = None;
    let bytes = future_bytes(|| {
        let (context, fence, command) = arguments.unwrap();
        commands
            .unwrap()
            .execute_observed(context, fence, command, Value::Null, false, None)
    });
    eprintln!("command dispatch future: {bytes} bytes");
    assert!(bytes <= 8192, "command dispatch future is {bytes} bytes");
}

#[test]
fn command_journal_future_stays_bounded() {
    let arguments: Option<(&Commands<'_>, &LeaseFence, &Arc<TaskContext>)> = None;
    let bytes = future_bytes(|| {
        let (commands, fence, context) = arguments.unwrap();
        commands.command_journal(
            TaskId::from_bytes([1; 16]),
            fence,
            OperationId::from_bytes([2; 16]),
            context,
            None,
        )
    });
    eprintln!("command journal future: {bytes} bytes");
    assert!(bytes <= 4096, "command journal future is {bytes} bytes");
}
