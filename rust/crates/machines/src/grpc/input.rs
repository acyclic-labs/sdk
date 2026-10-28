use super::*;

// Only the current frame is copied. Tonic pulls this iterator under its original
// HTTP/2 flow control; there is no background sender or whole-command frame queue.
pub(super) fn encode(
    machine: MachineId,
    command: ExecCommand,
) -> impl Iterator<Item = wire::ExecInput> + Send + 'static {
    use wire::exec_input::Input;
    let start = Input::Start(wire::ExecStart {
        protocol: Some(protocol()),
        machine: Some(encode_machine(machine)),
        timeout_ms: command.timeout_ms,
        expiry_unix_ms: command.expiry_unix_ms,
        maximum_output_bytes: command.maximum_output_bytes,
        cwd: command.cwd,
    });
    let arguments = command.argv.into_iter().flat_map(|argument| {
        Chunks::new(argument.into_bytes())
            .map(|(data, end)| Input::Argument(wire::ExecArgument { data, end }))
    });
    std::iter::once(start)
        .chain(arguments)
        .chain(std::iter::once(Input::CommandComplete(wire::ExecMarker {})))
        .chain(
            Chunks::new(command.stdin)
                .filter_map(|(data, _)| (!data.is_empty()).then_some(Input::Stdin(data))),
        )
        .chain(std::iter::once(Input::StdinEof(wire::ExecMarker {})))
        .map(|input| wire::ExecInput { input: Some(input) })
}

struct Chunks {
    bytes: Vec<u8>,
    offset: usize,
    done: bool,
}

impl Chunks {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            offset: 0,
            done: false,
        }
    }
}

impl Iterator for Chunks {
    type Item = (Vec<u8>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let end = self.bytes.len().min(self.offset.saturating_add(65_536));
        let data = self.bytes[self.offset..end].to_vec();
        self.offset = end;
        self.done = end == self.bytes.len();
        Some((data, self.done))
    }
}
