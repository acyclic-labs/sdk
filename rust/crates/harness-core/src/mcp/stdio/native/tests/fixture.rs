use super::*;
use std::{io, path::Path};

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}}"#;
const TOOLS: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;

pub(super) fn command(directory: &Path) -> Result<Command> {
    #[cfg(windows)]
    let mut command = {
        // cmd's SET /P discards subsequent lines already present in a pipe read.
        // Keep one buffered reader for the complete protocol conversation.
        let mut command = Command::new("powershell.exe");
        command.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!(
                "$null=[Console]::ReadLine(); [Console]::WriteLine('{INITIALIZE}'); $null=[Console]::ReadLine(); $null=[Console]::ReadLine(); [Console]::WriteLine('{TOOLS}'); $null=[Console]::ReadLine()"
            ),
        ]);
        command
    };
    #[cfg(not(windows))]
    let mut command = {
        let path = directory.join("server.sh");
        std::fs::write(
            &path,
            format!(
                "IFS= read -r input\nprintf '%s\\n' '{INITIALIZE}'\nIFS= read -r input\nIFS= read -r input\nprintf '%s\\n' '{TOOLS}'\nIFS= read -r input\n"
            ),
        )
        .map_err(Error::process_io)?;
        acyclic_native_runtime::shell_command(&format!(". \"{}\"", path.display()), Some("sh"))
    };
    command.current_dir(directory);
    Ok(command)
}

#[test]
fn coalesced_notification_and_request_preserve_both_protocol_lines() -> Result<()> {
    use acyclic_native_runtime::{CaptureInput, ProcessInput, ProcessOutput};
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let directory = tempfile::tempdir().map_err(Error::process_io)?;
    let mut command = command(directory.path())?;
    command.stdin(Stdio::piped()).stdout(Stdio::piped());
    let mut child = ProcessTree::spawn(&mut command).map_err(Error::process_io)?;
    let initial = b"{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"initialize\"}\n";
    let coalesced = b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n";
    let mut pending = Vec::new();
    let mut responses = Vec::new();
    child
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            Some(CaptureInput::Exchange {
                initial,
                maximum: 4096,
            }),
            || Ok(()),
            |chunk| {
                let ProcessOutput::Stdout(bytes) = chunk else {
                    return Ok(ProcessInput::default());
                };
                pending.extend_from_slice(bytes);
                let Some(end) = pending.iter().position(|byte| *byte == b'\n') else {
                    return Ok(ProcessInput::default());
                };
                let message: Value =
                    serde_json::from_slice(&pending[..=end]).map_err(io::Error::other)?;
                drop(pending.drain(..=end));
                responses.push(message);
                Ok(ProcessInput {
                    // Force the notification and request into one physical write.
                    bytes: if responses.len() == 1 {
                        coalesced.to_vec()
                    } else {
                        Vec::new()
                    },
                    complete: responses.len() == 2,
                })
            },
            None,
            None,
        )
        .map_err(|failure| invalid(failure.error))?;
    assert!(pending.is_empty());
    assert_eq!(
        responses,
        vec![
            serde_json::from_str::<Value>(INITIALIZE).map_err(invalid)?,
            serde_json::from_str::<Value>(TOOLS).map_err(invalid)?,
        ]
    );
    Ok(())
}
