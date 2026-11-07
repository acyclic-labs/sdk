//! The coordination tools a native host's model calls, served through the
//! shared stdio MCP tool server. Native CLIs own agent loops, tools and
//! policy; these tools only map onto [`NativeChildren`], and a wait parks
//! until its child is idle. Between passes the server sleeps on its client
//! and the children's ready queue together: no thread, timer or poll per
//! child, and no pass rebuilds or probes a quiet child's readiness.
use super::*;
use crate::mcp::stdio::{ServeError, Settled, ToolCall, ToolProvider, ToolServer, Wake};
use serde_json::json;
use std::collections::BTreeSet;

/// Every coordination tool, by the name its MCP server advertises.
const TOOLS: [&str; 6] = ["spawn", "send", "wait", "archive", "cancel", "resume"];
/// Waits parked at once, bounded like the host's request concurrency.
const WAITS: usize = 16;
/// Bound of one request line.
const FRAME_BYTES: u32 = 65_536;

/// The tools of one native host's parent, opening its children on first use.
struct Coordination {
    host: String,
    children: Option<NativeChildren>,
}

/// Serves `host`'s coordination tools on stdio until its client exits,
/// interrupting every running child before draining outstanding replies.
pub fn serve(host: &str) -> Result<()> {
    let mut coordination = Coordination {
        host: host.into(),
        children: None,
    };
    ToolServer {
        name: "acyclic-sessions",
        version: env!("CARGO_PKG_VERSION"),
        parked: WAITS,
        frame_bytes: FRAME_BYTES,
    }
    .serve(&mut coordination, std::io::stdin(), std::io::stdout())
    .map_err(|error| match error {
        ServeError::Io(error) => Error::Storage(error.to_string()),
        ServeError::Provider(error) => error,
    })
}

impl ToolProvider for Coordination {
    type Error = Error;

    fn tools(&self) -> Value {
        // Spawn admits new work through the harness approval hook. The others
        // act only on this owner's own children, keep their history, and say
        // so: an unannotated MCP tool counts as destructive and open-world, so
        // a host that never prompts would refuse it.
        json!({"tools":(TOOLS.map(|name| {
            let prompt=matches!(name,"spawn"|"send"|"resume");
            let mut tool=json!({"name":name,"description":format!("{name} a separate native session in an admitted harness view. Native tools and policy are preserved. Native subagents are unmanaged."),"inputSchema":{"type":"object","properties":{"reference":{"type":"string"},"prompt":{"type":"string"}},"required":if prompt {vec!["reference","prompt"]} else {vec!["reference"]},"additionalProperties":false}});
            if name!="spawn" {
                tool["annotations"]=json!({"readOnlyHint":name=="wait","destructiveHint":false,"openWorldHint":false});
            }
            tool
        }))})
    }

    fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall> {
        let object = arguments
            .as_object()
            .ok_or_else(|| invalid("tool arguments must be an object"))?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "reference" | "prompt"))
        {
            return Err(invalid("unsupported coordination argument"));
        }
        let reference = object
            .get("reference")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("reference is required"))?;
        let prompt = || {
            object
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| invalid("prompt is required"))
        };
        if self.children.is_none() {
            self.children = Some(NativeChildren::open(&self.host)?);
        }
        let children = self
            .children
            .as_mut()
            .ok_or_else(|| invalid("native children absent"))?;
        let answer = match name {
            "spawn" => children.spawn(reference, prompt()?)?,
            "send" => children.send(reference, prompt()?)?,
            "resume" => children.resume(reference, prompt()?)?,
            "archive" => children.archive(reference)?,
            "cancel" => children.cancel(reference)?,
            "wait" => match children.wait(reference)? {
                Some(answer) => answer,
                None => return Ok(ToolCall::Park(reference.into())),
            },
            _ => return Err(invalid("unknown coordination tool")),
        };
        Ok(ToolCall::Answer(answer))
    }

    fn closed(&mut self) -> Result<()> {
        self.children
            .as_mut()
            .map_or(Ok(()), NativeChildren::shutdown)
    }

    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Settled)>,
    ) -> Result<Option<Wake>> {
        let Some(children) = self.children.as_mut() else {
            return Ok(None);
        };
        // Only children whose readiness fired are pumped.
        children.pump()?;
        // A parked wait settles once its child is idle: its own process
        // exited, or a cancel or archive stopped it.
        for reference in parked {
            if let Some(answer) = children.wait(reference)? {
                settled.push((reference.clone(), Ok(answer)));
            }
        }
        // Only this owner's own processes can settle a wait, so with none
        // running it blocks on its host. Otherwise the next pass runs once a
        // child's pipe, stdin room or exit fires its own registration, or a
        // bounded pass left output unread and queued it again at once.
        if !children.running() {
            return Ok(None);
        }
        Ok(Some(Box::pin(children.woken())))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::super::tests::{children, claim, native_fixture, served, shell_pid};
    use super::*;
    use std::io::{self, Read as _, Seek as _};
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

    /// EOF is delivered only after the actual reply writer is held.
    struct Input {
        bytes: io::Cursor<Vec<u8>>,
        writing: Option<Receiver<()>>,
    }

    impl io::Read for Input {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            let count = self.bytes.read(bytes)?;
            if count == 0
                && let Some(writing) = self.writing.take()
            {
                writing
                    .recv()
                    .map_err(|_| io::Error::other("reply writer producer closed"))?;
            }
            Ok(count)
        }
    }

    struct Output {
        writing: Option<SyncSender<()>>,
        release: Receiver<()>,
    }

    impl io::Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(writing) = self.writing.take() {
                writing
                    .send(())
                    .map_err(|_| io::Error::other("input observer closed"))?;
                self.release
                    .recv()
                    .map_err(|_| io::Error::other("retirement observer closed"))?;
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Observes the real provider's close, then releases the held writer even
    /// when the original close failed. No completion depends on a timeout.
    struct Observed {
        owner: Coordination,
        process: std::fs::File,
        release: SyncSender<()>,
    }

    impl ToolProvider for Observed {
        type Error = Error;

        fn tools(&self) -> Value {
            self.owner.tools()
        }

        fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall> {
            self.owner.call(name, arguments)
        }

        fn progress(
            &mut self,
            parked: &BTreeSet<String>,
            settled: &mut Vec<(String, Settled)>,
        ) -> Result<Option<Wake>> {
            self.owner.progress(parked, settled)
        }

        fn closed(&mut self) -> Result<()> {
            let closed = self.owner.closed();
            let retired = (|| {
                let children = self
                    .owner
                    .children
                    .as_ref()
                    .ok_or_else(|| invalid("original child owner absent"))?;
                if children.running() {
                    return Err(invalid(
                        "EOF left native children running before reply drain",
                    ));
                }
                let mut stat = String::new();
                match self.process.read_to_string(&mut stat) {
                    Err(error)
                        if error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) => {
                    }
                    Err(error) => return Err(storage(error)),
                    Ok(_) => {
                        return Err(invalid("original process must retire before reply drain"));
                    }
                }
                let reply = children
                    .wait("held")?
                    .ok_or_else(|| invalid("retired child remains running"))?;
                if reply["control"] != "interrupt" || reply["status"]["settled"] != true {
                    return Err(invalid("EOF did not settle the original native claim"));
                }
                Ok(())
            })();
            let released = self
                .release
                .send(())
                .map_err(|_| invalid("reply writer stopped before retirement observation"));
            closed.and(retired).and(released)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eof_retires_native_children_before_draining_a_held_reply()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for host in ["codex", "claude-code", "pi"] {
            served(host, |host, cwd| {
                let (mut children, _) = children(host, cwd, "eof-retirement")?;
                let (mut running, view) = claim(&mut children, cwd, "held", SessionAction::Spawn { prompt: "wait for parent".into() })?;
                let mut command = native_fixture(&view, "", "0");
                running.process.spawn(&mut command, running.claim.operation)?;
                let pid = shell_pid(&mut running)?;
                let mut process = std::fs::File::open(format!("/proc/{pid}/stat"))?;
                let mut original = String::new();
                process.read_to_string(&mut original)?;
                assert!(!original.is_empty());
                process.rewind()?;
                children.children.insert(running.claim.agent.to_string(), running);
                let (writing, written) = sync_channel(1);
                let (release, released) = sync_channel(1);
                let mut observed = Observed { owner: Coordination { host: host.into(), children: Some(children) }, process, release };
                ToolServer { name: "acyclic-sessions", version: env!("CARGO_PKG_VERSION"), parked: WAITS, frame_bytes: FRAME_BYTES }.serve(
                    &mut observed,
                    Input { bytes: io::Cursor::new(b"{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\"}}\n".to_vec()), writing: Some(written) },
                    Output { writing: Some(writing), release: released },
                )?;
                Ok(())
            }).await?;
        }
        Ok(())
    }
}
