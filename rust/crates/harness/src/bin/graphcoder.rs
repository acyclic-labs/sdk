//! Native `GraphCoder` entrypoint. No durable product host is installed yet.

#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/fixture.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/terminal.rs"]
mod terminal;

#[cfg(target_arch = "wasm32")]
fn main() -> std::process::ExitCode {
    eprintln!("graphcoder requires a native terminal; use the public Harness WASM APIs");
    std::process::ExitCode::FAILURE
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("graphcoder: {}", terminal::text(&error.to_string()));
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
const HELP: &str = "graphcoder --fixture wire [--headless]\n\
    Explicit offline wire playback; no persistent session or recursive execution.\n\
    /next       Display one bounded activity delivery\n\
    /reset      Reset this fixture's transient display cursor\n\
    /request PATH  Dispatch a protobuf ClientFrame (fixture rejects mutations)\n\
    /help       Display these commands\n\
    /quit       Exit\n\
    Input, tree, diff, approvals, cancellation and durable reopen require a product host.\n";

#[cfg(not(target_arch = "wasm32"))]
fn options() -> acyclic_harness::Result<Option<bool>> {
    use acyclic_harness::Error;

    let mut fixture_selected = false;
    let mut headless = false;
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            print!("{HELP}");
            return Ok(None);
        }
        if argument == "--headless" && !headless {
            headless = true;
        } else if argument == "--fixture"
            && !fixture_selected
            && arguments.next().as_deref() == Some(std::ffi::OsStr::new("wire"))
        {
            fixture_selected = true;
        } else {
            return Err(Error::Invalid(
                "expected --fixture wire [--headless]".into(),
            ));
        }
    }
    if !fixture_selected {
        return Err(Error::Unsupported(
            "durable terminal host is unavailable; use --fixture wire for UI playback only".into(),
        ));
    }
    Ok(Some(headless))
}

#[cfg(not(target_arch = "wasm32"))]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    use std::io::{BufRead as _, IsTerminal as _, Read as _, Write as _};

    let Some(headless) = options()? else {
        return Ok(());
    };
    let api = fixture::WireFixture::load()?;
    let handshake = wire::client_frame::Frame::Handshake(wire::HandshakeRequest {
        protocol: Some(current_protocol()),
        required: None,
    });
    futures::executor::block_on(terminal::dispatch(&api, handshake))?;
    let stdin = std::io::stdin();
    let interactive = !headless && stdin.is_terminal();
    let mut input = stdin.lock();
    let mut output = std::io::stdout().lock();
    writeln!(
        output,
        "GraphCoder — fixture wire; UI playback only, no durable effects"
    )?;
    let mut cursor = api.start_cursor();
    loop {
        if interactive {
            write!(output, "fixture> ")?;
        }
        output.flush()?;
        let mut line = String::new();
        // A too-long line terminates instead of treating its remainder as a
        // second command. No background stdin thread or child process is owned.
        if (&mut input)
            .take(terminal::MAX_FRAME_BYTES as u64 + 1)
            .read_line(&mut line)?
            == 0
        {
            break;
        }
        if line.len() > terminal::MAX_FRAME_BYTES {
            return Err(Error::Invalid("terminal input exceeds bounds".into()).into());
        }
        let command = line.trim_end_matches(['\r', '\n']);
        match command {
            "/quit" => break,
            "/help" => write!(output, "{HELP}")?,
            "/reset" => {
                cursor = api.start_cursor();
                writeln!(output, "fixture display reset; no session restored")?;
            }
            "" => {}
            command => {
                let request = if command == "/next" {
                    Ok(wire::client_frame::Frame::Resume(wire::ResumeRequest {
                        protocol: Some(current_protocol()),
                        cursors: vec![cursor.clone()],
                    }))
                } else if let Some(path) = command.strip_prefix("/request ") {
                    read_request(path)
                } else {
                    Err(Error::Unsupported(
                        "operation requires a durable product host; fixture input is not queued"
                            .into(),
                    ))
                };
                let result = match request {
                    Ok(request) => futures::executor::block_on(terminal::dispatch(&api, request)),
                    Err(error) => Err(error),
                };
                match result {
                    Ok(Some(frame)) => {
                        let response = wire::ServerFrame { frame: Some(frame) };
                        let rendered = terminal::render(&response)?;
                        write!(output, "{rendered}")?;
                        output.flush()?;
                        if let Some(wire::server_frame::Frame::Delivery(delivery)) = response.frame
                        {
                            cursor = wire::ReplayCursor {
                                authority: delivery.authority,
                                generation: delivery.generation,
                                revision: delivery.through_revision,
                            };
                        }
                    }
                    Ok(None) => writeln!(output, "end of fixture activity")?,
                    Err(error) => {
                        if !interactive {
                            return Err(error.into());
                        }
                        writeln!(output, "{}", terminal::text(&error.to_string()))?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn read_request(path: &str) -> acyclic_harness::Result<acyclic_harness::wire::client_frame::Frame> {
    use acyclic_harness::{Error, wire};
    use prost::Message as _;
    use std::io::Read as _;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| {
            file.take(terminal::MAX_FRAME_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if bytes.len() > terminal::MAX_FRAME_BYTES {
        return Err(Error::Invalid("request exceeds terminal bounds".into()));
    }
    wire::ClientFrame::decode(bytes.as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?
        .frame
        .ok_or_else(|| Error::Invalid("request frame is missing".into()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_harness::Error;
    use std::io::Write as _;

    #[test]
    fn malformed_and_oversized_requests_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut request = tempfile::NamedTempFile::new()?;
        request.write_all(&[0xff])?;
        let path = request
            .path()
            .to_str()
            .ok_or("temporary path is not UTF-8")?;
        assert!(matches!(read_request(path), Err(Error::Invalid(_))));
        request
            .as_file()
            .set_len(terminal::MAX_FRAME_BYTES as u64 + 1)?;
        assert!(matches!(read_request(path), Err(Error::Invalid(_))));
        Ok(())
    }
}
