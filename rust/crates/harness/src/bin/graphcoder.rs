//! Native `GraphCoder` presentation over the public Harness runtime and wire API.

#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/commands/mod.rs"]
mod commands;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/fixture.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/options.rs"]
mod options;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/terminal.rs"]
mod terminal;
#[cfg(not(target_arch = "wasm32"))]
use options::{HELP, Selection};
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native/mod.rs"]
mod native;
#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "filesystem-local",
    feature = "native-execution"
))]
#[path = "graphcoder/native_scenario/mod.rs"]
mod native_scenario;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native_tools.rs"]
mod native_tools;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/native_writeback/mod.rs"]
mod native_writeback;
#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
#[path = "graphcoder/provider.rs"]
mod provider;

#[cfg(target_arch = "wasm32")]
fn main() -> std::process::ExitCode {
    eprintln!("graphcoder requires a native terminal; use the public Harness WASM APIs");
    std::process::ExitCode::FAILURE
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        .and_then(|runtime| runtime.block_on(run()));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("graphcoder: {}", terminal::text(&error.to_string()));
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
enum Backend {
    Fixture(fixture::WireFixture),
    #[cfg(feature = "filesystem-local")]
    Native(std::sync::Arc<native::NativeSession>),
}

#[cfg(not(target_arch = "wasm32"))]
impl Backend {
    fn api(&self) -> &dyn acyclic_harness::wire_api::HarnessWireApi {
        match self {
            Self::Fixture(api) => api,
            #[cfg(feature = "filesystem-local")]
            Self::Native(api) => api.as_ref(),
        }
    }

    fn cursor(&self) -> acyclic_harness::wire::ReplayCursor {
        match self {
            Self::Fixture(api) => api.start_cursor(),
            #[cfg(feature = "filesystem-local")]
            Self::Native(api) => api.start_cursor(),
        }
    }

    #[cfg(feature = "filesystem-local")]
    fn native(&self) -> acyclic_harness::Result<&std::sync::Arc<native::NativeSession>> {
        match self {
            Self::Native(api) => Ok(api),
            Self::Fixture(_) => Err(acyclic_harness::Error::Unsupported(
                "operation requires a durable product host; fixture input is not queued".into(),
            )),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn input_lines()
-> acyclic_harness::Result<tokio::sync::mpsc::Receiver<acyclic_harness::Result<String>>> {
    use std::io::{BufRead as _, Read as _};
    let (send, receive) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new()
        .name("graphcoder-input".into())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut input = stdin.lock();
            loop {
                let mut line = String::new();
                let result = match (&mut input)
                    .take(terminal::MAX_FRAME_BYTES as u64 + 1)
                    .read_line(&mut line)
                {
                    Ok(0) => break,
                    Ok(_) if line.len() <= terminal::MAX_FRAME_BYTES => Ok(line),
                    Ok(_) => Err(acyclic_harness::Error::Invalid(
                        "terminal input exceeds bounds".into(),
                    )),
                    Err(error) => Err(acyclic_harness::Error::Storage(error.to_string())),
                };
                let failed = result.is_err();
                if send.blocking_send(result).is_err() || failed {
                    break;
                }
            }
        })
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    Ok(receive)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::{Error, wire, wire_api::current_protocol};
    use std::io::{IsTerminal as _, Write as _};
    let Some((selection, headless)) = options::parse()? else {
        return Ok(());
    };
    let backend = match selection {
        Selection::Fixture => Backend::Fixture(fixture::WireFixture::load()?),
        Selection::Native(path) => {
            #[cfg(feature = "filesystem-local")]
            {
                Backend::Native(std::sync::Arc::new(
                    native::NativeSession::open(native::Config::from_file(&path)?).await?,
                ))
            }
            #[cfg(not(feature = "filesystem-local"))]
            {
                let _ = path;
                return Err(Error::Unsupported(
                    "native session requires filesystem-local feature".into(),
                )
                .into());
            }
        }
        Selection::Scenario(path) => {
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            {
                let cancellation = acyclic_fs::CancellationToken::new();
                let preparation = native_scenario::prepare(&path, &cancellation);
                tokio::pin!(preparation);
                let manifest = tokio::select! {
                    result = &mut preparation => result?,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        cancellation.cancel();
                        preparation.await?
                    }
                };
                println!(
                    "scenario-json={}",
                    serde_json::json!({"manifest": manifest})
                );
                return Ok(());
            }
            #[cfg(not(all(feature = "filesystem-local", feature = "native-execution")))]
            {
                let _ = path;
                return Err(Error::Unsupported(
                    "native scenario requires filesystem-local,native-execution features".into(),
                )
                .into());
            }
        }
    };
    let handshake = wire::client_frame::Frame::Handshake(wire::HandshakeRequest {
        protocol: Some(current_protocol()),
        required: None,
    });
    let api = &backend;
    terminal::dispatch(api.api(), handshake).await?;
    let mut cursor = api.cursor();
    let fixture_mode = matches!(api, Backend::Fixture(_));
    let mut backend = Some(backend);
    let stdin = std::io::stdin();
    let stdin_is_terminal = stdin.is_terminal();
    let interactive = !headless && stdin_is_terminal;
    let mut input = input_lines()?;
    let mut output = std::io::stdout().lock();
    let mut scrollback = terminal::Scrollback::default();
    #[cfg(feature = "filesystem-local")]
    let mut parent = None;
    #[cfg(feature = "filesystem-local")]
    let mut workers = tokio::task::JoinSet::<acyclic_harness::Result<String>>::new();
    writeln!(
        output,
        "terminal-mode stdin-is-terminal={stdin_is_terminal} interactive={interactive} headless={headless} process-id={}",
        std::process::id()
    )?;
    writeln!(
        output,
        "{}",
        if fixture_mode {
            "GraphCoder — fixture wire; UI playback only, no durable effects"
        } else {
            "GraphCoder — actual local Harness runtime; deterministic request-digest provider; native commands require explicit approved bindings"
        }
    )?;
    loop {
        if interactive {
            write!(
                output,
                "{}> ",
                if fixture_mode {
                    "fixture"
                } else {
                    "graphcoder"
                }
            )?;
        }
        output.flush()?;
        #[cfg(feature = "filesystem-local")]
        let line = loop {
            tokio::select! {
                line = input.recv() => break line,
                outcome = workers.join_next(), if !workers.is_empty() => {
                    let rendered = outcome.ok_or_else(|| Error::Conflict("worker handle missing".into()))?
                        .map_err(|error| Error::Storage(format!("frontend worker ended without a receipt; retain its original lease: {error}")))??;
                    scrollback.push(rendered)?;
                    if let Some(page) = scrollback.page(0) { write!(output, "{page}")?; }
                    if interactive { write!(output, "graphcoder> ")?; }
                    output.flush()?;
                }
            }
        };
        #[cfg(not(feature = "filesystem-local"))]
        let line = input.recv().await;
        let Some(line) = line else {
            #[cfg(feature = "filesystem-local")]
            if !workers.is_empty() {
                writeln!(
                    output,
                    "input closed with active leases; outcomes remain uncertain until authoritative reconciliation"
                )?;
            }
            break;
        };
        let line = line?;
        let command = line.trim_end_matches(['\r', '\n']);
        if command == "/quit" {
            #[cfg(feature = "filesystem-local")]
            if !workers.is_empty() {
                writeln!(
                    output,
                    "closing presentation with active leases; outcomes remain uncertain until authoritative reconciliation"
                )?;
            }
            break;
        }
        if command == "/help" {
            write!(output, "{HELP}")?;
            continue;
        }
        if command.is_empty() {
            continue;
        }
        if let Some(back) = command.strip_prefix("/back ") {
            let result = back
                .parse::<usize>()
                .ok()
                .and_then(|back| scrollback.page(back));
            if let Some(page) = result {
                write!(output, "{page}")?;
            } else {
                writeln!(
                    output,
                    "display page unavailable; request an explicit archival page"
                )?;
            }
            continue;
        }
        let result = commands::execute(
            command,
            &mut backend,
            &mut cursor,
            #[cfg(feature = "filesystem-local")]
            &mut parent,
            #[cfg(feature = "filesystem-local")]
            &mut workers,
            &mut output,
        )
        .await;
        match result {
            Ok(rendered) => {
                scrollback.push(rendered)?;
                if let Some(page) = scrollback.page(0) {
                    write!(output, "{page}")?;
                }
                output.flush()?;
            }
            Err(error) if interactive => {
                writeln!(output, "{}", terminal::text(&error.to_string()))?
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn read_bytes(path: &std::path::Path) -> acyclic_harness::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .and_then(|file| {
            file.take(terminal::MAX_FRAME_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    if bytes.len() > terminal::MAX_FRAME_BYTES {
        return Err(acyclic_harness::Error::Invalid(
            "request exceeds terminal bounds".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "filesystem-local"))]
fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> acyclic_harness::Result<T> {
    serde_json::from_slice(&read_bytes(path)?)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))
}

#[cfg(not(target_arch = "wasm32"))]
fn read_request(path: &str) -> acyclic_harness::Result<acyclic_harness::wire::client_frame::Frame> {
    use prost::Message as _;
    acyclic_harness::wire::ClientFrame::decode(read_bytes(std::path::Path::new(path))?.as_slice())
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?
        .frame
        .ok_or_else(|| acyclic_harness::Error::Invalid("request frame is missing".into()))
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
