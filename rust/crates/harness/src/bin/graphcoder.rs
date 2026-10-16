//! Native `GraphCoder` presentation over the public Harness runtime and wire API.

#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/command_line.rs"]
mod command_line;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/markdown.rs"]
pub mod markdown;

#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/commands/mod.rs"]
mod commands;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/fixture.rs"]
mod fixture;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/options.rs"]
mod options;
#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "filesystem-local",
    feature = "tui"
))]
#[path = "graphcoder/plans.rs"]
mod plans;
#[cfg(not(target_arch = "wasm32"))]
#[path = "graphcoder/terminal.rs"]
mod terminal;
#[cfg(not(target_arch = "wasm32"))]
use options::Selection;
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
    use acyclic_harness::Error;
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
    command_line::run(backend, headless)
        .await
        .map_err(Into::into)
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
