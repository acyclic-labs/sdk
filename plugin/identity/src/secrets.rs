//! CLI input only. The shared auth client owns the contract and all HTTP logic.
use super::{Error, Value, private_file};
use acyclic_auth::{
    contract::*,
    credential::CredentialProvider,
    secrets::{Client, HttpTransport},
};
use std::{path::PathBuf, sync::Arc};
use zeroize::{Zeroize as _, Zeroizing};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Put,
    List,
    Delete,
}

pub(super) struct Options {
    operation: Operation,
    endpoint: String,
    bearer_file: PathBuf,
    scope: String,
    name: Option<String>,
    kind: Option<SecretKind>,
    generation: Option<u64>,
    value_file: Option<PathBuf>,
    cursor: Option<SecretListCursor>,
    page_size: u32,
}

pub(super) fn usage() -> Error {
    Error::new(
        "usage_error",
        "secrets argument admission failed; recovery: acyclic secrets --help",
    )
}

impl Options {
    pub(super) fn from_command(command: super::cli::SecretCommand) -> Result<Self, Error> {
        use super::cli::{Kind, SecretCommand};
        let (operation, arguments, name, kind, generation, value_file, cursor, page_size) =
            match command {
                SecretCommand::Put(arguments) => (
                    Operation::Put,
                    arguments.mutation.scope,
                    Some(arguments.mutation.name),
                    Some(match arguments.kind {
                        Kind::Plain => SecretKind::Plain,
                        Kind::Protected => SecretKind::Protected,
                    }),
                    Some(arguments.mutation.expected_generation),
                    arguments.value_file,
                    None,
                    0,
                ),
                SecretCommand::Delete(arguments) => (
                    Operation::Delete,
                    arguments.scope,
                    Some(arguments.name),
                    None,
                    Some(arguments.expected_generation),
                    None,
                    None,
                    0,
                ),
                SecretCommand::List(arguments) => (
                    Operation::List,
                    arguments.scope,
                    None,
                    None,
                    None,
                    None,
                    arguments
                        .cursor_generation
                        .zip(arguments.after)
                        .map(|(generation, after)| SecretListCursor { generation, after }),
                    arguments.page_size,
                ),
            };
        if arguments.scope.is_empty() || name.as_ref().is_some_and(String::is_empty) {
            return Err(usage());
        }
        Ok(Self {
            operation,
            endpoint: arguments
                .endpoint
                .or_else(|| std::env::var("ACYCLIC_API_URL").ok())
                .ok_or_else(usage)?,
            bearer_file: arguments.bearer_file,
            scope: arguments.scope,
            name,
            kind,
            generation,
            value_file,
            cursor,
            page_size,
        })
    }
}

fn input_error() -> Error {
    Error::new(
        "secrets_input_error",
        "secrets: cannot read UTF-8 input; provide --bearer-file with an owner-only bearer and put values on stdin or in --value-file",
    )
}

pub(super) async fn execute(
    options: Options,
    context: Option<acyclic_auth::operation::Context>,
) -> Result<Value, Error> {
    let context = context
        .ok_or_else(|| client_error(acyclic_auth::secrets::Error::MissingOperationContext))?;
    let bearer_file = options.bearer_file;
    let bytes = context
        .run(async move {
            acyclic_native_runtime::run_blocking_io(
                move || {
                    private_file::read(
                        &bearer_file,
                        acyclic_auth::credential::MAX_CREDENTIAL_BYTES as u64,
                    )
                },
                None,
            )
            .map_err(|_| input_error())?
            .await
            .map_err(|_| input_error())?
            .map_err(|_| input_error())
        })
        .await
        .map_err(|stopped| client_error(acyclic_auth::secrets::Error::Stopped(stopped)))??;
    let token = std::str::from_utf8(&bytes.0)
        .map_err(|_| input_error())?
        .trim_end_matches(['\r', '\n']);
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(input_error());
    }
    let token = Arc::new(Zeroizing::new(token.to_owned()));
    let transport = HttpTransport::new(&options.endpoint).map_err(client_error)?;
    let client = Client::new(
        transport,
        CredentialProvider::new(move || {
            let token = token.clone();
            async move { Ok(token.to_string()) }
        }),
        Some(context.clone()),
    );
    match options.operation {
        Operation::List => {
            let result = client
                .list(&SecretListRequest {
                    scope: options.scope,
                    cursor: options.cursor,
                    page_size: options.page_size,
                })
                .await
                .map_err(client_error)?;
            serde_json::to_value(result)
                .map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
        Operation::Put => {
            let mut value = context
                .run(async {
                    let value = match options.value_file {
                        Some(file) => acyclic_native_runtime::run_blocking_io(
                            move || read_value_file(&file),
                            None,
                        )
                        .map_err(|_| input_error())?
                        .await
                        .map_err(|_| input_error())??,
                        None => read_stdin_value().await?,
                    };
                    Ok::<_, Error>(value)
                })
                .await
                .map_err(|stopped| {
                    client_error(acyclic_auth::secrets::Error::Stopped(stopped))
                })??;
            let request = Zeroizing::new(SecretPutRequest {
                scope: options.scope,
                name: options.name.ok_or_else(usage)?,
                kind: options.kind.ok_or_else(usage)?,
                value: std::mem::take(&mut *value),
                expected_generation: options.generation.ok_or_else(usage)?,
            });
            let result = client.put(&request).await;
            serde_json::to_value(result.map_err(client_error)?)
                .map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
        Operation::Delete => {
            let result = client
                .delete(&SecretDeleteRequest {
                    scope: options.scope,
                    name: options.name.ok_or_else(usage)?,
                    expected_generation: options.generation.ok_or_else(usage)?,
                })
                .await
                .map_err(client_error)?;
            serde_json::to_value(result)
                .map_err(|_| client_error(acyclic_auth::secrets::Error::InvalidResponse))
        }
    }
}

async fn read_stdin_value() -> Result<Zeroizing<String>, Error> {
    use tokio::io::AsyncReadExt as _;
    let mut input = acyclic_native_runtime::stdin().map_err(|_| input_error())?;
    let mut value = Zeroizing::new(Vec::new());
    let mut chunk = Zeroizing::new([0; 8192]);
    loop {
        let read = input.read(&mut *chunk).await.map_err(|_| input_error())?;
        if read == 0 {
            break;
        }
        value.extend_from_slice(&chunk[..read]);
        chunk[..read].zeroize();
    }
    match String::from_utf8(std::mem::take(&mut *value)) {
        Ok(value) => Ok(Zeroizing::new(value)),
        Err(error) => {
            let _bytes = Zeroizing::new(error.into_bytes());
            Err(input_error())
        }
    }
}

fn read_value_file(path: &std::path::Path) -> Result<Zeroizing<String>, Error> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        // Reject an open FIFO without waiting for its writer.
        options.custom_flags(libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    if !std::fs::metadata(path)
        .map_err(|_| input_error())?
        .is_file()
    {
        return Err(input_error());
    }
    let mut file = options.open(path).map_err(|_| input_error())?;
    if !file.metadata().map_err(|_| input_error())?.is_file() {
        return Err(input_error());
    }
    read_value(&mut file)
}

fn read_value(input: &mut impl std::io::Read) -> Result<Zeroizing<String>, Error> {
    let mut value = Zeroizing::new(String::new());
    input
        .read_to_string(&mut value)
        .map_err(|_| input_error())?;
    Ok(value)
}

fn client_error(error: acyclic_auth::secrets::Error) -> Error {
    Error::new(error.code(), error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_stdin_and_file_reader_preserve_exact_bytes_and_erase_io_diagnostics()
    -> Result<(), Box<dyn std::error::Error>> {
        let planted = "PLANTED_SECRET_DO_NOT_LOG\n";
        let value = read_value(&mut std::io::Cursor::new(planted.as_bytes()))?;
        assert_eq!(value.as_str(), planted);
        struct Broken;
        impl std::io::Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("PLANTED_SECRET_DO_NOT_LOG"))
            }
        }
        let Err(error) = read_value(&mut Broken) else {
            return Err("failed source admitted".into());
        };
        assert!(!format!("{error:?} {error}").contains("PLANTED_SECRET_DO_NOT_LOG"));
        Ok(())
    }

    #[tokio::test]
    async fn secrets_cli_value_file_uses_private_bearer_and_shared_http_with_redacted_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        const OPEN_INPUT: &str = "ACYCLIC_SECRETS_OPEN_INPUT_CONTROL";
        if let Some(mode) = std::env::var_os(OPEN_INPUT) {
            if mode == "value" {
                assert_eq!(*read_stdin_value().await?, "λ\n".repeat(30_000));
                return Ok(());
            }
            if mode == "invalid" {
                let Err(error) = read_stdin_value().await else {
                    return Err("invalid UTF-8 input admitted".into());
                };
                assert_eq!(error.code, "secrets_input_error");
                return Ok(());
            }
            let input = read_stdin_value();
            tokio::pin!(input);
            tokio::select! {
                biased;
                _ = &mut input => return Err("open empty stdin completed before EOF".into()),
                completion = acyclic_native_runtime::run_blocking_io(|| 7, None)? => {
                    assert_eq!(completion?, 7);
                }
            }
            // Cancel the EOF wait while the original parent still owns its writer.
            return Ok(());
        }
        for mode in ["open", "value", "invalid"] {
            let executable = std::env::current_exe()?;
            let output = acyclic_native_runtime::run_blocking_io(
            move || {
                let mut child = std::process::Command::new(executable)
                    .args([
                        "--exact",
                        "secrets::tests::secrets_cli_value_file_uses_private_bearer_and_shared_http_with_redacted_failures",
                        "--nocapture",
                    ])
                    .env(OPEN_INPUT, mode)
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()?;
                let mut writer = child.stdin.take().ok_or(std::io::ErrorKind::BrokenPipe)?;
                if mode != "open" {
                    use std::io::Write as _;
                    match mode {
                        "value" => writer.write_all("λ\n".repeat(30_000).as_bytes())?,
                        _ => writer.write_all(&[0xff])?,
                    }
                    drop(writer);
                    return child.wait_with_output();
                }
                let result = child.wait_with_output();
                drop(writer);
                result
            },
            None,
        )?
        .await??;
            assert!(
                output.status.success(),
                "{mode} input child failed: {output:?}"
            );
        }
        use axum::{
            Router,
            body::Bytes,
            http::{HeaderMap, StatusCode},
            routing::post,
        };
        use std::io::Write as _;
        let directory = tempfile::tempdir()?;
        let bearer = directory.path().join("bearer");
        let value = directory.path().join("value");
        let planted = "PLANTED_SECRET_DO_NOT_LOG";
        private_file::create(&bearer)?.write_all(b"fixture-bearer\n")?;
        std::fs::write(&value, planted)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let router = Router::new().route(
            "/v1/identity/secrets/put",
            post(|headers: HeaderMap, body: Bytes| async move {
                assert_eq!(
                    headers.get("authorization"),
                    Some(&axum::http::HeaderValue::from_static(
                        "Bearer fixture-bearer"
                    ))
                );
                let request = serde_json::from_slice::<SecretPutRequest>(&body);
                if request.is_ok_and(|request| {
                    request.value == "PLANTED_SECRET_DO_NOT_LOG" && request.expected_generation == 7
                }) {
                    (StatusCode::CONFLICT, "PLANTED_SECRET_DO_NOT_LOG")
                } else {
                    (StatusCode::BAD_REQUEST, "PLANTED_SECRET_DO_NOT_LOG")
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let args = vec![
            "put".into(),
            "--endpoint".into(),
            endpoint,
            "--scope".into(),
            "fixture".into(),
            "--name".into(),
            "key".into(),
            "--kind".into(),
            "protected".into(),
            "--expected-generation".into(),
            "7".into(),
            "--bearer-file".into(),
            bearer.display().to_string(),
            "--value-file".into(),
            value.display().to_string(),
        ];
        assert!(args.iter().all(|arg| !arg.contains(planted)));
        let (stop, stopped) = futures::channel::oneshot::channel::<()>();
        let context = acyclic_auth::operation::Context::from_stopped(0, async move {
            let _ = stopped.await;
            acyclic_auth::operation::Stopped::Cancelled
        });
        let args = std::iter::once("secrets".to_owned())
            .chain(args)
            .collect::<Vec<_>>();
        let Err(error) = super::super::run(&args, Some(context)).await else {
            return Err("CAS conflict admitted".into());
        };
        drop(stop);
        assert!(error.to_string().contains("generation conflict"));
        assert!(!format!("{error:?} {error}").contains(planted));
        server.abort();
        Ok(())
    }
}
