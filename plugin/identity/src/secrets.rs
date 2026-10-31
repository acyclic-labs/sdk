//! CLI input only. The shared auth client owns the contract and all HTTP logic.
use super::{Error, Value, private_file};
use acyclic_auth::{
    contract::*,
    credential::CredentialProvider,
    secrets::{Client, HttpTransport},
};
use std::{collections::HashSet, path::PathBuf, sync::Arc};
use zeroize::{Zeroize as _, Zeroizing};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Put,
    List,
    Delete,
}

struct Options {
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

fn usage() -> Error {
    Error::new(
        "usage_error",
        "secrets: use put|list|delete --scope S --bearer-file FILE; mutations require --name N --expected-generation G; put requires --kind plain|protected and reads stdin or --value-file FILE; list takes --page-size N and continues a listing with --cursor-generation G --after NAME from its nextCursor",
    )
}

impl Options {
    fn parse(arguments: &[String]) -> Result<Self, Error> {
        let mut arguments = arguments.iter();
        let operation = match arguments.next().map(String::as_str) {
            Some("put") => Operation::Put,
            Some("list") => Operation::List,
            Some("delete") => Operation::Delete,
            _ => return Err(usage()),
        };
        let mut endpoint = std::env::var("ACYCLIC_API_URL").ok();
        let (mut bearer_file, mut scope, mut name, mut kind, mut generation, mut value_file) =
            (None, None, None, None, None, None);
        let (mut cursor_generation, mut after, mut page_size) = (None, None, 0);
        let mut seen = HashSet::new();
        while let Some(flag) = arguments.next() {
            if !seen.insert(flag) {
                return Err(usage());
            }
            if flag == "--json" {
                continue;
            }
            if !matches!(
                flag.as_str(),
                "--endpoint"
                    | "--bearer-file"
                    | "--scope"
                    | "--name"
                    | "--kind"
                    | "--expected-generation"
                    | "--value-file"
                    | "--cursor-generation"
                    | "--after"
                    | "--page-size"
            ) {
                return Err(usage());
            }
            let value = arguments.next().ok_or_else(usage)?;
            match flag.as_str() {
                "--endpoint" => endpoint = Some(value.clone()),
                "--bearer-file" => bearer_file = Some(PathBuf::from(value)),
                "--scope" => scope = Some(value.clone()),
                "--name" if operation != Operation::List => name = Some(value.clone()),
                "--kind" if operation == Operation::Put => {
                    kind = Some(match value.as_str() {
                        "plain" => SecretKind::Plain,
                        "protected" => SecretKind::Protected,
                        _ => return Err(usage()),
                    });
                }
                "--expected-generation" if operation != Operation::List => {
                    generation = Some(value.parse().map_err(|_| usage())?);
                }
                "--value-file" if operation == Operation::Put => {
                    value_file = Some(PathBuf::from(value));
                }
                "--cursor-generation" if operation == Operation::List => {
                    cursor_generation = Some(value.parse().map_err(|_| usage())?);
                }
                "--after" if operation == Operation::List => after = Some(value.clone()),
                "--page-size" if operation == Operation::List => {
                    page_size = value.parse().map_err(|_| usage())?;
                }
                _ => return Err(usage()),
            }
        }
        let scope = scope.filter(|scope| !scope.is_empty()).ok_or_else(usage)?;
        if operation != Operation::List
            && (name.as_ref().is_none_or(String::is_empty) || generation.is_none())
            || operation == Operation::Put && kind.is_none()
        {
            return Err(usage());
        }
        let cursor = match (cursor_generation, after) {
            (Some(generation), Some(after)) => Some(SecretListCursor { generation, after }),
            (None, None) => None,
            _ => return Err(usage()),
        };
        Ok(Self {
            operation,
            endpoint: endpoint.ok_or_else(usage)?,
            bearer_file: bearer_file.ok_or_else(usage)?,
            scope,
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

pub(super) async fn execute(arguments: &[String]) -> Result<Value, Error> {
    let options = Options::parse(arguments)?;
    let transport = HttpTransport::new(&options.endpoint).map_err(client_error)?;
    let bearer_file = options.bearer_file;
    let bytes = acyclic_native_runtime::run_blocking_io(
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
    .map_err(|_| input_error())?;
    let token = std::str::from_utf8(&bytes.0)
        .map_err(|_| input_error())?
        .trim_end_matches(['\r', '\n']);
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(input_error());
    }
    let token = Arc::new(Zeroizing::new(token.to_owned()));
    let client = Client::new(
        transport,
        CredentialProvider::new(move || {
            let token = token.clone();
            async move { Ok(token.to_string()) }
        }),
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
            let mut value = match options.value_file {
                Some(file) => {
                    acyclic_native_runtime::run_blocking_io(move || read_value_file(&file), None)
                        .map_err(|_| input_error())?
                        .await
                        .map_err(|_| input_error())??
                }
                None => read_stdin_value().await?,
            };
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
    Error::new("secrets_error", error.to_string())
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
        if std::env::var_os(OPEN_INPUT).is_some() {
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
        let executable = std::env::current_exe()?;
        let output = acyclic_native_runtime::run_blocking_io(
            move || {
                let mut child = std::process::Command::new(executable)
                    .args([
                        "--exact",
                        "secrets::tests::secrets_cli_value_file_uses_private_bearer_and_shared_http_with_redacted_failures",
                        "--nocapture",
                    ])
                    .env(OPEN_INPUT, "1")
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()?;
                let writer = child.stdin.take().ok_or(std::io::ErrorKind::BrokenPipe)?;
                let result = child.wait_with_output();
                drop(writer);
                result
            },
            None,
        )?
        .await??;
        assert!(
            output.status.success(),
            "open-input child failed: {output:?}"
        );
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
        let Err(error) = execute(&args).await else {
            return Err("CAS conflict admitted".into());
        };
        assert!(error.to_string().contains("generation conflict"));
        assert!(!format!("{error:?} {error} {}", error.json()).contains(planted));
        server.abort();
        Ok(())
    }

    #[test]
    fn secrets_cli_rejects_argv_values_account_selection_and_unknown_operations_without_echoing()
    -> Result<(), Box<dyn std::error::Error>> {
        let planted = "PLANTED_SECRET_DO_NOT_LOG";
        for args in [
            vec!["put", "--value", planted],
            vec!["put", "--account", planted],
            vec!["get", planted],
            vec!["reveal", planted],
            vec!["export", planted],
            vec!["put", "--kind", planted],
            vec!["put", "--expected-generation", planted],
            vec!["list", "--after", planted],
            vec!["list", "--page-size", planted],
            vec!["put", "--after", planted],
        ] {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let Err(error) = Options::parse(&args) else {
                return Err("forbidden CLI shape admitted".into());
            };
            let diagnostic = format!("{error:?} {error} {}", error.json());
            assert!(
                !diagnostic.contains(planted),
                "CLI diagnostic exposed the planted value"
            );
            assert!(
                format!("unexpected argument: {planted}").contains(planted),
                "negative control must detect the former echo path"
            );
        }
        Ok(())
    }
}
