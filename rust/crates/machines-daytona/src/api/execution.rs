//! Native toolbox process ledger transport. Binary streams require the v2
//! toolbox API; the old session-marker protocol cannot represent arbitrary bytes.
use super::*;
use acyclic_machines::core::{ExecSession, bounded_sse, process_group_session};
use acyclic_machines::{ExecCommand, ExecOutput, ExecStream};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{StreamExt, stream};
use std::time::Duration;

#[derive(Deserialize)]
struct Process {
    id: String,
}
#[derive(Deserialize)]
struct Log {
    channel: String,
    data: String,
    encoding: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Terminal {
    reason: String,
    exit_code: Option<i32>,
}

impl DaytonaApi {
    /// Prepares streaming without dispatch. Cancellation ownership exists before
    /// the start request, including a response lost after provider acceptance.
    pub fn prepare_exec(&self, sandbox: &str, command: &ExecCommand, cwd: &str) -> ExecSession {
        let start_api = self.clone();
        let stop_api = self.clone();
        let start_sandbox = sandbox.to_owned();
        let stop_sandbox = sandbox.to_owned();
        let cwd = cwd.to_owned();
        let timeout = command.timeout_ms();
        process_group_session(
            command,
            move |argv| {
                Box::pin(async move {
                    start_api
                        .process_stream(&start_sandbox, argv, Some(cwd), timeout)
                        .await
                })
            },
            move |argv| {
                let api = stop_api.clone();
                let sandbox = stop_sandbox.clone();
                Box::pin(async move { api.process_stream(&sandbox, argv, None, 10_000).await })
            },
        )
    }

    async fn process_stream(
        &self,
        sandbox: &str,
        argv: Vec<String>,
        cwd: Option<String>,
        timeout: u32,
    ) -> Result<ExecStream, ProviderError> {
        let url = self.toolbox_url(sandbox, "processes").await?;
        let response = acyclic_machines::core::http::dispatch(self.authorized(self.http.post(&url))
            .timeout(Duration::from_secs(15))
            .json(&serde_json::json!({"kind": "exec", "argv": argv, "cwd": cwd, "timeoutMs": timeout, "keepLogs": "on_exit_ttl"}))
            ).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(ProviderError::Unsupported("Daytona streaming toolbox admission failed; recovery: register a snapshot with the documented v2 process ledger API".into()));
        }
        if !response.status().is_success() {
            return Err(status_error(
                response.status(),
                "process start failed; recovery: inspect the toolbox without redispatching",
            ));
        }
        let process: Process = response
            .json()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if process.id.is_empty()
            || !process
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            return Err(ProviderError::Unavailable);
        }
        let mut process_url = reqwest::Url::parse(&url).map_err(|_| ProviderError::Unavailable)?;
        process_url
            .path_segments_mut()
            .map_err(|()| ProviderError::Unavailable)?
            .push(&process.id);
        let mut logs = process_url.clone();
        logs.path_segments_mut()
            .map_err(|()| ProviderError::Unavailable)?
            .push("logs");
        let response = acyclic_machines::core::http::dispatch(
            self.authorized(self.http.get(logs))
                .query(&[("follow", "true"), ("encoding", "base64")])
                .timeout(Duration::from_millis(u64::from(timeout)) + Duration::from_secs(10)),
        )
        .await?;
        if !response.status().is_success() {
            return Err(status_error(
                response.status(),
                "process log attachment failed; recovery: inspect the toolbox without redispatching",
            ));
        }
        let bounded = bounded_sse(response.bytes_stream());
        let events = sse_stream::SseByteStream::new(bounded).boxed();
        let api = self.clone();
        let output = stream::try_unfold(
            (events, None::<Terminal>, false),
            move |(mut events, mut terminal, done)| {
                let api = api.clone();
                let process_url = process_url.clone();
                async move {
                    if done {
                        return Ok(None);
                    }
                    loop {
                        let event = events
                            .next()
                            .await
                            .ok_or(ProviderError::Unavailable)?
                            .map_err(|_| ProviderError::Unavailable)?;
                        let Some(data) = event.data.filter(|value| !value.is_empty()) else {
                            continue;
                        };
                        match event.event.as_deref() {
                            Some("log") => {
                                let frame: Log = serde_json::from_str(&data)
                                    .map_err(|_| ProviderError::Unavailable)?;
                                if frame
                                    .encoding
                                    .as_deref()
                                    .is_some_and(|value| value != "base64")
                                {
                                    return Err(ProviderError::Unavailable);
                                }
                                let bytes = URL_SAFE_NO_PAD
                                    .decode(frame.data)
                                    .map_err(|_| ProviderError::Unavailable)?;
                                if bytes.is_empty() {
                                    continue;
                                }
                                let output = match frame.channel.as_str() {
                                    "stdout" => ExecOutput::Stdout(bytes),
                                    "stderr" => ExecOutput::Stderr(bytes),
                                    _ => return Err(ProviderError::Unavailable),
                                };
                                return Ok(Some((output, (events, terminal, false))));
                            }
                            Some("state") if terminal.is_none() => {
                                terminal = Some(
                                    serde_json::from_str(&data)
                                        .map_err(|_| ProviderError::Unavailable)?,
                                )
                            }
                            Some("eof") => {
                                let terminal = terminal.take().ok_or(ProviderError::Unavailable)?;
                                let code = match (terminal.reason.as_str(), terminal.exit_code) {
                                    ("exited", Some(code)) => code,
                                    ("timed_out", _) => return Err(ProviderError::Cancelled),
                                    _ => return Err(ProviderError::Unavailable),
                                };
                                api.empty(
                                    "delete_process",
                                    api.authorized(api.http.delete(process_url))
                                        .timeout(Duration::from_secs(10)),
                                )
                                .await?;
                                return Ok(Some((
                                    ExecOutput::ExitCode(code),
                                    (events, None, true),
                                )));
                            }
                            // A retention warning means output was lost. It cannot be
                            // silently accepted as an exact shell transcript.
                            _ => return Err(ProviderError::Unavailable),
                        }
                    }
                }
            },
        )
        .boxed();
        Ok(output)
    }
}
