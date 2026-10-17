//! Native process entry point; task state stays in provider-backed files.
use super::{ExternalHost, volume};
use crate::{
    Error, Result,
    task_volume::{Component, TaskVolume},
};
use acyclic_stream::{StreamPath, http::HttpStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::Read as _,
    path::{Path, PathBuf},
    sync::Arc,
};

mod binding;
pub use binding::HookBinding;

#[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
mod task_volumes;
#[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
pub use task_volumes::local_hook_volumes;

mod handler;
pub use handler::handle_hook;
pub(in crate::external_executor) mod files;
#[cfg(feature = "test-support")]
pub(crate) use files::fuzz as fuzz_file_input;
pub use files::{native_input_files, run_native_files};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use handler::prepare as prepare_hook;
mod codex_parent;
#[cfg(feature = "test-support")]
pub(crate) use codex_parent::fuzz as fuzz_codex_parent;

/// Reads one native event and uses only the platform Streams provider.
pub async fn run_hook(host: &str, event: &str) -> Result<Value> {
    let host: ExternalHost = serde_json::from_value(Value::String(host.into())).map_err(storage)?;
    let mut input: Value = volume::decode(&read_bounded(
        std::io::stdin().lock(),
        acyclic_stream::MAX_RECORD_BYTES,
    )?)
    .map_err(storage)?;
    if host == ExternalHost::Codex {
        codex_parent::prepare(event, &mut input)?;
    }
    let cwd = match input.get("cwd").and_then(Value::as_str) {
        Some(path) => PathBuf::from(path),
        None => std::env::current_dir().map_err(storage)?,
    };
    let (mut request, mcp) = handler::prepare(host, event, input)?;
    let (answer, task, workspace) = run_request(&mut request, &cwd).await?;
    let mut answer = handler::answer(host, event, request, answer, task, mcp)?;
    if mcp && let Some(workspace) = workspace {
        super::handoff::qualify(&mut answer, &workspace)?;
    }
    Ok(answer)
}

pub(super) fn run_request<'a>(
    request: &'a mut super::ExternalRequest,
    cwd: &'a std::path::Path,
) -> impl Future<Output = Result<(Value, Component, Option<String>)>> + 'a {
    Box::pin(async move {
        match binding::configured()? {
            None => local_request(request, cwd, None).await,
            Some(HookBinding::Local { workspace, task }) => {
                local_request(request, &workspace, task).await
            }
            Some(HookBinding::Platform {
                endpoint,
                bearer_file,
                requests,
                results,
                task,
            }) => {
                let (requests, results) =
                    platform_volumes(&endpoint, &bearer_file, requests, results).await?;
                Ok((
                    request
                        .send(&requests, &results, task.clone())
                        .await
                        .map_err(storage)?,
                    task,
                    None,
                ))
            }
        }
    })
}

pub(super) fn read_tool_result(
    name: &str,
    arguments: Value,
) -> impl Future<Output = Result<Value>> + '_ {
    Box::pin(async move {
        match binding::configured()? {
            None => local_result(name, arguments, None, None).await,
            Some(HookBinding::Local { workspace, task }) => {
                local_result(name, arguments, Some(&workspace), task).await
            }
            Some(HookBinding::Platform {
                endpoint,
                bearer_file,
                requests,
                results,
                task,
            }) => {
                if super::handoff::reference(&arguments)?.0.is_some() {
                    return Err(Error::Unauthorized(
                        "local MCP reference is outside the platform binding".into(),
                    ));
                }
                let key = super::handoff::file_key(&arguments)?;
                if !matches!(&key,crate::task_volume::FileKey::Task {task:bound,..} if bound == &task)
                {
                    return Err(Error::Unauthorized(
                        "native result belongs to another bound task".into(),
                    ));
                }
                let (requests, results) =
                    platform_volumes(&endpoint, &bearer_file, requests, results).await?;
                super::handoff::read_result(name, arguments, &requests, &results, None).await
            }
        }
    })
}

async fn local_result(
    name: &str,
    arguments: Value,
    selected: Option<&Path>,
    bound: Option<crate::TaskId>,
) -> Result<Value> {
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    {
        let key = super::handoff::file_key(&arguments)?;
        let crate::task_volume::FileKey::Task { task, .. } = key else {
            return Err(Error::Invalid("native result is not a task file".into()));
        };
        let id = uuid::Uuid::parse_str(task.as_str()).map_err(storage)?;
        let id = binding::require_task(crate::TaskId::from_bytes(id.into_bytes()), bound)?;
        let workspace = super::handoff::reference(&arguments)?.0.ok_or_else(|| {
            Error::Unauthorized("local MCP result has no workspace binding".into())
        })?;
        let (client, workspace) = super::local_owner::existing_binding(workspace, selected).await?;
        let (requests, results) = local_hook_volumes(Arc::new(client), id).await?;
        super::handoff::read_result(name, arguments, &requests, &results, Some(&workspace)).await
    }
    #[cfg(not(all(feature = "filesystem-local", feature = "native-execution")))]
    {
        let _ = (name, arguments, selected, bound);
        Err(Error::Unsupported(
            "local hooks require filesystem-local and native-execution".into(),
        ))
    }
}

async fn platform_volumes(
    endpoint: &str,
    bearer_file: &Path,
    requests: StreamPath,
    results: StreamPath,
) -> Result<(TaskVolume<HttpStream>, TaskVolume<HttpStream>)> {
    let provider = platform_provider(endpoint, bearer_file).await?;
    let requests = TaskVolume::open(provider.clone(), requests)
        .await
        .map_err(storage)?;
    let results = TaskVolume::open(provider, results).await.map_err(storage)?;
    Ok((requests, results))
}

async fn platform_provider(endpoint: &str, bearer_file: &Path) -> Result<Arc<HttpStream>> {
    let token = read_bounded(
        std::fs::File::open(bearer_file).map_err(storage)?,
        acyclic_stream::MAX_BEARER_TOKEN_BYTES,
    )?;
    let token = std::str::from_utf8(&token).map_err(storage)?.trim();
    Ok(Arc::new(
        HttpStream::new(endpoint, token, acyclic_stream::MAX_RECORD_BYTES * 4).map_err(storage)?,
    ))
}

async fn local_request(
    request: &mut super::ExternalRequest,
    workspace: &Path,
    bound: Option<crate::TaskId>,
) -> Result<(Value, Component, Option<String>)> {
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    {
        let task_id = binding::require_task(request.task_id(), bound)?;
        let (client, workspace, canonical) = super::local_owner::binding(workspace).await?;
        binding::select_workspace(request, &canonical)?;
        let (requests, results) = local_hook_volumes(Arc::new(client), task_id).await?;
        let task = Component::new(task_id.to_string()).map_err(storage)?;
        Ok((
            request
                .send(&requests, &results, task.clone())
                .await
                .map_err(storage)?,
            task,
            Some(workspace),
        ))
    }
    #[cfg(not(all(feature = "filesystem-local", feature = "native-execution")))]
    {
        let _ = (request, workspace, bound);
        Err(Error::Unsupported(
            "local hooks require filesystem-local and native-execution".into(),
        ))
    }
}

pub(super) fn read_bounded(reader: impl std::io::Read, maximum: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() > maximum {
        return Err(Error::Invalid("hook input exceeds its record bound".into()));
    }
    Ok(bytes)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_hook_and_request_futures_stay_below_sixteen_kib() -> Result<()> {
        let mut request = super::super::request(
            ExternalHost::Pi,
            "session_start",
            serde_json::json!({"sessionId":"size","cwd":"project"}),
        )?;
        let hook = run_hook("pi", "session_start");
        let call = run_request(&mut request, std::path::Path::new("project"));
        let result = read_tool_result("bash", Value::Null);
        eprintln!(
            "native hook/request futures: {}/{} bytes",
            std::mem::size_of_val(&hook),
            std::mem::size_of_val(&call)
        );
        assert!(std::mem::size_of_val(&hook) < 16 * 1024);
        assert!(std::mem::size_of_val(&call) < 16 * 1024);
        assert!(std::mem::size_of_val(&result) < 16 * 1024);
        Ok(())
    }
}
