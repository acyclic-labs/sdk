//! Immutable native inputs use the ordinary file codec and provider query.
use super::*;
use crate::{
    TaskId,
    client::jsonl,
    task_volume::{FileKey, FileValue, Query},
};
use acyclic_stream::StreamProvider;
use futures::{Stream, StreamExt as _};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
};

/// Opens only the caller-writable task inputs, retaining the admitted binding.
/// Hooks use the same provider owner as ordinary requests, never LocalHarness.
pub async fn native_input_files(task: TaskId) -> Result<TaskVolume<dyn StreamProvider>> {
    let (provider, path): (Arc<dyn StreamProvider>, StreamPath) = match binding::configured()? {
        Some(HookBinding::Platform {
            endpoint,
            bearer_file,
            requests,
            task: bound,
            ..
        }) => {
            if bound.as_str() != task.to_string() {
                return Err(Error::Unauthorized(
                    "native input belongs to another task".into(),
                ));
            }
            (platform_provider(&endpoint, &bearer_file).await?, requests)
        }
        configured => {
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            {
                let (workspace, bound) = match configured {
                    Some(HookBinding::Local { workspace, task }) => (workspace, task),
                    None => (std::env::current_dir().map_err(storage)?, None),
                    _ => unreachable!(),
                };
                let task = binding::require_task(task, bound)?;
                let (provider, _, _) = super::super::local_owner::binding(&workspace).await?;
                (
                    Arc::new(provider),
                    super::super::local_paths::path(super::super::local_paths::REQUESTS, task),
                )
            }
            #[cfg(not(all(feature = "filesystem-local", feature = "native-execution")))]
            {
                let _ = configured;
                return Err(Error::Unsupported(
                    "local files require filesystem-local and native-execution".into(),
                ));
            }
        }
    };
    TaskVolume::open(provider, path).await.map_err(storage)
}

fn validate(task: TaskId, file: &FileValue) -> Result<()> {
    if file.requester().is_some() || file.channel.is_some() {
        return Err(Error::Unauthorized(
            "native input cannot claim an execution owner".into(),
        ));
    }
    if let FileKey::Task { task: selected, .. } = &file.key {
        if selected.as_str() != task.to_string() {
            return Err(Error::Unauthorized(
                "native input belongs to another task".into(),
            ));
        }
    }
    crate::task_volume::authorize_write(
        &file.key,
        file.bytes.len() as u64,
        acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .map_err(storage)
}

pub(super) fn parse(task: TaskId, line: &str) -> Result<FileValue> {
    let file = jsonl::parse_request(line.strip_suffix('\n').unwrap_or(line))?;
    validate(task, &file)?;
    Ok(file)
}

async fn line<R: AsyncBufRead + Unpin>(input: &mut R, bytes: &mut Vec<u8>) -> Result<bool> {
    // The codec bounds the JSON bytes; the optional line terminator is framing.
    let framed = jsonl::MAXIMUM_REQUEST_BYTES + 1;
    let remaining = framed.saturating_sub(bytes.len());
    (&mut *input)
        .take(remaining as u64 + 1)
        .read_until(b'\n', bytes)
        .await
        .map_err(storage)?;
    let payload = bytes.len() - usize::from(bytes.last() == Some(&b'\n'));
    if payload > jsonl::MAXIMUM_REQUEST_BYTES {
        return Err(Error::Invalid("stdin line exceeds byte bound".into()));
    }
    Ok(!bytes.is_empty())
}

async fn emit(output: &mut (impl AsyncWrite + Unpin), mut value: String) -> Result<()> {
    value.push('\n');
    output.write_all(value.as_bytes()).await.map_err(storage)?;
    output.flush().await.map_err(storage)
}

pub(in crate::external_executor) async fn drain(
    through: &mut u64,
    accepted: u64,
    changes: &mut (impl Stream<Item = crate::task_volume::Result<crate::task_volume::Change>> + Unpin),
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    while *through < accepted {
        let change = changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("native file follow ended".into()))?
            .map_err(storage)?;
        emit(output, jsonl::change(&change)?).await?;
        *through = change.cursor.position;
    }
    Ok(())
}

pub(super) async fn serve<P: StreamProvider + ?Sized>(
    files: &TaskVolume<P>,
    task: TaskId,
    key: FileKey,
    input: &mut (impl AsyncBufRead + Unpin),
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    // Exact-file queries have cardinality at most one, independent of history.
    let query = Query::File(key);
    let mut observed = files
        .observe(query.clone(), None, 1)
        .await
        .map_err(storage)?;
    let snapshot = observed
        .snapshot
        .take()
        .ok_or_else(|| Error::Storage("initial file snapshot missing".into()))?;
    let mut through = snapshot.cursor.position;
    emit(
        output,
        jsonl::snapshot(&query, &snapshot.into_heads().map_err(storage)?)?,
    )
    .await?;
    let mut bytes = Vec::new();
    loop {
        tokio::select! {
            incoming = line(input, &mut bytes) => {
                if !incoming? { return Ok(()); }
                let file = parse(task, std::str::from_utf8(&bytes).map_err(storage)?)?;
                super::super::append::publish(files, &file, None, None).await.map_err(storage)?;
                bytes.clear();
                let accepted = files.head().await.map_err(storage)?.position;
                // Push each accepted publication before accepting the next file.
                // An identical retry is already present in snapshot plus follow.
                drain(&mut through, accepted, &mut observed.changes, output).await?;
            }
            change = observed.changes.next() => {
                let change = change.ok_or_else(|| Error::Storage("native file follow ended".into()))?.map_err(storage)?;
                through = change.cursor.position;
                emit(output, jsonl::change(&change)?).await?;
            }
        }
    }
}

#[cfg(feature = "test-support")]
pub(crate) fn fuzz(bytes: &[u8]) {
    futures::executor::block_on(async {
        let mut source = bytes;
        let mut buffer = Vec::new();
        let task = TaskId::from_bytes([0; 16]);
        while line(&mut source, &mut buffer).await.unwrap_or(false) {
            if let Ok(line) = std::str::from_utf8(&buffer) {
                if let Ok(file) = parse(task, line) {
                    assert_eq!(parse(task, &jsonl::request(&file).unwrap()).unwrap(), file);
                }
            }
            buffer.clear();
        }
    });
}

/// Publishes immutable FileValue JSONL and emits the existing snapshot/follow
/// dialect for one logical path. One provider connection serves the whole input.
pub async fn run_native_files(task: &str, path: &str) -> Result<()> {
    let task = TaskId::parse(task)?;
    let key = FileKey::parse(path).map_err(storage)?;
    let files = native_input_files(task).await?;
    serve(
        &files,
        task,
        key,
        &mut tokio::io::BufReader::new(tokio::io::stdin()),
        &mut tokio::io::stdout(),
    )
    .await
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
