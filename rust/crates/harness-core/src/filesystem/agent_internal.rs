//! Internal files of one agent on its private volume, in the internal
//! interaction area: no content writer or tool write grant reaches it, so only
//! the agent's host creates them. Approval files and the effects of agent tool
//! calls both live here, each created exactly once. Settled turn outputs live
//! in the inherited conversation area, which an exact-version grant reaches.

use super::{FilesystemHost, InternalContentClass};
use crate::{
    IdempotencyKey, OperationId, Result,
    agent::TurnOutputs,
    approvals::{ApprovalCreate, ApprovalFiles},
    conversation::{ContentGrant, FileRef, VolumeRef},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, WorkBudget};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// Directory entries read per listing page.
const PAGE: u32 = 256;

/// Internal files of one agent, under its owner's private-volume write grant.
pub(in crate::filesystem) struct AgentInternalFiles<A: 'static, O: 'static> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
}

/// Storage path of internal-relative `path`.
fn internal(path: &str) -> String {
    format!("{}{path}", InternalContentClass::Interaction.prefix())
}

fn missing(error: &crate::Error) -> bool {
    error.code() == crate::wire::ErrorCode::NotFound
}

impl<A, O> AgentInternalFiles<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// One original-work read, retaining the provider's owned byte buffer.
    pub(in crate::filesystem) async fn read_with_io(
        &self,
        path: &str,
        maximum_bytes: u64,
        io: &super::PreparationIo,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let workspace =
            super::bounded_io::content_workspace(&self.host.provider, &self.volume, Some(io))?;
        let _workspace = io.guard_owned(workspace.logical_owned_bytes()?)?;
        let (path, _path) =
            original_text(&["/", InternalContentClass::Interaction.prefix(), path], io)?;
        match self
            .host
            .read(&workspace, None, &path, maximum_bytes, Some(io))
            .await
        {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if missing(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The same create-once publication with the original work and cancellation.
    pub(in crate::filesystem) async fn create_with_io(
        &self,
        path: &str,
        bytes: &[u8],
        maximum_bytes: u64,
        io: &super::PreparationIo,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        if let Some(existing) = self.read_with_io(path, maximum_bytes, io).await? {
            return Ok(Some(existing));
        }
        let (key, _key) = original_text(&["internal-file:", path], io)?;
        let key = IdempotencyKey(key);
        let (destination, _destination) =
            original_text(&[InternalContentClass::Interaction.prefix(), path], io)?;
        let written = self
            .host
            .put_internal_content(
                &self.volume,
                &self.grant,
                &destination,
                bytes,
                "application/json",
                path.rsplit('/').next().unwrap_or(path),
                maximum_bytes,
                &key,
                InternalContentClass::Interaction,
                Some(io),
            )
            .await;
        match written {
            Ok(_) => Ok(None),
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => self
                .read_with_io(path, maximum_bytes, io)
                .await?
                .map(Some)
                .ok_or(error),
            Err(error) => Err(error),
        }
    }

    /// Lists the original internal directory under one admitted operation.
    pub(in crate::filesystem) async fn list_with_io(
        &self,
        path: &str,
        io: &super::PreparationIo,
    ) -> Result<Vec<String>> {
        use crate::store::history::HistoryIo as _;
        let workspace_ref =
            super::bounded_io::content_workspace(&self.host.provider, &self.volume, Some(io))?;
        let _workspace = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
        let workspace = self.host.open(&workspace_ref, Some(io)).await?;
        let head = io.sdk(workspace.head(io, io.cancellation()).await)?;
        let (directory, _directory) =
            original_text(&["/", InternalContentClass::Interaction.prefix(), path], io)?;
        let mut names = Vec::new();
        let mut after = None;
        loop {
            let page = match io.sdk(
                head.list_directory(&directory, after.as_ref(), PAGE, io, io.cancellation())
                    .await,
            ) {
                Ok(page) => page.0,
                Err(error) if missing(&error) => return Ok(names),
                Err(error) => return Err(error),
            };
            for entry in &page.entries {
                if names.len() == names.capacity() {
                    let additional = names.len().max(1);
                    let bytes = additional * std::mem::size_of::<String>();
                    io.retain_owned(bytes as u64)?;
                    io.allocation_attempt(bytes as u64)?;
                    names
                        .try_reserve_exact(additional)
                        .map_err(crate::Error::Allocation)?;
                }
                let text = std::str::from_utf8(entry.name.as_bytes())
                    .map_err(|_| io.invalid("internal file name is not UTF-8"))?;
                names.push(io.history_io().clone_text(text)?);
            }
            if !page.has_more || page.entries.is_empty() {
                return Ok(names);
            }
            // Keep the provider's name allocation instead of cloning it.
            after = page.entries.into_iter().last().map(|entry| entry.name);
        }
    }
    pub(in crate::filesystem) const fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        grant: ContentGrant,
    ) -> Self {
        Self {
            host,
            volume,
            grant,
        }
    }

    /// The committed head of the private volume, whose bytes are durable.
    async fn head(&self) -> Result<acyclic_fs::Generation<A, O>> {
        let cancellation = CancellationToken::new();
        let workspace = self
            .host
            .filesystem
            .open_workspace(
                self.volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| super::map_error(failure.error))?
            .value;
        Ok(workspace
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| super::map_error(failure.error))?
            .value)
    }

    /// The bytes of internal file `path`, if it exists.
    pub(in crate::filesystem) async fn read(
        &self,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        Self::read_at(
            &self.head().await?,
            &format!("/{}", internal(path)),
            maximum_bytes,
        )
        .await
    }

    /// The bytes of volume file `path` in `head`, if it exists.
    async fn read_at(
        head: &acyclic_fs::Generation<A, O>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        let read = head
            .read(
                path,
                maximum_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await;
        match read {
            Ok(receipt) => Ok(Some(receipt.value.to_vec())),
            Err(failure) => {
                let error = super::map_error(failure.error);
                if missing(&error) {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Creates internal file `path` holding `bytes` once; returns the bytes of
    /// the file that already exists, unchanged, when it was created before.
    pub(in crate::filesystem) async fn create(
        &self,
        path: &str,
        bytes: &[u8],
        maximum_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(existing) = self.read(path, maximum_bytes).await? {
            return Ok(Some(existing));
        }
        // One retry identity per path: a second, different body conflicts
        // instead of replacing the first.
        let key = IdempotencyKey(format!("internal-file:{path}"));
        let written = self
            .host
            .put_internal_content(
                &self.volume,
                &self.grant,
                &internal(path),
                bytes,
                "application/json",
                path.rsplit('/').next().unwrap_or(path),
                maximum_bytes,
                &key,
                InternalContentClass::Interaction,
                None,
            )
            .await;
        match written {
            Ok(_) => Ok(None),
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {
                self.read(path, maximum_bytes).await?.map(Some).ok_or(error)
            }
            Err(error) => Err(error),
        }
    }

    /// The names of the files in internal directory `path`, in name order;
    /// empty when the directory does not exist.
    pub(in crate::filesystem) async fn list(&self, path: &str) -> Result<Vec<String>> {
        Self::list_at(&self.head().await?, &format!("/{}", internal(path))).await
    }

    /// The names of the entries of volume directory `directory` in `head`,
    /// in name order; empty when the directory does not exist.
    async fn list_at(head: &acyclic_fs::Generation<A, O>, directory: &str) -> Result<Vec<String>> {
        let cancellation = CancellationToken::new();
        let mut names = Vec::new();
        let mut after = None;
        loop {
            let page = match head
                .list_directory(
                    directory,
                    after.as_ref(),
                    PAGE,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
            {
                Ok(receipt) => receipt.value.0,
                Err(failure) => {
                    let error = super::map_error(failure.error);
                    return if missing(&error) {
                        Ok(names)
                    } else {
                        Err(error)
                    };
                }
            };
            for entry in &page.entries {
                names.push(
                    String::from_utf8(entry.name.as_bytes().to_vec()).map_err(|_| {
                        crate::Error::Storage("internal file name is not UTF-8".into())
                    })?,
                );
            }
            if !page.has_more || page.entries.is_empty() {
                return Ok(names);
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
}

/// Fund one exact text allocation before constructing an internal operand.
pub(in crate::filesystem) fn original_text<'a>(
    parts: &[&str],
    io: &'a super::PreparationIo,
) -> Result<(String, super::DecodeReservation<'a>)> {
    let length = parts
        .iter()
        .try_fold(0usize, |length, part| length.checked_add(part.len()))
        .ok_or_else(|| io.invalid("internal text length overflow"))?;
    let guard = io.reserve_decode(length as u64)?;
    io.allocation_attempt(length as u64)?;
    io.charge(acyclic_fs::WorkCounters {
        bytes_copied: length as u64,
        ..Default::default()
    })?;
    let mut text = String::new();
    text.try_reserve_exact(length)
        .map_err(crate::Error::Allocation)?;
    for part in parts {
        text.push_str(part);
    }
    Ok((text, guard))
}

impl<A, O> TurnOutputs for AgentInternalFiles<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn stage<'a>(
        &'a self,
        turn: OperationId,
        bytes: &'a [u8],
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            // One identity per turn: a re-run turn replays the same output.
            let path = format!(
                "{}turns/{turn}.json",
                InternalContentClass::Conversation.prefix()
            );
            self.host
                .put_internal_content(
                    &self.volume,
                    &self.grant,
                    &path,
                    bytes,
                    "application/json",
                    "output.json",
                    bytes.len() as u64,
                    &IdempotencyKey(format!("turn-output:{turn}")),
                    InternalContentClass::Conversation,
                    None,
                )
                .await
        })
    }

    fn partial(&self, turn: OperationId) -> BoxProviderFuture<'_, Result<String>> {
        Box::pin(async move {
            // The live reply of the turn's newest model dispatch: dispatch
            // directories are time-ordered names, reply chunks zero-padded
            // sequence names, so name order is staging order.
            let head = self.head().await?;
            let turn = format!("/{}/{turn}", super::TOOL_OUTPUT);
            for dispatch in Self::list_at(&head, &turn).await?.iter().rev() {
                let reply = format!("{turn}/{dispatch}/reply");
                let chunks = Self::list_at(&head, &reply).await?;
                if chunks.is_empty() {
                    continue;
                }
                let mut text = Vec::new();
                for chunk in chunks {
                    if let Some(bytes) =
                        Self::read_at(&head, &format!("{reply}/{chunk}"), u64::MAX).await?
                    {
                        text.extend_from_slice(&bytes);
                    }
                }
                return String::from_utf8(text)
                    .map_err(|_| crate::Error::Storage("live reply is not UTF-8".into()));
            }
            Ok(String::new())
        })
    }
}

impl<A, O> ApprovalFiles for AgentInternalFiles<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn read<'a>(
        &'a self,
        path: &'a str,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(Self::read(self, path, maximum_bytes))
    }

    fn create<'a>(
        &'a self,
        path: &'a str,
        bytes: Vec<u8>,
        maximum_bytes: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>> {
        Box::pin(async move {
            Ok(
                match Self::create(self, path, &bytes, maximum_bytes).await? {
                    Some(existing) => ApprovalCreate::Existing(existing),
                    None => ApprovalCreate::Created,
                },
            )
        })
    }
}
