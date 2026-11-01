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
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, AuthorityAllocation, CancellationToken, DirectoryPage,
    WorkBudget,
};
use acyclic_stream::{BoxProviderFuture, OwnedBytes};
use std::{borrow::Cow, sync::Arc};

/// Directory entries read per listing page.
const PAGE: u32 = 256;

/// Internal files of one agent, under its owner's private-volume write grant.
pub(in crate::filesystem) struct AgentInternalFiles<A: 'static, O: 'static> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
}

/// Reads from one original private-volume generation, including its directory
/// entries and file bodies. Later publications cannot change this selection.
pub(in crate::filesystem) struct InternalRead<A: 'static, O: 'static> {
    generation: acyclic_fs::Generation<A, O>,
}

/// Pages from one selected directory. The original page owns its allocation
/// until consumption ends; its last entry also supplies the next cursor.
pub(in crate::filesystem) struct InternalDirectory<'a, A: 'static, O: 'static> {
    generation: &'a acyclic_fs::Generation<A, O>,
    path: Cow<'a, str>,
    page: Option<(DirectoryPage, AuthorityAllocation)>,
    finished: bool,
}

impl<'a, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    InternalDirectory<'a, A, O>
{
    fn new(generation: &'a acyclic_fs::Generation<A, O>, path: Cow<'a, str>) -> Self {
        Self {
            generation,
            path,
            page: None,
            finished: false,
        }
    }

    /// Reads at most one page, without another read after the final page or
    /// an error. A missing directory is empty.
    pub(in crate::filesystem) async fn next(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<&DirectoryPage>> {
        if self.finished {
            self.page = None;
            return Ok(None);
        }
        let after = self
            .page
            .as_ref()
            .and_then(|(page, _)| page.entries.last())
            .map(|entry| &entry.name);
        let result = self
            .generation
            .list_directory(&self.path, after, PAGE, WorkBudget::UNBOUNDED, cancellation)
            .await;
        self.finished = true;
        self.page = None;
        let (page, allocation) = match result {
            Ok(receipt) => receipt.value,
            Err(failure) => {
                let error = super::map_error(failure.error);
                return if missing(&error) {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
        };
        if page.has_more && page.entries.is_empty() {
            return Err(crate::Error::Storage(
                "internal directory page has no continuation entry".into(),
            ));
        }
        self.finished = !page.has_more;
        self.page = Some((page, allocation));
        Ok(self.page.as_ref().map(|(page, _)| page))
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> InternalRead<A, O> {
    pub(in crate::filesystem) async fn read(
        &self,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Option<OwnedBytes>> {
        AgentInternalFiles::read_at(
            &self.generation,
            &format!("/{}{path}", InternalContentClass::Interaction.prefix()),
            maximum_bytes,
        )
        .await
    }

    pub(in crate::filesystem) async fn list(&self, path: &str) -> Result<Vec<String>> {
        AgentInternalFiles::list_at(
            &self.generation,
            &format!("/{}{path}", InternalContentClass::Interaction.prefix()),
        )
        .await
    }

    pub(in crate::filesystem) fn directory(&self, path: &str) -> InternalDirectory<'_, A, O> {
        InternalDirectory::new(
            &self.generation,
            Cow::Owned(format!(
                "/{}{path}",
                InternalContentClass::Interaction.prefix()
            )),
        )
    }
}

pub(in crate::filesystem) fn filename(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| crate::Error::Storage("internal file name is not UTF-8".into()))
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

    /// Stages the canonical roster `bytes` whose digest is `digest`, once per
    /// distinct roster: the file a conversation entry announces it by.
    pub(in crate::filesystem) async fn stage_roster(
        &self,
        digest: [u8; 32],
        bytes: &[u8],
    ) -> Result<FileRef> {
        let digest = blake3::Hash::from(digest).to_hex();
        self.conversation_file(
            format!("targets/{digest}.json"),
            "targets.json",
            IdempotencyKey(format!("targets:{digest}")),
            bytes,
        )
        .await
    }

    /// Creates the conversation-area file `path` once, keyed by `key`.
    async fn conversation_file(
        &self,
        path: String,
        name: &'static str,
        key: IdempotencyKey,
        bytes: &[u8],
    ) -> Result<FileRef> {
        let path = format!("{}{path}", InternalContentClass::Conversation.prefix());
        self.host
            .put_internal_content(
                &self.volume,
                &self.grant,
                &path,
                bytes,
                "application/json",
                name,
                bytes.len() as u64,
                &key,
                InternalContentClass::Conversation,
                None,
            )
            .await
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

    /// Selects one generation for an entire internal read, without another
    /// workspace open or head observation for its individual files.
    pub(in crate::filesystem) async fn reader(&self) -> Result<InternalRead<A, O>> {
        Ok(InternalRead {
            generation: self.head().await?,
        })
    }

    /// The original owned bytes of internal file `path`, if it exists.
    pub(in crate::filesystem) async fn read(
        &self,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Option<OwnedBytes>> {
        self.reader().await?.read(path, maximum_bytes).await
    }

    /// The bytes of volume file `path` in `head`, if it exists.
    async fn read_at(
        head: &acyclic_fs::Generation<A, O>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Option<OwnedBytes>> {
        let read = head
            .read(
                path,
                maximum_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await;
        match read {
            Ok(receipt) => Ok(Some(receipt.value)),
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
    ) -> Result<Option<OwnedBytes>> {
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
        self.reader().await?.list(path).await
    }

    /// The names of the entries of volume directory `directory` in `head`,
    /// in name order; empty when the directory does not exist.
    async fn list_at(head: &acyclic_fs::Generation<A, O>, directory: &str) -> Result<Vec<String>> {
        let cancellation = CancellationToken::new();
        let mut names = Vec::new();
        let mut pages = InternalDirectory::new(head, Cow::Borrowed(directory));
        while let Some(page) = pages.next(&cancellation).await? {
            for entry in &page.entries {
                names.push(filename(entry.name.as_bytes())?.to_owned());
            }
        }
        Ok(names)
    }
}

impl<A, O> TurnOutputs for AgentInternalFiles<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn tool_results(&self, turn: OperationId) -> Option<String> {
        Some(format!(
            "{}{turn}",
            InternalContentClass::ToolResult.prefix()
        ))
    }

    fn stage<'a>(
        &'a self,
        turn: OperationId,
        bytes: &'a [u8],
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        // One identity per turn: a re-run turn replays the same output.
        Box::pin(self.conversation_file(
            format!("turns/{turn}.json"),
            "output.json",
            IdempotencyKey(format!("turn-output:{turn}")),
            bytes,
        ))
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
        Box::pin(async move {
            Ok(Self::read(self, path, maximum_bytes)
                .await?
                .map(|bytes| bytes.to_vec()))
        })
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
                    Some(existing) => ApprovalCreate::Existing(existing.to_vec()),
                    None => ApprovalCreate::Created,
                },
            )
        })
    }
}
