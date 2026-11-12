//! Stream-backed inputs to the same UI View used by reader clients.
mod watch;
use super::{Affected, Children, Cut, Error, FilesystemCut, View, project::page_number};
use crate::{
    agent::{AgentPath, AgentRecord, AgentStream, ConversationEntry, InboxRecord, StatusRecord},
    conversation::ContentResidencyVerifier,
    filesystem::PreparationIo,
};
use acyclic_fs::{
    NativeMaterializationWork, WorkCounters, WorkError,
    demand::*,
    kernel::{
        KernelAccounting, MetadataDecodeError, NativeAccounting, OwnedSourceChunk, Retained,
        RetainedAllocation,
    },
};
use acyclic_stream::{
    ChildrenPageRequest, HeadProjection, HeadSet, Heads, HeadsRequest, NativeStreamOutput,
    NativeStreamProvider, ReadRequest, Revision, RevisionCut, RevisionScope, StreamError,
    StreamPath,
};
use futures::StreamExt;
use std::{borrow::Cow, ops::Range, sync::Arc};

/// Read-only UI inputs from an authenticated stream provider and the existing
/// pinned-content reader. Construction starts no IO, follow or background work.
/// `links` writes targets in the host's authorized navigation namespace.
pub struct StreamUiProvider<P: ?Sized, F: ?Sized, L> {
    identity: [u8; 16],
    streams: Arc<P>,
    files: Arc<F>,
    agent: AgentPath,
    view: View,
    links: L,
}

impl<P: ?Sized, F: ?Sized, L> StreamUiProvider<P, F, L> {
    /// Binds an existing agent, content authority and shared View configuration.
    /// `navigation_identity` names the immutable mapping used by `links`.
    pub fn new(
        streams: Arc<P>,
        files: Arc<F>,
        agent: AgentPath,
        view: View,
        navigation_identity: [u8; 16],
        links: L,
    ) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic:ui:stream:v1");
        hash.update(&navigation_identity);
        hash.update(agent.as_str().as_bytes());
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Self {
            identity,
            streams,
            files,
            agent,
            view,
            links,
        }
    }

    /// Serves the shared renderer with this provider's exact page configuration.
    pub fn into_source(self) -> Computed<super::FilesystemView<StreamUiCut>, Self>
    where
        Self: ViewProvider<Snapshot = StreamUiCut, Record = Affected>,
    {
        Computed::new(Arc::new(super::FilesystemView::new(self.view)), self)
    }
}

#[derive(serde::Deserialize)]
struct TurnText {
    text: String,
}
struct PreparedTurn {
    sequence: u64,
    text: Retained<TurnText>,
}

/// Only the records and immutable files selected by one filesystem demand.
/// All owned input fields keep their original producer's allocation credit.
pub struct StreamUiCut {
    identity: [u8; 16],
    agent: Retained<AgentPath>,
    status: Option<Retained<StatusRecord>>,
    adoption: Option<Retained<InboxRecord>>,
    tail: Option<u64>,
    turns: Retained<Vec<PreparedTurn>>,
    page: Option<(u32, bool)>,
    children: Retained<Vec<StreamPath>>,
    targets: Retained<Vec<OwnedSourceChunk>>,
}
impl StreamUiCut {
    fn empty(
        identity: [u8; 16],
        agent: &AgentPath,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        Ok(Self {
            identity,
            agent: RetainedAllocation::claim(agent.as_str().len() as u64, 1, ctx)?
                .into_value(agent.clone()),
            status: None,
            adoption: None,
            tail: None,
            page: None,
            turns: Retained::allocate(0, ctx)?,
            children: Retained::allocate(0, ctx)?,
            targets: Retained::allocate(0, ctx)?,
        })
    }
}
impl Cut for StreamUiCut {
    fn role(&self) -> super::Result<Cow<'_, str>> {
        let Some(InboxRecord::ForkAdopted(adopted)) = self.adoption.as_deref() else {
            return Err(Error::Pending);
        };
        if adopted.agent.as_str() != self.agent.id_text() {
            return Err(Error::Pending);
        }
        adopted
            .profile
            .as_ref()
            .and_then(|profile| profile.role())
            .map(Cow::Borrowed)
            .ok_or(Error::Pending)
    }
    fn agent(&self) -> &AgentPath {
        &self.agent
    }
    fn status(&self) -> super::Result<Cow<'_, StatusRecord>> {
        self.status
            .as_ref()
            .map(|status| Cow::Borrowed(&**status))
            .ok_or(Error::Pending)
    }
    fn tail(&self) -> Option<u64> {
        self.tail
    }
    fn turn(&self, sequence: u64) -> super::Result<Cow<'_, str>> {
        let index = self
            .turns
            .binary_search_by_key(&sequence, |turn| turn.sequence)
            .map_err(|_| Error::Pending)?;
        Ok(Cow::Borrowed(&self.turns[index].text.text))
    }
    fn children(&self, page: u32) -> super::Result<Option<Children<'_>>> {
        match self.page {
            Some((selected, more)) if selected == page => Ok(Some(Children {
                children: &self.children,
                more,
            })),
            _ => Err(Error::Pending),
        }
    }
}
impl FilesystemCut for StreamUiCut {
    fn identity(&self) -> [u8; 16] {
        self.identity
    }
    fn child_ui(&self, id: &str) -> super::Result<Option<&[u8]>> {
        // Native children are in name order and were validated when prepared.
        // A metadata lookup must not rescan the visible page for every row.
        Ok(self
            .children
            .binary_search_by(|path| path.as_str().rsplit('/').next().unwrap_or("").cmp(id))
            .ok()
            .map(|index| self.targets[index].bytes.as_slice()))
    }
}

enum Inputs<'a> {
    Reference,
    Conversation,
    Status,
    Role,
    Children(u32),
    Child(&'a str),
}

impl<P, F, L> StreamUiProvider<P, F, L>
where
    P: NativeStreamProvider + ?Sized,
    F: ContentResidencyVerifier + ?Sized,
    L: Fn(&StreamPath, &mut KernelAccounting<'_>) -> Result<OwnedSourceChunk, DemandError>
        + Send
        + Sync,
{
    fn revision(&self, source: &SourceReference) -> Result<Revision, DemandError> {
        if source.identity != self.identity {
            return Err(DemandError::StaleSource);
        }
        let bytes = source.payload()?;
        if bytes.len() < 17 || bytes[0] != 1 {
            return Err(DemandError::StaleSource);
        }
        Ok(Revision {
            incarnation: u128::from_le_bytes(
                bytes[1..17]
                    .try_into()
                    .map_err(|_| DemandError::StaleSource)?,
            ),
            scope: RevisionScope::new(source.components.slice(18..)),
            index: source.epoch,
        })
    }

    async fn heads(
        &self,
        inputs: Inputs<'_>,
        at: Option<Revision>,
        original: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<NativeStreamOutput<Heads>, DemandError> {
        let count = match inputs {
            Inputs::Reference => 4,
            Inputs::Conversation => 2,
            Inputs::Children(_) => 0,
            _ => 1,
        };
        // This is the request's one spilled HeadSet buffer, before dispatch.
        let _paths = RetainedAllocation::claim(
            if count > 1 {
                count * std::mem::size_of::<StreamPath>() as u64
            } else {
                0
            },
            u64::from(count > 1),
            ctx,
        )?;
        let _floor = RetainedAllocation::claim(
            if at.is_some() {
                std::mem::size_of::<Revision>() as u64
            } else {
                0
            },
            u64::from(at.is_some()),
            ctx,
        )?;
        let meter = original.runtime_meter();
        let mut paths = HeadSet::with_capacity(count as usize);
        let mut add = |stream| -> Result<(), DemandError> {
            paths.push(native_path(&self.agent.stream_parts(stream), original)?);
            Ok(())
        };
        match inputs {
            Inputs::Reference => {
                add(AgentStream::Status)?;
                add(AgentStream::Conversation)?;
                add(AgentStream::Membership)?;
                add(AgentStream::Inbox)?;
            }
            Inputs::Conversation => {
                add(AgentStream::Status)?;
                add(AgentStream::Conversation)?;
            }
            Inputs::Status => add(AgentStream::Status)?,
            Inputs::Role => add(AgentStream::Inbox)?,
            Inputs::Child(id) => paths.push(native_path(
                &[self.agent.as_str(), "/children/", id],
                original,
            )?),
            Inputs::Children(_) => {}
        }
        let children = match inputs {
            Inputs::Children(page) => Some(ChildrenPageRequest {
                parent: Some(native_path(&[self.agent.as_str(), "/children"], original)?),
                offset: u64::from(page) * u64::from(self.view.page_size),
                after: None,
                hierarchy_version: None,
                limit: self.view.page_size,
                tail_stream: None,
            }),
            _ => None,
        };
        let limit = if children.is_some() {
            self.view.page_size
        } else {
            count as u32
        };
        let projection = match inputs {
            Inputs::Conversation | Inputs::Status | Inputs::Role => HeadProjection::History,
            _ => HeadProjection::Tail,
        };
        native_result(
            self.streams
                .native_heads(
                    HeadsRequest {
                        at_least: at
                            .clone()
                            .map_or_else(RevisionCut::default, RevisionCut::of),
                        at,
                        paths,
                        retained: HeadSet::new(),
                        durable_through: None,
                        projection,
                        children,
                        limit,
                    },
                    meter,
                )
                .await,
            original,
        )
    }

    async fn prepare(
        &self,
        source: &SourceReference,
        query: ViewQuery<'_>,
        original: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<StreamUiCut, DemandError> {
        let at = self.revision(source)?;
        let path = match query {
            ViewQuery::Lookup(path)
            | ViewQuery::Open(path)
            | ViewQuery::Range { path, .. }
            | ViewQuery::Directory { path, .. } => path,
        };
        let text = path.unicode_text(ctx)?;
        let path = &text[1..];
        let cursor = match query {
            ViewQuery::Directory { cursor, .. } => cursor.map_or(0, SourceCursor::offset),
            _ => 0,
        };
        let maximum = match query {
            ViewQuery::Directory {
                maximum_entries, ..
            } => maximum_entries,
            _ => 0,
        };
        let mut cut = StreamUiCut::empty(self.identity, &self.agent, ctx)?;
        if path == "role" {
            let heads = self
                .heads(Inputs::Role, Some(at.clone()), original, ctx)
                .await?;
            let Some(head) = heads.value().heads.first().and_then(Option::as_ref) else {
                return Ok(cut);
            };
            // The inherited prefix belongs to the parent; the child's adoption
            // is the first record after its primitive fork origin.
            let from = head
                .origin
                .as_ref()
                .map_or(0, |origin| origin.inherited.forked_at);
            if from >= head.tail {
                return Ok(cut);
            }
            let _floor = RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, ctx)?;
            let mut records = native_result(
                self.streams
                    .native_read(
                        ReadRequest {
                            path: native_path(
                                &self.agent.stream_parts(AgentStream::Inbox),
                                original,
                            )?,
                            from,
                            limit: 1,
                            key: None,
                            at_least: RevisionCut::of(at),
                        },
                        original.runtime_meter(),
                    )
                    .await,
                original,
            )?;
            let record = native_result(
                records
                    .value_mut()
                    .next()
                    .await
                    .ok_or(DemandError::StaleSource)?,
                original,
            )?;
            if record.value().sequence != from {
                return Err(DemandError::StaleSource);
            }
            cut.adoption = Some(
                InboxRecord::decode_accounted(&record.value().value, ctx).map_err(record_error)?,
            );
            return Ok(cut);
        }
        let child_page = match path {
            "children" if matches!(query, ViewQuery::Directory { .. }) => {
                Some(u32::try_from(cursor >> 32).map_err(|_| DemandError::InvalidRequest)?)
            }
            "children/pages" if matches!(query, ViewQuery::Directory { .. }) => {
                Some(u32::try_from(cursor).map_err(|_| DemandError::InvalidRequest)?)
            }
            name if name.starts_with("children/pages/") => {
                match page_number(name, "children/pages/") {
                    Ok(page) => Some(page),
                    Err(_) => return Ok(cut),
                }
            }
            _ => None,
        };
        if let Some(page) = child_page {
            let heads = self
                .heads(Inputs::Children(page), Some(at), original, ctx)
                .await?;
            let listing = heads
                .value()
                .children
                .as_ref()
                .ok_or(DemandError::InvalidRequest)?;
            cut.page = Some((page, listing.next_after.is_some()));
            for child in &listing.children {
                AgentPath::parse_id(child.path.as_str()).map_err(Error::from)?;
                cut.targets.push((self.links)(&child.path, ctx)?, ctx)?;
                cut.children.push(child.path.clone(), ctx)?;
            }
            return Ok(cut);
        }
        if path == "children/pages" {
            return Ok(cut);
        }
        if let Some(id) = path
            .strip_prefix("children/")
            .filter(|name| !name.contains('/'))
        {
            let child = match native_path(&[self.agent.as_str(), "/children/", id], original) {
                Ok(child) if AgentPath::parse_id(child.as_str()).is_ok() => child,
                Ok(_) | Err(DemandError::InvalidRequest) => return Ok(cut),
                Err(error) => return Err(error),
            };
            let heads = self
                .heads(Inputs::Child(id), Some(at), original, ctx)
                .await?;
            if heads.value().heads.first().is_some_and(Option::is_some) {
                cut.targets.push((self.links)(&child, ctx)?, ctx)?;
                cut.children.push(child, ctx)?;
            }
            return Ok(cut);
        }
        if path.starts_with("turns/") && page_number::<u64>(path, "turns/").is_err() {
            return Ok(cut);
        }
        let root_directory = path.is_empty() && matches!(query, ViewQuery::Directory { .. });
        let turns = path == "index.md"
            || path.starts_with("turns/")
            || (path == "turns" && matches!(query, ViewQuery::Directory { .. }))
            || (root_directory && cursor == 0 && maximum > 0);
        let status = matches!(path, "status.md" | "glyph")
            || (root_directory
                && [1, 8]
                    .into_iter()
                    .any(|entry| cursor <= entry && cursor + u64::from(maximum) > entry));
        if !status && !turns {
            return Ok(cut);
        }
        let heads = self
            .heads(
                if turns {
                    Inputs::Conversation
                } else {
                    Inputs::Status
                },
                Some(at.clone()),
                original,
                ctx,
            )
            .await?;
        if let Some(record) = heads
            .value()
            .heads
            .first()
            .and_then(Option::as_ref)
            .and_then(|head| head.last.as_ref())
        {
            cut.status =
                Some(StatusRecord::decode_accounted(&record.value, ctx).map_err(record_error)?);
        }
        if !turns {
            return Ok(cut);
        }
        let tail = heads
            .value()
            .heads
            .get(1)
            .and_then(Option::as_ref)
            .map_or(0, |head| head.tail);
        drop(heads);
        cut.tail = Some(tail);
        let size = u64::from(self.view.page_size);
        let range = if path == "turns" {
            let first = cursor.min(tail.div_ceil(size));
            let end = first
                .saturating_add(u64::from(maximum))
                .min(tail.div_ceil(size));
            first.checked_mul(size).ok_or(WorkError::Overflow)?
                ..end.checked_mul(size).unwrap_or(u64::MAX).min(tail)
        } else {
            let page: u64 = if path.starts_with("turns/") {
                page_number(path, "turns/")?
            } else {
                tail.saturating_sub(1) / size
            };
            let Some(start) = page.checked_mul(size) else {
                return Ok(cut);
            };
            start..start.saturating_add(size).min(tail)
        };
        self.turns(range, at, original, ctx, &mut cut).await?;
        Ok(cut)
    }

    async fn turns(
        &self,
        range: Range<u64>,
        at: Revision,
        original: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
        cut: &mut StreamUiCut,
    ) -> Result<(), DemandError> {
        let io = PreparationIo::from_native_work(original).map_err(content_error)?;
        let mut from = range.start;
        while from < range.end {
            let limit = (range.end - from).min(acyclic_stream::MAX_ITEMS as u64) as u32;
            let _floor = RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, ctx)?;
            let mut opened = native_result(
                self.streams
                    .native_read(
                        ReadRequest {
                            path: native_path(
                                &self.agent.stream_parts(AgentStream::Conversation),
                                original,
                            )?,
                            from,
                            limit,
                            key: None,
                            at_least: RevisionCut::of(at.clone()),
                        },
                        original.runtime_meter(),
                    )
                    .await,
                original,
            )?;
            let end = from + u64::from(limit);
            while from < end {
                let record = native_result(
                    opened
                        .value_mut()
                        .next()
                        .await
                        .ok_or(DemandError::StaleSource)?,
                    original,
                )?;
                if record.value().sequence != from {
                    return Err(DemandError::StaleSource);
                }
                let entry = ConversationEntry::decode_accounted(&record.value().value, ctx)
                    .map_err(record_error)?;
                drop(record);
                let bytes = self
                    .files
                    .read(
                        &entry.output,
                        entry.output.descriptor().byte_length(),
                        None,
                        Some(&io),
                    )
                    .map_err(content_error)?
                    .await
                    .map_err(content_error)?;
                let text = ctx
                    .decode_json::<TurnText>(&bytes)
                    .map_err(|error| match error {
                        MetadataDecodeError::Work(error) => DemandError::from(error),
                        MetadataDecodeError::Codec(_) => DemandError::InvalidRequest,
                    })?;
                cut.turns.push(
                    PreparedTurn {
                        sequence: from,
                        text,
                    },
                    ctx,
                )?;
                from += 1;
            }
            if let Some(extra) = opened.value_mut().next().await {
                native_result(extra, original)?;
                return Err(DemandError::InvalidRequest);
            }
        }
        Ok(())
    }
}

impl<P, F, L> ViewProvider for StreamUiProvider<P, F, L>
where
    P: NativeStreamProvider + ?Sized + 'static,
    F: ContentResidencyVerifier + ?Sized + 'static,
    L: Fn(&StreamPath, &mut KernelAccounting<'_>) -> Result<OwnedSourceChunk, DemandError>
        + Send
        + Sync,
{
    type Snapshot = StreamUiCut;
    type Record = Affected;
    fn identity(&self) -> [u8; 16] {
        self.identity
    }
    async fn reference(
        &self,
        caller: &mut KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        let stop = caller.cancellation();
        let owner = NativeAccounting::take(caller)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: stop,
        };
        let result = async {
            let heads = self
                .heads(Inputs::Reference, None, &owner.work, &mut ctx)
                .await?;
            let heads = heads.value();
            let index = heads
                .heads
                .iter()
                .flatten()
                .map(|head| head.changed_at)
                .max()
                .unwrap_or(0);
            let payload = OwnedSourceChunk::produce(&mut ctx, |sink| {
                sink.write_all(&[1])?;
                sink.write_all(&heads.snapshot.incarnation.to_le_bytes())?;
                sink.write_all(heads.snapshot.scope.as_bytes())
                    .map_err(DemandError::from)
            })?;
            SourceReference::opaque(self.identity(), index, &payload.bytes, &mut ctx)
        }
        .await;
        drop(ctx);
        owner.restore(caller)?;
        result
    }
    fn cut(
        &self,
        source: &SourceReference,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<ViewCut, DemandError> {
        ctx.admit(WorkCounters::UNCHARGED)?;
        self.revision(source)?;
        // UI inputs are stream records and complete FileRefs. No authored
        // volume generation is read by this default projection.
        Ok(ViewCut {
            sequence: source.epoch,
            generation: [0; 32],
        })
    }
    async fn snapshot(
        &self,
        source: SourceReference,
        query: ViewQuery<'_>,
        caller: &mut KernelAccounting<'_>,
    ) -> Result<StreamUiCut, DemandError> {
        self.revision(&source)?;
        let stop = caller.cancellation();
        let owner = NativeAccounting::take(caller)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: stop,
        };
        let result = self.prepare(&source, query, &owner.work, &mut ctx).await;
        drop(ctx);
        owner.restore(caller)?;
        result
    }
    fn watch(
        &self,
        sink: Arc<dyn ViewChangeSink<Affected>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        let KernelAccounting::NativeOriginal { work, .. } = ctx else {
            return Err(DemandError::NativeWorkUnavailable);
        };
        let original = (*work).clone();
        let paths = [
            native_path(
                &self.agent.stream_parts(AgentStream::Conversation),
                &original,
            )?,
            native_path(&self.agent.stream_parts(AgentStream::Status), &original)?,
            native_path(&self.agent.stream_parts(AgentStream::Membership), &original)?,
            native_path(&self.agent.stream_parts(AgentStream::Inbox), &original)?,
        ];
        watch::start(Arc::clone(&self.streams), paths, original, sink, ctx).map(Some)
    }
}

fn native_path(
    parts: &[&str],
    work: &NativeMaterializationWork,
) -> Result<StreamPath, DemandError> {
    StreamPath::new_native_parts(parts, &work.runtime_meter())
        .map_err(|error| DemandError::from(work.runtime_error(error)))?
        .map_err(stream_error)
}
fn native_result<T>(
    result: acyclic_stream::NativeStreamResult<T>,
    work: &NativeMaterializationWork,
) -> Result<NativeStreamOutput<T>, DemandError> {
    result
        .map_err(|error| DemandError::from(work.runtime_error(error)))?
        .map_err(stream_error)
}
fn stream_error(error: StreamError) -> DemandError {
    match error {
        StreamError::NotFound => DemandError::Absent,
        StreamError::RevisionForeign | StreamError::PrefixNotRetained | StreamError::OutOfRange => {
            DemandError::StaleSource
        }
        StreamError::InvalidPath | StreamError::InvalidArgument | StreamError::LimitExceeded => {
            DemandError::InvalidRequest
        }
        StreamError::Unsupported => DemandError::NativeWorkUnavailable,
        error => {
            let kind = if matches!(error, StreamError::AccessDenied) {
                std::io::ErrorKind::PermissionDenied
            } else {
                std::io::ErrorKind::Other
            };
            DemandError::Io(std::io::Error::new(kind, error))
        }
    }
}
fn content_error(error: crate::Error) -> DemandError {
    if matches!(error, crate::Error::Cancelled) {
        return DemandError::Cancelled;
    }
    let kind = if error.code() == crate::wire::ErrorCode::Unauthorized {
        std::io::ErrorKind::PermissionDenied
    } else {
        std::io::ErrorKind::Other
    };
    DemandError::Io(std::io::Error::new(kind, error))
}
fn record_error(error: MetadataDecodeError<crate::agent::Error>) -> DemandError {
    match error {
        MetadataDecodeError::Work(error) => error.into(),
        MetadataDecodeError::Codec(_) => DemandError::InvalidRequest,
    }
}
