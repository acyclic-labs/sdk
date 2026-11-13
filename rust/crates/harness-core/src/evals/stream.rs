//! Read only the one original turn demanded by Computed; directory pages need
//! just the pinned tail, without decoding or indexing any conversation records.
use super::*;
use crate::agent::{AgentPath, AgentRecord};
use acyclic_fs::{NativeMaterializationWork, WorkCounters};
use acyclic_stream::{
    HeadProjection, HeadSet, HeadsRequest, NativeStreamOutput, NativeStreamProvider, ReadRequest,
    Revision, RevisionCut, RevisionScope,
};
use futures::StreamExt;
use std::{io::Write, sync::Arc};
mod watch;

struct Prepared {
    record: NativeStreamOutput<Record>,
    entry: Retained<ConversationEntry>,
    before: Option<Retained<HistoryCheckpoint>>,
    after: Retained<HistoryCheckpoint>,
}

/// Only the original record requested by a file demand, plus its pinned tail.
pub struct Snapshot {
    stream: Retained<StreamPath>,
    tail: u64,
    prepared: Option<Prepared>,
}
impl Inputs for Snapshot {
    fn stream(&self) -> &StreamPath {
        &self.stream
    }
    fn tail(&self) -> u64 {
        self.tail
    }
    fn entry(&self, sequence: u64) -> Result<(&Record, &ConversationEntry), DemandError> {
        let prepared = self
            .prepared
            .as_ref()
            .filter(|prepared| prepared.record.value().sequence == sequence)
            .ok_or(DemandError::SourceUnavailable)?;
        Ok((prepared.record.value(), &prepared.entry))
    }
    fn checkpoint(&self, sequence: u64) -> Result<&HistoryCheckpoint, DemandError> {
        let prepared = self
            .prepared
            .as_ref()
            .ok_or(DemandError::SourceUnavailable)?;
        if prepared.record.value().sequence == sequence {
            Ok(&prepared.after)
        } else if prepared.record.value().sequence.checked_sub(1) == Some(sequence) {
            prepared
                .before
                .as_deref()
                .ok_or(DemandError::SourceUnavailable)
        } else {
            Err(DemandError::SourceUnavailable)
        }
    }
}

/// A demand-only adapter over the caller's authenticated original provider.
pub struct StreamProvider<P: ?Sized> {
    streams: Arc<P>,
    path: StreamPath,
    history: StreamPath,
    identity: [u8; 16],
}
impl<P: ?Sized> StreamProvider<P> {
    /// Bind one agent without reading records, capturing a cut or starting work.
    pub fn new(streams: Arc<P>, agent: &AgentPath) -> Result<Self, crate::agent::Error> {
        let path = agent.conversation()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic:evals:stream:v1");
        hash.update(path.as_str().as_bytes());
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(Self {
            streams,
            path,
            identity,
            history: agent.history()?,
        })
    }
}
impl<P: NativeStreamProvider + ?Sized + 'static> StreamProvider<P> {
    /// Compose with the existing filesystem Computed mechanism, without a store.
    pub fn into_source(self) -> Computed<View<Snapshot>, Self> {
        Computed::new(Arc::new(View::default()), self)
    }
    fn revision(&self, source: &SourceReference) -> Result<(Revision, u64), DemandError> {
        let bytes = source.payload()?;
        if source.identity != self.identity || bytes.len() < 25 || bytes[0] != 1 {
            return Err(DemandError::StaleSource);
        }
        let incarnation = u128::from_le_bytes(
            bytes[1..17]
                .try_into()
                .map_err(|_| DemandError::StaleSource)?,
        );
        let tail = u64::from_le_bytes(
            bytes[17..25]
                .try_into()
                .map_err(|_| DemandError::StaleSource)?,
        );
        Ok((
            Revision {
                incarnation,
                index: source.epoch,
                scope: RevisionScope::new(source.components.slice(26..)),
            },
            tail,
        ))
    }
    async fn checkpoint(
        &self,
        sequence: u64,
        revision: &Revision,
        work: &NativeMaterializationWork,
    ) -> Result<Retained<HistoryCheckpoint>, DemandError> {
        let mut records = result(
            self.streams
                .native_read(
                    ReadRequest {
                        path: path(&self.history, work)?,
                        from: sequence,
                        limit: 1,
                        key: None,
                        at_least: RevisionCut::of(revision.clone()),
                    },
                    work.runtime_meter(),
                )
                .await,
            work,
        )?;
        let record = result(
            records
                .value_mut()
                .next()
                .await
                .ok_or(DemandError::StaleSource)?,
            work,
        )?;
        if record.value().sequence != sequence {
            return Err(DemandError::StaleSource);
        }
        let cancellation = work.cancellation();
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: &cancellation,
        };
        let checkpoint = HistoryCheckpoint::decode_accounted(&record.value().value, &mut ctx)
            .map_err(|error| match error {
                MetadataDecodeError::Work(error) => DemandError::from(error),
                MetadataDecodeError::Codec(_) => DemandError::InvalidRequest,
            })?;
        if checkpoint.conversation != sequence.checked_add(1).ok_or(DemandError::StaleSource)? {
            return Err(DemandError::StaleSource);
        }
        Ok(checkpoint)
    }
    async fn heads(
        &self,
        at: Option<Revision>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeStreamOutput<acyclic_stream::Heads>, DemandError> {
        let cancellation = work.cancellation();
        let mut ctx = KernelAccounting::NativeOriginal {
            work,
            cancellation: &cancellation,
        };
        let _paths =
            RetainedAllocation::claim((2 * std::mem::size_of::<StreamPath>()) as u64, 1, &mut ctx)?;
        let paths = [path(&self.path, work)?, path(&self.history, work)?]
            .into_iter()
            .collect();
        result(
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
                        projection: HeadProjection::Tail,
                        children: None,
                        limit: 2,
                    },
                    work.runtime_meter(),
                )
                .await,
            work,
        )
    }
}

impl<P: NativeStreamProvider + ?Sized + 'static> ViewProvider for StreamProvider<P> {
    type Snapshot = Snapshot;
    type Record = ();
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
        let output = async {
            let heads = self.heads(None, &owner.work).await?;
            let tail = heads
                .value()
                .heads
                .first()
                .and_then(Option::as_ref)
                .map_or(0, |head| head.tail);
            let revision = &heads.value().snapshot;
            let payload = OwnedSourceChunk::produce(&mut ctx, |sink| {
                sink.write_all(&[1])?;
                sink.write_all(&revision.incarnation.to_le_bytes())?;
                sink.write_all(&tail.to_le_bytes())?;
                sink.write_all(revision.scope.as_bytes())
                    .map_err(DemandError::from)
            })?;
            SourceReference::opaque(self.identity, revision.index, &payload.bytes, &mut ctx)
        }
        .await;
        drop(ctx);
        owner.restore(caller)?;
        output
    }
    fn cut(
        &self,
        source: &SourceReference,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<ViewCut, DemandError> {
        ctx.admit(WorkCounters::UNCHARGED)?;
        self.revision(source)?;
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
    ) -> Result<Snapshot, DemandError> {
        let (revision, tail) = self.revision(&source)?;
        let stop = caller.cancellation();
        let owner = NativeAccounting::take(caller)?;
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &owner.work,
            cancellation: stop,
        };
        let output = async {
            // A source reference is an address, never bearer authority. This
            // observation rechecks the current session even for directory-only
            // demands, and authenticates the retained tail before using it.
            let _floor =
                RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, &mut ctx)?;
            let heads = self.heads(Some(revision.clone()), &owner.work).await?;
            let retained_tail = heads
                .value()
                .heads
                .first()
                .and_then(Option::as_ref)
                .map_or(0, |head| head.tail);
            if retained_tail != tail {
                return Err(DemandError::StaleSource);
            }
            drop(heads);
            let stream = path(&self.path, &owner.work)?;
            let allocation =
                RetainedAllocation::claim(std::mem::size_of::<StreamPath>() as u64, 1, &mut ctx)?;
            let mut snapshot = Snapshot {
                stream: allocation.into_value(stream),
                tail,
                prepared: None,
            };
            let selected_path = match query {
                ViewQuery::Lookup(path)
                | ViewQuery::Open(path)
                | ViewQuery::Range { path, .. }
                | ViewQuery::Directory { path, .. } => path,
            };
            let selected = match sequence(selected_path, &mut ctx) {
                Ok(selected) => selected,
                Err(DemandError::Absent) => return Ok(snapshot),
                Err(error) => return Err(error),
            };
            if let Some(selected) = selected.filter(|selected| *selected < tail) {
                let _floor =
                    RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, &mut ctx)?;
                let mut records = result(
                    self.streams
                        .native_read(
                            ReadRequest {
                                path: path(&self.path, &owner.work)?,
                                from: selected,
                                limit: 1,
                                key: None,
                                at_least: RevisionCut::of(revision.clone()),
                            },
                            owner.work.runtime_meter(),
                        )
                        .await,
                    &owner.work,
                )?;
                let record = result(
                    records
                        .value_mut()
                        .next()
                        .await
                        .ok_or(DemandError::StaleSource)?,
                    &owner.work,
                )?;
                if record.value().sequence != selected {
                    return Err(DemandError::StaleSource);
                }
                let entry = ConversationEntry::decode_accounted(&record.value().value, &mut ctx)
                    .map_err(|error| match error {
                        MetadataDecodeError::Work(error) => DemandError::from(error),
                        MetadataDecodeError::Codec(_) => DemandError::InvalidRequest,
                    })?;
                let before = match selected.checked_sub(1) {
                    Some(at) => Some(self.checkpoint(at, &revision, &owner.work).await?),
                    None => None,
                };
                let after = self.checkpoint(selected, &revision, &owner.work).await?;
                snapshot.prepared = Some(Prepared {
                    record,
                    entry,
                    before,
                    after,
                });
            }
            Ok(snapshot)
        }
        .await;
        drop(ctx);
        owner.restore(caller)?;
        output
    }
    fn watch(
        &self,
        sink: Arc<dyn ViewChangeSink<()>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        let KernelAccounting::NativeOriginal { work, .. } = ctx else {
            return Err(DemandError::NativeWorkUnavailable);
        };
        let original = (*work).clone();
        let path = path(&self.path, &original)?;
        watch::start(Arc::clone(&self.streams), path, original, sink, ctx).map(Some)
    }
}
fn path(path: &StreamPath, work: &NativeMaterializationWork) -> Result<StreamPath, DemandError> {
    StreamPath::new_native_parts(&[path.as_str()], &work.runtime_meter())
        .map_err(|error| DemandError::from(work.runtime_error(error)))?
        .map_err(stream_error)
}
fn result<T>(
    result: acyclic_stream::NativeStreamResult<T>,
    work: &NativeMaterializationWork,
) -> Result<NativeStreamOutput<T>, DemandError> {
    result
        .map_err(|error| DemandError::from(work.runtime_error(error)))?
        .map_err(stream_error)
}
fn stream_error(error: acyclic_stream::StreamError) -> DemandError {
    use acyclic_stream::StreamError;
    match error {
        StreamError::NotFound => DemandError::Absent,
        StreamError::RevisionForeign | StreamError::PrefixNotRetained | StreamError::OutOfRange => {
            DemandError::StaleSource
        }
        StreamError::InvalidPath | StreamError::InvalidArgument | StreamError::LimitExceeded => {
            DemandError::InvalidRequest
        }
        StreamError::Unsupported => DemandError::NativeWorkUnavailable,
        error => DemandError::Io(std::io::Error::new(
            if error == StreamError::AccessDenied {
                std::io::ErrorKind::PermissionDenied
            } else {
                std::io::ErrorKind::Other
            },
            error,
        )),
    }
}
