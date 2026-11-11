//! Retained authorized source; preparation never changes catalog visibility.

use super::*;
use crate::executor::PreparationIo;
use acyclic_fs::{PublicationContext, PublicationEntry, VolumeConfig, VolumeId};
use acyclic_stream::session::{ResourceRevalidation, ResourceSubscription};
use acyclic_stream::{CommitId, FollowStream, FollowUpdate, Record, StreamError, StreamPath};
use futures::{
    StreamExt as _,
    future::{Either, select},
};

/// Original exclusive follow position. It conveys no read authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct SourceCursor {
    /// Next original source sequence.
    pub next: u64,
    /// Original preceding commit anchor, when present.
    pub preceding: Option<[u8; 32]>,
}

// The original tuple wire has one paid decoder beside its producer. The
// generation bytes retain the complete storage association, never just a digest.
#[derive(Deserialize, crate::HeapLayout)]
pub(crate) struct SkillSourceCheckpoint(
    u32,
    SourceCursor,
    SourceCursor,
    [u8; 16],
    Option<(String, [u8; 32], u64, [u8; 16], Vec<u8>, u64)>,
);

impl crate::filesystem::PreparationRecord for SkillSourceCheckpoint {}

impl SkillSourceCheckpoint {
    pub(crate) fn decode<'io>(
        bytes: &[u8],
        io: &'io PreparationIo,
    ) -> Result<(Self, crate::conversation::ContentHold<'io>)> {
        let (value, hold) = io.decode(
            std::marker::PhantomData::<Self>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        if value.0 != 1 || value.1.next >= value.2.next {
            return Err(StreamError::InvalidArgument.into());
        }
        if let Some((origin, _, end, _, generation, _)) = &value.4 {
            StreamPath::validate(origin)?;
            if *end > value.2.next
                || *end <= value.1.next
                || acyclic_fs::GenerationId::from_bytes(generation).is_none()
            {
                return Err(StreamError::InvalidArgument.into());
            }
        }
        Ok((value, hold))
    }
    pub(crate) fn cursor(&self) -> SourceCursor {
        self.2
    }
    pub(crate) fn volume(&self) -> [u8; 16] {
        self.3
    }
}

/// One metadata change, still invisible until the owning atomic admission.
pub enum SkillPublicationChange {
    /// Validated header with its original pinned lazy body authority.
    Skill(SkillMetadata),
    /// Exact removed path under its original declared read boundary.
    Removed(PinnedContextPath),
}

/// One filesystem member identified by its original authenticated producer.
pub struct SkillPublicationMember {
    context: PublicationContext,
    origin: StreamPath,
    commit: CommitId,
    end: u64,
}

impl SkillPublicationMember {
    /// Producer-authenticated exclusive end, independent of commit tail.
    pub fn end(&self) -> u64 {
        self.end
    }
    /// Original appending stream, including an ancestor when following a fork.
    pub fn origin(&self) -> &StreamPath {
        &self.origin
    }
    /// Original append identity; this is not a completion witness.
    pub fn commit(&self) -> CommitId {
        self.commit
    }
    /// Exact producer generation and authority sequence; neither grants authority.
    pub fn publication(&self) -> PublicationContext {
        self.context
    }
}

/// An original authorized follow cut awaiting the journal's confirmed admission.
/// No caller-supplied completion flag or mutable filesystem head is involved.
pub struct SkillPublicationCut {
    from: SourceCursor,
    cursor: SourceCursor,
    executing_volume: VolumeId,
    member: Option<SkillPublicationMember>,
    changes: Vec<SkillPublicationChange>,
    rejection: Option<Error>,
    // Dropped after the payload. This is the original preparation's ownership,
    // transferred once; retries retain it without recharging or copying.
    _custody: Option<crate::filesystem::PreparationCustody>,
}

struct Preparing<'a> {
    source: &'a mut SkillPublicationSource,
    completed: bool,
}

impl Drop for Preparing<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.source.failed = true;
            self.source.changes = Vec::new();
            self.source.pending = None;
            self.source.member = None;
            self.source.rejection = None;
            self.source.records = None;
            self.source.open = None;
        }
    }
}

impl SkillPublicationCut {
    pub(crate) fn executing_volume(&self) -> [u8; 16] {
        self.executing_volume.into_bytes()
    }

    /// Cursor against which this source checkpoint must be conditionally admitted.
    pub fn from(&self) -> SourceCursor {
        self.from
    }
    /// Exact cursor delivered by the authorized original follow.
    pub fn cursor(&self) -> SourceCursor {
        self.cursor
    }
    /// Completed visible member, if any. Fully filtered progress has no member
    /// metadata and must never be projected into a fabricated context update.
    pub fn member(&self) -> Option<&SkillPublicationMember> {
        self.member.as_ref()
    }
    /// Metadata to contribute through the single conversation admission path.
    pub fn changes(&self) -> &[SkillPublicationChange] {
        &self.changes
    }
    /// Invalid replacement metadata rejects this member's catalog changes.
    /// Its source checkpoint can still be admitted, allowing later repairs to
    /// arrive without replaying this rejected member forever.
    pub fn rejection(&self) -> Option<&Error> {
        self.rejection.as_ref()
    }

    /// Encodes the producer checkpoint for the original journal owner to fence
    /// and contribute. This method cannot choose a destination or submit it.
    pub fn encode_checkpoint(&self, io: &PreparationIo) -> Result<acyclic_stream::OwnedBytes> {
        let generation = self
            .member
            .as_ref()
            .map(|member| member.context.generation.to_bytes());
        let member = self
            .member
            .as_ref()
            .zip(generation.as_ref())
            .map(|(member, generation)| {
                (
                    member.origin.as_str(),
                    member.commit.as_bytes(),
                    member.end,
                    member.context.volume.into_bytes(),
                    &generation[..],
                    member.context.authority_sequence.get(),
                )
            });
        io.encode(
            &(
                1_u32,
                self.from,
                self.cursor,
                self.executing_volume.into_bytes(),
                member,
            ),
            io.remaining()?
                .bytes_encoded
                .min(acyclic_stream::MAX_RECORD_BYTES as u64),
            true,
        )
    }
}

/// One existing resource subscription retained by an executing agent's context
/// owner. Reading stops at a pending member until that owner's journal confirms
/// it. No task, watcher, history traversal, or replacement catalog is created.
pub struct SkillPublicationSource {
    root: ContextRoot,
    roots: Vec<ContextRoot>,
    volume: VolumeId,
    config: VolumeConfig,
    accepted: SourceCursor,
    filter: acyclic_stream::FollowFilter,
    authorization: acyclic_stream::session::ResourceAuthorization,
    observed: SourceCursor,
    revalidate: ResourceRevalidation,
    terminal: acyclic_stream::BoxProviderFuture<'static, StreamError>,
    open: Option<
        acyclic_stream::BoxProviderFuture<'static, std::result::Result<FollowStream, StreamError>>,
    >,
    records: Option<FollowStream>,
    member: Option<SkillPublicationMember>,
    changes: Vec<SkillPublicationChange>,
    rejection: Option<Error>,
    pending: Option<SkillPublicationCut>,
    failed: bool,
}

impl SkillPublicationSource {
    /// Retains the subscription returned by the original ChangeProvider. The
    /// owning binding supplies its original volume/config and accepted cursor;
    /// roots must all carry the same executing volume's read authority.
    pub fn new(
        subscription: ResourceSubscription,
        mut roots: Vec<ContextRoot>,
        volume: VolumeId,
        config: VolumeConfig,
        accepted: SourceCursor,
        filter: acyclic_stream::FollowFilter,
    ) -> Result<Self> {
        let first = roots
            .pop()
            .ok_or_else(|| Error::Invalid("publication has no context roots".into()))?;
        first.validate(&mut None)?;
        for root in &roots {
            root.validate(&mut None)?;
            if root.volume != first.volume {
                return Err(Error::Invalid(
                    "publication roots belong to different executing volumes".into(),
                ));
            }
        }
        Ok(Self {
            root: first,
            roots,
            volume,
            config,
            accepted,
            observed: accepted,
            filter,
            authorization: subscription.authorization,
            revalidate: subscription.revalidate,
            terminal: subscription.terminal,
            open: Some(subscription.open),
            records: None,
            member: None,
            changes: Vec::new(),
            rejection: None,
            pending: None,
            failed: false,
        })
    }

    pub(crate) fn executing_volume(&self) -> [u8; 16] {
        self.volume.into_bytes()
    }

    /// Read authority retained by this executing binding, never inferred from a
    /// catalog generation or the source's appending stream.
    pub fn volume(&self) -> &VolumeRef {
        &self.root.volume
    }

    /// Last checkpoint confirmed by the atomic publication owner.
    pub fn accepted(&self) -> SourceCursor {
        self.accepted
    }

    /// Borrows the same pending cut for retries. Revalidation remains mandatory
    /// immediately before the journal submits its contributions.
    pub fn pending(&self) -> Result<Option<&SkillPublicationCut>> {
        if self.failed {
            return Err(StreamError::Unavailable.into());
        }
        if self.pending.is_some() {
            (self.revalidate)()?;
        }
        Ok(self.pending.as_ref())
    }

    /// Called only by the filesystem journal owner after its complete receipt
    /// confirmation, together with catalog/registry publication. A conditional
    /// loser leaves the pending cut untouched for exact retry. The admitted
    /// owner receives the original payload and custody without cloning it.
    pub(crate) fn confirm_publication(
        &mut self,
        cursor: SourceCursor,
        confirm: impl FnOnce() -> Result<()>,
    ) -> Result<SkillPublicationCut> {
        let pending = self.pending()?.ok_or(StreamError::InvalidArgument)?;
        if pending.cursor != cursor {
            return Err(StreamError::InvalidArgument.into());
        }
        // All fallible source checks precede the other original checkpoint
        // confirmation. No source cursor or payload moves if it refuses.
        confirm()?;
        let admitted = self.pending.take().ok_or(StreamError::InvalidArgument)?;
        self.accepted = cursor;
        Ok(admitted)
    }

    /// Prepares one member using the original session reservation. The caller
    /// retains that session owner and its termination event; a turn deadline
    /// must not be used as this source's lifetime. No catalog is changed here.
    pub async fn prepare(
        &mut self,
        reader: &dyn ContentResidencyVerifier,
        limits: ContextDiscoveryLimits,
        session: &PreparationIo,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if session.original_native_work().is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "retained skill metadata parser has no native allocation admission",
            });
        }
        if self.failed {
            return Err(StreamError::Unavailable.into());
        }
        let scope = crate::filesystem::PreparationScope::new(Some(session))?;
        let io = scope.io().ok_or(StreamError::InvalidArgument)?;
        // This guard destroys partly read metadata before its allocation scope
        // releases ownership, including when the entire future is dropped.
        let mut preparing = Preparing {
            source: self,
            completed: false,
        };
        preparing.source.prepare_member(reader, limits, io).await?;
        if let Some(pending) = preparing.source.pending.as_mut()
            && pending._custody.is_none()
        {
            pending._custody = Some(io.take_owned_custody(io.owned_bytes()?)?);
        }
        preparing.completed = true;
        Ok(())
    }

    async fn prepare_member(
        &mut self,
        reader: &dyn ContentResidencyVerifier,
        limits: ContextDiscoveryLimits,
        session: &PreparationIo,
    ) -> Result<()> {
        limits.validate()?;
        session.check_cancellation()?;
        if self.pending.is_some() {
            gated(&mut self.terminal, session, std::future::ready(())).await?;
            (self.revalidate)()?;
            return Ok(());
        }
        if let Some(open) = self.open.take() {
            self.records = Some(gated(&mut self.terminal, session, open).await??);
        }
        loop {
            (self.revalidate)()?;
            let records = self.records.as_mut().ok_or(StreamError::Unavailable)?;
            let update = gated(&mut self.terminal, session, records.next())
                .await?
                .ok_or(StreamError::Unavailable)??;
            (self.revalidate)()?;
            match update {
                FollowUpdate::Opening { .. } => {}
                FollowUpdate::Record(record) => {
                    self.record(record, reader, limits, session).await?
                }
                FollowUpdate::Progress { next, preceding } => {
                    if next < self.observed.next {
                        return Err(StreamError::OutOfRange.into());
                    }
                    self.observed = SourceCursor {
                        next,
                        preceding: Some(*preceding.as_bytes()),
                    };
                }
            }
            if self.observed.next > self.accepted.next
                && self
                    .member
                    .as_ref()
                    .is_none_or(|member| self.observed.next >= member.end)
            {
                self.pending = Some(SkillPublicationCut {
                    from: self.accepted,
                    cursor: self.observed,
                    executing_volume: self.volume,
                    member: self.member.take(),
                    changes: std::mem::take(&mut self.changes),
                    rejection: self.rejection.take(),
                    _custody: None,
                });
                return Ok(());
            }
        }
    }

    async fn record(
        &mut self,
        record: Record,
        reader: &dyn ContentResidencyVerifier,
        limits: ContextDiscoveryLimits,
        session: &PreparationIo,
    ) -> Result<()> {
        if record.sequence < self.observed.next || !acyclic_stream::is_changes(&record.headers) {
            return Err(StreamError::InvalidArgument.into());
        }
        let changes = acyclic_stream::decode_changes(&record.value)?;
        let context = PublicationContext::from_bytes(&changes.context)?;
        let end = context.exclusive_end(record.sequence)?;
        // ChangeProvider authenticates context.volume against Record.origin
        // and applies the executing volume's policy. An inherited ancestor's
        // appending volume can differ from the executing volume legitimately.
        if let Some(member) = &self.member {
            if member.context.volume != context.volume
                || member.context.generation != context.generation
                || member.context.authority_sequence != context.authority_sequence
                || member.origin != record.origin
                || member.commit != record.commit_id
                || member.end != end
            {
                return Err(StreamError::InvalidArgument.into());
            }
        } else {
            self.member = Some(SkillPublicationMember {
                context,
                origin: record.origin,
                commit: record.commit_id,
                end,
            });
        }
        let mut generation = None;
        for entry in changes.entries {
            if self.rejection.is_some() {
                break;
            }
            let facts = PublicationEntry::from_parts(
                &entry.key,
                &entry.operation,
                &entry.metadata,
                self.config,
            )?;
            let Some(path) = crate::filesystem::content_path(&facts.path, session)? else {
                continue;
            };
            if let Some(reference) = facts.delta {
                let roots: Vec<_> = std::iter::once(&self.root)
                    .chain(&self.roots)
                    .filter(|root| within(&root.directory, &path) || within(&path, &root.directory))
                    .collect();
                let Some(first) = roots.first() else {
                    continue;
                };
                if generation.is_none() {
                    generation = Some(crate::filesystem::bounded_io::generation_ref(
                        first.volume.provider(),
                        context.generation,
                        Some(session),
                    )?);
                }
                let generation = generation.as_ref().ok_or(StreamError::InvalidArgument)?;
                let parent = match reference.origin_for(&self.filter)? {
                    acyclic_fs::DeltaOrigin::Empty => None,
                    acyclic_fs::DeltaOrigin::Generation(before) => {
                        Some(crate::filesystem::bounded_io::generation_ref(
                            first.volume.provider(),
                            before,
                            Some(session),
                        )?)
                    }
                };
                let compared = if let Some(parent) = &parent {
                    let compared =
                        reader.diff_private_paths(&roots, (parent, generation), Some(session))?;
                    Some(gated(&mut self.terminal, session, compared).await??)
                } else {
                    None
                };
                if compared
                    .as_ref()
                    .is_some_and(|values| values.len() != roots.len())
                {
                    return Err(StreamError::InvalidResponse.into());
                }
                for (index, root) in roots.into_iter().enumerate() {
                    let expanded = expand_delta(
                        &mut self.terminal,
                        reader,
                        root,
                        DeltaExpansion {
                            authorization: (self.volume, &self.authorization),
                            reference,
                            generation,
                            filter: &self.filter,
                            headers: &record.headers,
                            config: self.config,
                            scope: &path,
                        },
                        compared.as_ref().and_then(|values| values.get(index)),
                        limits,
                        session,
                    )
                    .await?;
                    match expanded {
                        Ok(changes) => {
                            for change in changes {
                                push_change(&mut self.changes, change, limits, session)?;
                            }
                        }
                        Err(error) => {
                            self.rejection = Some(error);
                            self.changes = Vec::new();
                            break;
                        }
                    }
                }
                continue;
            }
            for root in std::iter::once(&self.root).chain(&self.roots) {
                if !root.is_skill_path(&path, "SKILL.md") {
                    continue;
                }
                if generation.is_none() {
                    generation = Some(crate::filesystem::bounded_io::generation_ref(
                        root.volume.provider(),
                        context.generation,
                        Some(session),
                    )?);
                }
                let generation = generation.as_ref().ok_or(StreamError::InvalidArgument)?;
                let change = if facts.after_present {
                    let metadata = gated(
                        &mut self.terminal,
                        session,
                        root.skill_metadata_parsed_at(
                            reader,
                            &path,
                            &generation,
                            "SKILL.md",
                            limits,
                            Some(session),
                        ),
                    )
                    .await??
                    .ok_or(StreamError::InvalidArgument)?;
                    let metadata = match metadata {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            self.rejection = Some(error);
                            self.changes = Vec::new();
                            break;
                        }
                    };
                    SkillPublicationChange::Skill(metadata)
                } else if facts.before_present {
                    let source = crate::executor::with_history_io(Some(session), |work| {
                        PinnedContextPath::capture(root, generation, &path, work)
                    })?;
                    SkillPublicationChange::Removed(source)
                } else {
                    continue;
                };
                push_change(&mut self.changes, change, limits, session)?;
            }
        }
        (self.revalidate)()?;
        self.observed = SourceCursor {
            next: record
                .sequence
                .checked_add(1)
                .ok_or(StreamError::OutOfRange)?,
            preceding: Some(*record.commit_id.as_bytes()),
        };
        Ok(())
    }
}

fn push_change(
    changes: &mut Vec<SkillPublicationChange>,
    change: SkillPublicationChange,
    limits: ContextDiscoveryLimits,
    session: &PreparationIo,
) -> Result<()> {
    // This is the existing bounded selected metadata budget, not a limit on
    // the followed source, agents, or retained history.
    if changes.len() >= limits.entries as usize {
        return Err(Error::Invalid(
            "publication metadata exceeds admitted discovery work".into(),
        ));
    }
    let capacity = if changes.len() == changes.capacity() {
        changes
            .capacity()
            .checked_mul(2)
            .ok_or(StreamError::OutOfRange)?
            .max(1)
            .min(limits.entries as usize)
    } else {
        changes.capacity()
    };
    crate::executor::with_history_io(Some(session), |work| {
        crate::context::compaction::reserve_vector(changes, capacity, work)
    })?;
    changes.push(change);
    Ok(())
}

/// Whether content path `path` is `directory` or lies under it.
fn within(path: &str, directory: &str) -> bool {
    directory.is_empty()
        || path
            .strip_prefix(directory)
            .is_some_and(|tail| tail.is_empty() || tail.starts_with('/'))
}

/// Expand only changed skill files using the filesystem's immutable Merkle
/// comparison. An unchanged rejected replacement is never parsed again, and
/// an unchanged skill tree consumes no second discovery-directory allowance.
struct DeltaExpansion<'a> {
    authorization: (VolumeId, &'a acyclic_stream::session::ResourceAuthorization),
    reference: acyclic_fs::PublicationDelta,
    generation: &'a GenerationRef,
    filter: &'a acyclic_stream::FollowFilter,
    headers: &'a acyclic_stream::RecordHeaders,
    config: VolumeConfig,
    scope: &'a str,
}

async fn expand_delta(
    terminal: &mut acyclic_stream::BoxProviderFuture<'static, StreamError>,
    reader: &dyn ContentResidencyVerifier,
    root: &ContextRoot,
    selection: DeltaExpansion<'_>,
    compared: Option<&crate::context::DiffPublication>,
    limits: ContextDiscoveryLimits,
    session: &PreparationIo,
) -> Result<Result<Vec<SkillPublicationChange>>> {
    const NAME: &str = "SKILL.md";
    let generation = selection.generation;
    selection.reference.origin_for(selection.filter)?;
    let paths = if let Some(compared) = compared {
        compared
            .paths
            .iter()
            .filter(|change| root.is_skill_path(&change.path, NAME))
            .map(|change| (change.path.clone(), change.current.is_some()))
            .collect::<Vec<_>>()
    } else {
        let mut work = DiscoveryWork {
            limits,
            ..Default::default()
        };
        let mut source = VolumeDiscovery {
            reader,
            root,
            io: Some(session),
        };
        gated(
            terminal,
            session,
            super::source::skill_files(
                &mut source,
                &root.directory,
                NAME,
                &mut work,
                Some(generation),
                true,
            ),
        )
        .await??
        .map(|(_, files)| files)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, path)| (path, true))
        .collect()
    };
    let mut changes = Vec::new();
    for (path, present) in paths {
        if !within(&path, selection.scope) {
            continue;
        }
        // Context paths are volume-relative; namespace paths include the root.
        let namespace = acyclic_fs::kernel::NamespacePath::from_text_in_profile(
            &format!("/{path}"),
            selection.config.profile,
            selection.config.limits,
        )
        .map_err(|_| StreamError::InvalidArgument)?;
        if !selection.reference.selects(
            selection.authorization,
            &namespace,
            selection.config,
            selection.filter,
            selection.headers,
        )? {
            continue;
        }
        let change = if present {
            let metadata = gated(
                terminal,
                session,
                root.skill_metadata_parsed_at(
                    reader,
                    &path,
                    generation,
                    NAME,
                    limits,
                    Some(session),
                ),
            )
            .await??
            .ok_or(StreamError::InvalidArgument)?;
            match metadata {
                Ok(metadata) => SkillPublicationChange::Skill(metadata),
                Err(error) => return Ok(Err(error)),
            }
        } else {
            SkillPublicationChange::Removed(crate::executor::with_history_io(
                Some(session),
                |work| PinnedContextPath::capture(root, generation, &path, work),
            )?)
        };
        push_change(&mut changes, change, limits, session)?;
    }
    Ok(Ok(changes))
}

// Every wait races the original subscription's terminal event and original
// session cancellation. A dropped record producer is a typed Unavailable above.
async fn gated<T>(
    terminal: &mut acyclic_stream::BoxProviderFuture<'static, StreamError>,
    io: &PreparationIo,
    operation: impl std::future::Future<Output = T>,
) -> Result<T> {
    let cancelled = io.cancellation().cancelled();
    let cancelled = core::pin::pin!(cancelled);
    let operation = core::pin::pin!(operation);
    match select(select(terminal, cancelled), operation).await {
        Either::Left((Either::Left((error, _)), _)) => Err(error.into()),
        Either::Left((Either::Right(((), _)), _)) => {
            io.check_cancellation()?;
            Err(StreamError::Unavailable.into())
        }
        Either::Right((value, _)) => {
            io.check_cancellation()?;
            Ok(value)
        }
    }
}

#[cfg(test)]
mod delta_tests;
#[cfg(test)]
mod tests;
