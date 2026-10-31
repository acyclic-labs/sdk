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
                    generation.as_ref(),
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
            io.remaining()?.bytes_encoded,
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
    observed: SourceCursor,
    revalidate: ResourceRevalidation,
    terminal: acyclic_stream::BoxProviderFuture<'static, StreamError>,
    open: Option<
        acyclic_stream::BoxProviderFuture<'static, std::result::Result<FollowStream, StreamError>>,
    >,
    records: Option<FollowStream>,
    member: Option<SkillPublicationMember>,
    changes: Vec<SkillPublicationChange>,
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
            revalidate: subscription.revalidate,
            terminal: subscription.terminal,
            open: Some(subscription.open),
            records: None,
            member: None,
            changes: Vec::new(),
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
    /// loser leaves the pending cut untouched for exact retry.
    pub(crate) fn confirm_publication(&mut self, cursor: SourceCursor) -> Result<()> {
        let pending = self.pending()?.ok_or(StreamError::InvalidArgument)?;
        if pending.cursor != cursor {
            return Err(StreamError::InvalidArgument.into());
        }
        self.accepted = cursor;
        self.pending = None;
        Ok(())
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
            let facts = PublicationEntry::from_parts(
                &entry.key,
                &entry.operation,
                &entry.metadata,
                self.config,
            )?;
            let Some(path) = content_path(&facts.path, session)? else {
                continue;
            };
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
                        root.skill_metadata_at(reader, &path, &generation, "SKILL.md", limits),
                    )
                    .await??
                    .ok_or(StreamError::InvalidArgument)?;
                    SkillPublicationChange::Skill(metadata)
                } else if facts.before_present {
                    let source = PinnedContextPath {
                        root: root.clone(),
                        generation: generation.clone(),
                        path: path.clone(),
                    };
                    source.validate(&mut None)?;
                    SkillPublicationChange::Removed(source)
                } else {
                    continue;
                };
                // This is the existing bounded selected metadata budget, not a
                // limit on the followed source, agents, or retained history.
                if self.changes.len() >= limits.entries as usize {
                    return Err(Error::Invalid(
                        "publication metadata exceeds admitted discovery work".into(),
                    ));
                }
                let capacity = if self.changes.len() == self.changes.capacity() {
                    self.changes
                        .capacity()
                        .checked_mul(2)
                        .ok_or(StreamError::OutOfRange)?
                        .max(1)
                        .min(limits.entries as usize)
                } else {
                    self.changes.capacity()
                };
                crate::executor::with_history_io(Some(session), |work| {
                    crate::context::compaction::reserve_vector(&mut self.changes, capacity, work)
                })?;
                self.changes.push(change);
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

// Every wait races the original subscription's terminal event and original
// session cancellation. A dropped record producer is a typed Unavailable above.
async fn gated<T>(
    terminal: &mut acyclic_stream::BoxProviderFuture<'static, StreamError>,
    io: &PreparationIo,
    operation: impl std::future::Future<Output = T>,
) -> Result<T> {
    let cancelled = io.cancellation().cancelled();
    futures::pin_mut!(cancelled, operation);
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

fn content_path(
    path: &acyclic_fs::kernel::NamespacePath,
    io: &PreparationIo,
) -> Result<Option<String>> {
    let mut result = String::new();
    for name in path.components() {
        let (text, recorded) = name.unicode_text(io, io.cancellation());
        recorded?;
        let text = match text {
            Ok(text) => text,
            Err(acyclic_fs::FsError::Path(acyclic_fs::kernel::PathLookupError::NamespacePath(
                acyclic_fs::kernel::NamespacePathError::NonUnicodeProjection,
            ))) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if crate::conversation::validate_content_path(&text, &mut None).is_err()
            || text.contains('/')
        {
            return Ok(None);
        }
        for part in [if result.is_empty() { "" } else { "/" }, &text] {
            crate::executor::stock_string_push(
                &mut result,
                part,
                u64::MAX,
                "context path size overflow",
                Some(io),
            )?;
        }
        io.release(text.capacity() as u64)?;
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests;
