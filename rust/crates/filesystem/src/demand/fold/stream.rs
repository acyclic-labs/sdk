//! A contiguous checkpoint catalog makes predecessor selection an ordinal seek.
use super::*;
use crate::{
    AsyncObjectStore, NativeMaterializationWork, ObjectStoreError,
    kernel::{NativeAccounting, OwnedSourceChunk, RetainedAllocation},
};
use acyclic_stream::{
    AppendOutcome, AppendRecord, AppendRequest, HeadProjection, HeadSet, HeadsRequest,
    IdempotencyKey, NativeStreamOutput, NativeStreamProvider, ReadRequest, Record, Revision,
    RevisionCut, StreamPath, WriteOutcome, WriteRequest,
};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// Cache namespace. The authority's index is not part of its identity: a fold
/// over the same immutable input prefix reuses checkpoints at later cuts.
pub struct FoldCacheIdentity {
    /// Append-only input stream.
    pub input: StreamPath,
    /// Input authority scope and incarnation, never an offset in `input`.
    pub authority: Revision,
    /// Fold algorithm and encoded state format.
    pub view: [u8; 32],
    /// Exact authored input generation.
    pub generation: [u8; 32],
    /// Checkpoint chunk size. It does not bound the stream or folded state.
    pub chunk: NonZeroU64,
}

/// Objects-backed folds over an append-only stream, with no resident history,
/// readiness loop or background task. Every chunk boundary is cataloged in
/// order, so boundary `n * K` is at ordinal `n - 1`. Tail CAS refuses gaps;
/// deterministic retry identities detect conflicting state for the same prefix.
pub struct FoldCache<P: ?Sized, S> {
    streams: Arc<P>,
    objects: Arc<S>,
    identity: FoldCacheIdentity,
    catalog: StreamPath,
}

impl<P: NativeStreamProvider + ?Sized, S: AsyncObjectStore> FoldCache<P, S> {
    /// Allocates only the catalog name. No storage or subscription is opened.
    pub fn new(
        streams: Arc<P>,
        objects: Arc<S>,
        identity: FoldCacheIdentity,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Self, FoldObjectError> {
        let domain = b"acyclic:fold-catalog:v1";
        ctx.charge(WorkCounters {
            bytes_hashed: (domain.len()
                + identity.input.as_str().len()
                + identity.authority.scope.as_bytes().len()
                + 8
                + 16
                + 32
                + 32
                + 8) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(domain);
        hash.update(&(identity.input.as_str().len() as u64).to_le_bytes());
        hash.update(identity.input.as_str().as_bytes());
        hash.update(identity.authority.scope.as_bytes());
        hash.update(&identity.authority.incarnation.to_le_bytes());
        hash.update(&identity.view);
        hash.update(&identity.generation);
        hash.update(&identity.chunk.get().to_le_bytes());
        let digest = hash.finalize();
        let hex = digest.to_hex();
        let native = NativeAccounting::take(ctx)?;
        let path = StreamPath::new_native_parts(
            &["harness/folds/", hex.as_str()],
            &native.work.runtime_meter(),
        )
        .map_err(|error| {
            FoldObjectError::from(crate::FsError::from(native.work.runtime_error(error)))
        })
        .and_then(|result| result.map_err(FoldObjectError::from));
        native.restore(ctx)?;
        Ok(Self {
            streams,
            objects,
            identity,
            catalog: path?,
        })
    }

    /// Reopens the nearest cataloged boundary at or before the exact input cut,
    /// then folds only its tail. A collected object is a cache miss, recomputed
    /// from `seed`; malformed or conflicting cache data is never hidden.
    pub async fn fold_at<F, I>(
        &self,
        fold: F,
        at: Revision,
        seed: I,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Retained<FoldCheckpoint<F::State>>, FoldObjectError>
    where
        F: Fold<Record = Record>,
        F::State: DeserializeOwned + Serialize,
        I: FnOnce(
            &mut KernelAccounting<'_>,
        ) -> Result<Retained<FoldCheckpoint<F::State>>, FoldObjectError>,
    {
        if fold.identity() != self.identity.view
            || at.scope != self.identity.authority.scope
            || at.incarnation != self.identity.authority.incarnation
        {
            return Err(FoldObjectError::Mismatch);
        }
        let cancellation = ctx.cancellation();
        let native = NativeAccounting::take(ctx)?;
        let mut accounting = KernelAccounting::NativeOriginal {
            work: &native.work,
            cancellation,
        };
        let result = self
            .run(fold, at, seed, &native.work, &mut accounting)
            .await;
        native.restore(ctx)?;
        result
    }

    async fn run<F, I>(
        &self,
        fold: F,
        at: Revision,
        seed: I,
        work: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Retained<FoldCheckpoint<F::State>>, FoldObjectError>
    where
        F: Fold<Record = Record>,
        F::State: DeserializeOwned + Serialize,
        I: FnOnce(
            &mut KernelAccounting<'_>,
        ) -> Result<Retained<FoldCheckpoint<F::State>>, FoldObjectError>,
    {
        let input = self
            .head(&self.identity.input, Some(at.clone()), work, ctx)
            .await?;
        let tail = input
            .value()
            .heads
            .first()
            .and_then(Option::as_ref)
            .map_or(0, |head| head.tail);
        drop(input);
        let catalog = self.head(&self.catalog, None, work, ctx).await?;
        let head = catalog.value().heads.first().and_then(Option::as_ref);
        let boundary = tail / self.identity.chunk.get();
        let selected = head.map_or(0, |head| head.tail.min(boundary));
        let mut checkpoint = None;
        if selected > 0 {
            let ordinal = selected - 1;
            let record = if let Some(record) = head
                .and_then(|head| head.last.as_ref())
                .filter(|record| record.sequence == ordinal)
            {
                self.descriptor(record, selected, ctx)?
            } else {
                let floor = catalog.value().snapshot.clone();
                self.descriptor_at(ordinal, selected, floor, work, ctx)
                    .await?
            };
            checkpoint = match record.load(&*self.objects, ctx).await {
                Ok(checkpoint) => Some(checkpoint),
                Err(FoldObjectError::Object(ObjectStoreError::Missing)) => None,
                Err(error) => return Err(error),
            };
        }
        drop(catalog);
        let checkpoint = match checkpoint {
            Some(checkpoint) => checkpoint,
            None => {
                let checkpoint = seed(ctx)?;
                if checkpoint.sequence != 0 {
                    return Err(FoldObjectError::Mismatch);
                }
                checkpoint
            }
        };
        if checkpoint.view != self.identity.view
            || checkpoint.generation != self.identity.generation
            || checkpoint.sequence > tail
            || (selected == 0 && checkpoint.sequence != 0)
        {
            return Err(FoldObjectError::Mismatch);
        }
        let mut cursor = FoldCursor::resume(fold, checkpoint, self.identity.chunk)?;
        let mut from = cursor.checkpoint().sequence;
        while from < tail {
            // The protocol record bound chunks one finite read; it is not a
            // history limit. The selected prefix never includes later appends.
            let limit = (tail - from).min(u64::from(u32::MAX)) as u32;
            let _floor = RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, ctx)?;
            let mut records = output(
                self.streams
                    .native_read(
                        ReadRequest {
                            path: self.identity.input.clone(),
                            from,
                            limit,
                            key: None,
                            at_least: RevisionCut::of(at.clone()),
                        },
                        work.runtime_meter(),
                    )
                    .await,
                work,
            )?;
            let end = from + u64::from(limit);
            while let Some(record) = records.value_mut().next().await {
                let record = output(record, work)?;
                if from >= end || record.value().sequence != from {
                    return Err(FoldObjectError::Mismatch);
                }
                cursor = cursor.advance(from, record.value(), ctx)?;
                from += 1;
                if cursor.checkpoint_due() {
                    self.publish(cursor.checkpoint(), work, ctx).await?;
                }
            }
            if from != end {
                return Err(FoldObjectError::Mismatch);
            }
        }
        Ok(cursor.into_checkpoint())
    }

    async fn head(
        &self,
        path: &StreamPath,
        at: Option<Revision>,
        work: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<NativeStreamOutput<acyclic_stream::Heads>, FoldObjectError> {
        let _floor = RetainedAllocation::claim(
            if at.is_some() {
                std::mem::size_of::<Revision>() as u64
            } else {
                0
            },
            u64::from(at.is_some()),
            ctx,
        )?;
        output(
            self.streams
                .native_heads(
                    HeadsRequest {
                        at_least: at
                            .clone()
                            .map_or_else(RevisionCut::default, RevisionCut::of),
                        at,
                        paths: HeadSet::from_buf([path.clone()]),
                        retained: HeadSet::new(),
                        projection: HeadProjection::History,
                        durable_through: None,
                        children: None,
                        limit: 1,
                    },
                    work.runtime_meter(),
                )
                .await,
            work,
        )
    }

    fn descriptor(
        &self,
        record: &Record,
        boundary: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FoldCheckpointObject, FoldObjectError> {
        let descriptor = ctx.decode_json::<FoldCheckpointObject>(&record.value)?;
        let expected = FoldCheckpoint {
            version: 1,
            view: self.identity.view,
            sequence: boundary
                .checked_mul(self.identity.chunk.get())
                .ok_or(crate::WorkError::Overflow)
                .map_err(crate::FsError::from)?,
            generation: self.identity.generation,
            state: (),
        };
        if descriptor.version != 1 || descriptor.key != expected.accounted_key(ctx)? {
            return Err(FoldObjectError::Mismatch);
        }
        Ok(*descriptor)
    }

    async fn descriptor_at(
        &self,
        ordinal: u64,
        boundary: u64,
        floor: Revision,
        work: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FoldCheckpointObject, FoldObjectError> {
        let _floor = RetainedAllocation::claim(std::mem::size_of::<Revision>() as u64, 1, ctx)?;
        let mut records = output(
            self.streams
                .native_read(
                    ReadRequest {
                        path: self.catalog.clone(),
                        from: ordinal,
                        limit: 1,
                        key: None,
                        at_least: RevisionCut::of(floor),
                    },
                    work.runtime_meter(),
                )
                .await,
            work,
        )?;
        let record = output(
            records
                .value_mut()
                .next()
                .await
                .ok_or(FoldObjectError::Mismatch)?,
            work,
        )?;
        if record.value().sequence != ordinal {
            return Err(FoldObjectError::Mismatch);
        }
        let descriptor = self.descriptor(record.value(), boundary, ctx)?;
        if let Some(record) = records.value_mut().next().await {
            output(record, work)?;
            return Err(FoldObjectError::Mismatch);
        }
        Ok(descriptor)
    }

    async fn publish<T: Serialize>(
        &self,
        checkpoint: &FoldCheckpoint<T>,
        work: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), FoldObjectError> {
        let boundary = checkpoint.sequence / self.identity.chunk.get();
        if boundary == 0
            || !checkpoint
                .sequence
                .is_multiple_of(self.identity.chunk.get())
        {
            return Err(FoldObjectError::Mismatch);
        }
        let descriptor = FoldCheckpointObject::save(&*self.objects, checkpoint, ctx).await?;
        let bytes = OwnedSourceChunk::produce(ctx, |sink| {
            serde_json::to_writer(sink, &descriptor).map_err(FoldObjectError::from)
        })?;
        ctx.charge(WorkCounters {
            bytes_hashed: (self.catalog.as_str().len() + bytes.bytes.len()) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(self.catalog.as_str().as_bytes());
        hash.update(&bytes.bytes);
        let digest = hash.finalize();
        let key = OwnedSourceChunk::produce(ctx, |sink| {
            sink.write_all(digest.as_bytes())
                .map_err(DemandError::from)
                .map_err(FoldObjectError::from)
        })?;
        let mut records = Retained::allocate(1, ctx)?;
        records.push(AppendRecord::from(bytes.bytes), ctx)?;
        let Retained {
            value: records,
            allocation,
        } = records;
        let result = output(
            self.streams
                .native_write(
                    WriteRequest::Append(AppendRequest {
                        path: self.catalog.clone(),
                        records,
                        if_tail: Some(boundary - 1),
                        idempotency_key: IdempotencyKey::new(key.bytes)?,
                    }),
                    work.runtime_meter(),
                )
                .await,
            work,
        );
        drop(allocation);
        match result?.value() {
            WriteOutcome::Append(AppendOutcome::Committed(_)) => Ok(()),
            WriteOutcome::Append(AppendOutcome::TailConflict { actual_tail })
                if *actual_tail >= boundary =>
            {
                let head = self.head(&self.catalog, None, work, ctx).await?;
                let stored = self
                    .descriptor_at(
                        boundary - 1,
                        boundary,
                        head.value().snapshot.clone(),
                        work,
                        ctx,
                    )
                    .await?;
                if stored == descriptor {
                    Ok(())
                } else {
                    Err(FoldObjectError::Mismatch)
                }
            }
            _ => Err(FoldObjectError::Mismatch),
        }
    }
}

fn output<T>(
    result: acyclic_stream::NativeStreamResult<T>,
    work: &NativeMaterializationWork,
) -> Result<NativeStreamOutput<T>, FoldObjectError> {
    result
        .map_err(|error| crate::FsError::from(work.runtime_error(error)))?
        .map_err(FoldObjectError::from)
}
